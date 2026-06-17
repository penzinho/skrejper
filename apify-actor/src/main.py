"""Apify Actor entrypoint for the OpenClaw scrapers (HZZ + Meinestadt).

The scraping logic lives in `hzz.py` and `meinestadt.py` (extracted verbatim
from `app/scrapers/`). This module wires it up to the Apify runtime:

* reads parameters via `Actor.get_input()` (source, category, max_pages, ...),
* runs the *synchronous* Playwright scrapers in a worker thread so they do not
  block the asyncio event loop (sync Playwright cannot run inside a running
  loop),
* applies the same dedup + excluded-company filtering used by the FastAPI
  service, and
* persists results with `Actor.push_data()` instead of building an in-memory
  CSV.
"""

import asyncio
import csv
import datetime
import io
import os
import unicodedata
from collections import defaultdict

from apify import Actor

from .arbeitsagentur import scrape_arbeitsagentur
from .hzz import hzz_group_to_category, scrape_hzz
from .meinestadt import scrape_meinestadt

# Kept in sync with agent/main.py.
EXCLUDED_COMPANY_TERMS = ("djecji vrtic", "vrtic", "skola", "opcina")

DEFAULT_COUNTRY = {"hzz": "Hrvatska", "meinestadt": "Germany", "arbeitsagentur": "Germany"}


def _normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(c for c in normalized if not unicodedata.combining(c))
    return " ".join(normalized.casefold().split())


def _is_excluded_company(company: str) -> bool:
    key = _normalize_key(company)
    return any(term in key for term in EXCLUDED_COMPANY_TERMS)


def _dedupe_by_company(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for row in rows:
        key = _normalize_key(row.get("company") or "") or (row.get("email") or "").casefold()
        if key and key not in seen:
            seen.add(key)
            result.append(row)
    return result


def _build_hzz_rows(jobs: list[dict], country: str, category: str = "") -> list[dict]:
    """Mirror the /scrape/hzz row shaping: require email, drop excluded
    companies, dedupe by normalized company name."""
    rows: list[dict] = []
    seen_companies: set[str] = set()

    for job in jobs:
        email = (job.get("email") or "").strip()
        if not email:
            continue
        company = (job.get("company") or "").strip()
        if company and _is_excluded_company(company):
            continue
        # Dedupe by normalized company, but fall back to the email when the
        # company name is missing so distinct emails are not collapsed under "".
        key = _normalize_key(company) or email.casefold()
        if key in seen_companies:
            continue
        seen_companies.add(key)
        rows.append(
            {
                "email": email,
                "first_name": "",
                "last_name": "",
                "company": company,
                "city": (job.get("location") or "").strip(),
                "country": country,
                "source": "hzz",
                "category": category,
                "title": (job.get("title") or "").strip(),
                "group": (job.get("group") or "").strip(),
                "detail_url": (job.get("detail_url") or "").strip(),
            }
        )

    return rows


def _build_meinestadt_rows(jobs: list[dict], country: str) -> list[dict]:
    """Mirror the /scrape/meinestadt row shaping (dedup applied separately)."""
    rows: list[dict] = []

    for job in jobs:
        email = (job.get("employer_email") or job.get("email") or "").strip()
        if not email:
            continue
        rows.append(
            {
                "email": email,
                "first_name": "",
                "last_name": "",
                "company": (job.get("company") or "").strip(),
                "city": (job.get("location") or "").strip(),
                "country": country,
                "title": (job.get("title") or "").strip(),
                "source": "meinestadt",
                "category": (job.get("category") or "").strip(),
                "published_at": (job.get("published_at") or "").strip(),
                "detail_url": (job.get("detail_url") or "").strip(),
                "employer_website": (job.get("employer_website") or "").strip(),
            }
        )

    return rows


def _build_arbeitsagentur_rows(jobs: list[dict], country: str, category: str = "") -> list[dict]:
    """Mirror the e-mail-first shaping: require an e-mail, dedupe by normalized
    company name (falling back to the e-mail when the company is missing)."""
    rows: list[dict] = []
    seen_companies: set[str] = set()

    for job in jobs:
        email = (job.get("email") or "").strip()
        if not email:
            continue
        company = (job.get("company") or "").strip()
        if company and _is_excluded_company(company):
            continue
        key = _normalize_key(company) or email.casefold()
        if key in seen_companies:
            continue
        seen_companies.add(key)
        rows.append(
            {
                "email": email,
                "first_name": "",
                "last_name": "",
                "company": company,
                "city": (job.get("location") or "").strip(),
                "country": country,
                "source": "arbeitsagentur",
                "category": category or (job.get("category") or "").strip(),
                "title": (job.get("title") or "").strip(),
                "published_at": (job.get("published_at") or "").strip(),
                "detail_url": (job.get("detail_url") or "").strip(),
                "employer_website": (job.get("employer_website") or "").strip(),
            }
        )

    return rows


def _coerce_int(value, default: int | None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_list(value) -> list[str]:
    """Normalize an input field into a list of non-empty strings.

    Accepts a list (multi-select), a single string (legacy field), or None.
    """
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    return [str(v).strip() for v in value if str(v).strip()]


def _slug(value: str) -> str:
    """ASCII slug safe for use in key-value-store record keys / file names."""
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    slug = "".join(c if c.isalnum() else "-" for c in ascii_only.casefold())
    slug = "-".join(p for p in slug.split("-") if p)
    return slug[:80] or "uncategorized"


def _category_label(rows: list[dict]) -> str:
    """Slug of the distinct categories present in `rows`, joined with '-'.

    Used to build a descriptive CSV file name (e.g. `hzz-graditelji-kuca-...`).
    Falls back to "uncategorized" when no category info is available.
    """
    seen: list[str] = []
    for row in rows:
        category = (row.get("category") or "").strip()
        if category and category not in seen:
            seen.append(category)
    if not seen:
        return "uncategorized"
    return "-".join(_slug(c) for c in seen)[:80]


def _rows_to_csv(rows: list[dict]) -> str:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


async def _scrape_hzz_rows(actor_input: dict, country: str) -> list[dict]:
    """Run the HZZ scraper for every selected category and/or subgroup."""
    max_pages = _coerce_int(actor_input.get("max_pages"), 1) or 1
    company_limit = _coerce_int(actor_input.get("company_limit"), None)
    results_per_page = _coerce_int(actor_input.get("results_per_page"), 75)

    categories = _as_list(actor_input.get("hzz_categories"))
    groups = _as_list(actor_input.get("hzz_groups"))

    # Legacy single fields: `category` + `group` together means "just that
    # subgroup"; `category` alone means "the whole category".
    legacy_category = (actor_input.get("category") or "").strip()
    legacy_group = (actor_input.get("group") or "").strip()
    legacy_group_category = ""
    if legacy_group:
        groups.append(legacy_group)
        legacy_group_category = legacy_category  # category hint for resolution
    elif legacy_category:
        categories.append(legacy_category)

    categories = list(dict.fromkeys(categories))
    groups = list(dict.fromkeys(groups))

    group_to_category = hzz_group_to_category()
    all_rows: list[dict] = []

    # Nothing selected -> scrape the whole board (legacy default behaviour).
    if not categories and not groups:
        jobs = await asyncio.to_thread(
            scrape_hzz,
            max_pages=max_pages,
            category=None,
            company_limit=company_limit,
            results_per_page=results_per_page,
        )
        Actor.log.info("HZZ (all): %s raw jobs", len(jobs))
        return _build_hzz_rows(jobs, country)

    # Whole categories (all of their subgroups).
    for category in categories:
        jobs = await asyncio.to_thread(
            scrape_hzz,
            max_pages=max_pages,
            category=category,
            company_limit=company_limit,
            results_per_page=results_per_page,
            use_subgroups=True,
        )
        Actor.log.info("HZZ category '%s': %s raw jobs", category, len(jobs))
        all_rows += _build_hzz_rows(jobs, country, category)

    # Specific subgroups only.
    for group in groups:
        category = group_to_category.get(group) or legacy_group_category or None
        if category is None:
            Actor.log.warning("Skipping unknown HZZ subgroup '%s'", group)
            continue
        jobs = await asyncio.to_thread(
            scrape_hzz,
            max_pages=max_pages,
            category=category,
            group=group,
            company_limit=company_limit,
            results_per_page=results_per_page,
            use_subgroups=False,
        )
        Actor.log.info("HZZ subgroup '%s' (%s): %s raw jobs", group, category, len(jobs))
        all_rows += _build_hzz_rows(jobs, country, category)

    return all_rows


async def _scrape_meinestadt_rows(actor_input: dict, country: str) -> list[dict]:
    """Run the Meinestadt scraper for every selected category."""
    max_pages = _coerce_int(actor_input.get("max_pages"), 1) or 1
    company_limit = _coerce_int(actor_input.get("company_limit"), None)

    categories = _as_list(actor_input.get("meinestadt_categories"))
    categories += _as_list(actor_input.get("category"))  # legacy single field
    categories = list(dict.fromkeys(categories))

    all_rows: list[dict] = []

    if not categories:
        jobs = await asyncio.to_thread(
            scrape_meinestadt,
            category=None,
            max_pages=max_pages,
            company_limit=company_limit,
        )
        Actor.log.info("Meinestadt (all): %s raw jobs", len(jobs))
        return _build_meinestadt_rows(jobs, country)

    for category in categories:
        jobs = await asyncio.to_thread(
            scrape_meinestadt,
            category=category,
            max_pages=max_pages,
            company_limit=company_limit,
        )
        Actor.log.info("Meinestadt category '%s': %s raw jobs", category, len(jobs))
        rows = _build_meinestadt_rows(jobs, country)
        for row in rows:
            row["category"] = category  # our key, so per-category split is stable
        all_rows += rows

    return all_rows


async def _scrape_arbeitsagentur_rows(actor_input: dict, country: str) -> list[dict]:
    """Run the arbeitsagentur scraper for every selected Berufsfeld category.

    `max_pages` counts API result pages of `results_per_page` (max 100) each.
    An optional `arbeitsagentur_keyword` (free-text `was`) and
    `arbeitsagentur_location` (+ `arbeitsagentur_radius` km) narrow every query.
    """
    max_pages = _coerce_int(actor_input.get("max_pages"), 1) or 1
    company_limit = _coerce_int(actor_input.get("company_limit"), None)
    listing_limit = _coerce_int(actor_input.get("arbeitsagentur_listing_limit"), None)
    results_per_page = _coerce_int(actor_input.get("results_per_page"), 100) or 100

    keyword = (actor_input.get("arbeitsagentur_keyword") or "").strip() or None
    location = (actor_input.get("arbeitsagentur_location") or "").strip() or None
    radius = _coerce_int(actor_input.get("arbeitsagentur_radius"), None)

    categories = _as_list(actor_input.get("arbeitsagentur_categories"))
    categories += _as_list(actor_input.get("category"))  # legacy single field
    categories = list(dict.fromkeys(categories))

    all_rows: list[dict] = []

    # No category selected -> a single keyword/location (or board-wide) sweep.
    if not categories:
        jobs = await asyncio.to_thread(
            scrape_arbeitsagentur,
            category=None,
            max_pages=max_pages,
            company_limit=company_limit,
            results_per_page=results_per_page,
            keyword=keyword,
            location=location,
            radius=radius,
            listing_limit=listing_limit,
        )
        Actor.log.info("Arbeitsagentur (all): %s raw jobs", len(jobs))
        return _build_arbeitsagentur_rows(jobs, country)

    for category in categories:
        jobs = await asyncio.to_thread(
            scrape_arbeitsagentur,
            category=category,
            max_pages=max_pages,
            company_limit=company_limit,
            results_per_page=results_per_page,
            keyword=keyword,
            location=location,
            radius=radius,
            listing_limit=listing_limit,
        )
        Actor.log.info("Arbeitsagentur category '%s': %s raw jobs", category, len(jobs))
        all_rows += _build_arbeitsagentur_rows(jobs, country, category)

    return all_rows


async def _emit(rows: list[dict], output_mode: str, source: str) -> list[str]:
    """Push results, either combined into the default dataset or split into one
    CSV file per category / subgroup in the run's key-value store.

    Returns the list of CSV record keys written to the key-value store.
    """
    if not rows:
        Actor.log.warning("No rows to push (no jobs matched the filters).")
        return []

    if output_mode not in ("combined", "per_category", "per_subgroup"):
        output_mode = "combined"

    scrape_date = datetime.date.today().strftime("%d-%m-%Y")

    if output_mode == "combined":
        deduped = _dedupe_by_company(rows)
        await Actor.push_data(deduped)
        # Also write a ready-to-download CSV file to the key-value store, so the
        # result is usable in mailing tools without exporting the dataset. The
        # name carries the scraped category/categories and the scrape date, e.g.
        # `hzz-graditelji-kuca-17-06-2026.csv`.
        key = f"{source}-{_category_label(deduped)}-{scrape_date}.csv"
        await Actor.set_value(
            key, _rows_to_csv(deduped), content_type="text/csv; charset=utf-8"
        )
        Actor.log.info(
            "Output combined: %s rows (dataset + %s in the key-value store).",
            len(deduped),
            key,
        )
        return [key]

    if output_mode == "per_subgroup":
        def bucket_of(row: dict) -> tuple[str, ...]:
            return (row.get("category") or "uncategorized", row.get("group") or "all")
    else:  # per_category
        def bucket_of(row: dict) -> tuple[str, ...]:
            return (row.get("category") or "uncategorized",)

    buckets: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    for row in rows:
        buckets[bucket_of(row)].append(row)

    written: list[str] = []
    total = 0
    for parts, bucket_rows in buckets.items():
        deduped = _dedupe_by_company(bucket_rows)
        if not deduped:
            continue
        key = "-".join([source, *(_slug(p) for p in parts), scrape_date]) + ".csv"
        await Actor.set_value(
            key, _rows_to_csv(deduped), content_type="text/csv; charset=utf-8"
        )
        # Mirror everything into the default dataset too, for a tabular overview.
        await Actor.push_data(deduped)
        written.append(key)
        total += len(deduped)
        Actor.log.info("Output %s: %s rows -> %s", output_mode, len(deduped), key)

    Actor.log.info(
        "Output %s: %s rows across %s CSV file(s) in the key-value store.",
        output_mode,
        total,
        len(written),
    )
    return written


async def _email_csv_links(email_to: str, csv_keys: list[str], source: str) -> None:
    """Email download links to the generated CSV files via the apify/send-mail
    actor. Key-value-store records are publicly readable by URL, so no token is
    exposed. (send-mail does not support real attachments, hence links.)"""
    store_id = os.environ.get("APIFY_DEFAULT_KEY_VALUE_STORE_ID")
    if not store_id:
        store_id = (await Actor.open_key_value_store()).id

    base = f"https://api.apify.com/v2/key-value-stores/{store_id}/records"
    items = "".join(
        f'<li><a href="{base}/{key}?attachment=true">{key}</a></li>'
        for key in csv_keys
    )
    html = (
        f"<p>OpenClaw scrape (<b>{source}</b>) finished — "
        f"{len(csv_keys)} CSV file(s):</p><ul>{items}</ul>"
        "<p>Links download the CSV directly; import them into your mailing tool.</p>"
    )
    try:
        await Actor.call(
            "apify/send-mail",
            run_input={
                "to": email_to,
                "subject": f"OpenClaw scrape: {source} ({len(csv_keys)} CSV)",
                "html": html,
            },
        )
        Actor.log.info("Emailed %s CSV link(s) to %s", len(csv_keys), email_to)
    except Exception as exc:  # email is best-effort; don't fail the whole run
        Actor.log.warning("Failed to send email to %s: %s", email_to, exc)


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        source = (actor_input.get("source") or "hzz").strip().lower()
        output_mode = (actor_input.get("output_mode") or "combined").strip().lower()
        country = actor_input.get("country") or DEFAULT_COUNTRY.get(source, "")

        # Cloud runs are always headless; the scrapers honour the HEADLESS env var.
        os.environ.setdefault("HEADLESS", "true")

        Actor.log.info("Starting scrape: source=%s output_mode=%s", source, output_mode)

        if source == "hzz":
            rows = await _scrape_hzz_rows(actor_input, country or DEFAULT_COUNTRY["hzz"])
        elif source == "meinestadt":
            rows = await _scrape_meinestadt_rows(
                actor_input, country or DEFAULT_COUNTRY["meinestadt"]
            )
        elif source == "arbeitsagentur":
            rows = await _scrape_arbeitsagentur_rows(
                actor_input, country or DEFAULT_COUNTRY["arbeitsagentur"]
            )
        else:
            raise ValueError(
                f"Unknown source '{source}'. Supported sources: hzz, meinestadt, arbeitsagentur."
            )

        Actor.log.info("Collected %s rows before output dedup.", len(rows))
        csv_keys = await _emit(rows, output_mode, source)

        email_to = (actor_input.get("email_to") or "").strip()
        if email_to and csv_keys:
            await _email_csv_links(email_to, csv_keys, source)
