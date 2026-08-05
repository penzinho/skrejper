"""NSZ — Nacionalna služba za zapošljavanje (nsz.gov.rs), Srbija, supplementary.

Server-rendered, robots allows everything, and the contact phone sits right in
the ad body — so unlike the private portals, NSZ ads often carry a direct
contact. Ad detail pages live at ``/employee/jobs/preview/{id}`` with
sequential numeric ids (~102k and counting), so the crawl enumerates ids
downward from the newest (found on the search page) and remembers how far it
got, paying only for new ids on the next run. An id with no ad renders the
search shell with no title — that is the "gone / never existed" signal.

Two caveats the task calls out:

* **Public sector.** A large share is schools, health centres and municipal
  bodies. They are flagged via ``public_sector`` (not dropped — the export
  filters them by default) so a private "škola stranih jezika" stays reachable.
* **Mixed scripts.** Ads are Cyrillic or Latin, sometimes mixed; every value is
  kept as published, and normalization transliterates for scoring/dedupe. NSZ
  also mirrors Infostud and Lako-do-posla ads, which cross-source dedupe folds
  into one employer rather than any NSZ-specific code.
"""

import re

from app.leadgen import htmlparse as hp
from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text, extract_phone, parse_date, translit
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting

SOURCE = "nsz"
LABEL = "NSZ — nsz.gov.rs (Srbija)"
COUNTRY = "RS"
DEFAULT_ENABLED = True

BASE = "https://www.nsz.gov.rs"
SEARCH_URL = f"{BASE}/employee/jobs/search"
PREVIEW_URL = f"{BASE}/employee/jobs/preview/{{id}}"
PREVIEW_ID_RE = re.compile(r"jobs/preview/(\d+)")

# How many consecutive empty (no-title) ids to tolerate before assuming the
# rest downward is exhausted rather than a run of expired ads.
_EMPTY_STREAK_LIMIT = 40


class _Stopped(Exception):
    pass


def newest_id(search_html: str) -> int:
    """The highest preview id linked on the search page (the newest ad)."""
    ids = [int(m) for m in PREVIEW_ID_RE.findall(search_html)]
    return max(ids) if ids else 0


def _table_values(root) -> dict[str, str]:
    """The ``Label: value`` rows of the ad's data table, normalized-key keyed."""
    from app.leadgen.normalize import norm_text

    values: dict[str, str] = {}
    for row in root.select("div.table-row"):
        cols = row.select("div.table-col")
        if len(cols) >= 2:
            label = norm_text(cols[0].get_text(" ")).rstrip(":")
            value = clean_text(cols[1].get_text(" "))
            if label:
                values[label] = value
    return values


def parse_preview(html: str, url: str) -> tuple[dict, dict] | None:
    """One preview page -> (employer, posting), or None if no ad is present."""
    match = PREVIEW_ID_RE.search(url)
    ad_id = match.group(1) if match else ""

    root = hp.soup(html)
    title_el = root.select_one(".content-line-title")
    title = clean_text(title_el.get_text(" ")) if title_el else ""
    if not title:
        return None  # empty preview = no such ad

    # The employer line ("CHEMCO doo - Badnjevac, Kragujevac") sits in the
    # job-description paragraph right under the title.
    employer_line = ""
    desc = root.select_one("p.job-description")
    if desc is not None:
        employer_line = clean_text(desc.get_text(" "))
    company = employer_line.split(" - ")[0].strip() if employer_line else ""
    if not company:
        return None

    table = _table_values(root)
    city = re.sub(r";.*$", "", table.get("mesto rada", "")).strip()
    if not city and " - " in employer_line:
        city = employer_line.split(" - ", 1)[1].split(",")[-1].strip()

    duration = table.get("vremensko trajanje oglasa za posao", "")
    dates = re.findall(r"\d{1,2}\.\d{1,2}\.\d{4}", translit(duration))
    published = parse_date(dates[0]) if dates else ""
    expires = parse_date(dates[1]) if len(dates) >= 2 else ""

    body = translit(root.get_text(" "))
    phone = ""
    phone_marker = re.search(r"kontakt telefon[:\s]*([0-9/\s\-\+\(\)]{6,})", body, re.IGNORECASE)
    if phone_marker:
        phone = extract_phone(phone_marker.group(1)) or clean_text(phone_marker.group(1))

    # NSZ has no stable employer id; the normalized company name serves as one,
    # stable across that employer's ads (and it is how a firm's ads group).
    from app.leadgen.normalize import norm_company

    employer_id = norm_company(company).replace(" ", "-") or ad_id
    employer = make_employer(
        source=SOURCE,
        source_id=employer_id,
        name=company,
        city=city,
        country=COUNTRY,
        phone=phone,
        detail_url=url,
        public_sector_term=public_sector_term(company),
    )
    posting = make_posting(
        source=SOURCE,
        source_id=ad_id,
        employer_source=SOURCE,
        employer_source_id=employer_id,
        title=title,
        city=city,
        published_at=published,
        expires_at=expires,
        workers_count=table.get("broj radnika", ""),
        contact_phone=phone,
        detail_url=url,
    )
    return employer, posting


def scrape(
    db: LeadDb,
    http: Http,
    *,
    max_pages: int | None = None,
    full: bool = False,
    fetch_details: bool = False,
    on_event=None,
    should_stop=None,
) -> dict:
    """Enumerate preview ids downward from the newest. ``max_pages`` caps how
    many ids to try this run (each id is one fetch); the cursor remembers the
    lowest id reached so a later run continues downward, and the newest id seen
    so a later run also picks up anything added above it."""
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0,
             "details": 0, "public_sector": 0}

    try:
        search_html = http.get_or_none(SEARCH_URL, max_age_s=30 * 60)
        top_id = newest_id(search_html or "")
        if not top_id:
            emit("log", "[nsz] Ne mogu pročitati najnoviji ID sa search stranice.")
            return stats

        known_posts = db.known_posting_ids(SOURCE)
        known_employers = db.known_employer_ids(SOURCE)
        prev_low = int(db.get_cursor(SOURCE, "lowest_id", str(top_id + 1)) or top_id + 1)

        budget = max_pages if max_pages is not None else top_id
        current = top_id
        empty_streak = 0
        lowest_reached = prev_low

        while current >= 1 and budget > 0:
            if should_stop():
                raise _Stopped
            # Skip the already-covered middle band [prev_low, prev_max]; on the
            # first run prev_low is top_id+1 so nothing is skipped.
            if str(current) in known_posts:
                current -= 1
                continue

            url = PREVIEW_URL.format(id=current)
            html = http.get_or_none(url)
            budget -= 1
            stats["details"] += 1
            parsed = parse_preview(html or "", url) if html else None
            if parsed is None:
                empty_streak += 1
                if empty_streak >= _EMPTY_STREAK_LIMIT and current < prev_low:
                    emit("log", f"[nsz] {empty_streak} praznih ID-eva zaredom ispod prošlog ruba; stajem.")
                    break
                current -= 1
                continue
            empty_streak = 0

            employer, posting = parsed
            if employer["public_sector_term"]:
                stats["public_sector"] += 1
            is_new = employer["source_id"] not in known_employers
            if db.upsert_employer(employer):
                stats["employers_new" if is_new else "employers_updated"] += 1
                emit("employer", employer)
            known_employers.add(employer["source_id"])
            if db.upsert_posting(posting):
                stats["postings"] += 1
                emit("posting", posting, employer)
            known_posts.add(str(current))
            lowest_reached = min(lowest_reached, current)
            current -= 1

        db.set_cursor(SOURCE, "lowest_id", str(min(lowest_reached, current + 1)))
        db.set_cursor(SOURCE, "highest_id", str(max(top_id, int(db.get_cursor(SOURCE, "highest_id", "0") or 0))))
    except _Stopped:
        emit("log", "[nsz] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[nsz] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
