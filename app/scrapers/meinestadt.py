import html
import os
import re
import unicodedata
from typing import Any, Callable, TYPE_CHECKING
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse, urlunparse

if TYPE_CHECKING:
    from playwright.sync_api import Page
else:
    Page = Any

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
except ModuleNotFoundError:
    class PlaywrightTimeoutError(Exception):
        pass

    sync_playwright = None


BASE_URL = "https://jobs.meinestadt.de"
DEFAULT_SEARCH_URL = f"{BASE_URL}/deutschland"
DETAIL_ID_PARAM = "id"
BROWSER_ORDER = ("firefox", "chromium", "webkit")
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
CONSENT_BUTTON_TEXTS = (
    "Alle akzeptieren",
    "Akzeptieren",
    "Zustimmen",
    "Einverstanden",
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
MAILTO_RE = re.compile(r"mailto:([^?\"'#]+)", re.IGNORECASE)
HREF_RE = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.IGNORECASE)
DATE_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
PAGINATION_STATE_RE = re.compile(r"Seite\s+(\d+)\s+von\s+(\d+)", re.IGNORECASE)
PAGINATION_SELECT_SELECTOR = ".m-pagination__select"
# Markers of an Akamai block or a browser error page instead of real content.
BLOCKED_PAGE_MARKERS = (
    "access denied",
    "secure connection failed",
    "the connection was reset",
    "this site can’t be reached",
    "this site can't be reached",
    "err_connection",
)
EXCLUDED_EMAIL_DOMAINS = {"meinestadt.de"}
# Asset filenames like "jobs_premium_detail_960x378@2x.jpg" match the email regex.
INVALID_EMAIL_DOMAIN_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".css", ".js")
EXCLUDED_WEBSITE_DOMAINS = {
    "jobs.meinestadt.de",
    "meinestadt.de",
    "www.meinestadt.de",
    "home.meinestadt.de",
    "faq.meinestadt.de",
    "job-shop.meinestadt.de",
    "konto.meinestadt.de",
    "unternehmen.meinestadt.de",
    "facebook.com",
    "www.facebook.com",
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "www.x.com",
    "twitter.com",
    "www.twitter.com",
    "wa.me",
    "api.whatsapp.com",
    "whatsapp.com",
    "www.whatsapp.com",
}
LISTING_NOISE = {
    "sofort_bewerbung",
    "empfehlung",
    "top_job",
}
DETAIL_NOISE = {
    "standort:",
    "weitere informationen",
    "jetzt bewerben",
    "teilen",
    "drucken",
    "gemerkt",
    "merken",
    "zusammenfassung",
    "uber uns",
    "über uns",
    "so bewirbst du dich - unkompliziert & schnell",
}
MEINESTADT_CATEGORIES = {
    "office_admin": {"label": "Bürowesen", "path": "/deutschland/jk/0-15777"},
    "procurement": {"label": "Einkauf & Beschaffung", "path": "/deutschland/jkl/0-6598"},
    "financial_services": {"label": "Finanzdienstleistungen", "path": "/deutschland/jk/0-15231"},
    "hospitality_tourism": {"label": "Gastgewerbe & Tourismus", "path": "/deutschland/jk/0-15236"},
    "healthcare": {"label": "Gesundheitswesen", "path": "/deutschland/jk/0-15244"},
    "sales": {"label": "Handel, Vertrieb & Verkauf", "path": "/deutschland/jk/0-95907"},
    "craftsmanship_production": {"label": "Handwerk & Produktion", "path": "/deutschland/jk/0-15212"},
    "support_roles": {"label": "Hilfstätigkeiten", "path": "/deutschland/jk/0-4719"},
    "it_data_processing": {"label": "IT & Datenverarbeitung", "path": "/deutschland/jk/0-15711"},
    "logistics_transport": {"label": "Logistik, Lager & Verkehr", "path": "/deutschland/jk/0-15237"},
    "management": {"label": "Management", "path": "/deutschland/jk/0-15214"},
    "marketing_pr": {"label": "Marketing, Werbung & PR", "path": "/deutschland/jk/0-15234"},
    "human_resources": {"label": "Personalwesen", "path": "/deutschland/jk/0-96209"},
    "accounting": {"label": "Rechnungswesen", "path": "/deutschland/jk/0-15735"},
    "legal_tax": {"label": "Recht & Steuern", "path": "/deutschland/jk/0-15686"},
    "social_services": {"label": "Sozialwesen", "path": "/deutschland/jk/0-15796"},
    "technical_professions": {"label": "Technische Berufe", "path": "/deutschland/jk/0-96129"},
    "other_fields": {"label": "Weitere Bereiche", "path": "/deutschland/jk/0-15255"},
}


def _log(message: str) -> None:
    # flush so progress is visible in piped/captured logs (systemd, docker, tee)
    print(message, flush=True)


def _looks_blocked(body_text: str) -> bool:
    normalized = _clean_text(body_text).casefold()
    if not normalized:
        return True
    return any(marker in normalized for marker in BLOCKED_PAGE_MARKERS)


def _is_block_error(exc: Exception) -> bool:
    message = str(exc).casefold()
    return any(
        marker in message
        for marker in ("denied", "blocked", "ns_error_net", "err_connection", "connection was lost")
    )


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.casefold()).strip("_")


def _looks_like_date(value: str) -> bool:
    normalized = _clean_text(value)
    return bool(DATE_RE.search(normalized)) or normalized.casefold() in {"neu", "aktualisiert"}


def _normalize_email_candidate(value: str) -> str:
    normalized = html.unescape(value or "").strip()
    normalized = normalized.removeprefix("mailto:").strip(" <>\"'(),;:")
    return normalized.casefold()


def _is_valid_email_candidate(value: str) -> bool:
    if not value or value.count("@") != 1:
        return False

    local_part, domain = value.split("@", 1)
    if not local_part or "." not in domain:
        return False

    normalized_domain = domain.casefold()
    if normalized_domain.endswith(INVALID_EMAIL_DOMAIN_SUFFIXES):
        return False

    return normalized_domain not in EXCLUDED_EMAIL_DOMAINS


def _company_limit_key(company: str, detail_url: str) -> str:
    normalized_company = _slugify(company)
    if normalized_company:
        return normalized_company
    return detail_url


def get_meinestadt_categories() -> list[dict[str, str]]:
    return [
        {"key": key, "label": value["label"], "path": value["path"]}
        for key, value in MEINESTADT_CATEGORIES.items()
    ]


def _resolve_category(category: str | None) -> dict | None:
    if not category:
        return None

    candidate = _clean_text(category)
    if candidate in MEINESTADT_CATEGORIES:
        value = MEINESTADT_CATEGORIES[candidate]
        return {"key": candidate, "label": value["label"], "path": value["path"]}

    candidate_slug = _slugify(candidate)
    for key, value in MEINESTADT_CATEGORIES.items():
        if candidate_slug in {key, _slugify(value["label"]), _slugify(value["path"])}:
            return {"key": key, "label": value["label"], "path": value["path"]}

    available = ", ".join(MEINESTADT_CATEGORIES.keys())
    raise ValueError(f"Unknown meinestadt category '{category}'. Available keys: {available}")


def _build_search_url(category: dict | None = None) -> str:
    if not category:
        return DEFAULT_SEARCH_URL
    return urljoin(BASE_URL, category["path"])


def _build_paginated_url(current_url: str, page_value: int) -> str:
    parsed = urlparse(current_url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params["page"] = str(page_value)
    return urlunparse(parsed._replace(query=urlencode(query_params)))


def _parse_pagination_state(text: str) -> tuple[int, int] | None:
    match = PAGINATION_STATE_RE.search(_clean_text(text))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _looks_like_detail_url(url: str) -> bool:
    if not url:
        return False

    parsed = urlparse(url)
    if not parsed.netloc.endswith("jobs.meinestadt.de"):
        return False

    if DETAIL_ID_PARAM not in parse_qs(parsed.query):
        return False

    if any(fragment in parsed.path for fragment in ("/jk/", "/jkl/", "/skills/", "/magazin/", "/arbeitgeber/")):
        return False

    path_segments = [segment for segment in parsed.path.split("/") if segment]
    return len(path_segments) >= 2


def _wait_for_results(page: Page) -> None:
    last_body_text = ""

    for _ in range(20):
        try:
            body_text = _clean_text(page.locator("body").inner_text(timeout=5000))
            last_body_text = body_text
        except Exception:
            body_text = ""

        if "Access Denied" in body_text:
            raise RuntimeError("meinestadt denied access while loading search results")

        if "Leider gibt es keine Ergebnisse" in body_text or "Treffer" in body_text:
            page.wait_for_timeout(1000)
            return

        if _collect_listing_candidates(page):
            page.wait_for_timeout(1000)
            return

        page.wait_for_timeout(1500)

    raise PlaywrightTimeoutError(
        f"meinestadt results did not load. Last body text sample: {last_body_text[:240]}"
    )


def _dismiss_consent_overlay(page: Page) -> None:
    for frame in page.frames:
        for button_text in CONSENT_BUTTON_TEXTS:
            selectors = [
                f"button:has-text('{button_text}')",
                f"[role='button']:has-text('{button_text}')",
                f"text={button_text}",
            ]

            for selector in selectors:
                locator = frame.locator(selector).first
                try:
                    if locator.count() == 0 or not locator.is_visible():
                        continue
                    locator.click(timeout=3000)
                    page.wait_for_timeout(1000)
                    return
                except Exception:
                    continue


def _navigate_with_tolerance(page: Page, url: str) -> None:
    # Akamai's bot sensor flags the session cookie shortly after each page load
    # and then resets connections for follow-up requests, while cookie-free
    # requests pass. Navigating with a clean jar keeps every request "fresh".
    try:
        page.context.clear_cookies()
    except Exception:
        pass

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as exc:
        message = str(exc)
        if "ERR_HTTP2_PROTOCOL_ERROR" not in message and "NS_ERROR_NET" not in message:
            raise
        page.wait_for_timeout(2000)
    _dismiss_consent_overlay(page)


def _parse_listing_card_text(card_text: str, title: str) -> dict[str, str]:
    lines = [_clean_text(line) for line in (card_text or "").splitlines() if _clean_text(line)]
    filtered_lines: list[str] = []
    title_removed = False

    for line in lines:
        if not title_removed and line == title:
            title_removed = True
            continue
        filtered_lines.append(line)

    company = ""
    location = ""
    published_at = ""

    for line in filtered_lines:
        normalized = _slugify(line)
        if not company:
            if normalized in LISTING_NOISE or _looks_like_date(line):
                continue
            company = line
            continue

        if not location:
            if normalized in LISTING_NOISE or _looks_like_date(line):
                if not published_at and _looks_like_date(line):
                    published_at = line
                continue
            location = line
            continue

        if not published_at and _looks_like_date(line):
            published_at = line
            break

    return {
        "company": company,
        "location": location,
        "published_at": published_at,
    }


def _extract_location_from_detail_text(detail_text: str) -> str:
    lines = [_clean_text(line) for line in detail_text.splitlines() if _clean_text(line)]

    for index, line in enumerate(lines):
        if line.casefold() != "standort:":
            continue

        for candidate in lines[index + 1 : index + 5]:
            normalized = candidate.casefold()
            if normalized in DETAIL_NOISE or _looks_like_date(candidate):
                continue
            return candidate

    return ""


def _extract_company_from_detail_text(detail_text: str, title: str, location: str) -> str:
    lines = [_clean_text(line) for line in detail_text.splitlines() if _clean_text(line)]
    title_index = next((index for index, line in enumerate(lines) if line == title), -1)
    candidates = lines[title_index + 1 : title_index + 20] if title_index != -1 else lines[:20]

    for candidate in candidates:
        normalized = candidate.casefold()
        if candidate in {title, location}:
            continue
        if normalized in DETAIL_NOISE or _looks_like_date(candidate):
            continue
        if normalized.startswith("e-mail:") or normalized.startswith("web:"):
            continue
        if EMAIL_RE.search(candidate):
            continue
        return candidate

    return ""


def _extract_email_from_sources(*sources: str) -> str:
    for source in sources:
        if not source:
            continue

        mailto_matches = MAILTO_RE.findall(source)
        regex_matches = EMAIL_RE.findall(source)

        for match in [*mailto_matches, *regex_matches]:
            candidate = _normalize_email_candidate(match)
            if _is_valid_email_candidate(candidate):
                return candidate

    return ""


def _extract_external_website(detail_html: str) -> str:
    for match in HREF_RE.finditer(detail_html or ""):
        href = html.unescape(match.group(1) or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue

        absolute_url = urljoin(BASE_URL, href)
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        normalized_domain = parsed.netloc.casefold()
        if normalized_domain.endswith("meinestadt.de") or normalized_domain in EXCLUDED_WEBSITE_DOMAINS:
            continue
        return absolute_url

    return ""


def _collect_listing_candidates(page: Page) -> list[dict[str, str]]:
    anchors = page.locator("a[href]")
    try:
        raw_candidates = anchors.evaluate_all(
            """
            nodes => nodes.map(node => {
              const text = (node.innerText || node.textContent || "").replace(/\\s+/g, " ").trim();
              let container = node;
              let cardText = text;

              for (let depth = 0; depth < 6 && container; depth += 1) {
                const tagName = (container.tagName || "").toLowerCase();
                const candidateText = (container.innerText || "").trim();
                if (
                  ["article", "li", "section", "div"].includes(tagName) &&
                  candidateText.replace(/\\s+/g, " ").trim().length > text.length + 12 &&
                  candidateText.length < 1800
                ) {
                  cardText = candidateText;
                  break;
                }
                container = container.parentElement;
              }

              return {
                href: node.href || "",
                text,
                card_text: cardText,
              };
            })
            """
        )
    except Exception:
        return []

    candidates: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for raw_candidate in raw_candidates:
        href = _clean_text(raw_candidate.get("href", ""))
        title = _clean_text(raw_candidate.get("text", ""))
        if not title or href in seen_urls or not _looks_like_detail_url(href):
            continue

        parsed_card = _parse_listing_card_text(raw_candidate.get("card_text", ""), title)
        candidates.append(
            {
                "title": title,
                "company": parsed_card["company"],
                "location": parsed_card["location"],
                "published_at": parsed_card["published_at"],
                "detail_url": href,
                "listing_text": raw_candidate.get("card_text", ""),
            }
        )
        seen_urls.add(href)

    return candidates


def _read_pagination_state(page: Page) -> tuple[int, int] | None:
    try:
        text = page.locator(PAGINATION_SELECT_SELECTOR).first.inner_text(timeout=3000)
    except Exception:
        return None
    return _parse_pagination_state(text)


def _goto_listing_page(page: Page, search_url: str, page_number: int, attempts: int = 3) -> bool:
    target_url = search_url if page_number <= 1 else _build_paginated_url(search_url, page_number)

    for attempt in range(1, attempts + 1):
        try:
            _navigate_with_tolerance(page, target_url)
            _wait_for_results(page)

            state = _read_pagination_state(page)
            if state is not None and state[0] != page_number:
                raise RuntimeError(
                    f"meinestadt served page {state[0]} instead of requested page {page_number}"
                )
            return True
        except Exception as exc:
            message = str(exc)
            _log(
                f"[meinestadt] attempt {attempt}/{attempts} to open listing page "
                f"{page_number} failed: {message[:200]}"
            )
            if attempt >= attempts:
                return False
            cooldown_ms = 20000 if "denied" in message.casefold() else 3000 * attempt
            page.wait_for_timeout(cooldown_ms)

    return False


def _enrich_listing_from_detail(page: Page, listing: dict[str, str], category: dict | None) -> dict[str, str]:
    detail_url = listing["detail_url"]
    _navigate_with_tolerance(page, detail_url)
    page.wait_for_timeout(1500)

    detail_text = _clean_text(page.locator("body").inner_text(timeout=10000))
    if _looks_blocked(detail_text):
        raise RuntimeError(f"meinestadt blocked detail page {detail_url}: {detail_text[:120]}")
    detail_html = page.content()

    title = listing["title"]
    company = listing["company"] or _extract_company_from_detail_text(detail_text, title, listing["location"])
    location = listing["location"] or _extract_location_from_detail_text(detail_text)
    email = _extract_email_from_sources(detail_html, detail_text, listing.get("listing_text", ""))

    return {
        "title": title,
        "company": company,
        "location": location,
        "published_at": listing["published_at"],
        "detail_url": detail_url,
        "category": category["label"] if category else "",
        "employer_email": email,
        "employer_website": _extract_external_website(detail_html),
        "source": "meinestadt",
    }


def _enrich_with_block_recovery(
    detail_page: Page,
    listing: dict[str, str],
    category: dict | None,
) -> dict[str, str] | None:
    """Enrich a listing; on an Akamai block cool down and retry, then escalate.

    Escalating (re-raising) lets the caller rotate to a fresh browser engine
    instead of silently collecting rows scraped off error pages.
    """
    last_block_error: Exception | None = None

    for attempt, cooldown_ms in enumerate((0, 10000, 30000), start=1):
        if cooldown_ms:
            _log(
                f"[meinestadt] Detail page blocked, cooling down {cooldown_ms // 1000}s "
                f"(attempt {attempt}): {listing['detail_url']}"
            )
            detail_page.wait_for_timeout(cooldown_ms)

        try:
            return _enrich_listing_from_detail(detail_page, listing, category)
        except Exception as exc:
            if not _is_block_error(exc):
                _log(f"[meinestadt] Failed to enrich detail page {listing['detail_url']}: {exc}")
                return None
            last_block_error = exc

    raise RuntimeError(
        f"meinestadt keeps blocking detail page {listing['detail_url']}"
    ) from last_block_error


def _run_scrape_session(
    page: Page,
    detail_page: Page,
    search_url: str,
    resolved_category: dict | None,
    max_pages: int,
    company_limit: int | None,
    on_job: Callable[[dict], None] | None,
    state: dict,
    debug_progress: bool,
    detail_delay_ms: int,
) -> None:
    """Scrape listing pages starting from state['current_page'], mutating shared state.

    Raises on hard failures (e.g. persistent blocks) so the caller can retry with
    another browser engine while keeping the progress collected so far.
    """
    if not _goto_listing_page(page, search_url, state["current_page"]):
        raise RuntimeError(f"could not open listing page {state['current_page']}")

    while state["current_page"] <= max_pages:
        pagination_state = _read_pagination_state(page)
        total_pages = pagination_state[1] if pagination_state else None
        listing_candidates = _collect_listing_candidates(page)

        if debug_progress:
            _log(
                f"[meinestadt] page {state['current_page']}"
                f"{f'/{total_pages}' if total_pages else ''} url={page.url} "
                f"candidates={len(listing_candidates)} jobs={len(state['jobs'])}"
            )

        for listing in listing_candidates:
            if listing["detail_url"] in state["seen_detail_urls"]:
                continue

            state["seen_detail_urls"].add(listing["detail_url"])

            try:
                job = _enrich_with_block_recovery(detail_page, listing, resolved_category)
            except Exception:
                # Leave the listing unprocessed so a retry with another engine picks it up.
                state["seen_detail_urls"].discard(listing["detail_url"])
                raise

            if job is None:
                continue

            if not job.get("company") or not job.get("location"):
                continue

            if company_limit is not None:
                company_key = _company_limit_key(job.get("company", ""), job["detail_url"])
                if company_key in state["seen_company_keys"]:
                    continue
                state["seen_company_keys"].add(company_key)

            state["jobs"].append(job)
            if on_job is not None:
                try:
                    on_job(job)
                except Exception as exc:
                    _log(f"[meinestadt] on_job callback failed: {exc}")

            if company_limit is not None and len(state["seen_company_keys"]) >= company_limit:
                return

            detail_page.wait_for_timeout(detail_delay_ms)

        next_page = state["current_page"] + 1
        if next_page > max_pages:
            return
        if total_pages is not None and next_page > total_pages:
            if debug_progress:
                _log(f"[meinestadt] reached last page {state['current_page']} of {total_pages}")
            return

        if not _goto_listing_page(page, search_url, next_page):
            raise RuntimeError(f"could not open listing page {next_page}")
        state["current_page"] = next_page


def scrape_meinestadt(
    category: str | None = None,
    max_pages: int = 1,
    company_limit: int | None = None,
    on_job: Callable[[dict], None] | None = None,
) -> list[dict]:
    if sync_playwright is None:
        raise RuntimeError("Playwright is not installed")

    headless = os.getenv("HEADLESS", "true") == "true"
    debug_progress = os.getenv("MEINESTADT_DEBUG_PROGRESS", "false") == "true"
    detail_delay_ms = int(os.getenv("MEINESTADT_DETAIL_DELAY_MS", "1500"))
    resolved_category = _resolve_category(category)
    search_url = _build_search_url(resolved_category)

    # Progress survives browser-engine rotation: a failure on page 3 resumes at page 3.
    state: dict = {
        "jobs": [],
        "seen_detail_urls": set(),
        "seen_company_keys": set(),
        "current_page": 1,
    }

    with sync_playwright() as playwright:
        last_error: Exception | None = None

        for browser_name in BROWSER_ORDER:
            browser = None
            context = None

            try:
                browser_type = getattr(playwright, browser_name)
                browser = browser_type.launch(headless=headless)
                context = browser.new_context(
                    ignore_https_errors=True,
                    user_agent=USER_AGENT,
                    locale="de-DE",
                    viewport={"width": 1366, "height": 900},
                )
                page = context.new_page()
                detail_page = context.new_page()

                _run_scrape_session(
                    page,
                    detail_page,
                    search_url,
                    resolved_category,
                    max_pages,
                    company_limit,
                    on_job,
                    state,
                    debug_progress,
                    detail_delay_ms,
                )
                return state["jobs"]
            except PlaywrightTimeoutError as exc:
                _log(f"[meinestadt] Timeout while scraping with {browser_name}: {exc}")
                last_error = exc
            except Exception as exc:
                _log(f"[meinestadt] Scraper failed with {browser_name}: {exc}")
                last_error = exc
            finally:
                for closable in (context, browser):
                    if closable is None:
                        continue
                    try:
                        closable.close()
                    except Exception:
                        pass

        if last_error is not None:
            _log(
                f"[meinestadt] All browser engines failed on page {state['current_page']}. "
                f"Returning {len(state['jobs'])} jobs collected so far. Last error: {last_error}"
            )
        return state["jobs"]
