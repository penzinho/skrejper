import os
import re
import tempfile
import unicodedata
from urllib.parse import quote, urljoin, urlparse

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


BASE_URL = "https://hr.jooble.org/"
DETAIL_URL_KEYWORDS = ("/desc/", "/jdp/")
RESULT_LINK_SELECTOR = "a[href*='/desc/'], a[href*='/jdp/']"
RESULT_CONTAINER_XPATH = (
    "xpath=ancestor::*[self::article or self::li or self::section or self::div]"
    "[.//a[contains(@href, '/desc/') or contains(@href, '/jdp/')]][1]"
)
SEARCH_KEYWORD_INPUT_SELECTORS = [
    "input[name='ukw']",
    "input[placeholder*='poziciji' i]",
    "input[placeholder*='Traž' i]",
]
SEARCH_BUTTON_SELECTORS = [
    "button:has-text('Naći')",
    "button:has-text('Traži')",
    "button[type='submit']",
]
NEXT_PAGE_SELECTORS = [
    "a[rel='next']",
    "a[aria-label*='next' i]",
    "a[aria-label*='sljede' i]",
    "button[aria-label*='next' i]",
    "button[aria-label*='sljede' i]",
    "a:has-text('Sljede')",
    "button:has-text('Sljede')",
    "a:has-text('Dalje')",
    "button:has-text('Dalje')",
    "button:has-text('Učitaj još')",
    "button:has-text('Prikaži još')",
    "button:has-text('Show more')",
]
EXCLUDED_EXTERNAL_DOMAINS = (
    "jooble.org",
    "www.jooble.org",
    "hr.jooble.org",
    "help.jooble.org",
    "jooble.onelink.me",
    "google.com",
    "google.hr",
    "facebook.com",
    "linkedin.com",
    "doubleclick.net",
)
TIME_PATTERNS = (
    r"prije\s+\d+\s+(?:sat(?:a|i)?|dan(?:a)?|tjed(?:an|na|na)?|mjesec(?:a)?|godin(?:a|e)?)",
    r"prije\s+mjesec\s+dana",
    r"prije\s+nekoliko\s+dana",
    r"pre\s+\d+\s+(?:sat(?:a|i)?|dan(?:a)?|nedelj(?:a|e)|mesec(?:a|i)?|godin(?:a|e)?)",
    r"pre\s+nekoliko\s+dana",
    r"danas",
    r"jučer",
    r"novi",
)
TIME_RE = re.compile(rf"^(?:{'|'.join(TIME_PATTERNS)})$", re.IGNORECASE)
SALARY_RE = re.compile(
    r"(?:(?:\d{1,3}(?:[ .]\d{3})*)|\d+)\s*(?:-|–|do)?\s*"
    r"(?:(?:\d{1,3}(?:[ .]\d{3})*)|\d+)?\s*(?:€|\$|eur|usd)"
    r"(?:\s+(?:mjesečno|godišnje|neto|bruto))?",
    re.IGNORECASE,
)


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.casefold()).strip("_")


def _keyword_path_fragment(value: str) -> str:
    normalized = _clean_text(value).replace("/", " ")
    return quote(re.sub(r"\s+", "-", normalized), safe="-")


def _build_search_url(keyword: str = "", location: str = "Hrvatska") -> str:
    keyword = _clean_text(keyword)
    if keyword:
        return f"{BASE_URL}posao-{_keyword_path_fragment(keyword)}/Hrvatska"
    if keyword:
        return f"{BASE_URL}posao-{_keyword_path_fragment(keyword)}"
    return BASE_URL


def _find_first_visible(page: Page, selectors: list[str]):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() and locator.is_visible():
                return locator
        except Exception:
            continue
    return None


def _wait_for_results(page: Page) -> None:
    last_body_text = ""

    for _ in range(20):
        try:
            if page.locator(RESULT_LINK_SELECTOR).count() > 0:
                page.wait_for_timeout(1000)
                return
        except Exception:
            pass

        try:
            body_text = _clean_text(page.locator("body").inner_text(timeout=5000))
            last_body_text = body_text
        except Exception:
            body_text = ""

        if "Enable JavaScript and cookies to continue" in body_text or "Just a moment" in body_text:
            page.wait_for_timeout(2000)
            continue

        if re.search(r"\b0\s+(?:oglasa?|poslova?)\b", body_text, re.IGNORECASE):
            return

        page.wait_for_timeout(1500)

    raise PlaywrightTimeoutError(f"Jooble results did not load. Last body text sample: {last_body_text[:200]}")


def _apply_search(page: Page, keyword: str, location: str) -> None:
    keyword = _clean_text(keyword)
    if not keyword:
        return

    try:
        page.goto(_build_search_url(keyword=keyword, location=location), wait_until="domcontentloaded", timeout=60000)
        _wait_for_results(page)
    except Exception as exc:
        print(f"[jooble] Search via canonical URL failed: {exc}")
        page.goto(_build_search_url(keyword=keyword, location=location), wait_until="domcontentloaded", timeout=60000)
        _wait_for_results(page)


def _looks_like_time(value: str) -> bool:
    return bool(TIME_RE.match(_clean_text(value)))


def _looks_like_salary(value: str) -> bool:
    return bool(SALARY_RE.search(_clean_text(value)))


def _extract_summary(lines: list[str], title: str) -> str:
    filtered: list[str] = []

    for line in lines:
        if line == title:
            continue
        if _looks_like_time(line):
            continue
        if line.casefold() == "report":
            continue
        if line.casefold().startswith("reportimage"):
            continue
        if line == "Rad od kuće":
            continue
        filtered.append(line)

    return filtered[0] if filtered else ""


def _parse_card_text(card_text: str, title: str) -> dict[str, str]:
    lines = [_clean_text(line) for line in card_text.splitlines() if _clean_text(line)]
    if lines and lines[0] != title and title in lines:
        lines.remove(title)

    published_at = ""
    salary = ""
    company = ""
    location = ""
    summary = _extract_summary(lines, title)
    is_remote = "Rad od kuće" in lines

    for line in lines:
        if not salary and _looks_like_salary(line):
            salary = line

    report_index = next((index for index, line in enumerate(lines) if line.casefold().startswith("report")), -1)
    if report_index != -1:
        trailing = [
            line
            for line in lines[report_index + 1 :]
            if not line.casefold().startswith("image:")
            and not line.casefold().startswith("reportimage")
        ]
    else:
        trailing = lines[-3:]

    if trailing and _looks_like_time(trailing[-1]):
        published_at = trailing[-1]
        trailing = trailing[:-1]

    if len(trailing) >= 2:
        company = trailing[0]
        location = trailing[1]
    elif len(trailing) == 1:
        company = trailing[0]

    if not company:
        for index, line in enumerate(lines):
            if line.casefold().startswith("report") and index + 1 < len(lines):
                company = lines[index + 1]
                break

    return {
        "company": company,
        "location": location,
        "salary": salary,
        "published_at": published_at,
        "summary": summary,
        "is_remote": "true" if is_remote else "false",
    }


def _extract_job_from_anchor(page: Page, anchor_index: int) -> dict | None:
    anchors = page.locator(RESULT_LINK_SELECTOR)

    try:
        anchor = anchors.nth(anchor_index)
        title = _clean_text(anchor.inner_text(timeout=5000))
        href = anchor.get_attribute("href", timeout=5000)
        detail_url = urljoin(page.url, href or "")

        if not title or not detail_url or not any(keyword in detail_url for keyword in DETAIL_URL_KEYWORDS):
            return None

        container = anchor.locator(RESULT_CONTAINER_XPATH)
        card_text = container.inner_text(timeout=5000)
        parsed = _parse_card_text(card_text, title)

        return {
            "title": title,
            "company": parsed["company"],
            "location": parsed["location"],
            "salary": parsed["salary"],
            "published_at": parsed["published_at"],
            "summary": parsed["summary"],
            "is_remote": parsed["is_remote"],
            "detail_url": detail_url,
            "external_url": "",
            "employer_website": "",
            "source": "jooble",
        }
    except Exception as exc:
        print(f"[jooble] Failed to parse card {anchor_index}: {exc}")
        return None


def _collect_listing_jobs(page: Page) -> list[dict]:
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    anchors = page.locator(RESULT_LINK_SELECTOR)
    total = anchors.count()

    for index in range(total):
        job = _extract_job_from_anchor(page, index)
        if not job:
            continue
        if job["detail_url"] in seen_urls:
            continue

        jobs.append(job)
        seen_urls.add(job["detail_url"])

    return jobs


def _candidate_is_valid_external_url(url: str, detail_url: str) -> bool:
    if not url:
        return False

    normalized = url.strip()
    if normalized.startswith(("mailto:", "tel:", "javascript:")):
        return False

    parsed = urlparse(normalized)
    domain = parsed.netloc.casefold()
    if parsed.scheme not in {"http", "https"}:
        return False
    if not domain:
        return False
    if normalized.rstrip("/") == detail_url.rstrip("/"):
        return False
    if any(domain.endswith(excluded) for excluded in EXCLUDED_EXTERNAL_DOMAINS):
        return False
    if re.search(r"\.(?:png|jpe?g|gif|svg|webp|pdf)(?:$|\?)", normalized, re.IGNORECASE):
        return False

    return True


def _extract_external_url(detail_page: Page, detail_url: str) -> str:
    selectors = [
        "a[href]",
        "main a[href]",
        "article a[href]",
    ]

    for selector in selectors:
        locator = detail_page.locator(selector)
        total = locator.count()

        for index in range(total):
            try:
                href = locator.nth(index).get_attribute("href", timeout=3000) or ""
            except Exception:
                continue

            if _candidate_is_valid_external_url(href, detail_url):
                return href

    return ""


def _extract_employer_website(external_url: str) -> str:
    if not external_url:
        return ""

    parsed = urlparse(external_url)
    if not parsed.scheme or not parsed.netloc:
        return ""

    return f"{parsed.scheme}://{parsed.netloc}"


def _extract_detail_text(detail_page: Page) -> str:
    try:
        return detail_page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def _extract_detail_enrichment(detail_page: Page, detail_url: str) -> dict[str, str]:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(1000)
        text = _extract_detail_text(detail_page)
        external_url = _extract_external_url(detail_page, detail_url)
        salary_match = SALARY_RE.search(text)
        published_match = re.search(rf"(?:{'|'.join(TIME_PATTERNS)})", text, re.IGNORECASE)

        return {
            "external_url": external_url,
            "employer_website": _extract_employer_website(external_url),
            "salary": _clean_text(salary_match.group(0)) if salary_match else "",
            "published_at": _clean_text(published_match.group(0)) if published_match else "",
            "summary": _clean_text(text[:400]),
        }
    except Exception as exc:
        print(f"[jooble] Failed to enrich detail page {detail_url}: {exc}")
        return {
            "external_url": "",
            "employer_website": "",
            "salary": "",
            "published_at": "",
            "summary": "",
        }


def _matches_selected_keyword(job: dict, keyword: str) -> bool:
    normalized_keyword = _slugify(keyword)
    if not normalized_keyword:
        return True

    searchable = " ".join(
        [
            job.get("title", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("summary", ""),
        ]
    )
    return normalized_keyword in _slugify(searchable)


def _current_first_result_key(page: Page) -> str:
    try:
        first = page.locator(RESULT_LINK_SELECTOR).first
        href = first.get_attribute("href", timeout=3000) or ""
        title = _clean_text(first.inner_text(timeout=3000))
        return f"{href}|{title}"
    except Exception:
        return ""


def _go_to_next_page(page: Page) -> bool:
    before_count = page.locator(RESULT_LINK_SELECTOR).count()
    before_first = _current_first_result_key(page)

    for selector in NEXT_PAGE_SELECTORS:
        locator = page.locator(selector).first
        try:
            if not locator.count() or not locator.is_visible():
                continue
        except Exception:
            continue

        try:
            locator.click(timeout=10000)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeoutError:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            page.wait_for_timeout(1500)

            after_count = page.locator(RESULT_LINK_SELECTOR).count()
            after_first = _current_first_result_key(page)
            if after_count > before_count or after_first != before_first:
                return True
            return False
        except Exception as exc:
            print(f"[jooble] Failed to paginate using '{selector}': {exc}")

    return False


def scrape_jooble(keyword: str = "", location: str = "Hrvatska", max_pages: int = 3) -> list[dict]:
    headless = os.getenv("HEADLESS", "false") == "true"
    browser_channel = os.getenv("BROWSER_CHANNEL", "chrome")
    jobs: list[dict] = []
    seen_urls: set[str] = set()

    with sync_playwright() as playwright:
        profile_dir = tempfile.mkdtemp(prefix="jooble-profile-")
        context = playwright.chromium.launch_persistent_context(
            profile_dir,
            channel=browser_channel,
            headless=headless,
            locale="hr-HR",
        )
        page = context.pages[0] if context.pages else context.new_page()
        detail_page = context.new_page()

        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
            _apply_search(page, keyword=keyword, location=location)

            for page_number in range(1, max_pages + 1):
                listing_jobs = _collect_listing_jobs(page)

                for job in listing_jobs:
                    if job["detail_url"] in seen_urls:
                        continue

                    enrichment = _extract_detail_enrichment(detail_page, job["detail_url"])
                    job["external_url"] = enrichment["external_url"]
                    job["employer_website"] = enrichment["employer_website"]
                    if not job.get("salary"):
                        job["salary"] = enrichment["salary"]
                    if not job.get("published_at"):
                        job["published_at"] = enrichment["published_at"]
                    if not job.get("summary"):
                        job["summary"] = enrichment["summary"]

                    if not _matches_selected_keyword(job, keyword):
                        continue

                    jobs.append(job)
                    seen_urls.add(job["detail_url"])

                if page_number >= max_pages:
                    break

                if not _go_to_next_page(page):
                    break
        except PlaywrightTimeoutError as exc:
            print(f"[jooble] Timeout while scraping: {exc}")
            return []
        except Exception as exc:
            print(f"[jooble] Scraper failed: {exc}")
            return []
        finally:
            context.close()

    return jobs


if __name__ == "__main__":
    jobs = scrape_jooble(keyword="IT", location="Hrvatska", max_pages=2)
    print(len(jobs))
    print(jobs[:5])
