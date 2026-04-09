import os
import re
import unicodedata
from urllib.parse import quote_plus, urljoin, urlparse

from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


SEARCH_URL = "https://mojposao.hr/pretraga-poslova"
JOB_CARD_SELECTOR = "div.mp-card.job-card"
JOB_TITLE_SELECTOR = "h3[data-test='job-card-content-title']"
LOAD_MORE_TEXT = "Učitaj još poslova"
NO_MORE_RESULTS_TEXT = "Nemamo više poslova za ovu pretragu"
EXCLUDED_WEBSITE_DOMAINS = (
    "mojposao.hr",
    "www.mojposao.hr",
    "static.mojposao.hr",
    "storage.moj-posao.net",
    "mojaplaca.hr",
    "www.mojaplaca.hr",
    "poslodavac.hr",
    "www.poslodavac.hr",
    "almacareer.com",
    "www.almacareer.com",
    "jobs.cz",
    "profesia.sk",
    "profesia.cz",
    "prace.cz",
    "pracazarohom.sk",
    "pracezarohem.cz",
    "atmoskop.cz",
    "nelisa.com",
    "arnold-robot.com",
    "teamio.com",
    "seduo.cz",
    "seduo.sk",
    "platy.cz",
    "platy.sk",
    "paylab.com",
    "cvonline.lt",
    "cv.lv",
    "cv.ee",
    "dirbam.lt",
    "visidarbi.lv",
    "otsintood.ee",
    "personaloatrankos.lt",
    "recruitment.lv",
    "poslodavacpartner.org",
    "www.poslodavacpartner.org",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
)
MOJPOSAO_CATEGORIES = {
    "administrative_jobs": {"id": 1, "label": "Administrativna zanimanja"},
    "architecture": {"id": 2, "label": "Arhitektura"},
    "banking": {"id": 3, "label": "Bankarstvo"},
    "beauty_sports": {"id": 4, "label": "Briga o ljepoti, sport"},
    "design_arts": {"id": 5, "label": "Dizajn i umjetnost"},
    "electrical_engineering": {"id": 6, "label": "Elektrotehnika"},
    "pharma_biotech": {"id": 7, "label": "Farmaceutika i biotehnologija"},
    "economy_finance_accounting": {"id": 8, "label": "Ekonomija, Financije i računovodstvo"},
    "construction_geodesy_geology": {"id": 9, "label": "Graditeljstvo, geodezija, geologija"},
    "installations_maintenance_repairs": {"id": 10, "label": "Instalacije, održavanje i popravci"},
    "it_telecommunications": {"id": 11, "label": "IT, telekomunikacije"},
    "human_resources": {"id": 12, "label": "Ljudski resursi"},
    "marketing_pr_media": {"id": 13, "label": "Marketing, PR i mediji"},
    "education_science": {"id": 14, "label": "Obrazovanje i znanost"},
    "other": {"id": 15, "label": "Ostalo"},
    "agriculture_forestry_fishery": {"id": 16, "label": "Poljoprivreda, šumarstvo, ribarstvo"},
    "law": {"id": 17, "label": "Pravo"},
    "sales": {"id": 18, "label": "Prodaja (Trgovina)"},
    "manufacturing_crafts": {"id": 19, "label": "Proizvodnja i zanatske usluge"},
    "transport_maritime": {"id": 20, "label": "Promet, transport, pomorstvo"},
    "security_safety": {"id": 21, "label": "Sigurnost i zaštita"},
    "warehousing_logistics": {"id": 22, "label": "Skladištenje i logistika"},
    "care_services": {"id": 23, "label": "Skrb (o djeci, starijima...)"},
    "mechanical_engineering_shipbuilding": {"id": 24, "label": "Strojarstvo i brodogradnja"},
    "tourism_hospitality": {"id": 25, "label": "Turizam i ugostiteljstvo"},
    "healthcare_social_work": {"id": 26, "label": "Zdravstvo, socijalni rad"},
    "management": {"id": 27, "label": "Management"},
    "government_nonprofit": {"id": 28, "label": "Državna služba i neprofitne organizacije"},
}


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.casefold()).strip("_")


def _normalize_keyword(keyword: str) -> str:
    normalized = unicodedata.normalize("NFKD", keyword or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return _clean_text(ascii_only)


def get_mojposao_categories() -> list[dict[str, str]]:
    return [
        {"key": key, "label": value["label"], "id": str(value["id"])}
        for key, value in MOJPOSAO_CATEGORIES.items()
    ]


def _resolve_category(category: str | None) -> dict | None:
    if not category:
        return None

    candidate = _clean_text(category)
    if candidate in MOJPOSAO_CATEGORIES:
        value = MOJPOSAO_CATEGORIES[candidate]
        return {"key": candidate, "label": value["label"], "id": value["id"]}

    slug_candidate = _slugify(candidate)
    for key, value in MOJPOSAO_CATEGORIES.items():
        if slug_candidate in {key, _slugify(value["label"]), str(value["id"])}:
            return {"key": key, "label": value["label"], "id": value["id"]}

    available = ", ".join(MOJPOSAO_CATEGORIES.keys())
    raise ValueError(f"Unknown MojPosao category '{category}'. Available keys: {available}")


def _build_search_url(keyword: str = "", category: dict | None = None) -> str:
    params: list[str] = []
    normalized_keyword = _normalize_keyword(keyword)
    if normalized_keyword:
        params.append(f"query={quote_plus(normalized_keyword)}")
    if category:
        params.append(f"positions={category['id']}")
    if not params:
        return SEARCH_URL
    return f"{SEARCH_URL}?{'&'.join(params)}"


def _wait_for_results(page: Page) -> None:
    page.wait_for_selector(JOB_TITLE_SELECTOR, timeout=30000)
    page.wait_for_timeout(1500)


def _get_results_heading(page: Page) -> str:
    selectors = ["main h1", "h1", "main h2", "h2"]

    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count():
            try:
                text = _clean_text(locator.inner_text(timeout=5000))
                if text:
                    return text
            except Exception:
                continue

    return ""


def _get_results_limit(page: Page) -> int | None:
    heading = _get_results_heading(page)
    match = re.search(r"\((\d+)\)", heading)
    if not match:
        return None
    return int(match.group(1))


def _find_keyword_input(page: Page) -> Locator:
    selectors = [
        "input[name='positions']",
        "input[aria-label*='poslovi' i]",
        "input.selectable__input",
    ]

    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() and locator.is_visible():
            return locator

    raise ValueError("MojPosao keyword input not found")


def _apply_keyword_search(page: Page, keyword: str, category: dict | None = None) -> None:
    keyword = _clean_text(keyword)
    if not keyword:
        return

    heading_before = _get_results_heading(page)

    try:
        input_field = _find_keyword_input(page)
        input_field.click(timeout=10000)
        input_field.fill(keyword, timeout=10000)
        input_field.press("Enter", timeout=10000)
        page.wait_for_timeout(2000)
        _wait_for_results(page)
    except Exception as exc:
        print(f"[mojposao] Search via input failed, retrying with direct URL: {exc}")
        page.goto(_build_search_url(keyword=keyword, category=category), wait_until="domcontentloaded", timeout=60000)
        _wait_for_results(page)
        return

    heading_after = _get_results_heading(page)
    if heading_after == heading_before or keyword.casefold() not in heading_after.casefold():
        print(f"[mojposao] Search input did not update results, retrying with direct URL for '{_normalize_keyword(keyword)}'")
        page.goto(_build_search_url(keyword=keyword, category=category), wait_until="domcontentloaded", timeout=60000)
        _wait_for_results(page)


def _get_job_count(page: Page) -> int:
    return page.locator(JOB_TITLE_SELECTOR).count()


def _has_recommendation_banner(page: Page) -> bool:
    try:
        banner = page.locator(f"text={NO_MORE_RESULTS_TEXT}").first
        return banner.count() > 0
    except Exception:
        return False


def _find_load_more_button(page: Page) -> Locator:
    selectors = [
        f"button:has-text('{LOAD_MORE_TEXT}')",
        f"a:has-text('{LOAD_MORE_TEXT}')",
        f"text={LOAD_MORE_TEXT}",
    ]

    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() and locator.is_visible():
            return locator

    return page.locator("button").nth(9999)


def _load_all_jobs(page: Page, max_clicks: int) -> None:
    stagnant_iterations = 0

    for iteration in range(1, max_clicks + 1):
        if _has_recommendation_banner(page):
            print(f"[mojposao] Recommendation banner detected on iteration {iteration}; stopping before unrelated jobs")
            break

        before_count = _get_job_count(page)
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(1500)

        if _has_recommendation_banner(page):
            print(f"[mojposao] Recommendation banner detected after scroll on iteration {iteration}; stopping")
            break

        button = _find_load_more_button(page)
        clicked = False

        if button.count() and button.is_visible():
            try:
                button.click(timeout=10000)
                clicked = True
                page.wait_for_timeout(2500)
            except Exception as exc:
                print(f"[mojposao] Failed to click load more on iteration {iteration}: {exc}")

        after_count = _get_job_count(page)
        print(
            f"[mojposao] Iteration {iteration}: before={before_count}, after={after_count}, "
            f"clicked_load_more={clicked}"
        )

        if after_count <= before_count:
            stagnant_iterations += 1
        else:
            stagnant_iterations = 0

        if _has_recommendation_banner(page) or (not clicked and stagnant_iterations >= 2):
            break


def _extract_job_from_card(card: Locator, page: Page) -> dict | None:
    try:
        title = _clean_text(card.locator(JOB_TITLE_SELECTOR).first.inner_text(timeout=5000))
        link = card.locator(f"a:has({JOB_TITLE_SELECTOR})").first
        href = link.get_attribute("href", timeout=5000)
        location = _clean_text(
            card.locator(".content__info .info__child").first.inner_text(timeout=5000)
            if card.locator(".content__info .info__child").count()
            else ""
        )
        company = _clean_text(
            card.locator("img[alt]").first.get_attribute("alt", timeout=5000)
            if card.locator("img[alt]").count()
            else ""
        )

        detail_url = urljoin(page.url, href or "")
        if not title or not company or not location or not detail_url:
            return None

        return {
            "title": title,
            "company": company,
            "location": location,
            "detail_url": detail_url,
            "employer_website": "",
            "published_at": "",
            "category": "",
            "source": "mojposao",
        }
    except Exception as exc:
        print(f"[mojposao] Failed to extract job card: {exc}")
        return None


def _collect_listing_jobs(page: Page) -> list[dict]:
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    links = page.locator(f"a:has({JOB_TITLE_SELECTOR})")
    total_links = links.count()
    results_limit = _get_results_limit(page)

    for index in range(total_links):
        try:
            link = links.nth(index)
            href = link.get_attribute("href", timeout=3000)
            title = _clean_text(link.locator(JOB_TITLE_SELECTOR).first.inner_text(timeout=3000))
            detail_url = urljoin(page.url, href or "")

            if not title or not detail_url or detail_url in seen_urls:
                continue
            if "search_results_recommendations" in detail_url:
                continue

            jobs.append(
                {
                    "title": title,
                    "company": "",
                    "location": "",
                    "detail_url": detail_url,
                    "employer_website": "",
                    "published_at": "",
                    "category": "",
                    "source": "mojposao",
                }
            )
            seen_urls.add(detail_url)

            if results_limit is not None and len(jobs) >= results_limit:
                break
        except Exception as exc:
            print(f"[mojposao] Failed to collect listing link {index}: {exc}")

    return jobs


def _candidate_is_valid_website(url: str, detail_url: str) -> bool:
    if not url:
        return False

    normalized = url.strip()
    if normalized.startswith("mailto:") or normalized.startswith("tel:"):
        return False

    parsed = urlparse(normalized)
    domain = parsed.netloc.casefold()
    path = parsed.path.casefold()

    if parsed.scheme not in {"http", "https"}:
        return False
    if not domain:
        return False
    if domain.endswith(EXCLUDED_WEBSITE_DOMAINS):
        return False
    if "/apply" in path or "apply" in domain:
        return False
    if normalized.rstrip("/") == detail_url.rstrip("/"):
        return False
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp)(?:$|\?)", normalized, re.IGNORECASE):
        return False

    return True


def _extract_employer_website_from_detail(detail_page: Page, detail_url: str) -> str:
    selectors = [
        ".organization-card a[href]",
        "#job-htmlad a[href]",
        "article a[href]",
    ]

    for selector in selectors:
        locator = detail_page.locator(selector)
        total = locator.count()
        for index in range(total):
            try:
                href = locator.nth(index).get_attribute("href", timeout=3000)
                if _candidate_is_valid_website(href or "", detail_url):
                    return href or ""
            except Exception:
                continue

    try:
        nuxt_payload = detail_page.locator("#__NUXT_DATA__").inner_text(timeout=5000)
        candidates = re.findall(r"https?://[^\"'\\\s<>()]+", nuxt_payload)
        for candidate in candidates:
            if _candidate_is_valid_website(candidate, detail_url):
                return candidate
    except Exception as exc:
        print(f"[mojposao] Failed to inspect detail payload for website: {exc}")

    return ""


def _extract_category_from_detail(detail_page: Page) -> str:
    try:
        category_link = detail_page.locator(".grid__aside a[href*='positions=']").first
        if category_link.count():
            return _clean_text(category_link.inner_text(timeout=3000))
    except Exception as exc:
        print(f"[mojposao] Failed to extract category from detail page: {exc}")

    return ""


def _extract_published_at_from_detail(detail_page: Page) -> str:
    try:
        rows = detail_page.locator(".job-section__rows .row, .grid__aside .row")
        total = rows.count()
        for index in range(total):
            try:
                text = _clean_text(rows.nth(index).inner_text(timeout=3000))
            except Exception:
                continue

            if not text.startswith("Oglas objavljen"):
                continue

            normalized_lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
            if len(normalized_lines) >= 2:
                return normalized_lines[-1]

            cleaned = _clean_text(text.replace("Oglas objavljen", "", 1))
            if cleaned:
                return cleaned
    except Exception as exc:
        print(f"[mojposao] Failed to extract published date from detail page: {exc}")

    try:
        body_text = detail_page.locator("body").inner_text()
        match = re.search(r"Oglas objavljen\s*(\d{2}\.\s*\d{2}\.\s*\d{4}\.)", body_text)
        if match:
            return _clean_text(match.group(1))
    except Exception:
        return ""

    return ""


def _extract_company_from_detail(detail_page: Page) -> str:
    selectors = [
        ".organization-card__name",
        ".job__employer",
        "img[alt]",
    ]

    for selector in selectors:
        locator = detail_page.locator(selector).first
        if not locator.count():
            continue
        try:
            if selector == "img[alt]":
                value = locator.get_attribute("alt", timeout=3000) or ""
            else:
                value = locator.inner_text(timeout=3000)
            cleaned = _clean_text(value)
            if cleaned and cleaned.casefold() not in {"logo-site", "my-paycheck-site", "ppi-site"}:
                return cleaned
        except Exception:
            continue

    return ""


def _extract_location_from_detail(detail_page: Page) -> str:
    selectors = [
        ".job__additional-info .row",
        ".grid__main .row",
    ]

    for selector in selectors:
        rows = detail_page.locator(selector)
        total = rows.count()
        for index in range(total):
            try:
                text = _clean_text(rows.nth(index).inner_text(timeout=3000))
            except Exception:
                continue
            if text.startswith("Lokacija rada:"):
                return _clean_text(text.split(":", 1)[1])

    body_text = detail_page.locator("body").inner_text()
    match = re.search(r"Lokacija rada:\s*(.+)", body_text)
    if match:
        return _clean_text(match.group(1).splitlines()[0])

    return ""


def _enrich_job_from_detail(detail_page: Page, detail_url: str) -> dict[str, str]:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(1000)
        return {
            "company": _extract_company_from_detail(detail_page),
            "location": _extract_location_from_detail(detail_page),
            "employer_website": _extract_employer_website_from_detail(detail_page, detail_url),
            "published_at": _extract_published_at_from_detail(detail_page),
            "category": _extract_category_from_detail(detail_page),
        }
    except Exception as exc:
        print(f"[mojposao] Failed to enrich detail page {detail_url}: {exc}")
        return {
            "company": "",
            "location": "",
            "employer_website": "",
            "published_at": "",
            "category": "",
        }


def _matches_selected_filters(job: dict, keyword: str, category: dict | None) -> bool:
    normalized_keyword = _slugify(keyword)
    if normalized_keyword:
        if category and _slugify(job.get("category", "")) != _slugify(category["label"]):
            return False

        searchable = " ".join(
            [
                job.get("title", ""),
                job.get("company", ""),
                job.get("location", ""),
                job.get("category", ""),
            ]
        )
        if normalized_keyword not in _slugify(searchable):
            return False

    return True


def _extract_jobs(page: Page, detail_page: Page, keyword: str, category: dict | None) -> list[dict]:
    jobs: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    listing_jobs = _collect_listing_jobs(page)

    for index, job in enumerate(listing_jobs):
        try:
            job.update(_enrich_job_from_detail(detail_page, job["detail_url"]))
            if not job.get("company") or not job.get("location"):
                continue

            dedupe_key = (job["title"].casefold(), job["company"].casefold())
            if dedupe_key in seen_keys:
                continue

            if not _matches_selected_filters(job, keyword, category):
                continue

            jobs.append(job)
            seen_keys.add(dedupe_key)
        except Exception as exc:
            print(f"[mojposao] Skipping broken card {index}: {exc}")

    return jobs


def scrape_mojposao(keyword: str = "", max_clicks: int = 5, category: str | None = None) -> list[dict]:
    headless = os.getenv("HEADLESS", "true") == "true"
    resolved_category = _resolve_category(category)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        detail_page = context.new_page()

        try:
            page.goto(_build_search_url(category=resolved_category), wait_until="domcontentloaded", timeout=60000)
            _wait_for_results(page)
            _apply_keyword_search(page, keyword, resolved_category)
            _load_all_jobs(page, max_clicks=max_clicks)
            return _extract_jobs(page, detail_page, keyword, resolved_category)
        except PlaywrightTimeoutError as exc:
            print(f"[mojposao] Timeout while scraping: {exc}")
            return []
        except Exception as exc:
            print(f"[mojposao] Scraper failed: {exc}")
            return []
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    jobs = scrape_mojposao(keyword="konobar", max_clicks=5, category="tourism_hospitality")
    print(len(jobs))
    print(jobs[:5])
