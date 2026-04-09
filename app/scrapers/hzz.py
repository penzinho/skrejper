import os
import re
import time
from urllib.parse import urljoin

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


LANDING_URL = "https://burzarada.hzz.hr/Posloprimac_RadnaMjesta.aspx"
BASE_URL = "https://burzarada.hzz.hr/Posloprimac_RadnaMjesta.aspx?trazi=1"
DETAIL_URL_KEYWORD = "RadnoMjesto_Ispis.aspx"
EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
PHONE_RE = re.compile(
    r"(?:(?:\+|00)\s*385[\s/.-]*)?(?:0\d[\d\s/.-]{5,}\d|\d{2,3}[\s/.-]*\d{3}[\s/.-]*\d{3,4})"
)
GENERIC_EMAILS = {"burzarada@hzz.hr"}
DETAIL_SECTION_LABELS = [
    "Mjesto rada:",
    "Broj traženih radnika:",
    "Vrsta zaposlenja:",
    "Radno vrijeme:",
    "Način rada:",
    "Smještaj:",
    "Naknada za prijevoz:",
    "Bruto plaća:",
    "Natječaj vrijedi od:",
    "Natječaj vrijedi do:",
    "Uvjeti na radnom mjestu:",
    "Posloprimac",
    "Poslodavac",
    "Kontakt:",
    "Potrebna zvanja:",
    "Razina obrazovanja:",
    "Vozački ispit:",
    "Radno iskustvo:",
    "Ostale informacije:",
    "Opis posla:",
    "Nudimo:",
    "Karakteristike prave osobe za nas:",
]
HZZ_CATEGORIES = {
    "it": "Informatički, računalni i stručnjaci za Internet",
    "economy_admin": "Ekonomisti, pravnici, administrativci",
    "arts_culture": "Umjetnici, dizajneri, stručnjaci u kulturi",
    "construction": "Građevinari, arhitekti, geodete",
    "trade_marketing": "Trgovci, promotori, oglašivači",
    "hospitality_tourism": "Ugostitelji, radnici u turizmu",
    "geology_mining_metallurgy": "Geolozi, rudari, metalurzi",
    "mechanics_shipbuilding": "Strojari i brodograditelji",
    "electrical": "Stručnjaci za elektrotehniku",
    "chemical_food": "Kemijski i prehrambeni stručnjaci",
    "wood_paper_graphics": "Obrađivači drva, proizvođači papira, grafičari",
    "textiles_leather": "Tekstilci i kožari",
    "transport": "Stručnjaci u prometu",
    "education": "Učitelji, nastavnici, profesori",
    "social_humanities": "Društveno-humanistički stručnjaci",
    "science_math": "Prirodoslovno-matematički stručnjaci",
    "health_social_care": "Zdravstvo, socijalna skrb, njega",
    "agriculture_forestry_fishing": "Poljoprivredni, šumarski, ribarski stručnjaci",
    "home_communal_other": "Usluge u kući, komunalne usluge i ostala zanimanja",
    "directors_managers": "Direktori, ravnatelji, čelnici",
}


def extract_email(text: str) -> str:
    for match in EMAIL_RE.finditer(text or ""):
        email = match.group(0).strip()
        if email.casefold() not in GENERIC_EMAILS:
            return email
    return ""


def extract_phone(text: str) -> str:
    match = PHONE_RE.search(text or "")
    if not match:
        return ""

    phone = re.sub(r"\s+", " ", match.group(0)).strip(" .,-;")
    digits = re.sub(r"\D", "", phone)
    return phone if len(digits) >= 6 else ""


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify_category(value: str) -> str:
    normalized = (value or "").casefold()
    replacements = {
        "č": "c",
        "ć": "c",
        "š": "s",
        "ž": "z",
        "đ": "d",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def get_hzz_categories() -> list[dict[str, str]]:
    return [{"key": key, "label": label} for key, label in HZZ_CATEGORIES.items()]


def _resolve_category(category: str | None) -> str | None:
    if not category:
        return None

    candidate = category.strip()
    if candidate in HZZ_CATEGORIES:
        return HZZ_CATEGORIES[candidate]

    candidate_slug = _slugify_category(candidate)
    for key, label in HZZ_CATEGORIES.items():
        if candidate_slug in {key, _slugify_category(label)}:
            return label

    available = ", ".join(HZZ_CATEGORIES.keys())
    raise ValueError(f"Unknown HZZ category '{category}'. Available keys: {available}")


def _parse_row_text(text: str, title: str) -> tuple[str, str]:
    company = ""
    location = ""

    lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
    for line in lines:
        normalized = line.casefold()
        if normalized.startswith("poslodavac:"):
            company = line.split(":", 1)[1].strip()
        elif normalized.startswith("mjesto rada:"):
            location = line.split(":", 1)[1].strip()

    if company and location:
        return company, location

    flattened = " | ".join(lines)
    company_match = re.search(
        r"Poslodavac:\s*(.+?)(?=\s+\|\s+(?:Rok za prijavu|Mjesto rada|Traženo radnika)\s*:|$)",
        flattened,
        re.IGNORECASE,
    )
    location_match = re.search(
        r"Mjesto rada:\s*(.+?)(?=\s+\|\s+(?:Traženo radnika|Poslodavac|Rok za prijavu)\s*:|$)",
        flattened,
        re.IGNORECASE,
    )

    if company_match and not company:
        company = _clean_text(company_match.group(1))
    if location_match and not location:
        location = _clean_text(location_match.group(1))

    if not company or not location:
        filtered = [line for line in lines if line != title and ":" not in line]
        if filtered:
            if not location:
                location = filtered[0]
            if len(filtered) > 1 and not company:
                company = filtered[1]

    return company, location


def _collect_listing_rows(page: Page) -> list[dict]:
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    anchors = page.locator(f"a.TitleLink[href*='{DETAIL_URL_KEYWORD}']")
    total = anchors.count()

    for index in range(total):
        try:
            anchor = anchors.nth(index)
            title = _clean_text(anchor.inner_text(timeout=5000))
            href = anchor.get_attribute("href", timeout=5000)
            if not href:
                continue

            detail_url = urljoin(page.url, href)
            if not title or detail_url in seen_urls:
                continue

            container = anchor.locator(
                "xpath=ancestor::*[self::tr or self::article or self::li or self::div][.//a[contains(@href, 'RadnoMjesto_Ispis.aspx')]][1]"
            )
            row_text = container.inner_text(timeout=5000)
            company, location = _parse_row_text(row_text, title)

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "employees_needed": "",
                    "employment_type": "",
                    "working_hours": "",
                    "accommodation": "",
                    "valid_from": "",
                    "valid_to": "",
                    "jobseeker_info": "",
                    "detail_url": detail_url,
                }
            )
            seen_urls.add(detail_url)
        except Exception as exc:
            print(f"[hzz] Skipping broken row {index}: {exc}")

    return jobs


def _open_detail_page(detail_page: Page, detail_url: str) -> tuple[str, str]:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(300)
        text = detail_page.locator("body").inner_text(timeout=5000)
        email = extract_email(text)
        phone = extract_phone(text)
        return email, phone
    except Exception as exc:
        print(f"[hzz] Failed to scrape detail page {detail_url}: {exc}")
        return "", ""


def _extract_labeled_value(text: str, label: str, stop_labels: list[str] | None = None) -> str:
    stop_labels = stop_labels or []
    pattern = re.escape(label) + r"\s*(.+?)"
    if stop_labels:
        stop_pattern = "|".join(re.escape(stop_label) for stop_label in stop_labels)
        pattern += r"(?=\n(?:" + stop_pattern + r")|$)"
    else:
        pattern += r"(?=\n|$)"

    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _default_stop_labels(label: str, extra_stop_labels: list[str] | None = None) -> list[str]:
    extra_stop_labels = extra_stop_labels or []
    return [item for item in DETAIL_SECTION_LABELS if item != label] + extra_stop_labels


def _extract_hzz_value(text: str, label: str, extra_stop_labels: list[str] | None = None) -> str:
    return _extract_labeled_value(text, label, _default_stop_labels(label, extra_stop_labels))


def _extract_section(text: str, heading: str, stop_headings: list[str]) -> str:
    pattern = (
        re.escape(heading)
        + r"\s*(.+?)(?=\n(?:"
        + "|".join(re.escape(stop_heading) for stop_heading in stop_headings)
        + r")\s*$)"
    )
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _extract_detail_fields(text: str) -> dict:
    normalized_text = re.sub(r"\r\n?", "\n", text or "")
    posloprimac_info = _extract_section(
        normalized_text,
        "Posloprimac",
        ["Poslodavac"],
    )

    return {
        "employees_needed": _extract_hzz_value(normalized_text, "Broj traženih radnika:"),
        "employment_type": _extract_hzz_value(normalized_text, "Vrsta zaposlenja:"),
        "working_hours": _extract_hzz_value(normalized_text, "Radno vrijeme:"),
        "accommodation": _extract_hzz_value(normalized_text, "Smještaj:"),
        "valid_from": _extract_hzz_value(normalized_text, "Natječaj vrijedi od:"),
        "valid_to": _extract_hzz_value(normalized_text, "Natječaj vrijedi do:"),
        "jobseeker_info": posloprimac_info,
    }


def _scrape_detail_page(detail_page: Page, detail_url: str) -> dict:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(300)
        text = detail_page.locator("body").inner_text(timeout=5000)
        details = _extract_detail_fields(text)
        details["email"] = extract_email(text)
        details["phone"] = extract_phone(text)
        return details
    except Exception as exc:
        print(f"[hzz] Failed to scrape detail page {detail_url}: {exc}")
        return {
            "email": "",
            "phone": "",
            "employees_needed": "",
            "employment_type": "",
            "working_hours": "",
            "accommodation": "",
            "valid_from": "",
            "valid_to": "",
            "jobseeker_info": "",
        }


def _select_category(page: Page, category_label: str) -> None:
    page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1000)

    category_link = page.locator("a").filter(has_text=category_label).first
    if category_link.count() == 0:
        raise ValueError(f"HZZ category link not found for '{category_label}'")

    category_link.click(timeout=10000)
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    page.wait_for_timeout(1000)


def _go_to_next_page(page: Page, current_page: int) -> bool:
    try:
        next_button = page.locator(
            "ul.pagination a:has-text('Sljede'), ul.pagination a[aria-label*='Sljede'], "
            "a:has-text('Sljede'), button:has-text('Sljede'), input[value*='Sljede']"
        ).first
        if next_button.count() == 0:
            return False

        page_marker_before = page.locator(f"text=Stranica {current_page}").count()
        next_button.click(timeout=10000)
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        page.wait_for_timeout(500)

        if page_marker_before and page.locator(f"text=Stranica {current_page + 1}").count():
            return True

        return True
    except PlaywrightTimeoutError:
        print(f"[hzz] Timeout while moving to page {current_page + 1}")
        return False
    except Exception as exc:
        print(f"[hzz] Failed to paginate to page {current_page + 1}: {exc}")
        return False


def scrape_hzz(max_pages: int = 3, category: str | None = None) -> list[dict]:
    headless = os.getenv("HEADLESS", "true") == "true"
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    resolved_category = _resolve_category(category)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        detail_page = context.new_page()

        try:
            if resolved_category:
                _select_category(page, resolved_category)
            else:
                page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1000)

            for page_number in range(1, max_pages + 1):
                try:
                    page_jobs = _collect_listing_rows(page)
                    for job in page_jobs:
                        detail_url = job["detail_url"]
                        if detail_url in seen_urls:
                            continue

                        detail_fields = _scrape_detail_page(detail_page, detail_url)
                        job.update(detail_fields)
                        job["source"] = "hzz"
                        jobs.append(job)
                        seen_urls.add(detail_url)
                        time.sleep(0.2)
                except Exception as exc:
                    print(f"[hzz] Failed while scraping listing page {page_number}: {exc}")

                if page_number >= max_pages:
                    break

                if not _go_to_next_page(page, page_number):
                    break
        finally:
            context.close()
            browser.close()

    return jobs


if __name__ == "__main__":
    jobs = scrape_hzz(max_pages=2, category="hospitality_tourism")
    print(len(jobs))
    print(jobs[:3])
