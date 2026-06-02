import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
import os
import re
from urllib.parse import quote, unquote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


BASE_URL = "https://www.gelbeseiten.de"
DEFAULT_QUERY = "personalvermittlung"
DEFAULT_LOCATION = "bundesweit"
RESULT_CARD_SELECTOR = "article.mod-Treffer"
LOAD_MORE_BUTTON_SELECTOR = "#mod-LoadMore--button"
DETAIL_LINK_SELECTOR = "a[href*='/gsbiz/']"
EMAIL_RE = re.compile(r"mailto:([^?\"'#]+)")
EMAIL_CANDIDATE_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
CITY_RE = re.compile(r"^(?:\d{5}\s+)?(.+)$")
PHONE_RE = re.compile(r'href="tel:[^"]+"[^>]*>\s*<span[^>]*>([^<]+)</span>', re.IGNORECASE)
WEBSITE_RE = re.compile(r'detailseite_webadresse[^>]*href="([^"]+)"', re.IGNORECASE)
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
ADDRESS_BLOCK_RE = re.compile(
    r'<div class="mod-Kontaktdaten__address-container">(.+?)</div>\s*</address>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
CONTACT_PAGE_KEYWORDS = (
    "kontakt",
    "contact",
    "impressum",
    "about",
    "ueber-uns",
    "uber-uns",
)
DEFAULT_CONTACT_PATHS = (
    "/kontakt",
    "/kontakt/",
    "/contact",
    "/contact/",
    "/contact-us",
    "/impressum",
    "/impressum/",
)
NON_EMAIL_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".avif",
    ".pdf",
)
MAX_WEBSITE_PAGES = 4
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
AJAX_SEARCH_URL = f"{BASE_URL}/ajaxsuche"
LIST_TOTAL_RE = re.compile(r'<span id="mod-TrefferlisteInfo">(\d+)</span>')
LOAD_MORE_FORM_RE = re.compile(
    r'<form class="mod mod-LoadMore" id="mod-LoadMore".*?</form>',
    re.IGNORECASE | re.DOTALL,
)
HIDDEN_INPUT_RE = re.compile(
    r'<input[^>]+type="hidden"[^>]+name="([^"]+)"[^>]+value="([^"]*)"',
    re.IGNORECASE,
)
ARTICLE_BLOCK_RE = re.compile(
    r'<article class="mod mod-Treffer".*?</article>',
    re.IGNORECASE | re.DOTALL,
)
CARD_NAME_RE = re.compile(r'<h2 class="mod-Treffer__name"[^>]*>(.*?)</h2>', re.IGNORECASE | re.DOTALL)
CARD_DETAIL_URL_RE = re.compile(r'<a href="(https://www\.gelbeseiten\.de/gsbiz/[^"]+)"', re.IGNORECASE)
CARD_ADDRESS_RE = re.compile(
    r'<div class="mod-AdresseKompakt__adress-text">\s*(.*?)\s*</div>',
    re.IGNORECASE | re.DOTALL,
)
CARD_CITY_RE = re.compile(
    r'<span class="nobr mod-AdresseKompakt__adress__ort">(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
CARD_PHONE_RE = re.compile(
    r'<a class="mod-TelefonnummerKompakt__phoneNumber[^"]*"[^>]*>\s*([^<]+?)\s*</a>',
    re.IGNORECASE | re.DOTALL,
)
CARD_WEBSITE_RE = re.compile(r'data-webseiteLink="([^"]+)"', re.IGNORECASE)
DATA_PARAMETERS_RE = re.compile(r'data-parameters="([^"]+)"', re.IGNORECASE)
DETAIL_EMAIL_BUTTON_RE = re.compile(r'data-link="mailto:([^?"#]+)', re.IGNORECASE)
DETAIL_SAME_AS_RE = re.compile(r'"sameAs"\s*:\s*"([^"]+)"', re.IGNORECASE)
DEFAULT_ENRICHMENT_WORKERS = 16
DETAIL_HTTP_TIMEOUT_SECONDS = 15
WEBSITE_HTTP_TIMEOUT_SECONDS = 10


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify_path(value: str) -> str:
    normalized = _clean_text(value).replace("/", " ")
    hyphenated = re.sub(r"\s+", "-", normalized.casefold())
    return quote(hyphenated, safe="-")


def _build_search_url(query: str, location: str) -> str:
    return f"{BASE_URL}/suche/{_slugify_path(query or DEFAULT_QUERY)}/{_slugify_path(location or DEFAULT_LOCATION)}"


def _decode_base64_value(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    padding = "=" * (-len(cleaned) % 4)
    try:
        decoded = base64.b64decode((cleaned + padding).encode("ascii"), validate=False).decode("utf-8", errors="ignore")
    except Exception:
        return ""

    return _clean_text(unquote(decoded))


def _extract_city(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    match = CITY_RE.match(cleaned)
    return _clean_text(match.group(1)) if match else cleaned


def _extract_address_parts(card: Locator) -> tuple[str, str]:
    address_text = ""
    city_text = ""

    try:
        address_locator = card.locator(".mod-AdresseKompakt__adress-text").first
        if address_locator.count():
            address_text = _clean_text(address_locator.inner_text(timeout=3000))
    except Exception:
        address_text = ""

    try:
        city_locator = card.locator(".mod-AdresseKompakt__adress__ort").first
        if city_locator.count():
            city_text = _clean_text(city_locator.inner_text(timeout=3000))
    except Exception:
        city_text = ""

    city = _extract_city(city_text)
    street = address_text
    if city_text:
        street = street.replace(city_text, " ")
    street = re.sub(r"\([^)]*\)", " ", street)
    street = _clean_text(street).strip(",")
    street = street.rstrip(",")

    return street, city


def _extract_email_from_card(card: Locator) -> str:
    candidates = [
        ".contains-icon-chat[data-parameters]",
        "[data-parameters*='email']",
    ]

    for selector in candidates:
        locator = card.locator(selector).first
        try:
            if locator.count() == 0:
                continue
            raw = locator.get_attribute("data-parameters", timeout=3000)
            if not raw:
                continue
            decoded = html.unescape(raw)
            match = re.search(r'"email"\s*:\s*"([^"]+)"', decoded)
            if match:
                return _clean_text(match.group(1))
        except Exception:
            continue

    return ""


def _normalize_email_candidate(value: str) -> str:
    normalized = _clean_text(html.unescape(unquote(value or "")))
    normalized = normalized.removeprefix("mailto:").strip(" <>\"'(),;:")
    return normalized.lstrip("%20").lower()


def _is_valid_email_candidate(value: str) -> bool:
    if not value or value.count("@") != 1 or "/" in value:
        return False

    local_part, domain = value.split("@", 1)
    if not local_part or "." not in domain:
        return False

    domain = domain.lower()
    if domain.endswith(NON_EMAIL_SUFFIXES):
        return False

    if local_part.lower().startswith("favicon"):
        return False

    return True


def _extract_email_candidates(text: str) -> list[str]:
    emails: list[str] = []
    seen: set[str] = set()

    for match in EMAIL_RE.findall(text or "") + EMAIL_CANDIDATE_RE.findall(text or ""):
        candidate = _normalize_email_candidate(match)
        if not _is_valid_email_candidate(candidate) or candidate in seen:
            continue
        seen.add(candidate)
        emails.append(candidate)

    return emails


def _html_lines(fragment: str) -> list[str]:
    text = TAG_RE.sub("\n", html.unescape(fragment or ""))
    return [_clean_text(line) for line in text.splitlines() if _clean_text(line)]


def _fetch_html_via_http(url: str, data: dict[str, str] | None = None) -> str:
    _, body, _ = _fetch_html_via_http_response(url, data)
    return body


def _fetch_html_via_http_response(
    url: str,
    data: dict[str, str] | None = None,
    *,
    timeout: int = DETAIL_HTTP_TIMEOUT_SECONDS,
    retries: int = 2,
) -> tuple[str, str, str]:
    encoded_data = urlencode(data).encode("utf-8") if data else None
    last_exc: Exception | None = None

    for _ in range(max(retries, 0) + 1):
        request = Request(
            url,
            data=encoded_data,
            headers={
                "Accept-Language": "de-DE,de;q=0.9",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                final_url = response.geturl()
                content_type = response.headers.get("content-type", "")
                body = response.read().decode("utf-8", errors="ignore")
                return final_url, body, content_type
        except Exception as exc:
            last_exc = exc

    if last_exc is not None:
        raise last_exc

    raise RuntimeError(f"Failed to fetch {url}")


def _extract_total_results(search_html: str) -> int:
    match = LIST_TOTAL_RE.search(search_html or "")
    return int(match.group(1)) if match else 0


def _extract_load_more_params(search_html: str) -> dict[str, str]:
    form_match = LOAD_MORE_FORM_RE.search(search_html or "")
    if not form_match:
        return {}

    params: dict[str, str] = {}
    for name, value in HIDDEN_INPUT_RE.findall(form_match.group(0)):
        params[name] = html.unescape(value)

    return params


def _extract_article_blocks(search_html: str) -> list[str]:
    return ARTICLE_BLOCK_RE.findall(search_html or "")


def _extract_generic_card_data(card_html: str) -> dict[str, str]:
    for raw_value in DATA_PARAMETERS_RE.findall(card_html or ""):
        try:
            payload = json.loads(html.unescape(raw_value))
        except Exception:
            continue

        generic = (
            payload.get("inboxConfig", {})
            .get("organizationQuery", {})
            .get("generic", {})
        )
        if not isinstance(generic, dict):
            continue

        phone_candidates = generic.get("phones") or []
        phone = phone_candidates[0] if isinstance(phone_candidates, list) and phone_candidates else ""
        return {
            "email": _clean_text(generic.get("email", "")),
            "address": _clean_text(generic.get("street", "")),
            "city": _clean_text(generic.get("city", "")),
            "phone": _clean_text(phone),
        }

    return {"email": "", "address": "", "city": "", "phone": ""}


def _extract_address_parts_from_html(card_html: str) -> tuple[str, str]:
    address_match = CARD_ADDRESS_RE.search(card_html or "")
    city_match = CARD_CITY_RE.search(card_html or "")
    city_text = _clean_text(html.unescape(city_match.group(1))) if city_match else ""

    if not address_match:
        return "", _extract_city(city_text)

    address_lines = _html_lines(address_match.group(1))
    city = _extract_city(city_text)
    street_parts = [
        line
        for line in address_lines
        if line != city_text and line != city and not re.match(r"^\d{5}\s+", line)
    ]
    street = _clean_text(" ".join(street_parts)).strip(",")

    if not city:
        city_line = next((line for line in address_lines if re.match(r"^\d{5}\s+", line)), "")
        city = _extract_city(city_line)

    return street, city


def _extract_card_from_html(card_html: str) -> dict | None:
    detail_match = CARD_DETAIL_URL_RE.search(card_html or "")
    name_match = CARD_NAME_RE.search(card_html or "")

    if not detail_match or not name_match:
        return None

    company = _clean_text(html.unescape(name_match.group(1)))
    detail_url = _clean_text(html.unescape(detail_match.group(1)))
    if not company or not detail_url:
        return None

    generic = _extract_generic_card_data(card_html)
    address, city = _extract_address_parts_from_html(card_html)
    phone_match = CARD_PHONE_RE.search(card_html or "")
    website_match = CARD_WEBSITE_RE.search(card_html or "")
    email_candidates = _extract_email_candidates(card_html)

    phone = _clean_text(html.unescape(phone_match.group(1))) if phone_match else generic["phone"]
    website = _decode_base64_value(website_match.group(1)) if website_match else ""
    email = email_candidates[0] if email_candidates else generic["email"]

    if not address:
        address = generic["address"]
    if not city:
        city = generic["city"]

    return {
        "title": company,
        "company": company,
        "address": address,
        "city": city,
        "location": city,
        "email": email,
        "phone": phone,
        "website": website,
        "employer_address": address,
        "employer_website": website,
        "detail_url": detail_url,
        "source": "gelbeseiten",
    }


def _extract_detail_fields_from_html(body: str) -> dict[str, str]:
    details = {
        "email": "",
        "phone": "",
        "website": "",
        "address": "",
        "city": "",
    }

    email_candidates = _extract_email_candidates(body)
    if email_candidates:
        details["email"] = email_candidates[0]
    else:
        email_match = DETAIL_EMAIL_BUTTON_RE.search(body or "")
        if email_match:
            candidate = _normalize_email_candidate(email_match.group(1))
            if _is_valid_email_candidate(candidate):
                details["email"] = candidate

    phone_match = PHONE_RE.search(body or "")
    website_match = WEBSITE_RE.search(body or "")
    same_as_match = DETAIL_SAME_AS_RE.search(body or "")
    address_match = ADDRESS_BLOCK_RE.search(body or "")

    if phone_match:
        details["phone"] = _clean_text(html.unescape(phone_match.group(1)))
    if website_match:
        details["website"] = _clean_text(html.unescape(website_match.group(1)))
    elif same_as_match:
        details["website"] = _clean_text(html.unescape(same_as_match.group(1)))
    if address_match:
        lines = _html_lines(address_match.group(1))
        city_line = next((line for line in reversed(lines) if re.search(r"\d{5}\s+\S+", line)), "")
        street_line = next((line for line in lines if line != city_line and not re.search(r"\d{5}\s+\S+", line)), "")
        details["city"] = _extract_city(city_line)
        details["address"] = street_line

    return details


def _extract_email_from_website_via_http(website_url: str) -> str:
    if not website_url:
        return ""

    queue = [website_url]
    visited: set[str] = set()

    while queue and len(visited) < MAX_WEBSITE_PAGES:
        current_url = queue.pop(0)
        if current_url in visited:
            continue

        visited.add(current_url)

        try:
            final_url, body, content_type = _fetch_html_via_http_response(
                current_url,
                timeout=WEBSITE_HTTP_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            print(f"[gelbeseiten] Failed to fetch website page {current_url}: {exc}")
            continue

        if content_type and "html" not in content_type.lower():
            continue

        emails = _extract_email_candidates(body)
        if emails:
            return emails[0]

        if len(visited) == 1:
            queue.extend(
                candidate
                for candidate in _build_contact_page_candidates(final_url or current_url, body)
                if candidate not in visited
            )

    return ""


def _extract_detail_fields_via_http(detail_url: str) -> dict[str, str]:
    try:
        _, body, content_type = _fetch_html_via_http_response(detail_url)
    except Exception as exc:
        print(f"[gelbeseiten] Failed to fetch detail page {detail_url}: {exc}")
        return {"email": "", "phone": "", "website": "", "address": "", "city": ""}

    if content_type and "html" not in content_type.lower():
        return {"email": "", "phone": "", "website": "", "address": "", "city": ""}

    details = _extract_detail_fields_from_html(body)
    if not details["email"] and details["website"]:
        details["email"] = _extract_email_from_website_via_http(details["website"])

    return details


def _fetch_html(request_context, url: str) -> tuple[str, str]:
    response = request_context.get(url, timeout=30000)
    if not response.ok:
        return "", ""

    body = response.text()
    content_type = response.headers.get("content-type", "")
    if content_type and "html" not in content_type.lower():
        return "", ""

    return response.url, body


def _build_contact_page_candidates(base_url: str, body: str) -> list[str]:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return []

    root_url = f"{parsed.scheme}://{parsed.netloc}/"
    candidates: list[str] = []
    seen: set[str] = set()

    for path in DEFAULT_CONTACT_PATHS:
        candidate = urljoin(root_url, path)
        if candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    for href in HREF_RE.findall(body or ""):
        absolute_url = urljoin(base_url, html.unescape(href))
        parsed_href = urlparse(absolute_url)
        if parsed_href.scheme not in {"http", "https"} or parsed_href.netloc != parsed.netloc:
            continue

        lowered = absolute_url.lower()
        if not any(keyword in lowered for keyword in CONTACT_PAGE_KEYWORDS):
            continue

        normalized = absolute_url.split("#", 1)[0]
        if normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)

    return candidates


def _extract_email_from_website(request_context, website_url: str) -> str:
    if not website_url:
        return ""

    queue = [website_url]
    visited: set[str] = set()

    while queue and len(visited) < MAX_WEBSITE_PAGES:
        current_url = queue.pop(0)
        if current_url in visited:
            continue

        visited.add(current_url)

        try:
            final_url, body = _fetch_html(request_context, current_url)
        except Exception as exc:
            print(f"[gelbeseiten] Failed to fetch website page {current_url}: {exc}")
            continue

        if not body:
            continue

        emails = _extract_email_candidates(body)
        if emails:
            return emails[0]

        if len(visited) == 1:
            queue.extend(
                candidate for candidate in _build_contact_page_candidates(final_url or current_url, body) if candidate not in visited
            )

    return ""


def _extract_detail_fields(request_context, detail_url: str) -> dict[str, str]:
    details = {
        "email": "",
        "phone": "",
        "website": "",
        "address": "",
        "city": "",
    }

    try:
        final_url, body = _fetch_html(request_context, detail_url)
        if not body:
            return details

        email_candidates = _extract_email_candidates(body)
        phone_match = PHONE_RE.search(body)
        website_match = WEBSITE_RE.search(body)
        address_match = ADDRESS_BLOCK_RE.search(body)

        if email_candidates:
            details["email"] = email_candidates[0]
        if phone_match:
            details["phone"] = _clean_text(html.unescape(phone_match.group(1)))
        if website_match:
            details["website"] = _clean_text(html.unescape(website_match.group(1)))
        if address_match:
            lines = _html_lines(address_match.group(1))
            city_line = next((line for line in reversed(lines) if re.search(r"\d{5}\s+\S+", line)), "")
            street_line = next((line for line in lines if line != city_line and not re.search(r"\d{5}\s+\S+", line)), "")
            details["city"] = _extract_city(city_line)
            details["address"] = street_line

        if not details["email"]:
            website_url = details["website"]
            if website_url:
                details["email"] = _extract_email_from_website(request_context, website_url)

        return details
    except Exception as exc:
        print(f"[gelbeseiten] Failed to fetch detail page {detail_url}: {exc}")
        return details


def _merge_detail_fields(item: dict[str, str], detail_fields: dict[str, str], request_context) -> dict[str, str]:
    for source_key, detail_key in (
        ("email", "email"),
        ("phone", "phone"),
        ("website", "website"),
        ("address", "address"),
        ("city", "city"),
    ):
        if not item.get(source_key) and detail_fields.get(detail_key):
            item[source_key] = detail_fields[detail_key]

    if not item.get("email") and item.get("website") and request_context is not None:
        item["email"] = _extract_email_from_website(request_context, item["website"])

    item["location"] = item.get("city") or item.get("location") or ""
    item["employer_address"] = item.get("address", "")
    item["employer_website"] = item.get("website", "")

    return item


def _extract_card(card: Locator, page: Page) -> dict | None:
    try:
        link = card.locator(DETAIL_LINK_SELECTOR).first
        href = link.get_attribute("href", timeout=3000)
        detail_url = urljoin(page.url, href or "")
        company = _clean_text(card.locator(".mod-Treffer__name").first.inner_text(timeout=3000))
        address, city = _extract_address_parts(card)
        phone = _clean_text(
            card.locator(".mod-TelefonnummerKompakt__phoneNumber").first.inner_text(timeout=3000)
            if card.locator(".mod-TelefonnummerKompakt__phoneNumber").count()
            else ""
        )
        website = _decode_base64_value(
            card.locator(".mod-WebseiteKompakt__text").first.get_attribute("data-webseiteLink", timeout=3000)
            if card.locator(".mod-WebseiteKompakt__text").count()
            else ""
        )
        email = _extract_email_from_card(card)

        if not company or not detail_url:
            return None

        return {
            "title": company,
            "company": company,
            "address": address,
            "city": city,
            "location": city,
            "email": email,
            "phone": phone,
            "website": website,
            "employer_address": address,
            "employer_website": website,
            "detail_url": detail_url,
            "source": "gelbeseiten",
        }
    except Exception as exc:
        print(f"[gelbeseiten] Failed to extract result card: {exc}")
        return None


def _wait_for_results(page: Page) -> None:
    page.wait_for_selector(RESULT_CARD_SELECTOR, timeout=30000)
    page.wait_for_timeout(1000)


def _load_more_results(page: Page, current_count: int, iteration: int) -> bool:
    button = page.locator(LOAD_MORE_BUTTON_SELECTOR).first
    if button.count() == 0 or not button.is_visible():
        return False

    try:
        button.scroll_into_view_if_needed(timeout=5000)
        button.click(timeout=10000)
        for _ in range(20):
            page.wait_for_timeout(500)
            new_count = page.locator(RESULT_CARD_SELECTOR).count()
            if new_count > current_count:
                print(f"[gelbeseiten] Loaded more results on iteration {iteration}: {current_count} -> {new_count}")
                return True
    except Exception as exc:
        print(f"[gelbeseiten] Failed to load more results on iteration {iteration}: {exc}")

    return False


def scrape_gelbeseiten_fast(
    query: str = DEFAULT_QUERY,
    location: str = DEFAULT_LOCATION,
    max_pages: int = 1,
    company_limit: int | None = None,
) -> list[dict]:
    search_html = _fetch_html_via_http(_build_search_url(query, location))
    results: list[dict] = []
    seen_urls: set[str] = set()

    def add_cards(card_blocks: list[str]) -> bool:
        for card_html in card_blocks:
            item = _extract_card_from_html(card_html)
            if not item:
                continue

            detail_url = item["detail_url"]
            if detail_url in seen_urls:
                continue

            results.append(item)
            seen_urls.add(detail_url)

            if company_limit and len(results) >= company_limit:
                return True

        return False

    if add_cards(_extract_article_blocks(search_html)):
        return results

    if max_pages <= 1:
        return results

    load_more_params = _extract_load_more_params(search_html)
    if not load_more_params:
        return results

    total_results = _extract_total_results(search_html)
    position = int(load_more_params.get("position", "51"))
    batch_size = int(load_more_params.get("anzahl", "10"))

    for _ in range(2, max_pages + 1):
        if total_results and position > total_results:
            break

        payload = dict(load_more_params)
        payload["position"] = str(position)
        payload["anzahl"] = str(batch_size)
        response_text = _fetch_html_via_http(AJAX_SEARCH_URL, payload)
        response_data = json.loads(response_text)
        card_blocks = _extract_article_blocks(response_data.get("html", ""))
        if not card_blocks:
            break

        if add_cards(card_blocks):
            return results

        position += len(card_blocks)
        if len(card_blocks) < batch_size:
            break

    return results


def enrich_gelbeseiten_emails(
    items: list[dict],
    *,
    workers: int = DEFAULT_ENRICHMENT_WORKERS,
) -> list[dict]:
    if not items:
        return items

    detail_urls = {
        item["detail_url"]
        for item in items
        if item.get("detail_url") and not item.get("email")
    }
    detail_cache: dict[str, dict[str, str]] = {}

    if detail_urls:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            future_map = {
                executor.submit(_extract_detail_fields_via_http, detail_url): detail_url
                for detail_url in detail_urls
            }
            for future in as_completed(future_map):
                detail_url = future_map[future]
                try:
                    detail_cache[detail_url] = future.result()
                except Exception as exc:
                    print(f"[gelbeseiten] Failed to enrich detail {detail_url}: {exc}")
                    detail_cache[detail_url] = {"email": "", "phone": "", "website": "", "address": "", "city": ""}

    for item in items:
        if item.get("email"):
            continue
        detail_fields = detail_cache.get(item.get("detail_url", ""), {})
        if detail_fields:
            item.update(_merge_detail_fields(item, detail_fields, None))

    website_urls = {
        item["website"]
        for item in items
        if item.get("website") and not item.get("email")
    }
    website_email_cache: dict[str, str] = {}

    if website_urls:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            future_map = {
                executor.submit(_extract_email_from_website_via_http, website_url): website_url
                for website_url in website_urls
            }
            for future in as_completed(future_map):
                website_url = future_map[future]
                try:
                    website_email_cache[website_url] = future.result()
                except Exception as exc:
                    print(f"[gelbeseiten] Failed to enrich website {website_url}: {exc}")
                    website_email_cache[website_url] = ""

    reusable_by_company: dict[str, str] = {}
    for item in items:
        company_key = _clean_text(item.get("company", "")).casefold()
        if company_key and item.get("email"):
            reusable_by_company[company_key] = item["email"]

    for item in items:
        if item.get("email"):
            continue
        website_url = item.get("website", "")
        website_email = website_email_cache.get(website_url, "")
        if website_email:
            item["email"] = website_email
            continue

        company_key = _clean_text(item.get("company", "")).casefold()
        if company_key and reusable_by_company.get(company_key):
            item["email"] = reusable_by_company[company_key]

    return items


def scrape_gelbeseiten(
    query: str = DEFAULT_QUERY,
    location: str = DEFAULT_LOCATION,
    max_pages: int = 1,
    company_limit: int | None = None,
) -> list[dict]:
    search_url = _build_search_url(query, location)
    headless = os.getenv("HEADLESS", "true") == "true"
    results: list[dict] = []
    seen_urls: set[str] = set()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(locale="de-DE", user_agent=USER_AGENT)
        request_context = playwright.request.new_context(
            base_url=BASE_URL,
            extra_http_headers={
                "Accept-Language": "de-DE,de;q=0.9",
                "User-Agent": USER_AGENT,
            },
        )
        page = context.new_page()

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            _wait_for_results(page)

            for page_number in range(1, max_pages + 1):
                cards = page.locator(RESULT_CARD_SELECTOR)
                total_cards = cards.count()
                print(f"[gelbeseiten] Processing page batch {page_number} with {total_cards} visible cards")

                for index in range(total_cards):
                    item = _extract_card(cards.nth(index), page)
                    if not item:
                        continue

                    detail_url = item["detail_url"]
                    if detail_url in seen_urls:
                        continue

                    detail_fields = _extract_detail_fields(request_context, detail_url)
                    item = _merge_detail_fields(item, detail_fields, request_context)

                    results.append(item)
                    seen_urls.add(detail_url)

                    if company_limit and len(results) >= company_limit:
                        return results

                if page_number >= max_pages:
                    break

                if not _load_more_results(page, total_cards, page_number):
                    break
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"GelbeSeiten scraping timed out for {search_url}") from exc
        finally:
            request_context.dispose()
            context.close()
            browser.close()

    return results
