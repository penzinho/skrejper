import html
import json
import re
import time
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urljoin, urlparse
from urllib.request import Request, urlopen


BASE_URL = "https://www.omh.hr"
HOTEL_REST_URL = f"{BASE_URL}/wp-json/wp/v2/hotel"
CITY_REST_URL = f"{BASE_URL}/wp-json/wp/v2/city"
REGULAR_HOTELS_URL = f"{BASE_URL}/hoteli/"
ASSOCIATED_HOTELS_URL = f"{BASE_URL}/pridruzene-clanice/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SECONDS = 20
GENERIC_EMAILS = {"info@omh.hr"}
EXCLUDED_WEBSITE_HOST_PARTS = (
    "omh.hr",
    "omh.belgrade.dev",
    "google.",
    "schema.org",
    "instagram.",
    "facebook.",
    "twitter.",
    "x.com",
    "cookiedatabase.org",
)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
JET_PROPS_RE = re.compile(r'"props"\s*:\s*(\{.*?\})\s*,\s*"extra_props"', re.DOTALL)


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def _fetch_url(url: str, *, timeout: int = DEFAULT_TIMEOUT_SECONDS, retries: int = 2) -> tuple[str, dict[str, str]]:
    last_exc: Exception | None = None

    for _ in range(max(retries, 0) + 1):
        request = Request(
            url,
            headers={
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "hr-HR,hr;q=0.9,en;q=0.8",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
                headers = {key.lower(): value for key, value in response.headers.items()}
                return body, headers
        except Exception as exc:
            last_exc = exc
            time.sleep(0.5)

    raise RuntimeError(f"Failed to fetch {url}: {last_exc}") from last_exc


def _fetch_json(url: str) -> tuple[object, dict[str, str]]:
    body, headers = _fetch_url(url)
    return json.loads(body), headers


def _fetch_rest_pages(url: str) -> list[dict]:
    items: list[dict] = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        separator = "&" if "?" in url else "?"
        data, headers = _fetch_json(f"{url}{separator}per_page=100&page={page}")
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected REST response for {url}: {type(data).__name__}")

        items.extend(item for item in data if isinstance(item, dict))
        total_pages = int(headers.get("x-wp-totalpages") or total_pages)
        page += 1

    return items


def fetch_city_names() -> dict[int, str]:
    terms = _fetch_rest_pages(CITY_REST_URL)
    cities: dict[int, str] = {}
    for term in terms:
        term_id = term.get("id")
        name = _clean_text(term.get("name") or "")
        if isinstance(term_id, int) and name:
            cities[term_id] = name
    return cities


def fetch_hotel_posts(*, include_associated: bool = False) -> list[dict]:
    fields = "id,link,title,city,region"
    posts = [
        post
        for post in _fetch_rest_pages(f"{HOTEL_REST_URL}?_fields={fields}&orderby=date&order=desc")
        if _post_title(post)
    ]

    if include_associated:
        return posts

    return [post for post in posts if post.get("region")]


def fetch_listing_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for key, url in {"regular": REGULAR_HOTELS_URL, "associated": ASSOCIATED_HOTELS_URL}.items():
        body, _ = _fetch_url(url)
        counts[key] = _extract_listing_count(body)
    return counts


def _extract_listing_count(body: str) -> int:
    match = JET_PROPS_RE.search(body or "")
    if not match:
        return 0

    try:
        props = json.loads(match.group(1))
    except json.JSONDecodeError:
        return 0

    default_props = props.get("epro-loop-builder", {}).get("default", {})
    total = default_props.get("found_posts")
    return int(total) if isinstance(total, int) else 0


class _ContactListParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[dict[str, object]] = []
        self._current_item: dict[str, object] | None = None
        self._span_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        classes = set(attr_map.get("class", "").split())

        if tag == "li" and "elementor-icon-list-item" in classes:
            self._current_item = {"texts": [], "hrefs": []}
        elif self._current_item is not None and tag == "a":
            href = attr_map.get("href")
            if href:
                self._current_item["hrefs"].append(href)
        elif self._current_item is not None and tag == "span" and "elementor-icon-list-text" in classes:
            self._span_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if self._current_item is not None and tag == "span" and self._span_depth:
            self._span_depth -= 1
        elif tag == "li" and self._current_item is not None:
            texts = [_clean_text(text) for text in self._current_item["texts"] if _clean_text(text)]
            hrefs = [str(href) for href in self._current_item["hrefs"]]
            self.items.append({"text": _clean_text(" ".join(texts)), "hrefs": hrefs})
            self._current_item = None
            self._span_depth = 0

    def handle_data(self, data: str) -> None:
        if self._current_item is not None and self._span_depth:
            self._current_item["texts"].append(data)


def _email_from_href(href: str) -> str:
    parsed = urlparse(href)
    if parsed.scheme != "mailto":
        return ""

    raw_value = unquote(parsed.path).strip().lower()
    if not raw_value and parsed.query:
        raw_value = parse_qs(parsed.query).get("to", [""])[0].strip().lower()

    emails: list[str] = []
    for email in re.split(r"[,;]", raw_value):
        email = email.strip()
        if email in GENERIC_EMAILS or not EMAIL_RE.match(email):
            continue
        emails.append(email)

    return ", ".join(emails)


def _phone_from_href(href: str, text: str) -> str:
    if not href.startswith("tel:"):
        return ""

    phone = _clean_text(text) or _clean_text(unquote(href.removeprefix("tel:")))
    return phone.strip()


def _is_hotel_website(href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False

    host = parsed.netloc.lower().removeprefix("www.")
    return not any(part in host for part in EXCLUDED_WEBSITE_HOST_PARTS)


def _looks_like_address(text: str) -> bool:
    normalized = text.casefold()
    if "@" in text or normalized.startswith(("email", "submit", "newsletter")):
        return False
    return "," in text or bool(re.search(r"\d", text))


def _city_from_address(address: str) -> str:
    if "," not in address:
        return ""
    return _clean_text(address.rsplit(",", 1)[1])


def extract_contact_fields_from_html(body: str) -> dict[str, str]:
    parser = _ContactListParser()
    parser.feed(body or "")

    fields = {"address": "", "phone": "", "email": "", "website": ""}
    pending_address = ""

    for item in parser.items:
        text = _clean_text(str(item.get("text") or ""))
        hrefs = [str(href) for href in item.get("hrefs") or []]

        if not hrefs and text and _looks_like_address(text) and not fields["phone"] and not fields["email"]:
            pending_address = text
            continue

        for href in hrefs:
            email_value = _email_from_href(href)
            if email_value and not fields["email"]:
                fields["email"] = email_value
                if pending_address and not fields["address"]:
                    fields["address"] = pending_address

            phone_value = _phone_from_href(href, text)
            if phone_value and not fields["phone"]:
                fields["phone"] = phone_value
                if pending_address and not fields["address"]:
                    fields["address"] = pending_address

            if _is_hotel_website(href) and not fields["website"]:
                fields["website"] = href.rstrip("/")
                if pending_address and not fields["address"]:
                    fields["address"] = pending_address

        if fields["phone"] and fields["email"] and fields["website"]:
            break

    return fields


def _post_title(post: dict) -> str:
    title = post.get("title") or {}
    if isinstance(title, dict):
        return _clean_text(title.get("rendered") or "")
    return _clean_text(str(title))


def _post_city(post: dict, city_names: dict[int, str], address: str) -> str:
    city_ids = post.get("city") or []
    if isinstance(city_ids, list):
        for city_id in city_ids:
            city = city_names.get(city_id)
            if city:
                return city
    return _city_from_address(address)


def scrape_omh_hotels(
    *,
    include_associated: bool = False,
    limit: int | None = None,
    request_delay: float = 0.0,
) -> list[dict[str, str]]:
    city_names = fetch_city_names()
    posts = fetch_hotel_posts(include_associated=include_associated)
    if limit is not None:
        posts = posts[:limit]

    hotels: list[dict[str, str]] = []
    seen_links: set[str] = set()

    for index, post in enumerate(posts):
        detail_url = _clean_text(post.get("link") or "")
        if not detail_url or detail_url in seen_links:
            continue
        seen_links.add(detail_url)

        body, _ = _fetch_url(detail_url)
        fields = extract_contact_fields_from_html(body)
        hotels.append(
            {
                "hotel_name": _post_title(post),
                "address": fields["address"],
                "city": _post_city(post, city_names, fields["address"]),
                "email": fields["email"],
                "phone_number": fields["phone"],
                "website": fields["website"],
                "detail_url": detail_url,
            }
        )

        if request_delay and index < len(posts) - 1:
            time.sleep(request_delay)

    return hotels
