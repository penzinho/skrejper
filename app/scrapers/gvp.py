"""Scraper for the GVP member directory
(https://personaldienstleister.de/der-gvp/mitglieder/gvp-mitglieder/).

The GVP (Gesamtverband der Personaldienstleister) lists every member agency —
head offices *and* branches, ~8,700 entries — on one WordPress page. The page
itself only renders the first ten; the rest come from the theme's own REST
route, which the "Mehr laden" button and the filter form both call:

    POST https://personaldienstleister.de/wp-json/memberlist/v1/filter/
         search=&zip=&city=&area=&quality=&listtype=mitglieder&branchswitch=false&page=N

The response is JSON with ``data`` (an HTML fragment of ten accordion
entries), ``current_count`` and ``total_count``. There is no browser and no
API key involved: the route answers anonymous requests, and the parameters
have to travel in the POST body — the same names in the query string are
ignored and page 1 comes back every time. That is also why the loop watches
for a page that adds nothing new and stops, instead of walking 900 identical
pages.

Unlike the job boards, this is a *directory*: most entries carry no e-mail,
only a website, phone and postal address. Every member is returned (the caller
decides what to do with the ones without an address), and an optional
enrichment step looks for an address on the member's own website, where German
companies are legally obliged to publish one (the Impressum).
"""

import html
import json
import os
import re
import ssl
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable

PAGE_URL = "https://personaldienstleister.de/der-gvp/mitglieder/gvp-mitglieder/"
FILTER_URL = "https://personaldienstleister.de/wp-json/memberlist/v1/filter/"
LIST_TYPE = "mitglieder"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

# The filter form's own dropdown values, verbatim — they are matched server-side.
BUSINESS_AREAS = (
    "Contracting / Freelancer",
    "Personalentwicklung / Upskilling",
    "Personalvermittlung",
    "Zeitarbeit",
)
QUALITY_STANDARDS = (
    "QS Ausbildung",
    "QS Personalentwicklung",
    "QS Personalvermittlung",
    "QS Pflege",
    "QS internationale Mobilität",
    "QS pädagogischer Bereich",
)

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
MAILTO_RE = re.compile(r"mailto:([^?\"'#<>\s]+)", re.IGNORECASE)
INVALID_EMAIL_DOMAIN_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".css", ".js", ".woff", ".woff2",
)
# Addresses that show up on every third website and are never the company's.
EXCLUDED_EMAIL_DOMAINS = {
    "example.com", "example.de", "domain.de", "email.de", "sentry.io", "wixpress.com",
    "sentry-next.wixpress.com", "personaldienstleister.de", "gvp.de",
}
EXCLUDED_EMAIL_LOCAL_PARTS = {"noreply", "no-reply", "donotreply", "do-not-reply"}
# Which mailbox to pick when a site publishes several: the general ones first.
PREFERRED_LOCAL_PARTS = (
    "info", "kontakt", "contact", "office", "mail", "post", "zentrale", "bewerbung",
    "jobs", "personal", "karriere", "hallo", "hello", "welcome",
)

# Pages where the address lives, tried in this order on top of the homepage.
CONTACT_PATHS = (
    "/impressum", "/impressum/", "/impressum.html", "/impressum.php",
    "/kontakt", "/kontakt/", "/kontakt.html", "/kontakt.php",
    "/imprint", "/contact",
)
CONTACT_LINK_WORDS = ("impressum", "imprint", "kontakt", "contact")
HREF_RE = re.compile(r"""<a\b[^>]*?href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>""", re.IGNORECASE | re.DOTALL)

# Impressum pages routinely spell the address out to dodge harvesters.
OBFUSCATED_AT_RE = re.compile(r"\s*[\(\[\{]\s*(?:at|ät|@)\s*[\)\]\}]\s*", re.IGNORECASE)
OBFUSCATED_DOT_RE = re.compile(r"\s*[\(\[\{]\s*(?:dot|punkt)\s*[\)\]\}]\s*", re.IGNORECASE)
# Cloudflare's "email protection": the address is XOR-ed into a hex string,
# either in a data-cfemail attribute or after /cdn-cgi/l/email-protection#.
CFEMAIL_RE = re.compile(r"""(?:data-cfemail=["']|/cdn-cgi/l/email-protection#)([0-9a-f]{8,})""", re.IGNORECASE)

MAX_SITE_BYTES = 600_000

ENTRY_SPLIT_RE = re.compile(r'<div class="accordion__entry memberlist__entry"')
TITLE_RE = re.compile(
    r'<div class="accordion__title memberlist__title">\s*<h4>(.*?)</h4>', re.DOTALL
)
BUSINESS_RE = re.compile(r"Geschäftsfeld:\s*<span>(.*?)</span>", re.DOTALL)
ELEMENT_RE = re.compile(
    r'<div class="memberlist__element ([\w\-]+)">(.*?)</div>', re.DOTALL
)
ENTRY_VALUE_RE = re.compile(r'<span class="entry">(.*?)</span>', re.DOTALL)
TITLE_PARTS_RE = re.compile(r"^(?P<company>.+?)\s+-\s+(?P<city>[^()]+?)\s*\((?P<plz>[^)]*)\)\s*$")
TAG_RE = re.compile(r"<[^>]+>")

# Which markup class holds which field of ours.
ELEMENT_FIELDS = {
    "geschaeftsfuehrer": "managing_director",
    "telefon": "phone",
    "email": "email",
    "website": "website",
    "strasse": "street",
    "plz": "plz",
    "stadt": "city",
    "land": "country",
}


class GvpBlockedError(RuntimeError):
    """The site's firewall (Sucuri) refused us — a network, not a code, problem."""


class GvpUnavailableError(RuntimeError):
    """The directory stopped answering mid-walk; what was collected is kept."""


# Pacing. The site rate-limits: ~20 quick requests in a row earned an HTTP 429
# on a real run. So pages are spaced out by default, a 429 is waited out (the
# server's Retry-After when it sends one, else a growing pause), and every 429
# also widens the spacing for the rest of the run.
DEFAULT_PAGE_DELAY_MS = 1500
RATE_LIMIT_ATTEMPTS = 6
RATE_LIMIT_MAX_WAIT_S = 300


def _log(message: str) -> None:
    print(message, flush=True)


def _clean_text(value: str) -> str:
    text = html.unescape(TAG_RE.sub(" ", value or ""))
    return re.sub(r"\s+", " ", text).strip()


def normalize_member_key(value: str) -> str:
    """Stable id for a directory entry: the title, casefolded and de-accented.

    The directory has no ids of its own. The title ("Firma GmbH - Stadt (PLZ)")
    is unique per entry — branches of one company differ by city — and does not
    change between runs, so it is what ``app.seen_store`` remembers.
    """
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(c for c in normalized if not unicodedata.combining(c))
    return " ".join(normalized.casefold().split())


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


def _split_title(title: str, city: str, plz: str) -> str:
    """Company name out of "Firma GmbH - Stadt (PLZ)".

    The city and PLZ are also separate fields, so when they are known the
    suffix is peeled off exactly — company names with " - " in them stay whole.
    """
    title = _clean_text(title)
    if city:
        suffix = f" - {city} ({plz})"
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
    match = TITLE_PARTS_RE.match(title)
    if match:
        return match.group("company").strip()
    return title


def parse_members(fragment: str) -> list[dict]:
    """Turn the ``data`` HTML fragment (or the full page) into member dicts."""
    members: list[dict] = []
    for block in ENTRY_SPLIT_RE.split(fragment or "")[1:]:
        title_match = TITLE_RE.search(block)
        if not title_match:
            continue
        title = _clean_text(title_match.group(1))

        fields = {name: "" for name in ELEMENT_FIELDS.values()}
        website_text = ""
        for element_class, inner in ELEMENT_RE.findall(block):
            field = ELEMENT_FIELDS.get(element_class)
            if field is None:
                continue
            value_match = ENTRY_VALUE_RE.search(inner)
            raw = value_match.group(1) if value_match else inner
            if field == "website":
                href = re.search(r'href="([^"]+)"', raw)
                website_text = _clean_text(raw)
                fields[field] = html.unescape(href.group(1)).strip() if href else ""
            else:
                fields[field] = _clean_text(raw)

        if not fields["website"] and website_text:
            fields["website"] = website_text
        fields["website"] = _normalize_website(fields["website"])
        fields["email"] = _first_valid_email(fields["email"])

        business_match = BUSINESS_RE.search(block)
        members.append(
            {
                "member": title,
                "company": _split_title(title, fields["city"], fields["plz"]),
                "business_fields": _clean_text(business_match.group(1)) if business_match else "",
                **fields,
                "email_source": "gvp" if fields["email"] else "",
                "source": "gvp",
            }
        )
    return members


def _normalize_website(value: str) -> str:
    value = _clean_text(value)
    if not value:
        return ""
    if not re.match(r"^https?://", value, re.IGNORECASE):
        value = "https://" + value
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return value


# --------------------------------------------------------------------------
# E-mail extraction (directory field and websites alike)
# --------------------------------------------------------------------------


def _decode_cfemail(encoded: str) -> str:
    try:
        raw = bytes.fromhex(encoded)
    except ValueError:
        return ""
    key = raw[0]
    return bytes(b ^ key for b in raw[1:]).decode("utf-8", errors="replace")


def _deobfuscate(text: str) -> str:
    text = CFEMAIL_RE.sub(lambda m: " " + _decode_cfemail(m.group(1)) + " ", text)
    text = OBFUSCATED_AT_RE.sub("@", text)
    return OBFUSCATED_DOT_RE.sub(".", text)


def _is_valid_email(value: str) -> bool:
    if not value or value.count("@") != 1:
        return False
    local_part, domain = value.split("@", 1)
    if not local_part or "." not in domain or ".." in domain:
        return False
    if domain.endswith(INVALID_EMAIL_DOMAIN_SUFFIXES):
        return False
    if domain in EXCLUDED_EMAIL_DOMAINS or local_part in EXCLUDED_EMAIL_LOCAL_PARTS:
        return False
    # Version strings and CSS class soup ("foo@2x.png", "a@b.c") are not addresses.
    return not domain.startswith("2x.") and len(domain.rsplit(".", 1)[-1]) >= 2


def _normalize_candidate(value: str) -> str:
    return html.unescape(value or "").strip(" <>\"'(),;:").casefold()


def _first_valid_email(text: str) -> str:
    for match in EMAIL_RE.findall(_deobfuscate(html.unescape(text or ""))):
        candidate = _normalize_candidate(match)
        if _is_valid_email(candidate):
            return candidate
    return ""


def _site_domain(url: str) -> str:
    host = urllib.parse.urlparse(url).netloc.casefold().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def _rank_email(email: str, site_domain: str) -> tuple[int, int]:
    local_part, domain = email.split("@", 1)
    same_site = 0 if site_domain and (domain == site_domain or domain.endswith("." + site_domain)) else 1
    try:
        preference = PREFERRED_LOCAL_PARTS.index(local_part)
    except ValueError:
        preference = len(PREFERRED_LOCAL_PARTS)
    return same_site, preference


def extract_emails(page_html: str, site_url: str = "") -> list[str]:
    """Every plausible address on a page, best first.

    ``mailto:`` links come before addresses found in the text, addresses on the
    site's own domain before third-party ones, and a general mailbox (info@,
    kontakt@) before a personal one.
    """
    unescaped = html.unescape(page_html or "")
    text = _deobfuscate(unescaped)
    ordered: list[str] = []
    seen: set[str] = set()

    for source in (MAILTO_RE.findall(unescaped), EMAIL_RE.findall(text)):
        for match in source:
            candidate = _normalize_candidate(urllib.parse.unquote(match))
            if candidate in seen or not _is_valid_email(candidate):
                continue
            seen.add(candidate)
            ordered.append(candidate)

    site_domain = _site_domain(site_url)
    # Stable sort: mailto-first order survives among equal ranks.
    return sorted(ordered, key=lambda e: _rank_email(e, site_domain))


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


def _decode(body: bytes, content_type: str) -> str:
    match = re.search(r"charset=([\w\-]+)", content_type or "", re.IGNORECASE)
    for encoding in ((match.group(1),) if match else ()) + ("utf-8", "latin-1"):
        try:
            return body.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    return body.decode("utf-8", errors="replace")


def _fetch_text(url: str, timeout: int, data: bytes | None = None, headers: dict | None = None) -> str:
    """GET (or POST when ``data`` is given) and return the body as text.

    Company websites are the long tail of the web: a fair share have expired or
    self-signed certificates. Those are retried without verification — nothing
    secret is sent, we only read a public page — and the fallback is logged.
    """
    request = urllib.request.Request(
        url,
        data=data,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(MAX_SITE_BYTES)
            return _decode(body, response.headers.get("Content-Type", ""))
    except urllib.error.URLError as exc:
        if not isinstance(exc.reason, ssl.SSLError):
            raise
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
        body = response.read(MAX_SITE_BYTES)
        return _decode(body, response.headers.get("Content-Type", ""))


def _fetch_nonce(timeout: int) -> str:
    """The REST nonce the page embeds for its own JavaScript.

    The route answers without it, so this is belt and braces: sent when we have
    it, silently skipped when the page itself cannot be fetched.
    """
    try:
        page = _fetch_text(PAGE_URL, timeout)
    except Exception as exc:
        _log(f"[gvp] Could not read the members page for a nonce ({exc}); continuing without.")
        return ""
    match = re.search(r"nonce:\s*'([0-9a-f]+)'", page)
    return match.group(1) if match else ""


def _looks_blocked(body: str) -> bool:
    lowered = (body or "").casefold()
    return "sucuri" in lowered and "access denied" in lowered


def _retry_after_seconds(exc: urllib.error.HTTPError, attempt: int) -> float:
    """How long to wait after a 429: the server's word, else 15 s doubling."""
    header = ""
    try:
        header = exc.headers.get("Retry-After", "") if exc.headers else ""
    except Exception:
        pass
    try:
        wait = float(header)
    except (TypeError, ValueError):
        wait = 15.0 * (2 ** (attempt - 1))
    return max(1.0, min(wait, RATE_LIMIT_MAX_WAIT_S))


def _filter_page(
    page: int,
    filters: dict,
    nonce: str,
    timeout: int,
    attempts: int = 3,
    throttle: dict | None = None,
) -> dict | None:
    """One page of the directory via the theme's REST route.

    Returns the decoded payload (``data``, ``current_count``, ``total_count``)
    or None when the site never answered. A firewall block is raised, not
    returned: retrying it only digs the hole deeper. An HTTP 429 is waited out
    up to ``RATE_LIMIT_ATTEMPTS`` times and counted in ``throttle["hits"]`` so
    the caller can slow down for the rest of the run.
    """
    form = {
        "search": filters.get("search") or "",
        "zip": filters.get("zip") or "",
        "city": filters.get("city") or "",
        "area": filters.get("area") or "",
        "quality": filters.get("quality") or "",
        "listtype": LIST_TYPE,
        "branchswitch": "true" if filters.get("branches") else "false",
        "page": str(page),
    }
    data = urllib.parse.urlencode(form).encode("utf-8")
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://personaldienstleister.de",
        "Referer": PAGE_URL,
    }
    if nonce:
        headers["X-WP-Nonce"] = nonce

    last_error: Exception | None = None
    attempt = 0
    rate_limited = 0
    while True:
        attempt += 1
        try:
            body = _fetch_text(FILTER_URL, timeout, data=data, headers=headers)
            return _decode_payload(body)
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            if exc.code == 403 and _looks_blocked(body):
                raise GvpBlockedError(
                    "personaldienstleister.de je odbio zahtjev (Sucuri firewall, HTTP 403). "
                    "Ova IP adresa je na njihovoj crnoj listi — pokušaj s druge mreže."
                ) from exc
            if exc.code == 429:
                rate_limited += 1
                if throttle is not None:
                    throttle["hits"] = throttle.get("hits", 0) + 1
                if rate_limited >= RATE_LIMIT_ATTEMPTS:
                    _log(f"[gvp] Giving up on page {page}: HTTP 429 after {rate_limited} waits.")
                    return None
                wait = _retry_after_seconds(exc, rate_limited)
                _log(f"[gvp] HTTP 429 (zu viele Anfragen) auf Seite {page} — warte {wait:.0f}s.")
                time.sleep(wait)
                continue
            if exc.code in (400, 401, 403, 404):
                _log(f"[gvp] HTTP {exc.code} for page {page}: {body[:200]}")
                return None
            last_error = exc
        except Exception as exc:  # network / timeout / decode
            last_error = exc
        if attempt >= attempts:
            break
        time.sleep(2 * attempt)
    _log(f"[gvp] Giving up on page {page}: {last_error}")
    return None


def _decode_payload(body: str) -> dict:
    """The route returns JSON; a PHP string return would be JSON-encoded twice."""
    payload = json.loads(body)
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError(f"unexpected response shape: {type(payload).__name__}")
    return payload


# --------------------------------------------------------------------------
# Website enrichment
# --------------------------------------------------------------------------


def _candidate_pages(site_url: str, homepage_html: str) -> list[str]:
    """Where to look for an address: linked Impressum/Kontakt pages first, then
    the usual paths blind, in case the homepage hides its footer behind JS."""
    urls: list[str] = []
    seen: set[str] = set()

    def push(url: str) -> None:
        url = url.split("#", 1)[0]
        if url and url not in seen and urllib.parse.urlparse(url).scheme in {"http", "https"}:
            seen.add(url)
            urls.append(url)

    site_host = urllib.parse.urlparse(site_url).netloc.casefold()
    for href, text in HREF_RE.findall(homepage_html or ""):
        href = html.unescape(href).strip()
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        haystack = f"{href} {_clean_text(text)}".casefold()
        if any(word in haystack for word in CONTACT_LINK_WORDS):
            absolute = urllib.parse.urljoin(site_url, href)
            if urllib.parse.urlparse(absolute).netloc.casefold() == site_host:
                push(absolute)

    for path in CONTACT_PATHS:
        push(urllib.parse.urljoin(site_url, path))
    return urls


def find_email_on_website(
    site_url: str,
    timeout: int = 12,
    max_pages: int = 4,
    fetch: Callable[[str, int], str] | None = None,
) -> str:
    """Best-effort: the company's own address from its homepage or Impressum.

    ``max_pages`` bounds the requests per site (homepage included). Returns ""
    when nothing plausible turns up; never raises — a dead website is a normal
    outcome here, not an error.
    """
    fetch = fetch or _fetch_text
    site_url = _normalize_website(site_url)
    if not site_url:
        return ""

    budget = max(1, int(max_pages))
    homepage = ""
    for candidate in _homepage_variants(site_url):
        if budget <= 0:
            return ""
        budget -= 1
        try:
            homepage = fetch(candidate, timeout)
        except Exception:
            continue
        site_url = candidate
        break

    if homepage:
        found = extract_emails(homepage, site_url)
        if found:
            return found[0]

    for page_url in _candidate_pages(site_url, homepage):
        if budget <= 0:
            break
        budget -= 1
        try:
            page = fetch(page_url, timeout)
        except Exception:
            continue
        found = extract_emails(page, site_url)
        if found:
            return found[0]
    return ""


def _homepage_variants(site_url: str) -> list[str]:
    """The URL as listed, then with/without ``www.`` — DNS for the bare domain is
    a coin toss on small-company hosting."""
    parsed = urllib.parse.urlparse(site_url)
    host = parsed.netloc
    alternative = host[4:] if host.startswith("www.") else "www." + host
    return [site_url, urllib.parse.urlunparse(parsed._replace(netloc=alternative))]


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------


def scrape_gvp(
    search: str | None = None,
    city: str | None = None,
    zip_code: str | None = None,
    business_area: str | None = None,
    quality: str | None = None,
    branches: bool = False,
    max_pages: int | None = None,
    member_limit: int | None = None,
    enrich: bool = False,
    enrich_pages: int = 4,
    skip_enrich_ids: set[str] | None = None,
    on_member: Callable[[dict], None] | None = None,
    skip_ids: set[str] | None = None,
) -> list[dict]:
    """Walk the GVP member directory and return one dict per entry.

    * ``search`` / ``city`` / ``zip_code`` / ``business_area`` / ``quality`` map
      onto the page's own filter form; empty means "everything".
    * ``branches`` flips the page's "Hauptstelle / Niederlassung" switch.
    * ``max_pages`` caps the directory pages walked (ten entries each);
      ``member_limit`` stops after that many entries returned.
    * ``enrich`` looks up the company website (at most ``enrich_pages``
      requests per site) for entries the directory lists without an e-mail.
      ``skip_enrich_ids`` are entries that were already looked up in vain.
    * ``skip_ids`` are entry keys (see ``normalize_member_key``) already
      exported on an earlier run; they are not returned again.

    Every entry is returned, with or without an e-mail. ``email_source`` says
    where an address came from ("gvp" or "website"); ``enrich_tried`` marks the
    entries whose website was searched without success.
    """
    skip_ids = skip_ids or set()
    skip_enrich_ids = skip_enrich_ids or set()
    timeout = int(os.getenv("GVP_TIMEOUT", "60"))
    site_timeout = int(os.getenv("GVP_SITE_TIMEOUT", "12"))
    page_delay_ms = int(os.getenv("GVP_PAGE_DELAY_MS", str(DEFAULT_PAGE_DELAY_MS)))
    site_delay_ms = int(os.getenv("GVP_SITE_DELAY_MS", "200"))

    filters = {
        "search": search,
        "city": city,
        "zip": zip_code,
        "area": business_area,
        "quality": quality,
        "branches": branches,
    }
    nonce = _fetch_nonce(timeout)

    members: list[dict] = []
    seen_keys: set[str] = set()
    total: int | None = None
    page_size: int | None = None  # the site decides (ten); learned from page 1
    page = 1
    with_email = 0
    throttle = {"hits": 0}

    while max_pages is None or page <= max_pages:
        payload = _filter_page(page, filters, nonce, timeout, throttle=throttle)
        if payload is None:
            # Stopping quietly would report a partial directory as "done".
            walked = f"{len(seen_keys)}" + (f" od {total}" if total else "")
            raise GvpUnavailableError(
                f"personaldienstleister.de je prestao odgovarati na stranici {page}. "
                f"Prikupljeno je {walked} unosa i spremljeno. Pokreni ponovno za koju "
                "minutu — uz „Preskoči firme koje sam već skrejpao” već izvezene firme se "
                "ne ponavljaju."
            )

        parsed = parse_members(payload.get("data") or "")
        if total is None:
            total = _as_int(payload.get("total_count"))
            page_size = len(parsed) or None
            _log(f"[gvp] {total if total is not None else '?'} Einträge im Verzeichnis")

        fresh = [m for m in parsed if normalize_member_key(m["member"]) not in seen_keys]
        if not fresh:
            if parsed:
                # The route ignored our page parameter and served page 1 again.
                _log(f"[gvp] Page {page} repeats an earlier page — the site is not paging; stopping.")
            break

        for member in fresh:
            key = normalize_member_key(member["member"])
            seen_keys.add(key)
            if key in skip_ids:
                continue

            if enrich and not member["email"] and member["website"] and key not in skip_enrich_ids:
                found = find_email_on_website(member["website"], site_timeout, enrich_pages)
                if found:
                    member["email"] = found
                    member["email_source"] = "website"
                else:
                    member["enrich_tried"] = True
                if site_delay_ms:
                    time.sleep(site_delay_ms / 1000)

            if member["email"]:
                with_email += 1
            members.append(member)
            if on_member is not None:
                try:
                    on_member(member)
                except Exception as exc:
                    _log(f"[gvp] on_member callback failed: {exc}")

            if member_limit is not None and len(members) >= member_limit:
                _log(f"[gvp] Reached member_limit={member_limit}.")
                return members

        total_pages = f"/{-(-total // page_size)}" if total and page_size else ""
        _log(f"[gvp] Seite {page}{total_pages}: {len(seen_keys)} Einträge, {with_email} mit E-Mail")

        if total is not None and len(seen_keys) >= total:
            break
        if page_size and len(parsed) < page_size:
            break  # a short page is the last one
        page += 1
        # Each 429 so far doubles the spacing (up to 8×): the server has said
        # what pace it tolerates, and a second 429 costs far more than the wait.
        delay_ms = page_delay_ms * (2 ** min(throttle["hits"], 3))
        if delay_ms:
            time.sleep(delay_ms / 1000)

    return members


def _as_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
