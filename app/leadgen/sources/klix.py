"""Klix Posao (posao.klix.ba) — BiH, the primary source.

Two crawls, employer first:

* **Employer directory** (``/poslodavci``, ~3k firms). Each employer page
  carries the full legal name, JIB, PDV number, address, website and the
  *complete* ad history (active + expired, with dates) — everything the
  frequency scoring needs, in one page. The initial run sweeps the whole
  directory; after that an employer page is refetched only when a new ad of
  theirs shows up (or the cached copy ages out).
* **Ads listing** (``/oglasi?page=N``, newest first). Incremental runs walk it
  only until a page brings nothing new. New ads from unknown employers pull in
  that employer's page.

Ad *detail* pages add little the employer page doesn't already have, so they
are fetched only with ``fetch_details=True`` (fills description + contact).

Parsing is label-based (see ``app.leadgen.htmlparse``): entity links are found
by URL shape (``/poslodavci/{slug}/{id}``, ``/oglasi/{slug}/{id}``), fields by
their visible labels, so cosmetic redesigns don't break the adapter.
"""

import re

from app.leadgen import htmlparse as hp
from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text, extract_email, extract_phone, norm_text, parse_date
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting, now_iso

SOURCE = "klix"
LABEL = "Klix Posao (BiH)"
COUNTRY = "BA"
DEFAULT_ENABLED = True

BASE = "https://posao.klix.ba"
EMPLOYER_URL_RE = re.compile(r"/poslodavci/([^/?#]+)/(\d+)(?:[/?#]|$)")
AD_URL_RE = re.compile(r"/oglasi/([^/?#]+)/(\d+)(?:[/?#]|$)")
CITY_URL_RE = re.compile(r"/grad/([^/?#]+)")

# Hosts that are never the employer's own website (the site itself, its CDN,
# social profiles).
_NON_WEBSITE_HOSTS = (
    "klix.ba", "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "youtube.com", "tiktok.com", "google.com", "goo.gl", "wa.me",
    "whatsapp.com", "viber.com",
)

# How fresh a listing page must be (seconds); employer pages refresh slower.
LISTING_MAX_AGE_S = 30 * 60
EMPLOYER_MAX_AGE_S = 7 * 24 * 3600

_EXPIRY_HINTS = ("istek", "istic", "istič", "vrijedi do", "vazi do", "važi do", "rok")
_PUBLISH_HINTS = ("objavljen", "datum objave", "postavljen")


class _Stopped(Exception):
    pass


# --------------------------------------------------------------------------
# Parsers (HTML string in, dicts out — fixture-testable, no network)
# --------------------------------------------------------------------------


def parse_entity_links(html: str, pattern: re.Pattern, base_url: str = BASE) -> list[dict]:
    """Links to employers/ads on a listing page: slug, id, url, anchor text."""
    root = hp.soup(html)
    out = []
    for match, url, anchor in hp.links_matching(root, pattern, base_url):
        out.append({
            "slug": match.group(1),
            "id": match.group(2),
            "url": url,
            "text": clean_text(anchor.get_text(" ")),
            "anchor": anchor,
        })
    return out


def _classify_dates(text: str) -> tuple[str, str]:
    """(published_at, expires_at) from a card/row's text, by label hints."""
    key = norm_text(text)
    dates = hp.dates_in(text)
    if not dates:
        return "", ""
    has_expiry = any(hint in key for hint in _EXPIRY_HINTS)
    has_publish = any(hint in key for hint in _PUBLISH_HINTS)
    if has_expiry and has_publish and len(dates) >= 2:
        return dates[0], dates[-1]
    if has_expiry:
        return "", dates[-1]
    return dates[0], (dates[-1] if len(dates) >= 2 else "")


def _icon_values(root, icon_class_fragment: str) -> list[str]:
    """Texts next to a FontAwesome icon — how this site marks the address, the
    city and the website in the company-info sidebar (no text labels there).
    History cards carry the same icons, so parents containing an ad or city
    link are skipped."""
    values = []
    for icon in root.find_all("i"):
        classes = " ".join(icon.get("class") or ())
        if icon_class_fragment not in classes:
            continue
        parent = icon.parent
        if parent is None or parent.find("a", href=AD_URL_RE) or parent.find("a", href=CITY_URL_RE):
            continue
        value = clean_text(parent.get_text(" "))
        # "Poslovni XP" is the site's gamification badge, drawn with the same
        # map-marker icon as the city.
        if value and len(value) <= 60 and norm_text(value) != "poslovni xp":
            values.append(value)
    return values


def _icon_link(root, icon_class_fragment: str) -> str:
    """An outbound link next to a FontAwesome icon (the globe = website)."""
    for icon in root.find_all("i"):
        classes = " ".join(icon.get("class") or ())
        if icon_class_fragment not in classes:
            continue
        parent = icon.parent
        anchor = parent.find("a", href=True) if parent is not None else None
        if anchor and anchor["href"].startswith(("http://", "https://")):
            return anchor["href"]
    return ""



def parse_employer_page(html: str, url: str) -> tuple[dict, list[dict]]:
    """One employer page -> (employer record, its full posting history)."""
    root = hp.soup(html)
    match = EMPLOYER_URL_RE.search(url)
    employer_id = match.group(2) if match else ""

    name = hp.page_title(root)
    legal_name = hp.labeled_value(root, ("puni naziv", "pravni naziv", "naziv firme", "naziv pravnog lica"))
    # The JIB is labelled just "ID:" on the live site; the older aliases stay
    # as fallbacks.
    tax_id = hp.labeled_value(root, ("id", "jib", "id broj", "identifikacijski broj", "identifikacioni broj"))
    vat_id = hp.labeled_value(root, ("pdv", "pdv broj"))
    # The sidebar marks the street and the city with the same map-marker icon,
    # in that order; the street is the entry carrying a house number / "bb".
    address = hp.labeled_value(root, ("adresa", "sjedište", "sjediste", "ulica"))
    city = hp.labeled_value(root, ("grad", "mjesto", "opština", "opcina"))
    marker_values = _icon_values(root, "map-marker")
    looks_like_street = lambda v: bool(re.search(r"\d|(?<![a-z])bb(?![a-z])|b\.b", v, re.IGNORECASE))
    if not address:
        address = next((v for v in marker_values if looks_like_street(v)), "")
    if not city:
        city = next((v for v in marker_values if not looks_like_street(v)), "")
    website = (
        _icon_link(root, "globe")
        or hp.labeled_link(root, ("web", "web stranica", "website", "webstranica"), url)
    )
    if not city and address and "," in address:
        city = re.sub(r"\b\d{5}\b", "", address.rsplit(",", 1)[1]).strip()

    employer = make_employer(
        source=SOURCE,
        source_id=employer_id,
        name=name,
        legal_name=legal_name,
        tax_id=tax_id,
        vat_id=vat_id,
        address=address,
        city=city,
        country=COUNTRY,
        website=website,
        email=extract_email(html),
        detail_url=url,
        public_sector_term=public_sector_term(name or legal_name),
    )

    postings = []
    seen_ids: set[str] = set()
    for match, ad_url, anchor in hp.links_matching(root, AD_URL_RE, url):
        # Each card links the ad twice (title + logo); keep the titled one.
        title = clean_text(anchor.get_text(" "))
        if not title or match.group(2) in seen_ids:
            continue
        seen_ids.add(match.group(2))
        card = hp.container_of(anchor)
        published, expires = _classify_dates(card.get_text(" "))
        city_link = card.find("a", href=CITY_URL_RE)
        ad_city = clean_text(city_link.get_text(" ")) if city_link else city
        postings.append(make_posting(
            source=SOURCE,
            source_id=match.group(2),
            employer_source=SOURCE,
            employer_source_id=employer_id,
            title=title,
            city="" if ad_city.casefold().startswith("više lokacija") else ad_city,
            published_at=published,
            expires_at=expires,
            detail_url=ad_url,
        ))
    return employer, postings


def parse_ad_cards(html: str, base_url: str = BASE) -> list[dict]:
    """Ads on a listing page, with whatever the card itself reveals."""
    cards = []
    for entry in parse_entity_links(html, AD_URL_RE, base_url):
        card = hp.container_of(entry["anchor"])
        card_text = card.get_text(" ")
        published, expires = _classify_dates(card_text)
        employer_link = next(
            iter(hp.links_matching(card, EMPLOYER_URL_RE, base_url)), None
        )
        cards.append({
            "id": entry["id"],
            "url": entry["url"],
            "title": entry["text"],
            "published_at": published,
            "expires_at": expires,
            "employer_id": employer_link[0].group(2) if employer_link else "",
            "employer_url": employer_link[1] if employer_link else "",
            "employer_name": clean_text(employer_link[2].get_text(" ")) if employer_link else "",
        })
    return cards


def parse_ad_page(html: str, url: str) -> dict:
    """One ad detail page -> posting record (employer link included if shown)."""
    root = hp.soup(html)
    match = AD_URL_RE.search(url)
    employer_link = next(iter(hp.links_matching(root, EMPLOYER_URL_RE, url)), None)
    text = root.get_text(" ")

    # The city as a /grad/ link (page header) beats the "Mjesto rada:" label —
    # the ad body often repeats that label with a looser value ("Hercegovina").
    city_link = root.find("a", href=CITY_URL_RE)
    city = clean_text(city_link.get_text(" ")) if city_link else ""

    posting = make_posting(
        source=SOURCE,
        source_id=match.group(2) if match else "",
        employer_source=SOURCE,
        employer_source_id=employer_link[0].group(2) if employer_link else "",
        title=hp.page_title(root),
        city=city or hp.labeled_value(root, ("mjesto rada", "lokacija", "grad", "mjesto")),
        published_at=parse_date(hp.labeled_value(root, ("objavljeno", "objavljen", "datum objave"))),
        expires_at=parse_date(hp.labeled_value(root, ("ističe", "istice", "vrijedi do", "datum isteka", "rok za prijavu"))),
        workers_count=hp.labeled_value(root, ("broj izvršilaca", "broj izvrsilaca", "broj radnika", "broj pozicija")),
        category=hp.labeled_value(root, ("kategorije", "kategorija")),
        description=clean_text(text)[:3000],
        contact_email=extract_email(html),
        contact_phone=extract_phone(hp.labeled_value(root, ("telefon", "kontakt telefon", "kontakt"))),
        detail_url=url,
    )
    # Side-channel for the crawler: where this ad's employer page lives. Not a
    # schema field — callers pop it before upserting.
    posting["employer_url"] = employer_link[1] if employer_link else ""
    return posting


# --------------------------------------------------------------------------
# Crawl
# --------------------------------------------------------------------------


def _walk_listing(http: Http, first_url: str, pattern: re.Pattern, *,
                  max_pages: int | None, max_age_s: float, should_stop) -> list[dict]:
    """Walk a paginated listing, following declared next-links when present and
    falling back to ``?page=N``; stops on an empty page or the page cap."""
    entries: list[dict] = []
    seen_ids: set[str] = set()
    url = first_url
    page = 1
    while url:
        if should_stop():
            raise _Stopped
        html = http.get_or_none(url, max_age_s=max_age_s)
        if html is None:
            break
        found = parse_entity_links(html, pattern, url)
        new = [e for e in found if e["id"] not in seen_ids]
        for entry in new:
            seen_ids.add(entry["id"])
        entries.extend(new)
        if not new:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
        declared = hp.next_page_url(hp.soup(html), url)
        if declared and declared != url:
            url = declared
        else:
            separator = "&" if "?" in first_url else "?"
            url = f"{first_url}{separator}page={page}"
    return entries


def _scrape_employer(db: LeadDb, http: Http, url: str, stats: dict, emit,
                     *, max_age_s: float = EMPLOYER_MAX_AGE_S) -> dict | None:
    html = http.get_or_none(url, max_age_s=max_age_s)
    if html is None:
        return None
    employer, history = parse_employer_page(html, url)
    if not employer["source_id"]:
        return None
    is_new = employer["source_id"] not in db.known_employer_ids(SOURCE)
    if db.upsert_employer(employer):
        stats["employers_new" if is_new else "employers_updated"] += 1
        emit("employer", employer)
    for posting in history:
        if db.upsert_posting(posting):
            stats["postings"] += 1
            emit("posting", posting, employer)
    return employer


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
    """Run one Klix crawl. ``full`` sweeps the employer directory (initial run
    does so automatically); afterwards runs are incremental via ``/oglasi``.
    """
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0, "details": 0}

    try:
        directory_done = db.get_cursor(SOURCE, "directory_done")
        if full or not directory_done:
            emit("log", "[klix] Puni prolaz direktorija poslodavaca…")
            entries = _walk_listing(
                http, f"{BASE}/poslodavci", EMPLOYER_URL_RE,
                max_pages=max_pages, max_age_s=LISTING_MAX_AGE_S, should_stop=should_stop,
            )
            emit("log", f"[klix] Direktorij: {len(entries)} poslodavaca na listi.")
            known = db.known_employer_ids(SOURCE)
            for entry in entries:
                if should_stop():
                    raise _Stopped
                if not full and entry["id"] in known:
                    continue
                _scrape_employer(db, http, entry["url"], stats, emit)
            # Only a complete, uncapped sweep counts as done.
            if max_pages is None:
                db.set_cursor(SOURCE, "directory_done", now_iso())

        emit("log", "[klix] Provjeravam nove oglase…")
        known_posts = db.known_posting_ids(SOURCE)
        known_employers = db.known_employer_ids(SOURCE)
        url = f"{BASE}/oglasi"
        page = 1
        while True:
            if should_stop():
                raise _Stopped
            html = http.get_or_none(url, max_age_s=LISTING_MAX_AGE_S)
            if html is None:
                break
            cards = parse_ad_cards(html, url)
            new_cards = [c for c in cards if c["id"] and c["id"] not in known_posts]
            for card in new_cards:
                if should_stop():
                    raise _Stopped
                known_posts.add(card["id"])
                posting = make_posting(
                    source=SOURCE,
                    source_id=card["id"],
                    employer_source=SOURCE if card["employer_id"] else "",
                    employer_source_id=card["employer_id"],
                    title=card["title"],
                    published_at=card["published_at"],
                    expires_at=card["expires_at"],
                    detail_url=card["url"],
                )
                employer_url = card["employer_url"]
                need_detail = fetch_details or not card["employer_id"]
                if need_detail:
                    detail_html = http.get_or_none(card["url"])
                    if detail_html:
                        stats["details"] += 1
                        detailed = parse_ad_page(detail_html, card["url"])
                        for field, value in detailed.items():
                            if value:
                                posting[field] = value
                        employer_url = employer_url or detailed.get("employer_url", "")
                        posting.pop("employer_url", None)

                employer_id = posting["employer_source_id"]
                if employer_id and employer_url:
                    if employer_id not in known_employers:
                        # New employer: their page brings the JIB + full history.
                        if _scrape_employer(db, http, employer_url, stats, emit, max_age_s=0):
                            known_employers.add(employer_id)
                    else:
                        # Known employer with a fresh ad: refresh their history.
                        _scrape_employer(db, http, employer_url, stats, emit,
                                         max_age_s=24 * 3600)
                if db.upsert_posting(posting):
                    stats["postings"] += 1
                    emit("posting", posting, None)

            if not new_cards and page > 1:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break
            declared = hp.next_page_url(hp.soup(html), url)
            url = declared if declared and declared != url else f"{BASE}/oglasi?page={page}"
            if not cards:
                break
    except _Stopped:
        emit("log", "[klix] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[klix] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
