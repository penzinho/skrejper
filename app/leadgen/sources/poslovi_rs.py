"""Poslovi.rs — Srbija, primary source (blue-collar volume, server-rendered).

posao.rs itself is unreachable; poslovi.rs is the live Serbian board of that
family. It renders every ad server-side, and robots.txt allows ``/posao/``,
so no browser is needed. Ad detail URLs are listed in ``sitemap.xml``:

    https://www.poslovi.rs/posao/{company-slug}/{title-slug}-{id}

The employer has no per-ad numeric id, but the *company slug* in the URL is
stable across that employer's ads, so it serves as the employer id — which is
also how a firm's ads group for scoring. The ad page's ``<title>`` is a fixed
``Position | Company | City | poslovi.rs``, the cleanest thing to key on; the
body carries the expiry date and description. There is no PIB/address here, so
these employers are candidates for the (separate) enrichment stage.

Serbian content is Cyrillic or Latin depending on the ad; normalization
transliterates everything, so titles and cities compare across scripts.
"""

import re

from app.leadgen import htmlparse as hp
from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text, parse_date, translit
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting

SOURCE = "poslovi_rs"
LABEL = "Poslovi.rs (Srbija)"
COUNTRY = "RS"
DEFAULT_ENABLED = True

BASE = "https://www.poslovi.rs"
SITEMAP_URL = f"{BASE}/sitemap.xml"
AD_URL_RE = re.compile(r"/posao/([^/]+)/(?:.*?-)?(\d+)$")

SITEMAP_MAX_AGE_S = 60 * 60
_EXPIRY_HINTS = ("istice", "istek", "vazi do")


class _Stopped(Exception):
    pass


def parse_sitemap_ads(xml: str) -> list[dict]:
    """Ad URLs from the sitemap: {url, employer_slug, id}, newest id first."""
    ads = []
    for loc in re.findall(r"<loc>([^<]+)</loc>", xml):
        match = AD_URL_RE.search(loc.strip())
        if match:
            ads.append({"url": loc.strip(), "employer_slug": match.group(1), "id": match.group(2)})
    ads.sort(key=lambda ad: int(ad["id"]), reverse=True)
    return ads


def parse_ad_page(html: str, url: str) -> tuple[dict, dict] | None:
    """Ad detail page -> (employer, posting), or None if it isn't a real ad."""
    match = AD_URL_RE.search(url)
    if not match:
        return None
    employer_slug, ad_id = match.group(1), match.group(2)

    root = hp.soup(html)
    title_tag = root.find("title")
    raw_title = clean_text(title_tag.get_text()) if title_tag else ""
    # "Position | Company | City | poslovi.rs"
    parts = [clean_text(p) for p in raw_title.split("|")]
    if len(parts) < 3 or "404" in raw_title:
        return None
    position, company, city = parts[0], parts[1], parts[2]

    body_text = translit(root.get_text(" "))
    expires = ""
    marker = re.search(r"(isti[cč]e|va[zž]i do)[^0-9]{0,12}(\d{1,2}\.\d{1,2}\.\d{4})", body_text, re.IGNORECASE)
    if marker:
        expires = parse_date(marker.group(2))

    employer = make_employer(
        source=SOURCE,
        source_id=employer_slug,
        name=company,
        legal_name=company,
        city=city,
        country=COUNTRY,
        detail_url=f"{BASE}/kompanija/{employer_slug}",
        public_sector_term=public_sector_term(company),
    )
    posting = make_posting(
        source=SOURCE,
        source_id=ad_id,
        employer_source=SOURCE,
        employer_source_id=employer_slug,
        title=position,
        city=city,
        expires_at=expires,
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
    """``max_pages`` bounds the number of ad detail fetches (each ad is a page),
    so a smoke run stays cheap; a full run walks every new ad in the sitemap."""
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0, "details": 0}

    try:
        xml = http.get_or_none(SITEMAP_URL, max_age_s=SITEMAP_MAX_AGE_S)
        if xml is None:
            emit("log", "[poslovi_rs] Sitemap nedostupan; stajem.")
            return stats
        ads = parse_sitemap_ads(xml)
        emit("log", f"[poslovi_rs] Sitemap: {len(ads)} oglasa.")

        known_posts = db.known_posting_ids(SOURCE)
        known_employers = db.known_employer_ids(SOURCE)
        limit = max_pages if max_pages is not None else len(ads)
        fetched = 0
        for ad in ads:
            if should_stop():
                raise _Stopped
            if ad["id"] in known_posts:
                continue
            if fetched >= limit:
                emit("log", f"[poslovi_rs] Dosegnut limit ({limit} dohvata); ostalo ide idući put.")
                break
            html = http.get_or_none(ad["url"])
            fetched += 1
            stats["details"] += 1
            if html is None:
                continue
            parsed = parse_ad_page(html, ad["url"])
            if parsed is None:
                continue
            employer, posting = parsed
            is_new = employer["source_id"] not in known_employers
            if db.upsert_employer(employer):
                stats["employers_new" if is_new else "employers_updated"] += 1
                emit("employer", employer)
            known_employers.add(employer["source_id"])
            if db.upsert_posting(posting):
                stats["postings"] += 1
                known_posts.add(ad["id"])
                emit("posting", posting, employer)
    except _Stopped:
        emit("log", "[poslovi_rs] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[poslovi_rs] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
