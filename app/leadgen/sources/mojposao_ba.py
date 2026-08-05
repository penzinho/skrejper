"""MojPosao.ba (Alma Career BH) — BiH, secondary source.

The public site is a Nuxt SPA, but its own search API is server-side and
answers plain JSON without authentication:

    GET /api/proxy/jobs/search?page=N   ->  {totalPages, total, items: [
        {organization?, jobs: [{id (uuid), title, slug, publishedAt, startsAt,
                                endsAt, location, description, salary,
                                organization {id (uuid), name}}]}]}

so the adapter never parses the listing DOM. Job and employer ids are UUIDs —
no enumeration (the sitemap's numeric job URLs are a stale legacy index that
404s) — and the API lists only *active* ads, so posting history accumulates
across runs; that is exactly what the incremental database is for.

Employer pages (``/poslodavac/{uuid}/{slug}``) are server-rendered and carry
the employer's website; they are fetched once per newly seen employer.

robots.txt: ``Allow: /`` with ``Crawl-delay: 1`` (honoured via the fetch
layer's delay) and a ban on ``?source=`` tracking URLs, which we never use.
"""

import json
import re

from app.leadgen import htmlparse as hp
from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting

SOURCE = "mojposao_ba"
LABEL = "MojPosao.ba (BiH)"
COUNTRY = "BA"
DEFAULT_ENABLED = True

BASE = "https://www.mojposao.ba"
SEARCH_URL = f"{BASE}/api/proxy/jobs/search?page={{page}}"

LISTING_MAX_AGE_S = 30 * 60

# The employer's own site on its profile page: first outbound link that is not
# the board, its CDN/static hosts, or a social profile.
_NON_WEBSITE_HOSTS = (
    "mojposao.ba", "mojposao.hr", "moj-posao.net", "almacareer.com",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "google.com", "googleapis.com",
    "jsdelivr.net", "doubleclick.net", "cookiebot.com",
)


class _Stopped(Exception):
    pass


def _slugify(value: str) -> str:
    from app.leadgen.normalize import norm_text

    return re.sub(r"[^a-z0-9]+", "-", norm_text(value)).strip("-") or "x"


# --------------------------------------------------------------------------
# Parsers (JSON/HTML strings in, dicts out — fixture-testable)
# --------------------------------------------------------------------------


def _as_org(value) -> dict:
    """The API's ``organization`` is a dict on page 1 but sometimes a bare
    name string deeper in (incognito/legacy entries)."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        return {"name": value}
    return {}


def parse_search_page(payload_text: str) -> dict:
    """One API page -> {total_pages, postings: [...], employers: {id: name}}."""
    payload = json.loads(payload_text)
    postings = []
    employers: dict[str, str] = {}
    for item in payload.get("items") or []:
        for job in item.get("jobs") or []:
            job_id = (job.get("id") or "").strip()
            if not job_id:
                continue
            organization = _as_org(job.get("organization")) or _as_org(item.get("organization"))
            org_name = clean_text(organization.get("name") or "")
            # A name-only organization still becomes an employer record, keyed
            # by its slug (stable across runs), so its ads count toward scoring.
            org_id = (organization.get("id") or "").strip() or (
                f"name-{_slugify(org_name)}" if org_name else ""
            )
            if org_id:
                employers.setdefault(org_id, org_name)
            postings.append(make_posting(
                source=SOURCE,
                source_id=job_id,
                employer_source=SOURCE if org_id else "",
                employer_source_id=org_id,
                title=clean_text(job.get("title") or ""),
                city=clean_text(job.get("location") or ""),
                published_at=(job.get("publishedAt") or job.get("startsAt") or "")[:10],
                expires_at=(job.get("endsAt") or "")[:10],
                description=clean_text(job.get("description") or "")[:3000],
                detail_url=f"{BASE}/posao/{job_id}/{job.get('slug') or 'oglas'}",
            ))
    return {
        "total_pages": int(payload.get("totalPages") or 1),
        "postings": postings,
        "employers": employers,
    }


def parse_employer_page(html: str, url: str, org_id: str, name_hint: str = "") -> dict:
    """Employer profile page -> employer record (name + website)."""
    import urllib.parse

    root = hp.soup(html)
    name = hp.page_title(root) or name_hint

    website = ""
    for anchor in root.find_all("a", href=True):
        href = anchor["href"]
        if not href.startswith(("http://", "https://")):
            continue
        host = urllib.parse.urlparse(href).netloc.casefold().removeprefix("www.")
        if not host or any(host == h or host.endswith("." + h) for h in _NON_WEBSITE_HOSTS):
            continue
        # The profile shows the website as a bare-URL link; page furniture
        # links (ads, fonts) never render their own URL as the anchor text.
        text = clean_text(anchor.get_text(" "))
        if host in text.casefold():
            website = href
            break

    return make_employer(
        source=SOURCE,
        source_id=org_id,
        name=name,
        country=COUNTRY,
        website=website,
        detail_url=url,
        public_sector_term=public_sector_term(name),
    )


# --------------------------------------------------------------------------
# Crawl
# --------------------------------------------------------------------------


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
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0, "details": 0}

    known_employers = db.known_employer_ids(SOURCE)

    try:
        page = 1
        total_pages = 1
        while page <= total_pages:
            if should_stop():
                raise _Stopped
            text = http.get_or_none(SEARCH_URL.format(page=page), max_age_s=LISTING_MAX_AGE_S)
            if text is None:
                emit("log", f"[mojposao_ba] Stranica {page} nedostupna; stajem.")
                break
            parsed = parse_search_page(text)
            total_pages = parsed["total_pages"]
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)

            for posting in parsed["postings"]:
                if should_stop():
                    raise _Stopped
                org_id = posting["employer_source_id"]
                if org_id and org_id not in known_employers:
                    name = parsed["employers"].get(org_id, "")
                    # Name-only employers (org_id "name-...") have no profile
                    # page to fetch; UUID-keyed ones do.
                    html = None
                    url = ""
                    if re.fullmatch(r"[0-9a-f-]{36}", org_id):
                        url = f"{BASE}/poslodavac/{org_id}/{_slugify(name)}"
                        html = http.get_or_none(url)
                    employer = (
                        parse_employer_page(html, url, org_id, name)
                        if html is not None
                        else make_employer(source=SOURCE, source_id=org_id, name=name,
                                           country=COUNTRY, detail_url=url,
                                           public_sector_term=public_sector_term(name))
                    )
                    if db.upsert_employer(employer):
                        stats["employers_new"] += 1
                        emit("employer", employer)
                    known_employers.add(org_id)
                if db.upsert_posting(posting):
                    stats["postings"] += 1
                    emit("posting", posting, None)

            emit("log", f"[mojposao_ba] Stranica {page}/{total_pages}: {len(parsed['postings'])} oglasa.")
            page += 1
    except _Stopped:
        emit("log", "[mojposao_ba] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[mojposao_ba] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
