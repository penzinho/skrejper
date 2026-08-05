"""Infostud (poslovi.infostud.com) — Srbija. **Iza flaga, defaultno isključen.**

Uvjeti korištenja Infostuda izričito zabranjuju preuzimanje sadržaja u
komercijalne svrhe, pa ovaj adapter ima ``DEFAULT_ENABLED = False``: ne ulazi
u zadani run ni u GUI-jev default izbor, nego se mora izričito uključiti — i
to je svjesna odluka korisnika, ne ovog koda.

Tehnički je izvor uredan i bogat:

* ``sitemap-jobs.xml`` izlistava sve aktivne oglase
  (``/posao/{slug}/{company-slug}/{id}``, numerički id);
* stranica oglasa nosi **schema.org JobPosting JSON-LD** — naslov, datumi,
  poslodavac s adresom, broj izvršilaca — pa se ne parsira DOM;
* profil poslodavca (``/poslodavac/{slug}/{id}``) u ``__NEXT_DATA__``-u ima
  ``companyProfile`` s **PIB-om, adresom i webom**; mapa slug → profil dolazi
  iz ``sitemap-profiles-*.xml``.

robots.txt dopušta ``/posao/`` i ``/poslodavac/`` (zabranjen je ``/rss_feed/*``,
koji zato ne koristimo).
"""

import json
import re

from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text, parse_date
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting

SOURCE = "infostud"
LABEL = "Infostud (Srbija) — iza flaga"
COUNTRY = "RS"
DEFAULT_ENABLED = False  # ToS: zabranjeno komercijalno preuzimanje sadržaja

BASE = "https://poslovi.infostud.com"
JOBS_SITEMAP = f"{BASE}/sitemap-jobs.xml"
PROFILE_SITEMAPS = [f"{BASE}/sitemap-profiles-{letter}.xml" for letter in "abcde"]
AD_URL_RE = re.compile(r"/posao/([^/]+)/([^/]+)/(\d+)$")
PROFILE_URL_RE = re.compile(r"/poslodavac/([^/]+)/(\d+)$")

SITEMAP_MAX_AGE_S = 60 * 60
PROFILES_MAX_AGE_S = 7 * 24 * 3600


class _Stopped(Exception):
    pass


def parse_jobs_sitemap(xml: str) -> list[dict]:
    """{url, company_slug, id}, newest id first."""
    ads = []
    for loc in re.findall(r"<loc>([^<]+)</loc>", xml):
        match = AD_URL_RE.search(loc.strip())
        if match:
            ads.append({"url": loc.strip(), "company_slug": match.group(2), "id": match.group(3)})
    ads.sort(key=lambda ad: int(ad["id"]), reverse=True)
    return ads


def parse_profiles_sitemap(xml: str) -> dict[str, str]:
    """company slug -> profile URL."""
    out = {}
    for loc in re.findall(r"<loc>([^<]+)</loc>", xml):
        match = PROFILE_URL_RE.search(loc.strip())
        if match:
            out[match.group(1)] = loc.strip()
    return out


def _job_posting_ld(html: str) -> dict | None:
    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S):
        try:
            data = json.loads(block)
        except ValueError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
    return None


def parse_job_page(html: str, url: str) -> tuple[dict, dict] | None:
    """Ad page -> (employer, posting) from its JobPosting JSON-LD."""
    match = AD_URL_RE.search(url)
    if not match:
        return None
    company_slug, ad_id = match.group(2), match.group(3)
    ld = _job_posting_ld(html)
    if not ld:
        return None

    organization = ld.get("hiringOrganization") or {}
    company = clean_text(organization.get("name") or "")
    org_address = organization.get("address") or {}
    location = ld.get("jobLocation") or {}
    loc_address = location.get("address") if isinstance(location, dict) else {}
    locality = (loc_address or {}).get("addressLocality") or ""
    if isinstance(locality, list):
        locality = locality[0] if locality else ""

    # "Prodavac - Beograd - Lidl Srbija KD" -> keep just the position part.
    title = clean_text(ld.get("title") or "")
    for suffix in (f" - {company}",):
        if company and title.endswith(suffix):
            title = title[: -len(suffix)]

    employer = make_employer(
        source=SOURCE,
        source_id=company_slug,
        name=company,
        address=clean_text((org_address or {}).get("streetAddress") or ""),
        city=clean_text((org_address or {}).get("addressLocality") or "") or clean_text(locality),
        country=COUNTRY,
        detail_url=f"{BASE}/poslodavac/{company_slug}",
        public_sector_term=public_sector_term(company),
    )
    posting = make_posting(
        source=SOURCE,
        source_id=ad_id,
        employer_source=SOURCE,
        employer_source_id=company_slug,
        title=title,
        city=clean_text(locality),
        published_at=parse_date(str(ld.get("datePosted") or "")),
        expires_at=parse_date(str(ld.get("validThrough") or "")[:10]),
        workers_count=str(ld.get("totalJobOpenings") or ""),
        description=clean_text(ld.get("description") or "")[:3000],
        category=clean_text(ld.get("industry") or ""),
        detail_url=url,
    )
    return employer, posting


def parse_profile_page(html: str) -> dict:
    """Profile page -> {tax_id, address, website} out of __NEXT_DATA__."""
    match = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not match:
        return {}
    try:
        data = json.loads(match.group(1))
    except ValueError:
        return {}
    profile = (data.get("props") or {}).get("pageProps", {}).get("companyProfile") or {}

    def dig(node, key):
        if isinstance(node, dict):
            if isinstance(node.get(key), (str, int)):
                return str(node[key])
            for value in node.values():
                found = dig(value, key)
                if found:
                    return found
        return ""

    return {
        "tax_id": clean_text(dig(profile, "pib")),
        "address": clean_text(dig(profile, "address")),
        "website": clean_text(dig(profile, "website")),
    }


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
    """``max_pages`` caps ad-detail fetches per run (each ad is one page)."""
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0, "details": 0}

    try:
        xml = http.get_or_none(JOBS_SITEMAP, max_age_s=SITEMAP_MAX_AGE_S)
        if xml is None:
            emit("log", "[infostud] Sitemap nedostupan; stajem.")
            return stats
        ads = parse_jobs_sitemap(xml)
        emit("log", f"[infostud] Sitemap: {len(ads)} oglasa.")

        profiles: dict[str, str] = {}
        for sitemap_url in PROFILE_SITEMAPS:
            profile_xml = http.get_or_none(sitemap_url, max_age_s=PROFILES_MAX_AGE_S)
            if profile_xml:
                profiles.update(parse_profiles_sitemap(profile_xml))

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
                emit("log", f"[infostud] Dosegnut limit ({limit} dohvata); ostalo ide idući put.")
                break
            html = http.get_or_none(ad["url"])
            fetched += 1
            stats["details"] += 1
            if html is None:
                continue
            parsed = parse_job_page(html, ad["url"])
            if parsed is None:
                continue
            employer, posting = parsed

            is_new = employer["source_id"] not in known_employers
            if is_new and employer["source_id"] in profiles:
                profile_html = http.get_or_none(profiles[employer["source_id"]])
                if profile_html:
                    for field, value in parse_profile_page(profile_html).items():
                        if value and not employer[field]:
                            employer[field] = value
                    employer["detail_url"] = profiles[employer["source_id"]]
            if db.upsert_employer(employer):
                stats["employers_new" if is_new else "employers_updated"] += 1
                emit("employer", employer)
            known_employers.add(employer["source_id"])
            if db.upsert_posting(posting):
                stats["postings"] += 1
                known_posts.add(ad["id"])
                emit("posting", posting, employer)
    except _Stopped:
        emit("log", "[infostud] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[infostud] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
