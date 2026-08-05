"""LakoDoPosla.com — Srbija, primary blue-collar source.

The Next.js frontend talks to a public REST API that needs no auth and, unlike
every other source, returns the *full employer record inline with each ad*:

    GET https://prod.lakodoposla.net/api/postings?page=N
      -> {meta: {currentPage, lastPage, totalItems},
          data: [{id, slug, positionTitle, date, expiryDate, hasExpired,
                  locationName, viewCount, numOfApplication,
                  employer: {id, company, pib, address, location, postal,
                             phone, email, website, companyDescription}}]}

So one listing walk yields postings *and* fully enriched employers — PIB,
address, phone, website, even a contact e-mail — with no per-employer fetch and
no enrichment stage. ``date``/``expiryDate`` are ``dd.mm.yyyy``.

Numeric ids (``id``: 85216) are sequential, but the paginated listing already
returns them newest-first with the employer embedded, so there is no reason to
enumerate detail pages.
"""

import json

from app.leadgen.db import LeadDb
from app.leadgen.http import FetchError, Http
from app.leadgen.normalize import clean_text, extract_email, parse_date
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting

SOURCE = "lakodoposla"
LABEL = "Lako do posla (Srbija)"
COUNTRY = "RS"
DEFAULT_ENABLED = True

SITE = "https://www.lakodoposla.com"
API = "https://prod.lakodoposla.net/api"
POSTINGS_URL = f"{API}/postings?page={{page}}"

LISTING_MAX_AGE_S = 30 * 60


class _Stopped(Exception):
    pass


def _employer_from_json(node: dict) -> dict:
    employer_id = str(node.get("id") or "").strip()
    name = clean_text(node.get("company") or node.get("greetingName") or "")
    email = clean_text(node.get("email") or "")
    # The API sometimes returns the login username in ``email``; keep it only
    # if it is actually an address.
    email = extract_email(email)
    return make_employer(
        source=SOURCE,
        source_id=employer_id,
        name=name,
        legal_name=name,
        tax_id=clean_text(node.get("pib") or ""),
        address=clean_text(node.get("address") or ""),
        city=clean_text(node.get("location") or ""),
        country=COUNTRY,
        website=clean_text(node.get("website") or ""),
        email=email,
        phone=clean_text(node.get("phone") or ""),
        detail_url=f"{SITE}/poslodavci/{node.get('slug') or employer_id}",
        public_sector_term=public_sector_term(name),
    )


def parse_postings_page(payload_text: str) -> dict:
    """One API page -> {last_page, postings: [...], employers: [...]}."""
    payload = json.loads(payload_text)
    meta = payload.get("meta") or {}
    postings, employers = [], []
    for node in payload.get("data") or []:
        posting_id = str(node.get("id") or "").strip()
        if not posting_id:
            continue
        employer_node = node.get("employer") or {}
        employer_id = str(employer_node.get("id") or "").strip()
        if employer_id:
            employers.append(_employer_from_json(employer_node))
        location = clean_text(node.get("locationName") or "")
        postings.append(make_posting(
            source=SOURCE,
            source_id=posting_id,
            employer_source=SOURCE if employer_id else "",
            employer_source_id=employer_id,
            title=clean_text(node.get("positionTitle") or ""),
            city=location or clean_text(employer_node.get("location") or ""),
            published_at=parse_date(node.get("date") or ""),
            expires_at=parse_date(node.get("expiryDate") or ""),
            contact_email=extract_email(employer_node.get("email") or ""),
            contact_phone=clean_text(employer_node.get("phone") or ""),
            detail_url=f"{SITE}/oglasi/{node.get('slug') or posting_id}",
        ))
    return {
        "last_page": int(meta.get("lastPage") or 1),
        "postings": postings,
        "employers": employers,
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
    emit = on_event or (lambda kind, *args: None)
    should_stop = should_stop or (lambda: False)
    stats = {"employers_new": 0, "employers_updated": 0, "postings": 0, "details": 0}

    known_employers = db.known_employer_ids(SOURCE)
    try:
        page = 1
        last_page = 1
        while page <= last_page:
            if should_stop():
                raise _Stopped
            text = http.get_or_none(POSTINGS_URL.format(page=page), max_age_s=LISTING_MAX_AGE_S)
            if text is None:
                emit("log", f"[lakodoposla] Stranica {page} nedostupna; stajem.")
                break
            parsed = parse_postings_page(text)
            last_page = parsed["last_page"]
            if max_pages is not None:
                last_page = min(last_page, max_pages)

            for employer in parsed["employers"]:
                is_new = employer["source_id"] not in known_employers
                if db.upsert_employer(employer):
                    stats["employers_new" if is_new else "employers_updated"] += 1
                    emit("employer", employer)
                known_employers.add(employer["source_id"])
            for posting in parsed["postings"]:
                if db.upsert_posting(posting):
                    stats["postings"] += 1
                    emit("posting", posting, None)

            emit("log", f"[lakodoposla] Stranica {page}/{last_page}: {len(parsed['postings'])} oglasa.")
            page += 1
    except _Stopped:
        emit("log", "[lakodoposla] Zaustavljeno — dosad prikupljeno je spremljeno.")
    except FetchError as exc:
        emit("log", f"[lakodoposla] Prekid zbog mrežne greške: {exc}")

    stats["requests"] = http.requests_made
    stats["cache_hits"] = http.cache_hits
    return stats
