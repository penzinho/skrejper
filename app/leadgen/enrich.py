"""Enrichment — a separate, optional pipeline stage (requirement B).

On HZZ and NSZ the contact is in the ad. On the private portals you often get
only the company name, because applications go through the portal. This stage
fills the gaps *after* scraping, never inline in an adapter, and in cheap-first
order so the expensive/fragile lookups only run for what is still missing:

1. **Klix cross-fill (BiH, free).** The Klix directory already holds JIB,
   address and website for ~3000 firms. Any employer from another BiH source
   whose normalized name matches a Klix record inherits those fields — no
   network at all. (Cross-source dedupe groups them; this also copies the data
   onto the record so a firm that is *only* on MojPosao still benefits when it
   later appears on Klix.)

2. **Website → e-mail (all, high value).** An employer with a website but no
   e-mail is worth a couple of fetches: the homepage and a contact page, from
   which a ``mailto:`` or a plain address is harvested. This is what turns a
   "company name only" portal lead into something you can actually cold-mail.

3. **Registry lookup (fragile, best-effort).** For employers still missing a
   tax id, an external registry (pretraga.apr.gov.rs for RS, court registers
   for BiH) *could* supply the matični broj. These are form-driven, often
   behind a captcha, and — as of writing — pretraga.apr.gov.rs serves an
   incomplete TLS chain, so this is a pluggable interface with a null default
   that degrades gracefully rather than a hard dependency.

Everything here is idempotent: an already-filled field is never overwritten,
and the stage can be re-run as new data arrives.
"""

import re
import urllib.parse

from app.leadgen.db import LeadDb
from app.leadgen.http import Http
from app.leadgen.normalize import extract_email, norm_company
from app.leadgen.schema import now_iso

# Contact pages to try on an employer's own domain, in order.
_CONTACT_PATHS = ("", "/kontakt", "/contact", "/kontakt.html", "/o-nama", "/impressum", "/about")

# Addresses that are never a real employer contact.
_GENERIC_LOCAL_PARTS = ("noreply", "no-reply", "postmaster", "mailer-daemon", "webmaster")


class RegistryLookup:
    """Interface for an external company-registry lookup. The default returns
    nothing; a real implementation (APR, court registers) can be injected."""

    def by_name(self, name: str, city: str, country: str) -> dict:
        """Return any of {tax_id, address, website, phone} that can be found."""
        return {}


def _klix_index(db: LeadDb) -> dict[str, dict]:
    """Normalized Klix company name -> its richest record (has JIB/web)."""
    index: dict[str, dict] = {}
    for employer in db.employers("klix"):
        key = norm_company(employer.get("name") or employer.get("legal_name") or "")
        if key and (employer.get("tax_id") or employer.get("website") or employer.get("address")):
            index.setdefault(key, employer)
    return index


def _merge_missing(employer: dict, source: dict, fields: tuple[str, ...]) -> bool:
    """Copy ``fields`` from ``source`` into empty slots of ``employer``."""
    changed = False
    for field in fields:
        if not employer.get(field) and source.get(field):
            employer[field] = source[field]
            changed = True
    return changed


def _email_from_site(http: Http, website: str, log) -> str:
    base = website if website.startswith(("http://", "https://")) else f"https://{website}"
    host = urllib.parse.urlparse(base).netloc
    for path in _CONTACT_PATHS:
        url = urllib.parse.urljoin(base, path) if path else base
        html = http.get_or_none(url, max_age_s=None)
        if not html:
            continue
        # Prefer an explicit mailto:, then any address on the page.
        for match in re.findall(r'mailto:([^"\'>?\s]+)', html):
            email = extract_email(urllib.parse.unquote(match))
            if email and _plausible_contact(email, host):
                return email
        email = extract_email(html)
        if email and _plausible_contact(email, host):
            return email
    return ""


def _plausible_contact(email: str, host: str) -> bool:
    local, _, domain = email.partition("@")
    if local in _GENERIC_LOCAL_PARTS:
        return False
    # A contact on the company's own domain is the strong signal; anything else
    # (a gmail on the page) is still acceptable but not a CDN/image host.
    return not domain.endswith((".png", ".jpg", ".gif", ".svg", ".webp", ".js", ".css"))


def enrich(
    db: LeadDb,
    http: Http | None = None,
    *,
    do_klix: bool = True,
    do_website_email: bool = True,
    registry: RegistryLookup | None = None,
    limit: int | None = None,
    on_event=None,
) -> dict:
    """Run the enrichment stage over employers that still have gaps."""
    emit = on_event or (lambda kind, *args: None)
    stats = {"klix_filled": 0, "emails_found": 0, "registry_filled": 0, "scanned": 0}

    klix_index = _klix_index(db) if do_klix else {}
    http = http or (Http("enrich") if (do_website_email or registry) else None)

    processed = 0
    for employer in db.employers():
        if limit is not None and processed >= limit:
            break
        if employer["source"] == "klix":
            continue  # Klix is the reference, not a target
        stats["scanned"] += 1
        changed = False

        # 1. Klix cross-fill.
        if do_klix and not (employer["tax_id"] and employer["website"]):
            match = klix_index.get(norm_company(employer.get("name") or ""))
            if match and _merge_missing(employer, match, ("tax_id", "vat_id", "address", "website")):
                stats["klix_filled"] += 1
                changed = True

        # 2. Website -> e-mail.
        if do_website_email and http is not None and employer.get("website") and not employer.get("email"):
            email = _email_from_site(http, employer["website"], emit)
            if email:
                employer["email"] = email
                stats["emails_found"] += 1
                changed = True

        # 3. Registry lookup (best-effort).
        if registry is not None and not employer.get("tax_id"):
            found = registry.by_name(employer.get("name", ""), employer.get("city", ""), employer.get("country", ""))
            if _merge_missing(employer, found, ("tax_id", "address", "website", "phone")):
                stats["registry_filled"] += 1
                changed = True

        if changed:
            employer["updated_at"] = now_iso()
            db.upsert_employer(employer)
            emit("employer", employer)
        processed += 1

    emit("log", f"[enrich] {stats}")
    return stats
