"""The unified record shapes every source adapter produces.

Plain dicts move through the pipeline (SQLite rows are dicts anyway); this
module pins down *which keys exist* and provides constructors that fill in the
blanks, so an adapter can supply only what its site actually shows.

Keys are English for consistency with the existing scrapers; the mapping to the
originally requested Croatian names is: naziv=name, pravni_naziv=legal_name,
porezni_id=tax_id, adresa=address, grad=city, drzava=country, web=website,
izvor=source, izvor_id=source_id, pozicija=title, datum_objave=published_at,
datum_isteka=expires_at, broj_radnika=workers_count, opis=description.
"""

from datetime import datetime, timezone

# Employers and postings are keyed by (source, source_id) everywhere — in the
# database, in NDJSON dumps and across machines — so records merge without any
# machine-local numeric ids.
EMPLOYER_FIELDS = [
    "source", "source_id",
    "name", "legal_name",
    "tax_id",            # JIB (BiH) / PIB ili MB (RS) / OIB (HR), digits only
    "vat_id",
    "address", "city", "country",   # country: ISO-ish "BA", "RS", "HR", "DE"
    "website", "email", "phone",
    "detail_url",
    "public_sector_term",  # matched keyword when flagged, else "" (flag, not delete)
    "canonical_key",       # "{source}:{source_id}" of the dedupe group's primary
    "updated_at",
]

POSTING_FIELDS = [
    "source", "source_id",
    "employer_source", "employer_source_id",  # join key to employers
    "title", "city",
    "published_at", "expires_at",   # ISO dates ("" when unknown)
    "workers_count",
    "description",
    "contact_email", "contact_phone",
    "category",
    "detail_url",
    "updated_at",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def employer_key(source: str, source_id: str) -> str:
    return f"{source}:{source_id}"


def _build(fields: list[str], values: dict) -> dict:
    unknown = set(values) - set(fields)
    if unknown:
        raise ValueError(f"Unknown fields: {sorted(unknown)}")
    record = {field: "" for field in fields}
    record.update({key: (value or "").strip() if isinstance(value, str) else str(value or "")
                   for key, value in values.items()})
    if not record["source"] or not record["source_id"]:
        raise ValueError("source and source_id are required")
    record["updated_at"] = record["updated_at"] or now_iso()
    return record


def make_employer(**values) -> dict:
    return _build(EMPLOYER_FIELDS, values)


def make_posting(**values) -> dict:
    return _build(POSTING_FIELDS, values)
