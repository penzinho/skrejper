"""Import the existing HZZ / Arbeitsagentur CSV exports into the leadgen db.

The old pipeline stays untouched — it keeps writing its CSVs exactly as before.
This module reads those files after the fact, so the Croatian and German leads
sit in the same database as the BiH/RS ones and take part in dedupe + scoring.

Format detection is by header: the HZZ exports carry ``employer_address`` and
``valid_from``; the Arbeitsagentur ones carry ``refnr``. Employers on these
boards have no stable source id, so the normalized company name serves as one
(stable across files, which is what matters for the upsert).
"""

import csv
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.normalize import norm_company, parse_date
from app.leadgen.public_sector import public_sector_term
from app.leadgen.schema import make_employer, make_posting


def _employer_source_id(company: str) -> str:
    return norm_company(company).replace(" ", "-")


_COUNTRY_HINTS = {"hrvatska": ("hzz", "HR"), "germany": ("arbeitsagentur", "DE"),
                  "deutschland": ("arbeitsagentur", "DE"), "njemacka": ("arbeitsagentur", "DE")}


def _import_row(db: LeadDb, row: dict) -> bool:
    company = (row.get("company") or "").strip()
    if not company:
        return False

    if row.get("refnr"):
        source, country = "arbeitsagentur", "DE"
        posting_id = row["refnr"]
        published, expires = parse_date(row.get("published_at", "")), ""
    elif row.get("valid_from") or row.get("valid_to") or "hzz.hr" in (row.get("detail_url") or ""):
        source, country = "hzz", "HR"
        posting_id = (row.get("detail_url") or "").strip()
        published = parse_date(row.get("valid_from", ""))
        expires = parse_date(row.get("valid_to", ""))
    else:
        # Legacy minimal exports (email/company/city/country only): the country
        # column says which board it was. No posting id -> employer-only import.
        from app.leadgen.normalize import norm_text

        source, country = _COUNTRY_HINTS.get(norm_text(row.get("country", "")), ("hzz", "HR"))
        posting_id = (row.get("detail_url") or "").strip()
        published, expires = parse_date(row.get("published_at", "")), ""

    employer_id = _employer_source_id(company)
    if not employer_id:
        return False

    changed = db.upsert_employer(make_employer(
        source=source,
        source_id=employer_id,
        name=company,
        address=row.get("employer_address", ""),
        city=row.get("city", ""),
        country=country,
        website=row.get("employer_website", ""),
        email=row.get("email", ""),
        phone=row.get("phone", ""),
        public_sector_term=public_sector_term(company),
    ))
    if not posting_id:
        return changed
    return changed | db.upsert_posting(make_posting(
        source=source,
        source_id=posting_id,
        employer_source=source,
        employer_source_id=employer_id,
        title=row.get("title", ""),
        city=row.get("city", ""),
        published_at=published,
        expires_at=expires,
        category=row.get("group") or row.get("category") or "",
        contact_email=row.get("email", ""),
        contact_phone=row.get("phone", ""),
        detail_url=row.get("detail_url", ""),
    ))


def import_csv_files(db: LeadDb, paths: list[Path | str], log=print) -> dict:
    stats = {"files": 0, "rows": 0, "imported": 0}
    for path in paths:
        path = Path(path)
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames or "company" not in reader.fieldnames:
                    log(f"[import] Preskačem {path.name}: nema 'company' kolone.")
                    continue
                stats["files"] += 1
                for row in reader:
                    stats["rows"] += 1
                    if _import_row(db, row):
                        stats["imported"] += 1
        except OSError as exc:
            log(f"[import] Ne mogu pročitati {path}: {exc}")
    log(f"[import] {stats['files']} datoteka, {stats['rows']} redova, {stats['imported']} novih/izmijenjenih zapisa.")
    return stats
