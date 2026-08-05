"""Ranked leads -> CSV/XLSX.

Aggregates employers by their dedupe group (``canonical_key``), sums the
frequency scores across sources, drops the public sector by default, and
writes the same utf-8-sig QUOTE_ALL CSV (and optional XLSX) the rest of the
project produces, via ``app.desktop.exporters``.
"""

from collections import defaultdict
from pathlib import Path

from app.desktop import exporters
from app.leadgen.db import LeadDb
from app.leadgen.schema import employer_key

LEAD_FIELDS = [
    "score", "company", "legal_name", "city", "country", "tax_id",
    "website", "email", "phone", "address",
    "ads_24m", "distinct_titles_24m", "repeated_titles_24m",
    "last_ad", "first_ad", "total_ads",
    "sources", "detail_url",
]


def build_leads(
    db: LeadDb,
    *,
    include_public_sector: bool = False,
    min_ads_24m: int = 0,
    countries: tuple[str, ...] = (),
) -> list[dict]:
    scores = {
        (row["employer_source"], row["employer_source_id"]): row for row in db.scores()
    }

    groups: dict[str, list[dict]] = defaultdict(list)
    for employer in db.employers():
        key = employer.get("canonical_key") or employer_key(employer["source"], employer["source_id"])
        groups[key].append(employer)

    leads = []
    for members in groups.values():
        if not include_public_sector and all(m.get("public_sector_term") for m in members):
            continue
        if countries and not any((m.get("country") or "") in countries for m in members):
            continue

        def first(field: str) -> str:
            for member in members:
                if member.get(field):
                    return member[field]
            return ""

        member_scores = [
            scores.get((m["source"], m["source_id"])) for m in members
        ]
        member_scores = [s for s in member_scores if s]
        ads_24m = sum(s["ads_24m"] for s in member_scores)
        if ads_24m < min_ads_24m:
            continue

        leads.append({
            "score": round(sum(s["score"] for s in member_scores), 1),
            "company": first("name") or first("legal_name"),
            "legal_name": first("legal_name"),
            "city": first("city"),
            "country": first("country"),
            "tax_id": first("tax_id"),
            "website": first("website"),
            "email": first("email"),
            "phone": first("phone"),
            "address": first("address"),
            "ads_24m": ads_24m,
            "distinct_titles_24m": sum(s["distinct_titles_24m"] for s in member_scores),
            "repeated_titles_24m": sum(s["repeated_titles_24m"] for s in member_scores),
            "last_ad": max((s["last_ad"] for s in member_scores), default=""),
            "first_ad": min((s["first_ad"] for s in member_scores if s["first_ad"]), default=""),
            "total_ads": sum(s["total_ads"] for s in member_scores),
            "sources": ", ".join(sorted({m["source"] for m in members})),
            "detail_url": first("detail_url"),
        })

    leads.sort(key=lambda lead: (-lead["score"], lead["company"]))
    return leads


def export_leads(
    db: LeadDb,
    output_dir: Path | str,
    *,
    basename: str = "leadovi",
    write_xlsx: bool = True,
    **filters,
) -> dict:
    """Write leads to ``<output_dir>/<basename>.csv`` (+ .xlsx). Returns paths+count."""
    from datetime import datetime, timezone

    leads = build_leads(db, **filters)
    rows = [{key: str(value) for key, value in lead.items()} for lead in leads]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = Path(output_dir)
    csv_path = exporters.write_csv(output_dir / f"{basename}-{date_str}.csv", rows, LEAD_FIELDS)
    xlsx_path = None
    if write_xlsx and exporters.xlsx_available():
        xlsx_path = exporters.write_xlsx(
            output_dir / f"{basename}-{date_str}.xlsx", rows, LEAD_FIELDS, sheet_title="Leadovi"
        )
    return {"csv": str(csv_path), "xlsx": str(xlsx_path) if xlsx_path else None, "rows": len(rows)}
