import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.gelbeseiten import enrich_gelbeseiten_emails, scrape_gelbeseiten_fast


CSV_FIELDS = ["agency_name", "address", "city", "email", "phone_number", "website"]


def build_rows(agencies: list[dict]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for agency in agencies:
        rows.append(
            {
                "agency_name": (agency.get("company") or "").strip(),
                "address": (agency.get("address") or "").strip(),
                "city": (agency.get("city") or agency.get("location") or "").strip(),
                "email": (agency.get("email") or "").strip(),
                "phone_number": (agency.get("phone") or "").strip(),
                "website": (agency.get("website") or "").strip(),
            }
        )

    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export GelbeSeiten agencies to CSV."
    )
    parser.add_argument("--query", default="personalvermittlung")
    parser.add_argument("--location", default="bundesweit")
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--company-limit", type=int, default=None)
    parser.add_argument("--skip-email-enrichment", action="store_true")
    parser.add_argument("--output", default="output/gelbeseiten-personalvermittlung.csv")
    args = parser.parse_args()

    agencies = scrape_gelbeseiten_fast(
        query=args.query,
        location=args.location,
        max_pages=args.max_pages,
        company_limit=args.company_limit,
    )
    if not args.skip_email_enrichment:
        agencies = enrich_gelbeseiten_emails(agencies)
    rows = build_rows(agencies)
    output_path = Path(args.output)
    write_csv(output_path, rows)

    print(f"Scraped agencies: {len(agencies)}")
    print(f"CSV rows: {len(rows)}")
    print(f"CSV: {output_path}")


if __name__ == "__main__":
    main()
