import argparse
import csv
import sys
import unicodedata
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.hzz import scrape_hzz


CSV_FIELDS = ["email", "first_name", "last_name", "company", "city", "country"]
EXCLUDED_COMPANY_TERMS = ("djecji vrtic", "vrtic", "skola", "opcina")


def normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(normalized.casefold().split())


def is_excluded_company(company: str) -> bool:
    normalized = normalize_key(company)
    return any(term in normalized for term in EXCLUDED_COMPANY_TERMS)


def load_existing_contacts(paths: list[str]) -> tuple[set[str], set[str]]:
    emails: set[str] = set()
    companies: set[str] = set()

    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            print(f"Exclude CSV not found, skipping: {path}")
            continue

        with path.open(newline="", encoding="utf-8-sig") as csv_file:
            for row in csv.DictReader(csv_file):
                email = (row.get("email") or "").strip()
                company = (row.get("company") or "").strip()
                if email:
                    emails.add(email.casefold())
                if company:
                    companies.add(normalize_key(company))

    return emails, companies


def build_rows(
    jobs: list[dict],
    *,
    country: str,
    dedupe_company: bool,
    excluded_emails: set[str] | None = None,
    excluded_companies: set[str] | None = None,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    rows: list[dict[str, str]] = []
    seen_companies: set[str] = set()
    excluded_emails = excluded_emails or set()
    excluded_companies = excluded_companies or set()
    stats = {
        "without_email": 0,
        "excluded_company": 0,
        "excluded_existing": 0,
        "duplicate_company": 0,
    }

    for job in jobs:
        email = (job.get("email") or "").strip()
        if not email:
            stats["without_email"] += 1
            continue

        company = (job.get("company") or "").strip()
        if is_excluded_company(company):
            stats["excluded_company"] += 1
            continue

        city = (job.get("location") or "").strip()
        company_key = normalize_key(company)
        if email.casefold() in excluded_emails or company_key in excluded_companies:
            stats["excluded_existing"] += 1
            continue

        if dedupe_company and company_key in seen_companies:
            stats["duplicate_company"] += 1
            continue

        seen_companies.add(company_key)
        rows.append(
            {
                "email": email,
                "first_name": "",
                "last_name": "",
                "company": company,
                "city": city,
                "country": country,
            }
        )

    return rows, stats


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=CSV_FIELDS,
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export HZZ category listings with email addresses to CSV."
    )
    parser.add_argument("--category", default="hospitality_tourism")
    parser.add_argument("--group", default=None)
    parser.add_argument("--all-subgroups", action="store_true")
    parser.add_argument("--no-subgroups", action="store_true")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--results-per-page", type=int, default=75)
    parser.add_argument("--country", default="Hrvatska")
    parser.add_argument("--output", default="output/hzz-ugostitelji-radnici-u-turizmu.csv")
    parser.add_argument("--allow-duplicate-companies", action="store_true")
    parser.add_argument(
        "--exclude-csv",
        action="append",
        default=[],
        help="Existing CSV to exclude by email and company. Can be passed multiple times.",
    )
    args = parser.parse_args()

    excluded_emails, excluded_companies = load_existing_contacts(args.exclude_csv)
    jobs = scrape_hzz(
        max_pages=args.max_pages,
        category=args.category,
        group=args.group,
        start_page=args.start_page,
        results_per_page=args.results_per_page,
        use_subgroups=not args.no_subgroups,
    )
    rows, stats = build_rows(
        jobs,
        country=args.country,
        dedupe_company=not args.allow_duplicate_companies,
        excluded_emails=excluded_emails,
        excluded_companies=excluded_companies,
    )
    output_path = Path(args.output)
    write_csv(output_path, rows)

    print(f"Scraped jobs: {len(jobs)}")
    print(f"Skipped without email: {stats['without_email']}")
    print(f"Skipped excluded companies: {stats['excluded_company']}")
    print(f"Skipped existing contacts: {stats['excluded_existing']}")
    print(f"Skipped duplicate companies: {stats['duplicate_company']}")
    print(f"CSV rows: {len(rows)}")
    print(f"CSV: {output_path}")


if __name__ == "__main__":
    main()
