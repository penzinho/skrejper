import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.meinestadt import get_meinestadt_categories, scrape_meinestadt
from scripts.dedupe_csv_by_company import dedupe_rows, write_rows


CSV_FIELDS = [
    "email",
    "first_name",
    "last_name",
    "company",
    "city",
    "country",
    "title",
    "source",
    "category",
    "published_at",
    "detail_url",
    "employer_website",
]


def build_row(job: dict, *, country: str) -> dict[str, str] | None:
    email = (job.get("employer_email") or job.get("email") or "").strip()
    if not email:
        return None

    return {
        "email": email,
        "first_name": "",
        "last_name": "",
        "company": (job.get("company") or "").strip(),
        "city": (job.get("location") or "").strip(),
        "country": country,
        "title": (job.get("title") or "").strip(),
        "source": "meinestadt",
        "category": (job.get("category") or "").strip(),
        "published_at": (job.get("published_at") or "").strip(),
        "detail_url": (job.get("detail_url") or "").strip(),
        "employer_website": (job.get("employer_website") or "").strip(),
    }


def list_categories() -> None:
    for category in get_meinestadt_categories():
        print(f"{category['key']}: {category['label']} ({category['path']})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export meinestadt job listings with application email addresses to CSV."
    )
    parser.add_argument("--category", default="hospitality_tourism")
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--company-limit", type=int, default=None)
    parser.add_argument("--country", default="Germany")
    parser.add_argument("--output", default="output/meinestadt-hospitality-tourism.csv")
    parser.add_argument("--list-categories", action="store_true")
    args = parser.parse_args()

    if args.list_categories:
        list_categories()
        return

    output_path = Path(args.output)
    raw_output_path = output_path.with_name(f"{output_path.stem}-raw{output_path.suffix}")
    raw_output_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str]] = []
    stats = {"without_email": 0, "scraped": 0}

    # Stream rows to the raw CSV as they are scraped so a crash mid-run keeps progress.
    with raw_output_path.open("w", newline="", encoding="utf-8-sig") as raw_file:
        writer = csv.DictWriter(raw_file, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        raw_file.flush()

        def on_job(job: dict) -> None:
            stats["scraped"] += 1
            row = build_row(job, country=args.country)
            if row is None:
                stats["without_email"] += 1
                return
            rows.append(row)
            writer.writerow(row)
            raw_file.flush()

        scrape_meinestadt(
            category=args.category,
            max_pages=args.max_pages,
            company_limit=args.company_limit,
            on_job=on_job,
        )

    deduped_rows, dedupe_stats = dedupe_rows(rows)
    write_rows(output_path, deduped_rows, CSV_FIELDS)

    print(f"Scraped jobs: {stats['scraped']}")
    print(f"Rows with email before dedupe: {len(rows)}")
    print(f"Skipped without email: {stats['without_email']}")
    print(f"Skipped duplicate companies: {dedupe_stats['skipped_duplicate_company']}")
    print(f"Final CSV rows: {dedupe_stats['kept']}")
    print(f"Raw CSV: {raw_output_path}")
    print(f"Deduped CSV: {output_path}")


if __name__ == "__main__":
    main()
