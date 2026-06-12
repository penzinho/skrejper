import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.omh import fetch_listing_counts, scrape_omh_hotels


CSV_FIELDS = [
    "hotel_name",
    "address",
    "city",
    "email",
    "phone_number",
    "website",
    "detail_url",
]


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export OMH hotels to CSV.")
    parser.add_argument(
        "--include-associated",
        action="store_true",
        help="Include Pridružene članice in addition to regular /hoteli/ listings.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit hotels for test runs.")
    parser.add_argument("--request-delay", type=float, default=0.0)
    parser.add_argument("--output", default="output/omh-hoteli.csv")
    args = parser.parse_args()

    counts = fetch_listing_counts()
    hotels = scrape_omh_hotels(
        include_associated=args.include_associated,
        limit=args.limit,
        request_delay=args.request_delay,
    )
    output_path = Path(args.output)
    write_csv(output_path, hotels)

    print(f"Regular listing count: {counts.get('regular', 0)}")
    print(f"Associated listing count: {counts.get('associated', 0)}")
    print(f"Scraped hotels: {len(hotels)}")
    print(f"CSV rows: {len(hotels)}")
    print(f"CSV: {output_path}")


if __name__ == "__main__":
    main()
