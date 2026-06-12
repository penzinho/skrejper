import argparse
import csv
import sys
import unicodedata
from pathlib import Path


def normalize_company_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_only.casefold().split())


def read_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = [dict(row) for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def dedupe_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, int]]:
    deduped: list[dict[str, str]] = []
    seen_companies: set[str] = set()
    stats = {
        "kept": 0,
        "skipped_missing_email": 0,
        "skipped_duplicate_company": 0,
    }

    for row in rows:
        email = (row.get("email") or "").strip()
        if not email:
            stats["skipped_missing_email"] += 1
            continue

        company = (row.get("company") or "").strip()
        company_key = normalize_company_key(company) or email.casefold()
        if company_key in seen_companies:
            stats["skipped_duplicate_company"] += 1
            continue

        seen_companies.add(company_key)
        deduped.append(row)
        stats["kept"] += 1

    return deduped, stats


def write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate a CSV on company level.")
    parser.add_argument("input_csv")
    parser.add_argument("output_csv")
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    rows, fieldnames = read_rows(input_path)
    deduped_rows, stats = dedupe_rows(rows)
    write_rows(output_path, deduped_rows, fieldnames)

    print(f"Input rows: {len(rows)}")
    print(f"Skipped without email: {stats['skipped_missing_email']}")
    print(f"Skipped duplicate companies: {stats['skipped_duplicate_company']}")
    print(f"Output rows: {stats['kept']}")
    print(f"CSV: {output_path}")


if __name__ == "__main__":
    main()
