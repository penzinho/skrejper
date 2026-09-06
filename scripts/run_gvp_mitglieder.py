"""Standalone runner for the GVP member directory — saves results to output/.

Two files, like the desktop app:

    gvp-mitglieder-<date>.csv                 members with an e-mail
    gvp-mitglieder-<date>-missing-emails.csv  members still without one

Set GVP_ENRICH=false to skip the website lookup (Impressum / Kontakt) for the
members the directory lists without an address; it is what takes the time.
"""

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import seen_store
from app.desktop.pipeline import GVP_FIELDS, LeadCollector
from app.scrapers.gvp import scrape_gvp

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

ENRICH = os.getenv("GVP_ENRICH", "true").casefold() != "false"
MAX_PAGES = int(os.getenv("GVP_MAX_PAGES", "0")) or None

date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = OUTPUT_DIR / f"gvp-mitglieder-{date_str}.csv"
missing_path = OUTPUT_DIR / f"gvp-mitglieder-{date_str}-missing-emails.csv"


def _write(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=GVP_FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    collector = LeadCollector(
        source="gvp",
        seen_emails=seen_store.load_seen("gvp-emails"),
        keep_without_email=True,
    )
    enrich_failed: list[str] = []

    def on_member(member: dict) -> None:
        row = collector.add(member)
        if row is not None:
            print(f"  [{collector.stats.kept}] {row['company']} <{row['email']}>  ({row['city']})")
        if member.get("enrich_tried") and not member.get("email"):
            enrich_failed.append(member["member"])

    print(f"Scraping GVP Mitglieder (enrich={ENRICH}, max_pages={MAX_PAGES or 'all'})...")
    try:
        scrape_gvp(
            max_pages=MAX_PAGES,
            enrich=ENRICH,
            skip_enrich_ids=seen_store.load_seen("gvp-enriched"),
            on_member=on_member,
            skip_ids=seen_store.load_seen("gvp"),
        )
    finally:
        collector.finish()
        _write(output_path, collector.rows)
        _write(missing_path, collector.missing_rows)
        seen_store.add_seen("gvp", collector.new_ids)
        seen_store.add_seen("gvp-emails", collector.new_emails)
        seen_store.add_seen("gvp-enriched", enrich_failed)

    print(f"\nDone. {len(collector.rows)} rows with e-mail -> {output_path}")
    print(f"      {len(collector.missing_rows)} without e-mail -> {missing_path}")


if __name__ == "__main__":
    main()
