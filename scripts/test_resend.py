"""Smoke test — scrape 5 listings from bau_ausbau and send the CSV via Resend."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPTS_DIR.parent

for _p in (str(PROJECT_ROOT), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.scrapers.arbeitsagentur import scrape_arbeitsagentur
from send_report import send as send_report

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "company", "city", "title",
    "category", "published_at", "detail_url", "employer_website",
    "refnr", "source",
]

rows: list[dict] = []


def on_job(job: dict) -> None:
    email = (job.get("email") or "").strip()
    if not email:
        return
    rows.append({
        "email": email,
        "company": (job.get("company") or "").strip(),
        "city": (job.get("location") or "").strip(),
        "title": (job.get("title") or "").strip(),
        "category": (job.get("category") or "").strip(),
        "published_at": (job.get("published_at") or "").strip(),
        "detail_url": (job.get("detail_url") or "").strip(),
        "employer_website": (job.get("employer_website") or "").strip(),
        "refnr": (job.get("refnr") or "").strip(),
        "source": "arbeitsagentur",
    })
    print(f"  [{len(rows)}] {job.get('company', '')} <{email}>", flush=True)


print("Scraping 5 listings (bau_ausbau)...", flush=True)
scrape_arbeitsagentur(category="bau_ausbau", max_pages=1, listing_limit=5, on_job=on_job)

date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = OUTPUT_DIR / f"arbeitsagentur-test-{date_str}.csv"

with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n{len(rows)} redova zapisano u {output_path}", flush=True)
print("Šaljem email...", flush=True)

ok = send_report(output_path)
sys.exit(0 if ok else 1)
