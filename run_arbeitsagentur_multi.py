"""Run Arbeitsagentur scraper for multiple categories — saves each to output/."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.arbeitsagentur import scrape_arbeitsagentur

CATEGORIES = [
    "logistik_verkehr",       # Logistik, Lager & Verkehr
    "produktion_fertigung",   # Produktion & Fertigung
    "gastronomie_tourismus",  # Gastronomie, Hotellerie & Tourismus
]

MAX_PAGES = 999  # effectively unlimited; scraper stops naturally when results run out
LISTING_LIMIT = None  # no cap — scrape everything

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "company", "city", "title",
    "category", "published_at", "detail_url", "employer_website",
    "refnr", "source",
]

date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run_category(category: str) -> None:
    rows: list[dict] = []
    seen_companies: set[str] = set()

    def on_job(job: dict) -> None:
        email = (job.get("email") or "").strip()
        if not email:
            return
        company_key = (job.get("company") or "").strip().casefold()
        if company_key and company_key in seen_companies:
            return
        if company_key:
            seen_companies.add(company_key)
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
        print(f"  [{len(rows)}] {job.get('company', '')} <{email}>  ({job.get('location', '')})", flush=True)

    output_path = OUTPUT_DIR / f"arbeitsagentur-{category}-{date_str}.csv"

    print(f"\n{'=' * 60}", flush=True)
    print(f"Scraping '{category}'  (full, no limit)", flush=True)
    print(f"Output -> {output_path}", flush=True)
    print("=" * 60, flush=True)

    scrape_arbeitsagentur(
        category=category,
        max_pages=MAX_PAGES,
        on_job=on_job,
    )

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. {len(rows)} rows with e-mail saved to {output_path}", flush=True)


for cat in CATEGORIES:
    run_category(cat)

print("\nAll categories finished.", flush=True)
