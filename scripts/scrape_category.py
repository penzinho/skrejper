"""Shared runner — imported by every run_<category>.py script."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPTS_DIR.parent

for _p in (str(PROJECT_ROOT), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.scrapers.arbeitsagentur import ARBEITSAGENTUR_GROUPS, scrape_arbeitsagentur
from send_report import send as _send_report

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "company", "city", "title",
    "category", "published_at", "detail_url", "employer_website",
    "refnr", "source",
]


def run(category: str) -> None:
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

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = OUTPUT_DIR / f"arbeitsagentur-{category}-{date_str}.csv"
    label = ARBEITSAGENTUR_GROUPS.get(category, {}).get("label", category)

    print(f"\n{'=' * 60}", flush=True)
    print(f"Kategorija: {label}", flush=True)
    print(f"Output -> {output_path}", flush=True)
    print("=" * 60, flush=True)

    scrape_arbeitsagentur(
        category=category,
        max_pages=999,
        on_job=on_job,
    )

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nGotovo. {len(rows)} redova s e-mailom → {output_path}", flush=True)
    _send_report(output_path)
