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
from app import seen_store
from send_report import send as _send_report

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "company", "city", "title",
    "category", "published_at", "detail_url", "employer_website",
    "refnr", "source",
]


def run(category: str) -> None:
    seen_refnrs = seen_store.load_seen("arbeitsagentur")
    seen_emails = seen_store.load_seen("arbeitsagentur-emails")
    rows: list[dict] = []
    seen_companies: set[str] = set()
    run_emails: set[str] = set()
    new_refnrs: list[str] = []
    new_emails: list[str] = []

    def on_job(job: dict) -> None:
        email = (job.get("email") or "").strip()
        if not email:
            return
        refnr = (job.get("refnr") or "").strip()
        if refnr:
            new_refnrs.append(refnr)
        email_key = email.casefold()
        if email_key in seen_emails or email_key in run_emails:
            return
        company_key = (job.get("company") or "").strip().casefold()
        if company_key and company_key in seen_companies:
            return
        run_emails.add(email_key)
        new_emails.append(email_key)
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
        skip_ids=seen_refnrs,
    )

    seen_store.add_seen("arbeitsagentur", new_refnrs)
    seen_store.add_seen("arbeitsagentur-emails", new_emails)

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nGotovo. {len(rows)} redova s e-mailom → {output_path}", flush=True)
    _send_report(output_path)
