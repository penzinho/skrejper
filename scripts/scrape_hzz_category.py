"""Shared HZZ runner — imported by every run_hzz_<category>.py script."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPTS_DIR.parent

for _p in (str(PROJECT_ROOT), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.scrapers.hzz import HZZ_CATEGORIES, scrape_hzz
from send_report import send as _send_report

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "phone", "company", "city", "employer_address",
    "title", "group", "employment_type", "working_hours",
    "valid_from", "valid_to", "detail_url", "source",
]


def run(category: str, groups: list[str]) -> None:
    rows: list[dict] = []
    seen_companies: set[str] = set()
    label = HZZ_CATEGORIES.get(category, category)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = OUTPUT_DIR / f"hzz-{category}-{date_str}.csv"

    print(f"\n{'=' * 60}", flush=True)
    print(f"Kategorija: {label}", flush=True)
    print(f"Podkategorije: {len(groups)}", flush=True)
    print(f"Output -> {output_path}", flush=True)
    print("=" * 60, flush=True)

    for group in groups:
        print(f"\n  >> {group}", flush=True)
        jobs = scrape_hzz(
            category=category,
            group=group,
            max_pages=999,
            results_per_page=75,
            use_subgroups=False,
        )
        for job in jobs:
            email = (job.get("email") or "").strip()
            if not email:
                continue
            company_key = (job.get("company") or "").strip().casefold()
            if company_key and company_key in seen_companies:
                continue
            if company_key:
                seen_companies.add(company_key)
            rows.append({
                "email": email,
                "phone": (job.get("phone") or "").strip(),
                "company": (job.get("company") or "").strip(),
                "city": (job.get("location") or "").strip(),
                "employer_address": (job.get("employer_address") or "").strip(),
                "title": (job.get("title") or "").strip(),
                "group": (job.get("group") or group).strip(),
                "employment_type": (job.get("employment_type") or "").strip(),
                "working_hours": (job.get("working_hours") or "").strip(),
                "valid_from": (job.get("valid_from") or "").strip(),
                "valid_to": (job.get("valid_to") or "").strip(),
                "detail_url": (job.get("detail_url") or "").strip(),
                "source": "hzz",
            })
            print(f"    [{len(rows)}] {job.get('company', '')} <{email}>  ({job.get('location', '')})", flush=True)

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nGotovo. {len(rows)} redova s e-mailom → {output_path}", flush=True)
    _send_report(output_path)
