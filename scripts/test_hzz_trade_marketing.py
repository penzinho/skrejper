"""Smoke test — scrape 5 listings from hzz trade_marketing and send the CSV via Resend."""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPTS_DIR.parent

for _p in (str(PROJECT_ROOT), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.scrapers.hzz import scrape_hzz
from send_report import send as send_report

OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDS = [
    "email", "phone", "company", "city", "employer_address",
    "title", "group", "employment_type", "working_hours",
    "valid_from", "valid_to", "detail_url", "source",
]

GROUPS = [
    "Blagajnici/blagajnice, prodavači/prodavačice ulaznica i srodna zanimanja",
    "Prodavači/prodavačice u trgovinama",
    "Prodavači/prodavačice, d. n.",
    "Punjači/punjačice polica",
]

LIMIT = 5

rows: list[dict] = []

print(f"HZZ smoke test — trade_marketing, limit {LIMIT} oglasa", flush=True)

for group in GROUPS:
    if len(rows) >= LIMIT:
        break
    remaining = LIMIT - len(rows)
    print(f"\n  >> {group}", flush=True)
    jobs = scrape_hzz(
        category="trade_marketing",
        group=group,
        max_pages=1,
        results_per_page=75,
        use_subgroups=False,
    )
    for job in jobs[:remaining]:
        rows.append({
            "email": (job.get("email") or "").strip(),
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
        email = rows[-1]["email"]
        print(f"    [{len(rows)}] {job.get('company', '')} <{email}>  ({job.get('location', '')})", flush=True)

date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = OUTPUT_DIR / f"hzz-test-trade_marketing-{date_str}.csv"

with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n{len(rows)} redova zapisano u {output_path}", flush=True)
print("Šaljem email...", flush=True)

ok = send_report(output_path)
sys.exit(0 if ok else 1)
