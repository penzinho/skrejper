#!/usr/bin/env python3
"""CLI for the leadgen (BiH/Srbija) pipeline — each stage runs separately.

    python scripts/leadgen.py scrape [--source klix] [--max-pages N] [--full]
                                     [--details]
    python scripts/leadgen.py score
    python scripts/leadgen.py dedupe
    python scripts/leadgen.py export [--out DIR] [--min-ads N]
                                     [--include-public-sector] [--country BA]
    python scripts/leadgen.py import-csv output/*.csv
    python scripts/leadgen.py sync-drive
    python scripts/leadgen.py stats
    python scripts/leadgen.py fetch-fixtures [--source klix]

``run`` chains scrape -> score -> dedupe -> export, which is what a cron wants.

State (database + HTML cache) lives in ``output/state`` (override with
``SKREJPER_STATE_DIR``); polite-crawl knobs via env: ``LEADGEN_DELAY_S``,
``SKREJPER_CONTACT``.
"""

import argparse
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.leadgen import registry
from app.leadgen.db import LeadDb
from app.leadgen.http import Http

OUTPUT_DIR = PROJECT_ROOT / "output"

# Pages saved by fetch-fixtures, used by the parser tests.
FIXTURES = {
    "klix": [
        ("https://posao.klix.ba/poslodavci", "poslodavci-list.html"),
        ("https://posao.klix.ba/oglasi", "oglasi-list.html"),
    ],
    "mojposao_ba": [
        ("https://www.mojposao.ba/api/proxy/jobs/search?page=1", "search-p1.json"),
    ],
    "lakodoposla": [
        ("https://prod.lakodoposla.net/api/postings?page=1", "postings-p1.json"),
    ],
    "poslovi_rs": [
        ("https://www.poslovi.rs/sitemap.xml", "sitemap-sample.xml"),
    ],
}


def _print_event(kind: str, *args) -> None:
    if kind == "log":
        print(args[0], flush=True)
    elif kind == "employer":
        employer = args[0]
        print(f"  [firma] {employer['name']}  ({employer['city']})  JIB={employer['tax_id'] or '-'}", flush=True)
    elif kind == "posting":
        posting = args[0]
        print(f"    [oglas] {posting['title'][:60]}  ({posting['published_at'] or posting['expires_at'] or '?'})", flush=True)


def cmd_scrape(args) -> int:
    sources = args.source or registry.default_sources()
    with LeadDb() as db:
        for name in sources:
            module = registry.get(name)
            if not module.DEFAULT_ENABLED and not args.source:
                continue
            print(f"== {module.LABEL} ==", flush=True)
            http = Http(name, log=lambda line: print(line, flush=True))
            stats = module.scrape(
                db, http,
                max_pages=args.max_pages, full=args.full,
                fetch_details=args.details, on_event=_print_event,
            )
            print(f"[{name}] {stats}", flush=True)
        print(f"Baza: {db.counts()}", flush=True)
    return 0


def cmd_score(args) -> int:
    from app.leadgen.score import compute_scores

    with LeadDb() as db:
        count = compute_scores(db)
        print(f"Izračunato bodovanje za {count} poslodavaca.")
    return 0


def cmd_dedupe(args) -> int:
    from app.leadgen.dedupe import dedupe_employers

    with LeadDb() as db:
        grouped = dedupe_employers(db)
        print(f"Grupirano {grouped} zapisa poslodavaca (preko izvora).")
    return 0


def cmd_export(args) -> int:
    from app.leadgen.export import export_leads

    with LeadDb() as db:
        result = export_leads(
            db, args.out,
            include_public_sector=args.include_public_sector,
            min_ads_24m=args.min_ads,
            countries=tuple(args.country or ()),
        )
    print(f"{result['rows']} leadova → {result['csv']}")
    if result["xlsx"]:
        print(f"          → {result['xlsx']}")
    return 0


def cmd_run(args) -> int:
    code = cmd_scrape(args)
    if code:
        return code
    cmd_score(args)
    cmd_dedupe(args)
    return cmd_export(args)


def cmd_import_csv(args) -> int:
    from app.leadgen.import_csv import import_csv_files

    with LeadDb() as db:
        import_csv_files(db, args.paths)
        print(f"Baza: {db.counts()}")
    return 0


def cmd_sync_drive(args) -> int:
    from app.leadgen import sync

    if not sync.sync():
        print("Google Drive nije povezan (poveži ga u desktop aplikaciji).")
        return 1
    return 0


def cmd_stats(args) -> int:
    with LeadDb() as db:
        counts = db.counts()
        print(f"Poslodavci: {counts['employers']}, oglasi: {counts['postings']}")
        for source in sorted({e["source"] for e in db.employers()}):
            employers = len(db.known_employer_ids(source))
            postings = len(db.known_posting_ids(source))
            print(f"  {source}: {employers} poslodavaca, {postings} oglasa")
    return 0


def cmd_fetch_fixtures(args) -> int:
    """Save live listing pages (plus a few detail pages found on them) as test
    fixtures. Run this from a machine with normal internet access."""
    import re

    fixtures_dir = PROJECT_ROOT / "tests" / "fixtures"
    for source in (args.source or list(FIXTURES)):
        http = Http(source, log=lambda line: print(line, flush=True))
        target_dir = fixtures_dir / source
        target_dir.mkdir(parents=True, exist_ok=True)
        for url, filename in FIXTURES.get(source, []):
            html = http.get(url, use_cache=False)
            (target_dir / filename).write_text(html, encoding="utf-8")
            print(f"{url} → {target_dir / filename}")
        if source == "klix":
            from app.leadgen.sources import klix

            listing = (target_dir / "poslodavci-list.html").read_text(encoding="utf-8")
            entries = klix.parse_entity_links(listing, klix.EMPLOYER_URL_RE)
            for index, entry in enumerate(entries[:2], start=1):
                html = http.get(entry["url"], use_cache=False)
                (target_dir / f"poslodavac-{index}.html").write_text(html, encoding="utf-8")
                print(f"{entry['url']} → poslodavac-{index}.html")
            ads_listing = (target_dir / "oglasi-list.html").read_text(encoding="utf-8")
            ads = klix.parse_entity_links(ads_listing, klix.AD_URL_RE)
            for index, entry in enumerate(ads[:2], start=1):
                html = http.get(entry["url"], use_cache=False)
                (target_dir / f"oglas-{index}.html").write_text(html, encoding="utf-8")
                print(f"{entry['url']} → oglas-{index}.html")
    print("Fixtureovi spremljeni — commitaj tests/fixtures/ da ih testovi koriste.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    scrape = sub.add_parser("scrape", help="skrejpaj izvore u bazu")
    run = sub.add_parser("run", help="scrape + score + dedupe + export")
    for p in (scrape, run):
        p.add_argument("--source", action="append", choices=[s["name"] for s in registry.available()],
                       help="izvor (ponovljivo); default: svi koji nisu iza flaga")
        p.add_argument("--max-pages", type=int, default=None, help="limit stranica listinga (za probu)")
        p.add_argument("--full", action="store_true", help="puni prolaz direktorija (Klix)")
        p.add_argument("--details", action="store_true", help="dohvati i detalj svakog oglasa")

    sub.add_parser("score", help="izračunaj frequency scoring")
    sub.add_parser("dedupe", help="upari poslodavce preko izvora")

    export = sub.add_parser("export", help="izvezi rangirane leadove u CSV/XLSX")
    for p in (export, run):
        p.add_argument("--out", default=str(OUTPUT_DIR), help="mapa za izvoz")
        p.add_argument("--min-ads", type=int, default=0, help="min. oglasa u 24 mj.")
        p.add_argument("--include-public-sector", action="store_true")
        p.add_argument("--country", action="append", help="filtar države (BA, RS, HR, DE; ponovljivo)")

    import_csv = sub.add_parser("import-csv", help="uvezi postojeće HZZ/AA CSV-ove")
    import_csv.add_argument("paths", nargs="+")

    sub.add_parser("sync-drive", help="sinkroniziraj bazu kroz Google Drive")
    sub.add_parser("stats", help="brojke po izvoru")

    fixtures = sub.add_parser("fetch-fixtures", help="spremi žive stranice kao test fixtureove")
    fixtures.add_argument("--source", action="append", choices=list(FIXTURES))

    args = parser.parse_args(argv)
    handlers = {
        "scrape": cmd_scrape, "run": cmd_run, "score": cmd_score, "dedupe": cmd_dedupe,
        "export": cmd_export, "import-csv": cmd_import_csv, "sync-drive": cmd_sync_drive,
        "stats": cmd_stats, "fetch-fixtures": cmd_fetch_fixtures,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
