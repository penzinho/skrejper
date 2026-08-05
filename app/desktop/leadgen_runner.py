"""Child-process side of a leadgen (BiH/Srbija) run.

Delegated to from ``app.desktop.runner`` when ``config["source"] == "leadgen"``,
so it speaks the exact same NDJSON event protocol the GUI already consumes.

Unlike the HZZ/AA runner there is no CSV-per-category: postings and employers
stream into the SQLite database as they arrive (each upsert commits, so Stop
loses nothing), and the final step computes scoring + dedupe and exports the
ranked leads file.
"""

import time
from pathlib import Path

from app.leadgen import registry
from app.leadgen.db import LeadDb
from app.leadgen.http import Http

_PROGRESS_INTERVAL_S = 0.4


def run(config: dict, events) -> int:
    sources = config.get("sources") or registry.default_sources()
    output_dir = Path(config.get("output_dir") or ".")
    total = len(sources) + 1  # + the scoring/export step

    events.emit("start", source="leadgen", targets=total)

    stats_total = {"employers": 0, "postings": 0, "requests": 0}
    files: list[str] = []
    cancelled = False

    with LeadDb() as db:
        for index, name in enumerate(sources, start=1):
            if cancelled:
                break
            module = registry.get(name)
            events.emit("target_start", index=index, total=total, label=module.LABEL)
            http = Http(name, log=lambda line: events.emit("log", line=line))

            last_progress = 0.0

            def on_event(kind, *args):
                nonlocal last_progress
                if kind == "log":
                    events.emit("log", line=args[0])
                    return
                if kind == "employer":
                    stats_total["employers"] += 1
                elif kind == "posting":
                    posting = args[0]
                    employer = args[1] if len(args) > 1 else None
                    stats_total["postings"] += 1
                    events.emit("row", row={
                        "company": (employer or {}).get("name", ""),
                        "title": posting.get("title", ""),
                        "city": posting.get("city", "") or (employer or {}).get("city", ""),
                        "published_at": posting.get("published_at") or posting.get("expires_at", ""),
                        "source": name,
                    })
                now = time.monotonic()
                if now - last_progress >= _PROGRESS_INTERVAL_S:
                    last_progress = now
                    stats_total["requests"] = http.requests_made
                    events.emit("progress", stats=dict(stats_total))

            try:
                stats = module.scrape(
                    db, http,
                    max_pages=config.get("max_pages") or None,
                    full=config.get("full", False),
                    fetch_details=config.get("fetch_details", False),
                    on_event=on_event,
                )
            except KeyboardInterrupt:
                cancelled = True
                stats = {}
            stats_total["requests"] = http.requests_made
            events.emit(
                "target_done", index=index, total=total, label=module.LABEL,
                csv=None, xlsx=None, rows=stats.get("postings", 0),
                stats=dict(stats_total),
            )

        # Scoring + dedupe + export always run, also after a Stop — whatever is
        # in the database is worth a fresh leads file.
        events.emit("target_start", index=total, total=total, label="Bodovanje i izvoz leadova")
        try:
            from app.leadgen.dedupe import dedupe_employers
            from app.leadgen.export import export_leads
            from app.leadgen.score import compute_scores

            scored = compute_scores(db)
            grouped = dedupe_employers(db)
            events.emit("log", line=f"[leadgen] Bodovano {scored} poslodavaca, {grouped} upareno preko izvora.")
            result = export_leads(
                db, output_dir,
                include_public_sector=config.get("include_public_sector", False),
                min_ads_24m=int(config.get("min_ads_24m") or 0),
                write_xlsx=config.get("write_xlsx", True),
            )
            files = [p for p in (result["csv"], result["xlsx"]) if p]
            events.emit(
                "target_done", index=total, total=total, label="Bodovanje i izvoz leadova",
                csv=result["csv"], xlsx=result["xlsx"], rows=result["rows"],
                stats=dict(stats_total),
            )
            rows = result["rows"]
        except KeyboardInterrupt:
            cancelled = True
            rows = 0

    if cancelled:
        events.emit("cancelled", files=files, rows=rows)
        return 130
    events.emit("done", results=[], rows=rows, files=files)
    return 0
