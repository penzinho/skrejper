"""Child-process side of a scrape run.

The GUI never calls a scraper in-process. Playwright's sync API cannot share a
thread with a Qt event loop, and neither scraper has any cancellation support —
no stop flag, no way to break out of the page loop — so the only honest Stop
button is killing a child process. This module is that child.

It speaks NDJSON on stdout: one JSON object per line, consumed by
``app.desktop.process``. Because the scrapers themselves ``print()`` progress
(and ``hzz.py`` does so without ``flush=True``), the real stdout is duplicated
away at startup and ``sys.stdout`` is replaced with a shim that turns every
stray print into a ``log`` event — otherwise those lines would corrupt the
stream.

Rows are written to the CSV as they are accepted, not at the end, so pressing
Stop mid-run still leaves a usable file.
"""

import csv
import io
import json
import os
import sys
import time
import traceback
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from app.desktop import paths

paths.configure_environment()

from app.desktop import exporters
from app.desktop.pipeline import EXCLUDED_COMPANY_TERMS, SOURCE_FIELDS, LeadCollector

_PROGRESS_INTERVAL_S = 0.4


class _EventWriter:
    """Emits NDJSON on the real stdout, which nothing else can write to."""

    def __init__(self, stream: io.TextIOBase) -> None:
        self._stream = stream

    def emit(self, event: str, **payload) -> None:
        payload["event"] = event
        self._stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._stream.flush()


class _LogRedirector(io.TextIOBase):
    """Stand-in for ``sys.stdout`` that forwards scraper prints as log events."""

    def __init__(self, events: _EventWriter) -> None:
        self._events = events
        self._buffer = ""

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._events.emit("log", line=line.rstrip())
        return len(text)

    def flush(self) -> None:
        if self._buffer.strip():
            self._events.emit("log", line=self._buffer.strip())
        self._buffer = ""

    def isatty(self) -> bool:
        return False


class _IncrementalCsv:
    """Writes rows as they arrive so a killed run still leaves a valid file."""

    def __init__(self, path: Path, fields: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._handle = path.open("w", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._handle, fieldnames=fields, quoting=csv.QUOTE_ALL)
        self._writer.writeheader()
        self._handle.flush()

    def write(self, row: dict) -> None:
        self._writer.writerow(row)
        self._handle.flush()

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()


def _slug(value: str) -> str:
    """Filename-safe category slug.

    Underscores are preserved so a category key like ``bau_ausbau`` produces the
    same filename the ``scripts/run_*.py`` runners already produce. Diacritics are
    folded away because a keyword like "Schweisser München" also ends up here, and
    non-ASCII filenames travel badly between macOS and Windows.
    """
    folded = unicodedata.normalize("NFKD", (value or "").strip().casefold())
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    keep = [c if (c.isalnum() and c.isascii()) or c == "_" else "-" for c in folded]
    return "-".join(part for part in "".join(keep).split("-") if part) or "all"


def _run_target(config: dict, target: dict, events: _EventWriter, index: int, total: int) -> dict:
    from app import seen_store

    source = config["source"]
    fields = SOURCE_FIELDS[source]
    category = target.get("category") or ""
    label = target.get("label") or category or "sve"
    output_dir = Path(config["output_dir"])
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    csv_path = output_dir / f"{source}-{_slug(category or label)}-{date_str}.csv"

    seen_emails = seen_store.load_seen(f"{source}-emails") if config.get("skip_seen", True) else set()
    skip_ids = seen_store.load_seen(source) if config.get("skip_seen", True) else set()

    collector = LeadCollector(
        source=source,
        seen_emails=seen_emails,
        dedupe_company=config.get("dedupe_company", True),
        exclude_terms=EXCLUDED_COMPANY_TERMS if config.get("exclude_public_sector") else (),
    )

    events.emit(
        "target_start",
        index=index,
        total=total,
        label=label,
        category=category,
        output=str(csv_path),
    )

    writer = _IncrementalCsv(csv_path, fields)
    last_progress = 0.0

    def on_job(job: dict, group_label: str = "") -> None:
        nonlocal last_progress
        row = collector.add(job, group_label)
        if row is not None:
            writer.write(row)
            events.emit("row", row=row)
        now = time.monotonic()
        if now - last_progress >= _PROGRESS_INTERVAL_S:
            last_progress = now
            events.emit("progress", stats=collector.stats.as_dict())

    try:
        if source == "hzz":
            _scrape_hzz_target(config, target, on_job, skip_ids)
        else:
            _scrape_arbeitsagentur_target(config, target, on_job, skip_ids)
    finally:
        writer.close()
        if config.get("skip_seen", True):
            seen_store.add_seen(source, collector.new_ids)
            seen_store.add_seen(f"{source}-emails", collector.new_emails)

    xlsx_path = None
    if config.get("write_xlsx", True) and exporters.xlsx_available():
        xlsx_path = csv_path.with_suffix(".xlsx")
        exporters.write_xlsx(xlsx_path, collector.rows, fields, sheet_title=label)

    events.emit(
        "target_done",
        index=index,
        total=total,
        label=label,
        csv=str(csv_path),
        xlsx=str(xlsx_path) if xlsx_path else None,
        rows=len(collector.rows),
        stats=collector.stats.as_dict(),
    )
    return {
        "csv": str(csv_path),
        "xlsx": str(xlsx_path) if xlsx_path else None,
        "rows": len(collector.rows),
        "stats": collector.stats.as_dict(),
    }


def _scrape_hzz_target(config: dict, target: dict, on_job, skip_ids: set[str]) -> None:
    from app.scrapers.hzz import scrape_hzz

    options = config.get("options", {})
    category = target.get("category") or None
    groups = [g for g in (target.get("groups") or []) if g]

    common = dict(
        max_pages=int(options.get("max_pages") or 999),
        results_per_page=options.get("results_per_page") or 75,
        start_page=int(options.get("start_page") or 1),
        company_limit=options.get("company_limit") or None,
        skip_ids=skip_ids,
    )

    if groups:
        # One call per subgroup, exactly like scripts/scrape_hzz_category.py.
        for group in groups:
            print(f"  >> {group}")
            scrape_hzz(
                category=category,
                group=group,
                use_subgroups=False,
                on_job=lambda job, _g=group: on_job(job, _g),
                **common,
            )
    else:
        scrape_hzz(
            category=category,
            use_subgroups=True,
            on_job=lambda job: on_job(job, ""),
            **common,
        )


def _scrape_arbeitsagentur_target(config: dict, target: dict, on_job, skip_ids: set[str]) -> None:
    from app.scrapers.arbeitsagentur import scrape_arbeitsagentur

    options = config.get("options", {})
    scrape_arbeitsagentur(
        category=target.get("category") or None,
        max_pages=int(options.get("max_pages") or 1),
        results_per_page=int(options.get("results_per_page") or 100),
        company_limit=options.get("company_limit") or None,
        listing_limit=options.get("listing_limit") or None,
        keyword=options.get("keyword") or None,
        location=options.get("location") or None,
        radius=options.get("radius") or None,
        on_job=lambda job: on_job(job, target.get("label") or ""),
        skip_ids=skip_ids,
    )


def _claim_stdout():
    """Take a private handle on stdout before it is replaced by the log shim.

    Duplicating the file descriptor keeps the event channel out of reach of the
    redirection. When stdout has no descriptor (a test harness, an embedded
    interpreter), the original object is used directly — the shim replaces
    ``sys.stdout``, not this reference, so events still get through.
    """
    try:
        return os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8", buffering=1)
    except (AttributeError, OSError, io.UnsupportedOperation):
        return sys.stdout


def _install_stop_handler() -> None:
    """Turn the GUI's Stop (SIGTERM) into a KeyboardInterrupt.

    That unwinds through the ``finally`` blocks, so the CSV is closed cleanly and
    the ids harvested so far still land in seen_store. Windows has no SIGTERM —
    there the process is killed outright, which is why rows are flushed to disk
    as they arrive rather than at the end.
    """
    import signal

    if not hasattr(signal, "SIGTERM"):
        return

    def _stop(_signum, _frame):
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGTERM, _stop)
    except (ValueError, OSError):
        pass


def _facet_names(payload: dict) -> list[str]:
    facets = payload.get("facetten")
    return sorted(facets) if isinstance(facets, dict) else []


def _facet_values(payload: dict, limit: int = 30) -> list[str]:
    """Best-effort read of the occupation facet, whatever the board calls it.

    The response shape is not pinned down anywhere we can rely on, so this
    tolerates a dict of counts, a list of dicts, or a plain list, and gives up
    quietly rather than breaking the probe.
    """
    facets = payload.get("facetten")
    if not isinstance(facets, dict):
        return []

    for key, facet in facets.items():
        if "beruf" not in key.casefold():
            continue
        counts = facet.get("counts") if isinstance(facet, dict) else facet
        if isinstance(counts, dict):
            ordered = sorted(counts.items(), key=lambda kv: -(kv[1] or 0))
            return [f"{name} ({count})" for name, count in ordered[:limit]]
        if isinstance(counts, list):
            out = []
            for item in counts[:limit]:
                if isinstance(item, dict):
                    label = item.get("value") or item.get("name") or item.get("label")
                    out.append(f"{label} ({item.get('count', '?')})")
                else:
                    out.append(str(item))
            return out
    return []


def _probe(config: dict, events: _EventWriter) -> int:
    """Answer "does this work, and if not why" in one click.

    A full run takes minutes and buries the answer in per-category noise. This
    makes two requests — one unfiltered, one with a Berufsfeld — because the
    difference between them is what separates "the board is down" from "our
    category names no longer match the board's".
    """
    events.emit("log", line=f"[gui] Skrejper {paths.version_string()}")

    from app.scrapers import arbeitsagentur

    timeout = int(os.getenv("ARBEITSAGENTUR_SEARCH_TIMEOUT", "60"))
    keyword = config.get("keyword") or None

    payload = arbeitsagentur._search_json(None, keyword, None, None, 1, 1, timeout)
    if payload is None:
        # _search_json has already logged the status and the paths it tried.
        events.emit(
            "probe",
            ok=False,
            message="Nijedan poznati endpoint ne odgovara — detalji su u logu.",
        )
        return 1

    total = payload.get("maxErgebnisse")
    endpoint = arbeitsagentur._search_url or ""

    # Which Berufsfeld to test with: whatever the tab had selected, else the
    # first one we know about.
    berufsfeld = config.get("berufsfeld")
    if not berufsfeld:
        groups = arbeitsagentur.get_arbeitsagentur_categories()
        berufsfeld = groups[0]["berufsfelder"][0] if groups else None

    field_total = None
    if berufsfeld:
        field_payload = arbeitsagentur._search_json(berufsfeld, None, None, None, 1, 1, timeout)
        field_total = (field_payload or {}).get("maxErgebnisse")
        _emit_line = f"[arbeitsagentur] berufsfeld={berufsfeld!r} -> {field_total} oglasa"
        events.emit("log", line=_emit_line)

    names = _facet_names(payload)
    if names:
        events.emit("log", line=f"[arbeitsagentur] facetten: {', '.join(names)}")
    for value in _facet_values(payload):
        events.emit("log", line=f"[arbeitsagentur]   {value}")

    dump_path = None
    output_dir = config.get("output_dir")
    if output_dir:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dump = Path(output_dir) / f"arbeitsagentur-probe-{date_str}.json"
        dump.parent.mkdir(parents=True, exist_ok=True)
        dump.write_text(
            json.dumps(
                {"endpoint": endpoint, "unfiltered": payload, "berufsfeld": berufsfeld},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        dump_path = str(dump)

    events.emit(
        "probe",
        ok=True,
        endpoint=endpoint,
        total=total,
        berufsfeld=berufsfeld,
        berufsfeld_total=field_total,
        dump=dump_path,
    )
    return 0


def run(config: dict, events: _EventWriter) -> int:
    if config.get("mode") == "probe":
        try:
            return _probe(config, events)
        except Exception as exc:
            events.emit("error", message=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc())
            return 1

    targets = config.get("targets") or [{}]
    # First line of every run, so a pasted log always says which build wrote it.
    events.emit("log", line=f"[gui] Skrejper {paths.version_string()}")
    events.emit("start", source=config.get("source"), targets=len(targets))

    results = []
    total_rows = 0
    try:
        for index, target in enumerate(targets, start=1):
            result = _run_target(config, target, events, index, len(targets))
            results.append(result)
            total_rows += result["rows"]
    except KeyboardInterrupt:
        events.emit("cancelled", files=[r["csv"] for r in results], rows=total_rows)
        return 130
    except Exception as exc:
        events.emit(
            "error",
            message=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc(),
            files=[r["csv"] for r in results],
        )
        return 1

    events.emit("done", results=results, rows=total_rows)
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: --scrape-worker <config.json>", file=sys.stderr)
        return 2

    events = _EventWriter(_claim_stdout())
    sys.stdout = _LogRedirector(events)
    _install_stop_handler()

    try:
        config = json.loads(Path(argv[0]).read_text(encoding="utf-8"))
    except Exception as exc:
        events.emit("error", message=f"Neispravna konfiguracija: {exc}", traceback=traceback.format_exc())
        return 2

    try:
        return run(config, events)
    finally:
        sys.stdout.flush()
        sys.stdout = sys.__stdout__
