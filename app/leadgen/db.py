"""SQLite store for the leadgen pipeline, plus NDJSON dump/merge.

One file, no server. Lives next to the ``seen-*.txt`` files (same
``SKREJPER_STATE_DIR`` override), so the desktop app and the server scripts
share the location logic that already exists.

Records are keyed by ``(source, source_id)`` — never by rowid — so a dump from
one machine imports into another as a plain upsert: the record with the newer
``updated_at`` wins, and an empty incoming field never erases a filled local
one. That makes the Google Drive sync a union, exactly like the seen store.

Presence in the ``postings`` table doubles as the "already scraped" set: an
adapter checks ``known_posting_ids`` before paying for a detail fetch.
"""

import json
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path

from app import seen_store
from app.leadgen.schema import EMPLOYER_FIELDS, POSTING_FIELDS, now_iso

DB_FILE = "leadgen.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS employers (
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    legal_name TEXT NOT NULL DEFAULT '',
    tax_id TEXT NOT NULL DEFAULT '',
    vat_id TEXT NOT NULL DEFAULT '',
    address TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    country TEXT NOT NULL DEFAULT '',
    website TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    detail_url TEXT NOT NULL DEFAULT '',
    public_sector_term TEXT NOT NULL DEFAULT '',
    canonical_key TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, source_id)
);
CREATE TABLE IF NOT EXISTS postings (
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    employer_source TEXT NOT NULL DEFAULT '',
    employer_source_id TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    expires_at TEXT NOT NULL DEFAULT '',
    workers_count TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    contact_email TEXT NOT NULL DEFAULT '',
    contact_phone TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    detail_url TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_postings_employer
    ON postings (employer_source, employer_source_id);
CREATE TABLE IF NOT EXISTS cursors (
    source TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, key)
);
CREATE TABLE IF NOT EXISTS scores (
    employer_source TEXT NOT NULL,
    employer_source_id TEXT NOT NULL,
    total_ads INTEGER NOT NULL DEFAULT 0,
    ads_24m INTEGER NOT NULL DEFAULT 0,
    distinct_titles_24m INTEGER NOT NULL DEFAULT 0,
    repeated_titles_24m INTEGER NOT NULL DEFAULT 0,
    first_ad TEXT NOT NULL DEFAULT '',
    last_ad TEXT NOT NULL DEFAULT '',
    score REAL NOT NULL DEFAULT 0,
    computed_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (employer_source, employer_source_id)
);
"""

_TABLE_FIELDS = {
    "employers": EMPLOYER_FIELDS,
    "postings": POSTING_FIELDS,
}


def db_path() -> Path:
    return seen_store.state_dir() / DB_FILE


class LeadDb:
    """Thin wrapper; one connection, safe for the single-writer use we have."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "LeadDb":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- upserts ---------------------------------------------------------

    def _upsert(self, table: str, record: dict) -> bool:
        """Field-wise merge upsert. Returns True when the row changed.

        The *newer* record's non-empty values win; empty values never erase
        filled ones. ``canonical_key`` is preserved from the existing row (it
        is derived data, recomputed by dedupe, not part of a scrape).
        """
        fields = _TABLE_FIELDS[table]
        key = (record["source"], record["source_id"])
        with self._lock:
            row = self._conn.execute(
                f"SELECT * FROM {table} WHERE source=? AND source_id=?", key
            ).fetchone()
            if row is None:
                merged = {field: record.get(field, "") for field in fields}
            else:
                existing = dict(row)
                incoming_newer = (record.get("updated_at") or "") >= (existing.get("updated_at") or "")
                preferred, fallback = (record, existing) if incoming_newer else (existing, record)
                merged = {
                    field: (preferred.get(field) or fallback.get(field) or "")
                    for field in fields
                }
                merged["source"], merged["source_id"] = key
                if "canonical_key" in existing and existing["canonical_key"]:
                    merged["canonical_key"] = existing["canonical_key"]
                if merged == {f: existing.get(f, "") for f in fields}:
                    return False
                merged["updated_at"] = max(
                    record.get("updated_at") or "", existing.get("updated_at") or ""
                ) or now_iso()
            placeholders = ", ".join("?" for _ in fields)
            self._conn.execute(
                f"INSERT OR REPLACE INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
                [merged.get(field, "") for field in fields],
            )
            self._conn.commit()
            return True

    def upsert_employer(self, employer: dict) -> bool:
        return self._upsert("employers", employer)

    def upsert_posting(self, posting: dict) -> bool:
        return self._upsert("postings", posting)

    # ---- reads -----------------------------------------------------------

    def get_employer(self, source: str, source_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM employers WHERE source=? AND source_id=?", (source, source_id)
        ).fetchone()
        return dict(row) if row else None

    def employers(self, source: str | None = None) -> list[dict]:
        if source:
            rows = self._conn.execute("SELECT * FROM employers WHERE source=?", (source,))
        else:
            rows = self._conn.execute("SELECT * FROM employers")
        return [dict(row) for row in rows]

    def postings(self, source: str | None = None) -> list[dict]:
        if source:
            rows = self._conn.execute("SELECT * FROM postings WHERE source=?", (source,))
        else:
            rows = self._conn.execute("SELECT * FROM postings")
        return [dict(row) for row in rows]

    def known_posting_ids(self, source: str) -> set[str]:
        rows = self._conn.execute("SELECT source_id FROM postings WHERE source=?", (source,))
        return {row["source_id"] for row in rows}

    def known_employer_ids(self, source: str) -> set[str]:
        rows = self._conn.execute("SELECT source_id FROM employers WHERE source=?", (source,))
        return {row["source_id"] for row in rows}

    def counts(self) -> dict[str, int]:
        out = {}
        for table in ("employers", "postings"):
            out[table] = self._conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        return out

    # ---- cursors (incremental scrape state, per machine — not synced) ----

    def get_cursor(self, source: str, key: str, default: str = "") -> str:
        row = self._conn.execute(
            "SELECT value FROM cursors WHERE source=? AND key=?", (source, key)
        ).fetchone()
        return row["value"] if row else default

    def set_cursor(self, source: str, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO cursors (source, key, value) VALUES (?, ?, ?)",
                (source, key, str(value)),
            )
            self._conn.commit()

    # ---- scores (derived; recomputed locally, not synced) ----------------

    def write_scores(self, rows: Iterable[dict]) -> int:
        columns = [
            "employer_source", "employer_source_id", "total_ads", "ads_24m",
            "distinct_titles_24m", "repeated_titles_24m", "first_ad", "last_ad",
            "score", "computed_at",
        ]
        count = 0
        with self._lock:
            self._conn.execute("DELETE FROM scores")
            for row in rows:
                self._conn.execute(
                    f"INSERT OR REPLACE INTO scores ({', '.join(columns)}) "
                    f"VALUES ({', '.join('?' for _ in columns)})",
                    [row.get(column, "" if isinstance(row.get(column), str) else 0) for column in columns],
                )
                count += 1
            self._conn.commit()
        return count

    def scores(self) -> list[dict]:
        return [dict(row) for row in self._conn.execute("SELECT * FROM scores")]

    def set_canonical_keys(self, mapping: dict[tuple[str, str], str]) -> None:
        """Dedupe result: (source, source_id) -> canonical "source:id" key."""
        with self._lock:
            for (source, source_id), canonical in mapping.items():
                self._conn.execute(
                    "UPDATE employers SET canonical_key=? WHERE source=? AND source_id=?",
                    (canonical, source, source_id),
                )
            self._conn.commit()

    # ---- NDJSON dump / merge (Drive sync + manual backup) ----------------

    def export_ndjson(self, path: Path | str) -> int:
        """Full dump of employers + postings, one JSON object per line."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with path.open("w", encoding="utf-8") as handle:
            for table in ("employers", "postings"):
                for row in self._conn.execute(f"SELECT * FROM {table}"):
                    record = dict(row)
                    record["_table"] = table
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
        return count

    def import_ndjson(self, path: Path | str) -> int:
        """Merge a dump in; returns how many rows changed locally."""
        changed = 0
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                table = record.pop("_table", "")
                if table not in _TABLE_FIELDS:
                    continue
                record.pop("canonical_key", None)  # derived locally
                clean = {
                    field: str(record.get(field) or "") for field in _TABLE_FIELDS[table]
                }
                if not clean["source"] or not clean["source_id"]:
                    continue
                if self._upsert(table, clean):
                    changed += 1
        return changed
