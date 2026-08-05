"""Google Drive sync of the leadgen database, alongside the seen-file sync.

The SQLite file itself never travels: a binary file synced whole would mean
last-writer-wins and silent data loss the first time two computers scrape on
the same day. Instead each machine uploads a full NDJSON dump of its employers
and postings under its own name (``leadgen-<machine>.ndjson``), and on sync
imports every *other* machine's dump as an upsert (newer ``updated_at`` wins
per record, empty fields never erase filled ones — see ``db.import_ndjson``).
Local ∪ remote, order-independent, no conflict case — the same contract as the
seen-file sync.

Reuses the OAuth token and Drive plumbing from ``app.drive_sync`` without
modifying it; if Drive is not connected this module is a no-op.
"""

import secrets

from app import drive_sync, seen_store
from app.leadgen.db import LeadDb

_MACHINE_FILE = "leadgen-machine-id.txt"
_DUMP_PREFIX = "leadgen-"
_DUMP_SUFFIX = ".ndjson"


def machine_id() -> str:
    """Stable random id per install, so each machine owns one dump file."""
    path = seen_store.state_dir() / _MACHINE_FILE
    if path.exists():
        value = path.read_text(encoding="utf-8").strip()
        if value:
            return value
    value = secrets.token_hex(6)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return value


def sync(db: LeadDb | None = None, log=print) -> bool:
    """Pull other machines' dumps, merge, push our own. False when skipped."""
    if not drive_sync.is_connected():
        return False

    own_name = f"{_DUMP_PREFIX}{machine_id()}{_DUMP_SUFFIX}"
    close_db = db is None
    db = db or LeadDb()
    try:
        remote = drive_sync._remote_files()

        merged = 0
        for name, file_id in remote.items():
            if not name.startswith(_DUMP_PREFIX) or not name.endswith(_DUMP_SUFFIX):
                continue
            if name == own_name:
                continue
            content = drive_sync._download(file_id)
            dump_path = seen_store.state_dir() / f".incoming-{name}"
            dump_path.write_text(content, encoding="utf-8")
            try:
                merged += db.import_ndjson(dump_path)
            finally:
                dump_path.unlink(missing_ok=True)
        if merged:
            log(f"[drive] Leadgen baza: {merged} zapisa preuzeto s drugih računala.")

        dump_path = seen_store.state_dir() / f".outgoing-{own_name}"
        count = db.export_ndjson(dump_path)
        try:
            drive_sync._upload(own_name, dump_path.read_text(encoding="utf-8"), remote.get(own_name))
        finally:
            dump_path.unlink(missing_ok=True)
        log(f"[drive] Leadgen baza: poslan dump ({count} zapisa).")
        return True
    finally:
        if close_db:
            db.close()
