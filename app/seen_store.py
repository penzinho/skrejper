"""Persistent "already scraped" store, keyed per source.

The expensive part of every scraper is the *per-posting detail fetch* (a full
browser page load for HZZ, a detail HTTP request for Arbeitsagentur). The unique
posting id, however, is known *before* that step — from the listing alone:

* HZZ           -> ``detail_url`` (carries ``WebSifra``, a monotonic db id)
* Arbeitsagentur-> ``refnr`` (``{employer}-{posting token}-S``)

Neither id is ever recycled when a posting expires, so a permanent set of seen
ids lets a daily run skip the detail fetch for everything it already scraped and
only pay for genuinely new postings.

State lives under ``output/state`` because ``output`` is already a persisted
bind-mount in docker-compose, so the set survives container restarts/redeploys.
"""

import os
import threading
from collections.abc import Iterable
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = _PROJECT_ROOT / "output" / "state"

_LOCK = threading.Lock()


def state_dir() -> Path:
    """Where the seen-id files live.

    Defaults to ``output/state`` next to the repo, which is the docker-compose
    bind-mount. The desktop app overrides it via ``SKREJPER_STATE_DIR`` because a
    packaged .app/.exe bundle is read-only, so the state has to live in the
    user's application-data directory to survive an app update.
    """
    override = os.getenv("SKREJPER_STATE_DIR", "").strip()
    return Path(override).expanduser() if override else STATE_DIR


def _path(source: str) -> Path:
    return state_dir() / f"seen-{source}.txt"


def load_seen(source: str) -> set[str]:
    """Return the set of posting ids already scraped for ``source``."""
    path = _path(source)
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}


def add_seen(source: str, ids: Iterable[str]) -> int:
    """Append new posting ids for ``source``; returns how many were written.

    Caller is expected to pass only ids that were not already in ``load_seen``
    (the scrapers skip known ids, so everything they return is new), so this is a
    plain append — no dedup pass over the file.
    """
    new_ids = [str(i).strip() for i in ids if str(i).strip()]
    if not new_ids:
        return 0
    with _LOCK:
        state_dir().mkdir(parents=True, exist_ok=True)
        with _path(source).open("a", encoding="utf-8") as handle:
            handle.write("\n".join(new_ids) + "\n")
    return len(new_ids)
