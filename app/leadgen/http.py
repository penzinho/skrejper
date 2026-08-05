"""Polite fetch layer: rate limit per host, honest UA, backoff, disk cache.

Every leadgen adapter fetches through one ``Http`` instance. Development and
re-parsing hit the on-disk cache instead of the source site; incremental runs
pass a ``max_age`` so listing pages refresh while detail pages, whose content
never changes once published, stay cached forever.

The cache lives under ``<state dir>/cache/<source>/`` — next to the database,
covered by the same ``SKREJPER_STATE_DIR`` override — gzipped, one file per
URL, with a tiny ``.json`` sidecar recording the URL and fetch time.
"""

import gzip
import hashlib
import json
import os
import random
import time
import urllib.parse
from pathlib import Path

import requests

from app import seen_store

DEFAULT_CONTACT = os.getenv("SKREJPER_CONTACT", "app@protalent.hr")
USER_AGENT = f"SkrejperLeadgen/1.0 (B2B kontakt istrazivanje; kontakt: {DEFAULT_CONTACT})"

# Seconds between two requests to the same host; override per adapter or via env.
DEFAULT_DELAY_S = float(os.getenv("LEADGEN_DELAY_S", "1.5"))


class FetchError(Exception):
    """Definitive failure after retries; carries the last HTTP status if any."""

    def __init__(self, url: str, status: int | None, message: str) -> None:
        super().__init__(message)
        self.url = url
        self.status = status


def cache_dir() -> Path:
    return seen_store.state_dir() / "cache"


class Http:
    def __init__(
        self,
        source: str,
        min_delay_s: float = DEFAULT_DELAY_S,
        timeout_s: float = 30,
        cache_root: Path | None = None,
        log=print,
    ) -> None:
        self.source = source
        self.min_delay_s = min_delay_s
        self.timeout_s = timeout_s
        self.cache_root = Path(cache_root) if cache_root else cache_dir()
        self.log = log
        self._last_request: dict[str, float] = {}  # host -> monotonic time
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "bs,hr,sr;q=0.9,en;q=0.5",
        })
        self.requests_made = 0
        self.cache_hits = 0

    # ---- cache -----------------------------------------------------------

    def _cache_paths(self, url: str) -> tuple[Path, Path]:
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
        base = self.cache_root / self.source / digest[:2]
        return base / f"{digest}.html.gz", base / f"{digest}.json"

    def _cache_read(self, url: str, max_age_s: float | None) -> str | None:
        body_path, meta_path = self._cache_paths(url)
        if not body_path.exists() or not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if max_age_s is not None and time.time() - float(meta.get("fetched_at", 0)) > max_age_s:
                return None
            return gzip.decompress(body_path.read_bytes()).decode(
                meta.get("encoding") or "utf-8", errors="replace"
            )
        except (OSError, ValueError, EOFError):
            return None  # corrupt cache entry -> refetch

    def _cache_write(self, url: str, text: str) -> None:
        body_path, meta_path = self._cache_paths(url)
        body_path.parent.mkdir(parents=True, exist_ok=True)
        body_path.write_bytes(gzip.compress(text.encode("utf-8")))
        meta_path.write_text(
            json.dumps({"url": url, "fetched_at": time.time(), "encoding": "utf-8"}),
            encoding="utf-8",
        )

    # ---- fetching --------------------------------------------------------

    def _throttle(self, url: str) -> None:
        host = urllib.parse.urlparse(url).netloc
        elapsed = time.monotonic() - self._last_request.get(host, 0.0)
        wait = self.min_delay_s + random.uniform(0, self.min_delay_s / 4) - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request[host] = time.monotonic()

    def get(
        self,
        url: str,
        *,
        max_age_s: float | None = None,
        use_cache: bool = True,
        attempts: int = 4,
    ) -> str:
        """GET a page as text. ``max_age_s=None`` means any cached copy is fine
        (right for detail pages); pass e.g. ``3600`` for listing pages. A 404
        raises ``FetchError(status=404)`` immediately — expired postings are a
        routine condition the caller handles.
        """
        if use_cache:
            cached = self._cache_read(url, max_age_s)
            if cached is not None:
                self.cache_hits += 1
                return cached

        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            self._throttle(url)
            try:
                response = self._session.get(url, timeout=self.timeout_s, allow_redirects=True)
                self.requests_made += 1
            except requests.TooManyRedirects as exc:
                # A redirect loop is not transient; retrying just burns delay.
                raise FetchError(url, None, f"Redirect petlja: {url}") from exc
            except requests.RequestException as exc:
                last_error = exc
                self.log(f"[{self.source}] {url}: {type(exc).__name__}, pokušaj {attempt}/{attempts}")
            else:
                if response.status_code == 200:
                    text = response.text
                    if use_cache:
                        self._cache_write(url, text)
                    return text
                if response.status_code in (404, 410):
                    raise FetchError(url, response.status_code, f"HTTP {response.status_code} za {url}")
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "")
                    pause = float(retry_after) if retry_after.isdigit() else 30.0 * attempt
                    self.log(f"[{self.source}] 429 — čekam {pause:.0f}s ({url})")
                    time.sleep(pause)
                    continue
                last_error = FetchError(url, response.status_code, f"HTTP {response.status_code}")
                if response.status_code in (401, 403):
                    # A block is not transient; retrying faster makes it worse.
                    break
            if attempt < attempts:
                time.sleep(2 ** attempt + random.uniform(0, 1))

        raise FetchError(url, getattr(last_error, "status", None), f"Odustajem od {url}: {last_error}")

    def get_or_none(self, url: str, **kwargs) -> str | None:
        """Like ``get`` but expired/blocked pages come back as ``None``."""
        try:
            return self.get(url, **kwargs)
        except FetchError:
            return None
