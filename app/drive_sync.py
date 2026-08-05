"""Sync the seen-id store through Google Drive, so every computer running the
app skips the same postings.

Why this shape:

* **User OAuth, not a service account.** Since Google's 2025 storage-quota
  change, service accounts own no storage: their uploads outside a Workspace
  Shared Drive fail with ``storageQuotaExceeded``. On a personal @gmail.com
  account there are no Shared Drives, so a service account is a dead end. A
  one-time browser consent per computer (installed-app flow with PKCE) is the
  path that works.
* **``appDataFolder``.** The seen files land in Drive's hidden per-app area:
  invisible in the user's Drive UI, impossible to move or rename by accident,
  and scoped so this app sees only its own files (scope ``drive.appdata``).
* **Union merge.** Seen files are append-only sets of posting ids, one per
  line. Local ∪ remote is always correct regardless of which computer ran
  last, so there is no conflict case at all.

The module is stdlib-only on purpose — no Google SDK in the packaged .exe.

Setup (once, by the app's owner): create an OAuth "Desktop app" client in
Google Cloud Console with the Drive API enabled, and paste its client id and
secret into the app. The secret of a desktop-app client is not confidential
(Google's own docs say so); PKCE carries the actual proof.
"""

import base64
import hashlib
import json
import os
import secrets
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from app import seen_store

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
API_BASE = "https://www.googleapis.com/drive/v3"
UPLOAD_BASE = "https://www.googleapis.com/upload/drive/v3"
SCOPE = "https://www.googleapis.com/auth/drive.appdata"

TOKEN_FILE = "google-drive-token.json"


def _token_path() -> Path:
    return seen_store.state_dir() / TOKEN_FILE


def is_connected() -> bool:
    return _token_path().exists()


def disconnect() -> None:
    _token_path().unlink(missing_ok=True)


def _load_token() -> dict:
    return json.loads(_token_path().read_text(encoding="utf-8"))


def _save_token(token: dict) -> None:
    path = _token_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(token), encoding="utf-8")
    try:  # the refresh token is a credential; keep it owner-readable only
        os.chmod(path, 0o600)
    except OSError:
        pass


def _post_form(url: str, fields: dict) -> dict:
    body = urllib.parse.urlencode(fields).encode("ascii")
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


class _CodeCatcher(BaseHTTPRequestHandler):
    """One-shot loopback endpoint for the OAuth redirect."""

    code: str | None = None
    error: str | None = None

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        _CodeCatcher.code = (params.get("code") or [None])[0]
        _CodeCatcher.error = (params.get("error") or [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        message = (
            "Skrejper je povezan s Google Driveom. Možeš zatvoriti ovu karticu."
            if _CodeCatcher.code
            else f"Povezivanje nije uspjelo: {_CodeCatcher.error}"
        )
        self.wfile.write(f"<html><body><h3>{message}</h3></body></html>".encode("utf-8"))

    def log_message(self, *args):  # keep the console quiet
        pass


def connect(client_id: str, client_secret: str, timeout_s: int = 180) -> None:
    """Run the installed-app OAuth flow: open a browser, wait for consent.

    Stores the refresh token (plus the client credentials, so later syncs need
    nothing else) in the state dir. Raises on failure or timeout.
    """
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(48)).rstrip(b"=").decode("ascii")
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )

    with socket.socket() as probe:  # find a free loopback port
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    _CodeCatcher.code = None
    _CodeCatcher.error = None
    server = HTTPServer(("127.0.0.1", port), _CodeCatcher)
    server.timeout = 1
    redirect_uri = f"http://127.0.0.1:{port}"

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",  # guarantees a refresh_token, even on re-consent
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    webbrowser.open(f"{AUTH_URL}?{urllib.parse.urlencode(params)}")

    deadline = time.monotonic() + timeout_s
    try:
        while _CodeCatcher.code is None and _CodeCatcher.error is None:
            if time.monotonic() > deadline:
                raise TimeoutError("Google prijava nije dovršena na vrijeme.")
            server.handle_request()
    finally:
        server.server_close()

    if not _CodeCatcher.code:
        raise RuntimeError(f"Google je odbio prijavu: {_CodeCatcher.error}")

    token = _post_form(
        TOKEN_URL,
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": _CodeCatcher.code,
            "code_verifier": verifier,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
    )
    if "refresh_token" not in token:
        raise RuntimeError("Google nije vratio refresh token — pokušaj ponovno.")
    _save_token(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": token["refresh_token"],
            "access_token": token.get("access_token", ""),
            "expires_at": time.time() + int(token.get("expires_in", 0)),
        }
    )


def _access_token() -> str:
    token = _load_token()
    if token.get("access_token") and time.time() < token.get("expires_at", 0) - 60:
        return token["access_token"]
    refreshed = _post_form(
        TOKEN_URL,
        {
            "client_id": token["client_id"],
            "client_secret": token["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
        },
    )
    token["access_token"] = refreshed["access_token"]
    token["expires_at"] = time.time() + int(refreshed.get("expires_in", 3600))
    _save_token(token)
    return token["access_token"]


def _api(method: str, url: str, body: bytes | None = None, content_type: str | None = None) -> bytes:
    headers = {"Authorization": f"Bearer {_access_token()}"}
    if content_type:
        headers["Content-Type"] = content_type
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def _remote_files() -> dict[str, str]:
    """name -> file id for everything in the app's hidden Drive folder."""
    query = urllib.parse.urlencode(
        {"spaces": "appDataFolder", "fields": "files(id,name)", "pageSize": "100"}
    )
    payload = json.loads(_api("GET", f"{API_BASE}/files?{query}"))
    return {item["name"]: item["id"] for item in payload.get("files", [])}


def _download(file_id: str) -> str:
    return _api("GET", f"{API_BASE}/files/{file_id}?alt=media").decode("utf-8")


def _upload(name: str, content: str, file_id: str | None) -> None:
    if file_id:
        _api(
            "PATCH",
            f"{UPLOAD_BASE}/files/{file_id}?uploadType=media",
            content.encode("utf-8"),
            "text/plain; charset=utf-8",
        )
        return
    boundary = "skrejper-" + secrets.token_hex(8)
    metadata = json.dumps({"name": name, "parents": ["appDataFolder"]})
    body = (
        f"--{boundary}\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{metadata}\r\n"
        f"--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{content}\r\n"
        f"--{boundary}--"
    ).encode("utf-8")
    _api(
        "POST",
        f"{UPLOAD_BASE}/files?uploadType=multipart",
        body,
        f"multipart/related; boundary={boundary}",
    )


def merge_lines(*texts: str) -> str:
    """Union of non-empty lines, sorted for a stable file. Order never mattered:
    the seen store is read into a set."""
    lines = {line.strip() for text in texts for line in text.splitlines() if line.strip()}
    return "\n".join(sorted(lines)) + ("\n" if lines else "")


def sync(log=print) -> int:
    """Two-way sync of every seen-*.txt: local ∪ remote wins on both sides.

    Returns the number of files touched. Raises if not connected.
    """
    state = seen_store.state_dir()
    state.mkdir(parents=True, exist_ok=True)
    remote = _remote_files()
    names = sorted({p.name for p in state.glob("seen-*.txt")} | set(remote) - {TOKEN_FILE})

    touched = 0
    for name in names:
        local_path = state / name
        local = local_path.read_text(encoding="utf-8") if local_path.exists() else ""
        remote_id = remote.get(name)
        remote_text = _download(remote_id) if remote_id else ""
        merged = merge_lines(local, remote_text)

        if merged != local:
            local_path.write_text(merged, encoding="utf-8")
        if merged != merge_lines(remote_text):
            _upload(name, merged, remote_id)
        if merged != local or merged != merge_lines(remote_text):
            touched += 1
            log(f"[drive] {name}: {len(merged.splitlines())} zapisa nakon spajanja")

    if not touched:
        log("[drive] Sve je već usklađeno.")
    return touched
