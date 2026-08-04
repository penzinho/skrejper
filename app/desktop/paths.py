"""Where the desktop app keeps its files.

A packaged .app/.exe bundle is read-only and gets replaced wholesale on every
update, so nothing writable may live next to the executable: browsers, the
seen-id state and the exports all go to per-user directories instead.
"""

import os
import sys
from pathlib import Path

APP_NAME = "Skrejper"
ORG_NAME = "Protalent"


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def project_root() -> Path:
    """Repo root when running from source; the bundle dir when frozen."""
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def resource_dir() -> Path:
    """Bundled images and styles.

    PyInstaller unpacks data files to ``sys._MEIPASS``, which is neither next to
    the executable nor next to this module — inside a macOS .app it is a
    different directory tree entirely.
    """
    unpacked = getattr(sys, "_MEIPASS", None)
    if unpacked:
        return Path(unpacked) / "app" / "desktop" / "resources"
    return Path(__file__).resolve().parent / "resources"


def app_data_dir() -> Path:
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    elif os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
    else:
        base = Path(os.getenv("XDG_DATA_HOME") or Path.home() / ".local" / "share")
    return base / APP_NAME


def browsers_dir() -> Path:
    """Playwright browser cache. Not bundled — it is ~150 MB and downloaded once."""
    return app_data_dir() / "ms-playwright"


def state_dir() -> Path:
    """Where ``app.seen_store`` keeps the "already scraped" id lists."""
    return app_data_dir() / "state"


def default_output_dir() -> Path:
    documents = Path.home() / "Documents"
    base = documents if documents.is_dir() else Path.home()
    return base / APP_NAME


def configure_environment() -> None:
    """Point Playwright and seen_store at the per-user directories.

    Must run before ``playwright`` is imported. Existing values win, so a power
    user can still override either from the shell.
    """
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(browsers_dir()))
    os.environ.setdefault("SKREJPER_STATE_DIR", str(state_dir()))
