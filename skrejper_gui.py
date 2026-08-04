#!/usr/bin/env python3
"""Skrejper desktop — entry point.

Also the worker entry point: a PyInstaller bundle has no separate python to
spawn, so the app re-launches *itself* with a mode flag. The dispatch has to
happen before Qt is imported, which is why this file stays tiny.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _dispatch(argv: list[str]) -> int:
    mode = argv[1] if len(argv) > 1 else ""

    if mode == "--scrape-worker":
        from app.desktop.runner import main as worker_main

        return worker_main(argv[2:])

    if mode == "--install-browsers":
        from app.desktop.browsers import install_cli

        return install_cli(argv[2:])

    from app.desktop.app import main as gui_main

    return gui_main(argv)


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    raise SystemExit(_dispatch(sys.argv))
