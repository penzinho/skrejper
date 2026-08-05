# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller build for the Skrejper desktop app.

onedir, not onefile, on both platforms: onefile cannot produce a well-formed
macOS .app, and on Windows it would unpack Playwright's node driver on every
launch, which makes startup crawl. The output is zipped by CI instead.

Build:  pyinstaller packaging/skrejper.spec --noconfirm
"""

import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_all

ROOT = Path(SPECPATH).resolve().parent
RESOURCES = ROOT / "app" / "desktop" / "resources"

# Playwright ships a Node driver and its own package data; without collect_all
# the frozen app can find neither.
playwright_datas, playwright_binaries, playwright_hidden = collect_all("playwright")

datas = playwright_datas + [(str(RESOURCES), "app/desktop/resources")]

# The server stack is installed in some environments but never imported by the
# app; excluding it keeps the bundle from doubling in size.
EXCLUDES = [
    "fastapi", "uvicorn", "starlette", "pydantic", "pydantic_core", "resend",
    "celery", "redis", "supabase", "pandas", "numpy", "selenium",
    "trio", "trio_websocket", "matplotlib", "tkinter", "IPython", "pytest",
    "PySide6.QtWebEngineCore", "PySide6.QtWebEngineWidgets", "PySide6.Qt3DCore",
    "PySide6.QtCharts", "PySide6.QtDataVisualization", "PySide6.QtMultimedia",
    "PySide6.QtQuick", "PySide6.QtQml",
]


def platform_icon():
    candidate = RESOURCES / ("icon.icns" if sys.platform == "darwin" else "icon.ico")
    return str(candidate) if candidate.exists() else None


a = Analysis(
    [str(ROOT / "skrejper_gui.py")],
    pathex=[str(ROOT)],
    binaries=playwright_binaries,
    datas=datas,
    # build_info is written by CI and imported inside a try/except, which static
    # analysis does not see; it simply does not exist in a source checkout.
    hiddenimports=(
        playwright_hidden
        + ["openpyxl", "app.desktop.runner", "app.desktop.browsers"]
        # The leadgen source adapters are loaded by name through
        # importlib.import_module (see app/leadgen/registry.py), which static
        # analysis cannot see. Listing them from disk keeps the spec in sync
        # with new adapters automatically; their static dependencies
        # (requests, bs4, lxml, rapidfuzz) follow through modulegraph.
        + [
            f"app.leadgen.sources.{path.stem}"
            for path in sorted((ROOT / "app" / "leadgen" / "sources").glob("*.py"))
            if path.stem != "__init__"
        ]
        + (["app.desktop.build_info"] if (ROOT / "app/desktop/build_info.py").exists() else [])
    ),
    hookspath=[],
    runtime_hooks=[],
    excludes=EXCLUDES,
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Skrejper",
    debug=False,
    strip=False,
    upx=False,
    # The app re-launches itself as a scrape worker. Keeping it windowed means no
    # console flashes on Windows; the worker's output is read through a pipe, not
    # a terminal, so nothing is lost.
    console=False,
    icon=platform_icon(),
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name="Skrejper",
)

if sys.platform == "darwin":
    app = BUNDLE(
        coll,
        name="Skrejper.app",
        icon=platform_icon(),
        bundle_identifier="hr.protalent.skrejper",
        info_plist={
            "CFBundleName": "Skrejper",
            "CFBundleDisplayName": "Skrejper",
            "CFBundleShortVersionString": "1.0.0",
            "CFBundleVersion": "1.0.0",
            "NSHighResolutionCapable": True,
            "LSMinimumSystemVersion": "11.0",
            "LSApplicationCategoryType": "public.app-category.productivity",
        },
    )
