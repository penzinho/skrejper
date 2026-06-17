"""One-off helper: discover HZZ subgroups for every category and dump them
as a Python dict literal we can paste statically into hzz.py.

Run from the repo root:
    ./venv/Scripts/python.exe apify-actor/discover_subgroups.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from playwright.sync_api import sync_playwright  # noqa: E402

from hzz import (  # noqa: E402
    HZZ_CATEGORIES,
    LANDING_URL,
    _discover_category_group_links,
)


def main() -> None:
    result: dict[str, list[str]] = {}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)

        for key, label in HZZ_CATEGORIES.items():
            groups = _discover_category_group_links(page, label)
            labels = [g["label"] for g in groups]
            result[key] = labels
            print(f"{key}: {len(labels)} subgroups", file=sys.stderr)
            for lab in labels:
                print(f"    - {lab}", file=sys.stderr)

        browser.close()

    out = Path(__file__).resolve().parent / "subgroups.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=4), encoding="utf-8")
    print(f"Wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
