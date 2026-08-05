"""Source adapter registry.

A new job board = one new module in ``app/leadgen/sources/`` registered here.
Each module defines:

* ``SOURCE``           short id, also the database's source key
* ``LABEL``            display name
* ``COUNTRY``          "BA" / "RS" / ...
* ``DEFAULT_ENABLED``  False for sources behind a deliberate flag (ToS or
                       robots concerns — Infostud, FZZZ, direct ZZZ RS); they
                       never run unless explicitly requested by name
* ``scrape(db, http, *, max_pages=None, full=False, fetch_details=False,
            on_event=None, should_stop=None) -> dict`` (stats)
"""

from importlib import import_module

_SOURCE_MODULES = {
    "klix": "app.leadgen.sources.klix",
    "mojposao_ba": "app.leadgen.sources.mojposao_ba",
    "lakodoposla": "app.leadgen.sources.lakodoposla",
    "poslovi_rs": "app.leadgen.sources.poslovi_rs",
    "nsz": "app.leadgen.sources.nsz",
    "infostud": "app.leadgen.sources.infostud",  # DEFAULT_ENABLED=False (ToS)
    # Još neimplementirano — s cloud egressa nedostupni (provjereno 2026-08-05):
    #   "boljiposao"  prazan HTML shell (JS-only render iza Cloudflarea) — treba headless
    #   "fzzz"        fzzz.ba TCP-resetira datacenter IP-eve; prvi korak je s
    #                 korisnikovog računala snimiti XHR pozive /joboffer stranice
    #   "zzzrs"       prijava.zzzrs.net isto TCP-resetira; alternativa Glas Srpske
    #                 (tjedni bilteni) vraća Cloudflare 403 na ovaj egress
}


def get(name: str):
    if name not in _SOURCE_MODULES:
        raise KeyError(f"Nepoznat izvor {name!r}; postoje: {sorted(_SOURCE_MODULES)}")
    return import_module(_SOURCE_MODULES[name])


def available() -> list[dict]:
    out = []
    for name in _SOURCE_MODULES:
        module = get(name)
        out.append({
            "name": name,
            "label": module.LABEL,
            "country": module.COUNTRY,
            "default_enabled": module.DEFAULT_ENABLED,
        })
    return out


def default_sources() -> list[str]:
    return [entry["name"] for entry in available() if entry["default_enabled"]]
