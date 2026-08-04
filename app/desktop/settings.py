"""Remember what was typed last time, per tab."""

from PySide6.QtCore import QSettings

from app.desktop.paths import APP_NAME, ORG_NAME


def settings() -> QSettings:
    return QSettings(ORG_NAME, APP_NAME)


def load(section: str, defaults: dict) -> dict:
    store = settings()
    store.beginGroup(section)
    values = {}
    for key, default in defaults.items():
        raw = store.value(key, default)
        if isinstance(default, bool):
            # QSettings round-trips booleans as the strings "true"/"false" on some
            # backends, so coerce rather than trusting the type.
            values[key] = raw if isinstance(raw, bool) else str(raw).lower() == "true"
        elif isinstance(default, int):
            try:
                values[key] = int(raw)
            except (TypeError, ValueError):
                values[key] = default
        elif isinstance(default, list):
            values[key] = list(raw) if isinstance(raw, (list, tuple)) else default
        else:
            values[key] = "" if raw is None else str(raw)
    store.endGroup()
    return values


def save(section: str, values: dict) -> None:
    store = settings()
    store.beginGroup(section)
    for key, value in values.items():
        store.setValue(key, value)
    store.endGroup()
    store.sync()
