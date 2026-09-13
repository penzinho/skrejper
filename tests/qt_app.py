"""The one Qt application object a test run is allowed to have.

Qt permits exactly one, and its type decides what the rest of the suite can do:
a bare `QCoreApplication` is enough for signals and timers but cannot host a
widget, so whichever module happened to create it first used to decide whether
the widget tests could run at all — with an abort, not a skip.

So every module asks for the app through here. A widget-capable `QApplication`
is created when the platform can actually open one (a display, or an explicit
`QT_QPA_PLATFORM` such as the `offscreen` CI uses); otherwise a
`QCoreApplication`, because instantiating a `QApplication` with no platform
plugin kills the process instead of raising.
"""

import os


def _platform_available() -> bool:
    return bool(
        os.environ.get("QT_QPA_PLATFORM")
        or os.environ.get("DISPLAY")
        or os.environ.get("WAYLAND_DISPLAY")
    )


def qt_app():
    """The shared application object, created on first use."""
    from PySide6.QtCore import QCoreApplication

    existing = QCoreApplication.instance()
    if existing is not None:
        return existing
    if _platform_available():
        try:
            from PySide6.QtWidgets import QApplication

            return QApplication([])
        except ImportError:  # Qt built without the widget libraries
            pass
    return QCoreApplication([])


def widget_app():
    """The shared application object, or None when it cannot host widgets."""
    app = qt_app()
    try:
        from PySide6.QtWidgets import QApplication
    except ImportError:  # pragma: no cover - no widget libraries
        return None
    return app if isinstance(app, QApplication) else None
