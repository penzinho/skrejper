"""Light-touch styling.

The native platform style is kept — it is what makes the window feel like a Mac
or Windows app rather than a toolkit demo — and only a few widgets are restyled.
Every colour is derived from the active palette, so the app follows the system
light/dark setting instead of imposing its own.
"""

from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

ACCENT = "#2f6fed"
ACCENT_HOVER = "#2a63d4"
ACCENT_PRESSED = "#2557bb"
DANGER = "#c0392b"


def is_dark(app: QApplication) -> bool:
    return app.palette().color(QPalette.Window).lightness() < 128


def _mix(color: QColor, other: QColor, ratio: float) -> str:
    return QColor(
        round(color.red() * (1 - ratio) + other.red() * ratio),
        round(color.green() * (1 - ratio) + other.green() * ratio),
        round(color.blue() * (1 - ratio) + other.blue() * ratio),
    ).name()


def stylesheet(app: QApplication) -> str:
    palette = app.palette()
    window = palette.color(QPalette.Window)
    text = palette.color(QPalette.WindowText)
    base = palette.color(QPalette.Base)
    dark = is_dark(app)

    border = _mix(window, text, 0.22 if dark else 0.18)
    subtle = _mix(window, text, 0.10 if dark else 0.06)
    muted = _mix(window, text, 0.45)
    console_bg = _mix(base, text, 0.05) if dark else _mix(base, text, 0.04)

    return f"""
    QWidget {{ font-size: 13px; }}

    QTabWidget::pane {{
        border: 1px solid {border};
        border-radius: 8px;
        top: -1px;
    }}
    QTabBar::tab {{
        padding: 7px 18px;
        margin-right: 2px;
        border: 1px solid transparent;
        border-top-left-radius: 7px;
        border-top-right-radius: 7px;
        color: {muted};
    }}
    QTabBar::tab:selected {{
        color: {text.name()};
        background: {subtle};
        border-color: {border};
        border-bottom-color: transparent;
    }}

    QGroupBox {{
        border: 1px solid {border};
        border-radius: 8px;
        margin-top: 14px;
        padding-top: 10px;
        font-weight: 600;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 10px;
        padding: 0 4px;
        color: {muted};
    }}

    QPushButton {{
        padding: 6px 14px;
        border: 1px solid {border};
        border-radius: 6px;
        background: {subtle};
    }}
    QPushButton:hover {{ background: {_mix(window, text, 0.14)}; }}
    QPushButton:disabled {{ color: {muted}; }}

    QPushButton#primary {{
        background: {ACCENT};
        border-color: {ACCENT};
        color: white;
        font-weight: 600;
        padding: 7px 22px;
    }}
    QPushButton#primary:hover {{ background: {ACCENT_HOVER}; border-color: {ACCENT_HOVER}; }}
    QPushButton#primary:pressed {{ background: {ACCENT_PRESSED}; border-color: {ACCENT_PRESSED}; }}
    QPushButton#primary:disabled {{
        background: {subtle};
        border-color: {border};
        color: {muted};
    }}
    QPushButton#danger {{ color: {DANGER}; font-weight: 600; }}

    QLineEdit, QSpinBox, QComboBox, QListWidget, QTableWidget, QPlainTextEdit {{
        border: 1px solid {border};
        border-radius: 6px;
        padding: 4px 6px;
        background: {base.name()};
        selection-background-color: {ACCENT};
        selection-color: white;
    }}
    QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {{
        border-color: {ACCENT};
    }}

    QPlainTextEdit#console {{
        background: {console_bg};
        font-family: "SF Mono", "Cascadia Mono", "Menlo", "Consolas", monospace;
        font-size: 11px;
        padding: 8px;
    }}

    QTableWidget {{ gridline-color: {border}; }}
    QHeaderView::section {{
        background: {subtle};
        border: none;
        border-bottom: 1px solid {border};
        padding: 6px 8px;
        font-weight: 600;
    }}

    QProgressBar {{
        border: 1px solid {border};
        border-radius: 6px;
        height: 8px;
        text-align: center;
        background: {subtle};
    }}
    QProgressBar::chunk {{ background: {ACCENT}; border-radius: 5px; }}

    QLabel#hint {{ color: {muted}; }}
    QLabel#stat {{ font-weight: 600; }}
    """


def apply(app: QApplication) -> None:
    app.setStyleSheet(stylesheet(app))
