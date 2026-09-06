"""Fluent-style theming, modelled on Windows 11.

Two deliberate choices:

* **Fusion, not the native style.** The platform styles ignore most of what a
  stylesheet asks for — macOS Aqua in particular repaints controls its own way —
  so a consistent Fluent look needs a neutral base to paint on.
* **Glyphs are drawn, not shipped.** Qt stylesheets cannot draw a checkmark or a
  chevron, and bundling icon fonts for two platforms is a lot of weight for four
  small shapes. They are rendered once at startup into the app-data directory
  and referenced by path.

Colours follow the Windows 11 design tokens (surfaces, control strokes, the
two-tone control border, the #0067C0 / #4CC2FF accent) and both schemes are
defined, so the app follows the system light/dark setting.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QFont, QImage, QPainter, QPalette, QPen
from PySide6.QtWidgets import QApplication

from app.desktop import paths


@dataclass(frozen=True)
class Tokens:
    """Windows 11 design tokens, light or dark."""

    name: str
    # Surfaces, back to front.
    background: str          # window / mica base
    layer: str               # cards sitting on the background
    control: str             # inputs and buttons
    control_hover: str
    control_pressed: str
    subtle_hover: str        # flat items (list rows, tabs)
    # Strokes.
    border: str
    border_strong: str       # the darker bottom edge of a control
    divider: str
    # Text.
    text: str
    text_secondary: str
    text_disabled: str
    # Accent.
    accent: str
    accent_hover: str
    accent_pressed: str
    on_accent: str
    danger: str


LIGHT = Tokens(
    name="light",
    background="#f3f3f3",
    layer="#ffffff",
    control="#fdfdfd",
    control_hover="#f6f6f6",
    control_pressed="#f0f0f0",
    subtle_hover="#ebebeb",
    border="#e5e5e5",
    border_strong="#d0d0d0",
    divider="#e0e0e0",
    text="#1a1a1a",
    text_secondary="#5f5f5f",
    text_disabled="#9d9d9d",
    accent="#0067c0",
    accent_hover="#0e78cc",
    accent_pressed="#2b88d8",
    on_accent="#ffffff",
    danger="#c42b1c",
)

DARK = Tokens(
    name="dark",
    background="#202020",
    layer="#2b2b2b",
    control="#333333",
    control_hover="#3a3a3a",
    control_pressed="#2f2f2f",
    subtle_hover="#383838",
    border="#3d3d3d",
    border_strong="#484848",
    divider="#383838",
    text="#ffffff",
    text_secondary="#c8c8c8",
    text_disabled="#787878",
    accent="#4cc2ff",
    accent_hover="#47b1e8",
    accent_pressed="#42a1d2",
    on_accent="#000000",
    danger="#ff99a4",
)


def is_dark(app: QApplication) -> bool:
    hints = app.styleHints()
    scheme = getattr(hints, "colorScheme", None)
    if scheme is not None:
        try:
            return scheme() == Qt.ColorScheme.Dark
        except (AttributeError, TypeError):
            pass
    return app.palette().color(QPalette.Window).lightness() < 128


def tokens_for(app: QApplication) -> Tokens:
    return DARK if is_dark(app) else LIGHT


def ui_font(base: QFont | None = None) -> QFont:
    """Segoe UI Variable on Windows 11, the platform UI face elsewhere.

    Only the family is chosen. The size stays whatever the platform decided —
    macOS and Linux already pick sensible defaults, and overriding them makes
    every label a couple of pixels too large, which is enough to push a form
    column out of the window.
    """
    if sys.platform == "win32":
        families = ["Segoe UI Variable Text", "Segoe UI"]
    elif sys.platform == "darwin":
        families = ["SF Pro Text", "Helvetica Neue"]
    else:
        families = ["Inter", "Noto Sans", "Cantarell", "DejaVu Sans"]

    font = QFont(base) if base is not None else QFont()
    font.setFamilies(families)
    if sys.platform == "win32":
        font.setPointSizeF(10)  # Windows 11 body size
    return font


# --------------------------------------------------------------------------
# Glyphs
# --------------------------------------------------------------------------

GLYPH_SCALE = 4  # drawn oversized, scaled down by the stylesheet -> crisp on HiDPI


def _glyph_dir(tokens: Tokens) -> Path:
    directory = paths.app_data_dir() / "glyphs" / tokens.name
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _new_image(size: int) -> QImage:
    image = QImage(size * GLYPH_SCALE, size * GLYPH_SCALE, QImage.Format_ARGB32)
    image.fill(Qt.transparent)
    return image


def _pen(color: str, width: float) -> QPen:
    pen = QPen(QColor(color))
    pen.setWidthF(width * GLYPH_SCALE)
    pen.setCapStyle(Qt.RoundCap)
    pen.setJoinStyle(Qt.RoundJoin)
    return pen


def _draw_check(path: Path, color: str, size: int = 12) -> Path:
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    painter.setPen(_pen(color, 1.6))
    s = size * GLYPH_SCALE
    painter.drawPolyline(
        [QPointF(s * 0.20, s * 0.53), QPointF(s * 0.42, s * 0.74), QPointF(s * 0.80, s * 0.28)]
    )
    painter.end()
    image.save(str(path), "PNG")
    return path


def _draw_chevron(path: Path, color: str, size: int = 12, up: bool = False) -> Path:
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    painter.setPen(_pen(color, 1.3))
    s = size * GLYPH_SCALE
    if up:
        points = [QPointF(s * 0.22, s * 0.62), QPointF(s * 0.5, s * 0.36), QPointF(s * 0.78, s * 0.62)]
    else:
        points = [QPointF(s * 0.22, s * 0.38), QPointF(s * 0.5, s * 0.64), QPointF(s * 0.78, s * 0.38)]
    painter.drawPolyline(points)
    painter.end()
    image.save(str(path), "PNG")
    return path


def _draw_dash(path: Path, color: str, size: int = 12) -> Path:
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    painter.setPen(_pen(color, 1.6))
    s = size * GLYPH_SCALE
    painter.drawLine(QPointF(s * 0.24, s * 0.5), QPointF(s * 0.76, s * 0.5))
    painter.end()
    image.save(str(path), "PNG")
    return path


def _draw_document(path: Path, color: str, size: int = 20) -> Path:
    """Nav icon for HZZ — a job listing."""
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    s = size * GLYPH_SCALE
    painter.setPen(_pen(color, 1.4))
    painter.drawRoundedRect(
        s * 0.20, s * 0.13, s * 0.60, s * 0.74, s * 0.08, s * 0.08
    )
    painter.setPen(_pen(color, 1.2))
    for index, right in enumerate((0.66, 0.60, 0.66)):
        y = s * (0.35 + index * 0.16)
        painter.drawLine(QPointF(s * 0.32, y), QPointF(s * right, y))
    painter.end()
    image.save(str(path), "PNG")
    return path


def _draw_globe(path: Path, color: str, size: int = 20) -> Path:
    """Nav icon for Arbeitsagentur — a foreign job board."""
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    s = size * GLYPH_SCALE
    painter.setPen(_pen(color, 1.4))
    painter.setBrush(Qt.NoBrush)
    radius = s * 0.33
    centre = QPointF(s * 0.5, s * 0.5)
    painter.drawEllipse(centre, radius, radius)
    painter.drawEllipse(centre, radius * 0.45, radius)
    painter.drawLine(QPointF(s * 0.17, s * 0.5), QPointF(s * 0.83, s * 0.5))
    painter.drawArc(int(s * 0.17), int(s * 0.28), int(s * 0.66), int(s * 0.30), 0, -180 * 16)
    painter.end()
    image.save(str(path), "PNG")
    return path


def _draw_people(path: Path, color: str, size: int = 20) -> Path:
    """Nav icon for GVP — a member directory."""
    image = _new_image(size)
    painter = QPainter(image)
    painter.setRenderHint(QPainter.Antialiasing)
    s = size * GLYPH_SCALE
    painter.setPen(_pen(color, 1.4))
    painter.setBrush(Qt.NoBrush)
    # Front person: head and shoulders.
    painter.drawEllipse(QPointF(s * 0.40, s * 0.33), s * 0.14, s * 0.14)
    painter.drawArc(int(s * 0.14), int(s * 0.54), int(s * 0.52), int(s * 0.50), 0, 180 * 16)
    # Second person, half hidden behind.
    painter.drawArc(int(s * 0.56), int(s * 0.24), int(s * 0.22), int(s * 0.22), -60 * 16, 210 * 16)
    painter.drawArc(int(s * 0.58), int(s * 0.54), int(s * 0.30), int(s * 0.50), 20 * 16, 100 * 16)
    painter.end()
    image.save(str(path), "PNG")
    return path


def nav_icons(tokens: Tokens) -> dict[str, str]:
    """Icons for the navigation rail, drawn in the primary text colour."""
    directory = _glyph_dir(tokens)
    return {
        "hzz": str(_draw_document(directory / "nav-hzz.png", tokens.text)),
        "arbeitsagentur": str(_draw_globe(directory / "nav-arbeitsagentur.png", tokens.text)),
        "gvp": str(_draw_people(directory / "nav-gvp.png", tokens.text)),
    }


def build_glyphs(tokens: Tokens) -> dict[str, str]:
    """Render the stylesheet's icons and return QSS-safe paths."""
    directory = _glyph_dir(tokens)
    glyphs = {
        "check": _draw_check(directory / "check.png", tokens.on_accent),
        "dash": _draw_dash(directory / "dash.png", tokens.on_accent),
        "chevron_down": _draw_chevron(directory / "chevron-down.png", tokens.text_secondary),
        "chevron_up": _draw_chevron(directory / "chevron-up.png", tokens.text_secondary, up=True),
    }
    # Qt wants forward slashes in stylesheet urls, on every platform.
    return {key: str(value).replace("\\", "/") for key, value in glyphs.items()}


# --------------------------------------------------------------------------
# Palette + stylesheet
# --------------------------------------------------------------------------


def build_palette(t: Tokens) -> QPalette:
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(t.background))
    palette.setColor(QPalette.WindowText, QColor(t.text))
    palette.setColor(QPalette.Base, QColor(t.control))
    palette.setColor(QPalette.AlternateBase, QColor(t.subtle_hover))
    palette.setColor(QPalette.Text, QColor(t.text))
    palette.setColor(QPalette.Button, QColor(t.control))
    palette.setColor(QPalette.ButtonText, QColor(t.text))
    palette.setColor(QPalette.Highlight, QColor(t.accent))
    palette.setColor(QPalette.HighlightedText, QColor(t.on_accent))
    palette.setColor(QPalette.ToolTipBase, QColor(t.layer))
    palette.setColor(QPalette.ToolTipText, QColor(t.text))
    palette.setColor(QPalette.PlaceholderText, QColor(t.text_disabled))
    for group in (QPalette.Disabled,):
        palette.setColor(group, QPalette.Text, QColor(t.text_disabled))
        palette.setColor(group, QPalette.ButtonText, QColor(t.text_disabled))
        palette.setColor(group, QPalette.WindowText, QColor(t.text_disabled))
    return palette


def stylesheet(t: Tokens, glyphs: dict[str, str]) -> str:
    return f"""
    QWidget {{
        color: {t.text};
        background: transparent;
    }}
    QMainWindow, QDialog {{ background: {t.background}; }}
    QStatusBar {{ color: {t.text_secondary}; background: transparent; }}
    QStatusBar::item {{ border: none; }}

    QMenuBar {{ background: {t.background}; padding: 2px 4px; }}
    QMenuBar::item {{ padding: 5px 10px; border-radius: 5px; }}
    QMenuBar::item:selected {{ background: {t.subtle_hover}; }}
    QMenu {{
        background: {t.layer};
        border: 1px solid {t.border};
        border-radius: 8px;
        padding: 4px;
    }}
    QMenu::item {{ padding: 7px 24px 7px 12px; border-radius: 5px; }}
    QMenu::item:selected {{ background: {t.subtle_hover}; }}
    QMenu::separator {{ height: 1px; background: {t.divider}; margin: 4px 8px; }}

    /* ---- Navigation rail (Win 11 NavigationView) ---- */
    QWidget#navRail {{ background: transparent; }}
    QLabel#appName {{ font-size: 15px; font-weight: 700; }}
    QLabel#appTag {{ color: {t.text_secondary}; font-size: 11px; }}
    QListWidget#nav {{
        background: transparent;
        border: none;
        padding: 2px;
        outline: none;
    }}
    QListWidget#nav::item {{
        color: {t.text};
        padding: 10px 12px 10px 13px;
        margin: 2px 4px;
        border-radius: 6px;
    }}
    QListWidget#nav::item:hover {{ background: {t.subtle_hover}; }}
    QListWidget#nav::item:selected {{
        background: {t.subtle_hover};
        color: {t.text};
        font-weight: 600;
        /* the accent bar Win 11 puts against the selected entry */
        border-left: 3px solid {t.accent};
        padding-left: 10px;
    }}

    /* ---- Content surface ---- */
    QWidget#content {{
        background: {t.layer};
        border: 1px solid {t.border};
        border-radius: 10px;
    }}

    /* ---- Type scale ---- */
    QLabel#title {{ font-size: 19px; font-weight: 600; }}
    QLabel#subtitle {{ color: {t.text_secondary}; font-size: 12px; }}
    QLabel#sectionTitle {{ font-weight: 600; }}
    QLabel#empty {{ color: {t.text_disabled}; font-size: 13px; }}
    QLabel#chip {{
        background: {t.subtle_hover};
        color: {t.text_secondary};
        border-radius: 10px;
        padding: 3px 11px;
        font-size: 11px;
        font-weight: 600;
    }}
    QLabel#chipAccent {{
        background: {t.accent};
        color: {t.on_accent};
        border-radius: 10px;
        padding: 3px 11px;
        font-size: 11px;
        font-weight: 600;
    }}
    QFrame#divider {{ background: {t.divider}; max-height: 1px; border: none; }}

    /* ---- Result tabs: pills, no pane (they sit inside the content card) ---- */
    QTabWidget::pane {{ border: none; background: transparent; }}
    QTabBar {{ qproperty-drawBase: 0; }}
    QTabBar::tab {{
        background: transparent;
        color: {t.text_secondary};
        padding: 5px 14px;
        margin: 0 4px 6px 0;
        border-radius: 12px;
        min-width: 60px;
    }}
    QTabBar::tab:hover {{ color: {t.text}; }}
    QTabBar::tab:selected {{
        background: {t.subtle_hover};
        color: {t.text};
        font-weight: 600;
    }}

    /* ---- Inset panels inside the content card ---- */
    QGroupBox {{
        background: {t.background};
        border: none;
        border-radius: 8px;
        margin-top: 13px;
        padding: 12px;
        font-weight: 600;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 2px;
        padding: 0 2px;
        color: {t.text_secondary};
        font-weight: 600;
    }}

    /* ---- Buttons ---- */
    QPushButton {{
        background: {t.control};
        color: {t.text};
        border: 1px solid {t.border};
        border-bottom-color: {t.border_strong};
        border-radius: 5px;
        padding: 6px 14px;
        min-height: 20px;
        font-weight: 600;
    }}
    QPushButton:hover {{ background: {t.control_hover}; }}
    QPushButton:pressed {{ background: {t.control_pressed}; color: {t.text_secondary}; }}
    QPushButton:disabled {{ background: {t.control_pressed}; color: {t.text_disabled}; border-color: {t.border}; }}

    QPushButton#primary {{
        background: {t.accent};
        color: {t.on_accent};
        border: 1px solid {t.accent};
        padding: 7px 20px;
    }}
    QPushButton#primary:hover {{ background: {t.accent_hover}; border-color: {t.accent_hover}; }}
    QPushButton#primary:pressed {{ background: {t.accent_pressed}; border-color: {t.accent_pressed}; }}
    QPushButton#primary:disabled {{
        background: {t.control_pressed};
        border-color: {t.border};
        color: {t.text_disabled};
    }}
    QPushButton#danger {{ color: {t.danger}; }}
    QPushButton#danger:disabled {{ color: {t.text_disabled}; }}

    /* ---- Inputs ---- */
    QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {{
        background: {t.control};
        border: 1px solid {t.border};
        border-bottom: 2px solid {t.border_strong};
        border-radius: 5px;
        padding: 4px 9px;
        min-height: 19px;
        selection-background-color: {t.accent};
        selection-color: {t.on_accent};
    }}
    QLineEdit:hover, QSpinBox:hover, QComboBox:hover {{ background: {t.control_hover}; }}
    QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {{
        background: {t.layer};
        border-bottom: 2px solid {t.accent};
    }}
    QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled {{ color: {t.text_disabled}; }}

    QComboBox::drop-down {{ border: none; width: 26px; }}
    QComboBox::down-arrow {{ image: url("{glyphs['chevron_down']}"); width: 12px; height: 12px; }}
    QComboBox QAbstractItemView {{
        background: {t.layer};
        border: 1px solid {t.border};
        border-radius: 8px;
        padding: 4px;
        outline: none;
        selection-background-color: {t.subtle_hover};
        selection-color: {t.text};
    }}

    QSpinBox::up-button, QSpinBox::down-button {{
        subcontrol-origin: border;
        width: 24px;
        border: none;
        border-radius: 4px;
        margin: 2px;
    }}
    QSpinBox::up-button {{ subcontrol-position: top right; }}
    QSpinBox::down-button {{ subcontrol-position: bottom right; }}
    QSpinBox::up-button:hover, QSpinBox::down-button:hover {{ background: {t.subtle_hover}; }}
    QSpinBox::up-arrow {{ image: url("{glyphs['chevron_up']}"); width: 10px; height: 10px; }}
    QSpinBox::down-arrow {{ image: url("{glyphs['chevron_down']}"); width: 10px; height: 10px; }}

    /* ---- Checkboxes ---- */
    QCheckBox {{ spacing: 9px; padding: 1px 0; }}
    QCheckBox::indicator, QListWidget::indicator {{
        width: 18px;
        height: 18px;
        border: 1px solid {t.border_strong};
        border-radius: 4px;
        background: {t.control};
    }}
    QCheckBox::indicator:hover, QListWidget::indicator:hover {{ background: {t.control_hover}; }}
    QCheckBox::indicator:checked, QListWidget::indicator:checked {{
        background: {t.accent};
        border-color: {t.accent};
        image: url("{glyphs['check']}");
    }}
    QCheckBox::indicator:indeterminate, QListWidget::indicator:indeterminate {{
        background: {t.accent};
        border-color: {t.accent};
        image: url("{glyphs['dash']}");
    }}
    QCheckBox::indicator:disabled, QListWidget::indicator:disabled {{
        background: {t.control_pressed};
        border-color: {t.border};
    }}

    /* ---- Lists and tables ---- */
    QListWidget, QTableWidget {{
        background: {t.control};
        border: 1px solid {t.border};
        border-radius: 6px;
        outline: none;
        padding: 3px;
    }}
    QListWidget::item {{ padding: 5px 6px; border-radius: 4px; }}
    QListWidget::item:hover {{ background: {t.subtle_hover}; }}
    QTableWidget {{ gridline-color: transparent; }}
    QTableWidget::item {{ padding: 5px 8px; border-bottom: 1px solid {t.divider}; }}
    QTableWidget::item:selected {{ background: {t.subtle_hover}; color: {t.text}; }}
    QHeaderView::section {{
        background: transparent;
        color: {t.text_secondary};
        border: none;
        border-bottom: 1px solid {t.divider};
        padding: 7px 8px;
        font-weight: 600;
    }}
    QTableCornerButton::section {{ background: transparent; border: none; }}

    QPlainTextEdit#console {{
        background: {t.background};
        border: 1px solid {t.border};
        border-bottom: 1px solid {t.border};
        font-family: "Cascadia Mono", "SF Mono", "Menlo", "Consolas", monospace;
        font-size: 11px;
        padding: 8px;
    }}

    /* ---- Progress ---- */
    QProgressBar {{
        background: {t.divider};
        border: none;
        border-radius: 2px;
        height: 4px;
        text-align: center;
    }}
    QProgressBar::chunk {{ background: {t.accent}; border-radius: 2px; }}

    /* ---- Scrollbars: thin, no arrows ---- */
    QScrollBar:vertical {{ background: transparent; width: 12px; margin: 2px; }}
    QScrollBar:horizontal {{ background: transparent; height: 12px; margin: 2px; }}
    QScrollBar::handle:vertical {{
        background: {t.border_strong};
        border-radius: 3px;
        min-height: 28px;
        margin: 0 4px;
    }}
    QScrollBar::handle:horizontal {{
        background: {t.border_strong};
        border-radius: 3px;
        min-width: 28px;
        margin: 4px 0;
    }}
    QScrollBar::handle:hover {{ background: {t.text_disabled}; }}
    QScrollBar::add-line, QScrollBar::sub-line {{ height: 0; width: 0; border: none; }}
    QScrollBar::add-page, QScrollBar::sub-page {{ background: transparent; }}
    /* Without this the square where the two scrollbars would meet picks up the
       checkbox indicator style and paints a stray empty box. */
    QAbstractScrollArea::corner {{ background: transparent; border: none; }}

    QSplitter::handle {{ background: transparent; height: 8px; }}

    QToolTip {{
        background: {t.layer};
        color: {t.text};
        border: 1px solid {t.border};
        border-radius: 6px;
        padding: 5px 8px;
    }}

    QScrollArea {{ background: transparent; border: none; }}
    QScrollArea > QWidget > QWidget {{ background: transparent; }}

    QLabel#hint {{ color: {t.text_secondary}; }}
    QLabel#stat {{ color: {t.text_secondary}; font-weight: 600; }}
    """


def apply(app: QApplication) -> Tokens:
    app.setStyle("Fusion")
    app.setFont(ui_font(app.font()))
    tokens = tokens_for(app)
    app.setPalette(build_palette(tokens))
    app.setStyleSheet(stylesheet(tokens, build_glyphs(tokens)))
    return tokens
