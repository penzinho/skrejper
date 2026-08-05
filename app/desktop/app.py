"""Application window: a navigation rail on the left, one page per source."""

import sys

from PySide6.QtCore import QSettings, QSize, Qt, QUrl
from PySide6.QtGui import QAction, QDesktopServices, QIcon, QPixmap
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from app.desktop import paths, theme
from app.desktop.paths import APP_NAME, ORG_NAME
from app.desktop.tabs.arbeitsagentur_tab import ArbeitsagenturTab
from app.desktop.tabs.hzz_tab import HzzTab

ICON_PATH = paths.resource_dir() / "icon.png"
RAIL_WIDTH = 208
# Reverse-domain id, same as the macOS bundle identifier.
APP_USER_MODEL_ID = "hr.protalent.skrejper"


def claim_windows_app_identity() -> None:
    """Give Windows an explicit AppUserModelID before Qt starts.

    The taskbar groups buttons by this id, and a frozen Python app that does not
    set one inherits the launching process's identity — which is why the taskbar
    shows a blank default icon even though the .exe has an icon embedded and the
    window icon is set. Must run before the QApplication exists.
    """
    if sys.platform != "win32":
        return
    import ctypes

    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_USER_MODEL_ID)
    except (AttributeError, OSError):
        pass  # Older Windows, or shell32 unavailable — only costs us the icon.


def app_icon() -> QIcon | None:
    """The .ico on Windows (it carries every size the shell asks for), else the PNG.

    icon.ico is generated at build time by packaging/make_icons.py, so running
    from source falls back to the PNG.
    """
    names = ("icon.ico", "icon.png") if sys.platform == "win32" else ("icon.png",)
    for name in names:
        candidate = paths.resource_dir() / name
        if candidate.exists():
            return QIcon(str(candidate))
    return None


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setMinimumSize(1060, 800)

        self.hzz_tab = HzzTab()
        self.arbeitsagentur_tab = ArbeitsagenturTab()

        self.pages = QStackedWidget()
        self.pages.addWidget(self.hzz_tab)
        self.pages.addWidget(self.arbeitsagentur_tab)

        central = QWidget()
        layout = QHBoxLayout(central)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        layout.addWidget(self._build_rail())

        content = QWidget()
        content.setObjectName("content")
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.addWidget(self.pages)
        layout.addWidget(content, 1)

        self.setCentralWidget(central)
        self._build_menu()
        self._restore_geometry()

    def _build_rail(self) -> QWidget:
        rail = QWidget()
        rail.setObjectName("navRail")
        rail.setFixedWidth(RAIL_WIDTH)
        layout = QVBoxLayout(rail)
        layout.setContentsMargins(6, 8, 6, 8)
        layout.setSpacing(12)

        brand = QHBoxLayout()
        brand.setSpacing(9)
        if ICON_PATH.exists():
            mark = QLabel()
            mark.setPixmap(
                QPixmap(str(ICON_PATH)).scaled(
                    28, 28, Qt.KeepAspectRatio, Qt.SmoothTransformation
                )
            )
            brand.addWidget(mark)
        titles = QVBoxLayout()
        titles.setSpacing(0)
        name = QLabel(APP_NAME)
        name.setObjectName("appName")
        tag = QLabel("Oglasi → kontakti")
        tag.setObjectName("appTag")
        titles.addWidget(name)
        titles.addWidget(tag)
        brand.addLayout(titles)
        brand.addStretch(1)
        layout.addLayout(brand)

        icons = theme.nav_icons(theme.tokens_for(self._app()))
        self.nav = QListWidget()
        self.nav.setObjectName("nav")
        self.nav.setIconSize(QSize(19, 19))
        self.nav.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        for label, key in (("HZZ", "hzz"), ("Arbeitsagentur", "arbeitsagentur")):
            item = QListWidgetItem(label)
            item.setIcon(QIcon(icons[key]))
            self.nav.addItem(item)
        self.nav.setCurrentRow(0)
        self.nav.currentRowChanged.connect(self.pages.setCurrentIndex)
        # Sized to its rows so the rail's empty space stays part of the rail
        # rather than an obviously scrollable list.
        self.nav.setFixedHeight(self.nav.sizeHintForRow(0) * self.nav.count() + 16)
        layout.addWidget(self.nav)
        layout.addStretch(1)
        return rail

    @staticmethod
    def _app():
        from PySide6.QtWidgets import QApplication

        return QApplication.instance()

    def _build_menu(self) -> None:
        file_menu = self.menuBar().addMenu("&Datoteka")

        open_output = QAction("Otvori mapu s rezultatima", self)
        open_output.triggered.connect(self._open_output)
        file_menu.addAction(open_output)

        open_state = QAction("Otvori mapu aplikacije", self)
        open_state.triggered.connect(
            lambda: QDesktopServices.openUrl(QUrl.fromLocalFile(str(paths.app_data_dir())))
        )
        file_menu.addAction(open_state)

        file_menu.addSeparator()
        quit_action = QAction("Zatvori", self)
        quit_action.setMenuRole(QAction.QuitRole)
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        help_menu = self.menuBar().addMenu("&Pomoć")
        about = QAction(f"O aplikaciji {APP_NAME}", self)
        about.setMenuRole(QAction.AboutRole)
        about.triggered.connect(self._about)
        help_menu.addAction(about)

    def _open_output(self) -> None:
        current = self.pages.currentWidget()
        target = getattr(current, "output_edit", None)
        path = target.text().strip() if target else str(paths.default_output_dir())
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def _about(self) -> None:
        QMessageBox.about(
            self,
            f"O aplikaciji {APP_NAME}",
            f"<b>{APP_NAME}</b><br><br>"
            "Lokalni skrejper oglasa za HZZ i Arbeitsagentur.<br>"
            "Radi bez servera i baze — sve ostaje na ovom računalu.<br><br>"
            f"Podaci aplikacije:<br><code>{paths.app_data_dir()}</code>",
        )

    def _restore_geometry(self) -> None:
        store = QSettings(ORG_NAME, APP_NAME)
        geometry = store.value("window/geometry")
        if geometry:
            self.restoreGeometry(geometry)

    def _running_tabs(self) -> list:
        return [tab for tab in (self.hzz_tab, self.arbeitsagentur_tab) if tab.running]

    def closeEvent(self, event) -> None:
        if self._running_tabs():
            answer = QMessageBox.question(
                self,
                "Skrejpanje je u tijeku",
                "Skrejpanje još radi. Zatvoriti aplikaciju i prekinuti ga?\n"
                "Ono što je do sada prikupljeno već je spremljeno.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if answer != QMessageBox.Yes:
                event.ignore()
                return
            running = self._running_tabs()
            for tab in running:
                tab.stop()
            # Blocking here is right: without it the app would exit and leave the
            # workers (and a headless Chromium) running with nobody reading them.
            for tab in running:
                tab.wait_for_exit()

        store = QSettings(ORG_NAME, APP_NAME)
        store.setValue("window/geometry", self.saveGeometry())
        store.sync()
        event.accept()


def build_application(argv: list[str] | None = None):
    from PySide6.QtWidgets import QApplication

    paths.configure_environment()
    QApplication.setApplicationName(APP_NAME)
    QApplication.setOrganizationName(ORG_NAME)
    QApplication.setApplicationDisplayName(APP_NAME)

    claim_windows_app_identity()
    app = QApplication.instance() or QApplication(argv if argv is not None else sys.argv)
    icon = app_icon()
    if icon is not None:
        app.setWindowIcon(icon)
    theme.apply(app)

    window = MainWindow()
    if icon is not None:
        # Also on the window: some shells read the window's icon rather than the
        # application's when deciding what to draw.
        window.setWindowIcon(icon)
    return app, window


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv if argv is None else argv)
    self_test = "--self-test" in argv
    if self_test:
        argv = [a for a in argv if a != "--self-test"]

    app, window = build_application(argv)

    if self_test:
        # Headless sanity check: everything constructed, dropdowns populated,
        # and both pages can produce a worker config.
        window.hzz_tab.build_config()
        window.arbeitsagentur_tab.categories.set_all(True)
        window.arbeitsagentur_tab.build_config()
        print("self-test ok")
        return 0

    window.show()
    return app.exec()
