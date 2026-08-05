"""Application window: a navigation rail on the left, one page per source."""

import sys
import threading

from PySide6.QtCore import QSettings, QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QAction, QDesktopServices, QIcon, QPixmap
from PySide6.QtWidgets import (
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
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
    # Emitted from worker threads; the queued connection lands it on the GUI
    # thread, so the status bar can be updated safely.
    drive_status = Signal(str)

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

        self.drive_status.connect(lambda line: self.statusBar().showMessage(line, 15000))
        # Pick up what other computers scraped before this one starts, and push
        # this computer's additions after every finished run.
        for tab in (self.hzz_tab, self.arbeitsagentur_tab):
            tab._process.finished.connect(self._drive_auto_sync)
        QTimer.singleShot(1500, self._drive_auto_sync)

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

        # The raw seen-*.txt files, one action per store: plain text, one
        # posting id (or e-mail) per line, openable in whatever edits .txt.
        seen_menu = file_menu.addMenu("Popis viđenih")
        for label, source in (
            ("HZZ — oglasi", "hzz"),
            ("HZZ — e-mailovi", "hzz-emails"),
            ("Arbeitsagentur — oglasi", "arbeitsagentur"),
            ("Arbeitsagentur — e-mailovi", "arbeitsagentur-emails"),
        ):
            action = QAction(label, self)
            action.triggered.connect(
                lambda _checked=False, s=source, l=label: self._open_seen(s, l)
            )
            seen_menu.addAction(action)

        file_menu.addSeparator()
        quit_action = QAction("Zatvori", self)
        quit_action.setMenuRole(QAction.QuitRole)
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        drive_menu = self.menuBar().addMenu("&Google Drive")
        connect_action = QAction("Poveži Google račun…", self)
        connect_action.triggered.connect(self._drive_connect)
        drive_menu.addAction(connect_action)
        sync_action = QAction("Sinkroniziraj sada", self)
        sync_action.triggered.connect(self._drive_sync_now)
        drive_menu.addAction(sync_action)
        drive_menu.addSeparator()
        disconnect_action = QAction("Odspoji", self)
        disconnect_action.triggered.connect(self._drive_disconnect)
        drive_menu.addAction(disconnect_action)

        help_menu = self.menuBar().addMenu("&Pomoć")
        about = QAction(f"O aplikaciji {APP_NAME}", self)
        about.setMenuRole(QAction.AboutRole)
        about.triggered.connect(self._about)
        help_menu.addAction(about)

    # ---- Google Drive sync ----------------------------------------------
    #
    # Seen-id files sync through the hidden appDataFolder of the user's own
    # Google account (see app/drive_sync.py for why not a service account).
    # All network work runs in plain threads; results come back over the
    # drive_status signal.

    def _drive_connect(self) -> None:
        from app import drive_sync

        store = QSettings(ORG_NAME, APP_NAME)
        client_id, ok = QInputDialog.getText(
            self,
            "Google Drive",
            "OAuth Client ID (Google Cloud Console → Credentials,\n"
            "tip \"Desktop app\", uz uključen Google Drive API):",
            QLineEdit.Normal,
            str(store.value("drive/client_id", "")),
        )
        if not ok or not client_id.strip():
            return
        client_secret, ok = QInputDialog.getText(
            self,
            "Google Drive",
            "OAuth Client secret:",
            QLineEdit.Normal,
            str(store.value("drive/client_secret", "")),
        )
        if not ok or not client_secret.strip():
            return
        store.setValue("drive/client_id", client_id.strip())
        store.setValue("drive/client_secret", client_secret.strip())

        def run():
            try:
                drive_sync.connect(client_id.strip(), client_secret.strip())
                self.drive_status.emit("[drive] Povezano — sinkroniziram…")
                drive_sync.sync(log=self.drive_status.emit)
                self.drive_status.emit("[drive] Google Drive spojen i usklađen.")
            except Exception as exc:
                self.drive_status.emit(f"[drive] Povezivanje nije uspjelo: {exc}")

        self.drive_status.emit("[drive] Otvaram Google prijavu u pregledniku…")
        threading.Thread(target=run, daemon=True).start()

    def _drive_sync_now(self) -> None:
        from app import drive_sync

        if not drive_sync.is_connected():
            QMessageBox.information(
                self,
                "Google Drive",
                "Google račun još nije povezan — odaberi „Poveži Google račun”.",
            )
            return
        self._drive_auto_sync()

    def _drive_auto_sync(self) -> None:
        from app import drive_sync

        if not drive_sync.is_connected():
            return

        def run():
            try:
                drive_sync.sync(log=self.drive_status.emit)
            except Exception as exc:
                self.drive_status.emit(f"[drive] Sinkronizacija nije uspjela: {exc}")

        threading.Thread(target=run, daemon=True).start()

    def _drive_disconnect(self) -> None:
        from app import drive_sync

        drive_sync.disconnect()
        self.statusBar().showMessage("[drive] Odspojeno — lokalni podaci ostaju.", 10000)

    def _open_seen(self, source: str, label: str) -> None:
        from app import seen_store

        path = seen_store.state_dir() / f"seen-{source}.txt"
        if not path.exists():
            QMessageBox.information(
                self, "Popis viđenih", f"Za „{label}” još nema zapisa."
            )
            return
        count = len(seen_store.load_seen(source))
        self.statusBar().showMessage(f"{label}: {count} zapisa — {path}", 10000)
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    def _open_output(self) -> None:
        current = self.pages.currentWidget()
        target = getattr(current, "output_edit", None)
        path = target.text().strip() if target else str(paths.default_output_dir())
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def _about(self) -> None:
        QMessageBox.about(
            self,
            f"O aplikaciji {APP_NAME}",
            f"<b>{APP_NAME}</b> {paths.version_string()}<br><br>"
            "Lokalni skrejper oglasa za HZZ i Arbeitsagentur.<br>"
            "Radi bez servera i baze — sve ostaje na ovom računalu.<br><br>"
            "Napravio Penzo, 2026.<br><br>"
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
