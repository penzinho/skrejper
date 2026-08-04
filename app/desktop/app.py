"""Application window."""

import sys

from PySide6.QtCore import QSettings, Qt, QUrl
from PySide6.QtGui import QAction, QDesktopServices, QIcon
from PySide6.QtWidgets import QApplication, QMainWindow, QMessageBox, QTabWidget

from app.desktop import paths, theme
from app.desktop.paths import APP_NAME, ORG_NAME
from app.desktop.tabs.arbeitsagentur_tab import ArbeitsagenturTab
from app.desktop.tabs.hzz_tab import HzzTab

ICON_PATH = paths.resource_dir() / "icon.png"


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} — skrejper oglasa")
        self.setMinimumSize(1040, 760)

        self.tabs = QTabWidget()
        self.hzz_tab = HzzTab()
        self.arbeitsagentur_tab = ArbeitsagenturTab()
        self.tabs.addTab(self.hzz_tab, "HZZ")
        self.tabs.addTab(self.arbeitsagentur_tab, "Arbeitsagentur")
        self.setCentralWidget(self.tabs)

        self._build_menu()
        self._restore_geometry()
        self.statusBar().showMessage(f"Rezultati se spremaju u {paths.default_output_dir()}")

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
        current = self.tabs.currentWidget()
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


def build_application(argv: list[str] | None = None) -> tuple[QApplication, MainWindow]:
    paths.configure_environment()
    QApplication.setApplicationName(APP_NAME)
    QApplication.setOrganizationName(ORG_NAME)
    QApplication.setApplicationDisplayName(APP_NAME)

    app = QApplication.instance() or QApplication(argv if argv is not None else sys.argv)
    if ICON_PATH.exists():
        app.setWindowIcon(QIcon(str(ICON_PATH)))
    theme.apply(app)

    window = MainWindow()
    return app, window


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv if argv is None else argv)
    self_test = "--self-test" in argv
    if self_test:
        argv = [a for a in argv if a != "--self-test"]

    app, window = build_application(argv)

    if self_test:
        # Headless sanity check: everything constructed, dropdowns populated,
        # and both tabs can produce a worker config.
        window.hzz_tab.build_config()
        window.arbeitsagentur_tab.categories.set_all(True)
        window.arbeitsagentur_tab.build_config()
        print("self-test ok")
        return 0

    window.show()
    return app.exec()
