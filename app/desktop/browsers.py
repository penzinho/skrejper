"""First-run Chromium download for the HZZ tab.

Playwright's browsers are ~150 MB and live outside site-packages, so bundling
them into the .app/.exe would triple the download for everyone — including the
people who only ever use Arbeitsagentur, which needs no browser at all. Instead
the app fetches Chromium once, into the user's application-data directory, the
first time an HZZ run is started.
"""

import subprocess
import sys

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QVBoxLayout,
    QWidget,
)

from app.desktop import paths
from app.desktop.process import worker_argv

_MARKER = ".skrejper-chromium-ok"


def chromium_installed() -> bool:
    directory = paths.browsers_dir()
    if (directory / _MARKER).exists():
        return True
    # Playwright lays browsers out as <browsers_dir>/chromium-<revision>/...
    return any(directory.glob("chromium-*/")) if directory.is_dir() else False


def _mark_installed() -> None:
    directory = paths.browsers_dir()
    directory.mkdir(parents=True, exist_ok=True)
    (directory / _MARKER).write_text("ok\n", encoding="utf-8")


def install_cli(argv: list[str]) -> int:
    """Worker mode: run `playwright install chromium` and stream its output."""
    paths.configure_environment()
    paths.browsers_dir().mkdir(parents=True, exist_ok=True)

    from playwright._impl._driver import compute_driver_executable, get_driver_env

    driver_executable, driver_cli = compute_driver_executable()
    browsers = argv or ["chromium"]
    completed = subprocess.run(
        [str(driver_executable), str(driver_cli), "install", *browsers],
        env=get_driver_env(),
    )
    if completed.returncode == 0:
        _mark_installed()
    return completed.returncode


class _InstallDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Preuzimanje preglednika")
        self.setModal(True)
        self.setMinimumWidth(520)

        layout = QVBoxLayout(self)
        message = QLabel(
            "HZZ se skrejpa kroz pravi preglednik, pa ga treba jednom preuzeti (~150 MB).\n"
            "Sprema se u mapu aplikacije i ostaje za sljedeći put."
        )
        message.setWordWrap(True)
        layout.addWidget(message)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        layout.addWidget(self.progress)

        self.log = QPlainTextEdit()
        self.log.setObjectName("console")
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(400)
        layout.addWidget(self.log)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Cancel)
        self.buttons.rejected.connect(self._cancel)
        layout.addWidget(self.buttons)

        self._process = None
        self.succeeded = False

    def _cancel(self) -> None:
        if self._process is not None and self._process.state() != self._process.NotRunning:
            self._process.kill()
        self.reject()

    def run(self) -> bool:
        from PySide6.QtCore import QProcess, QProcessEnvironment

        argv = worker_argv()
        # worker_argv ends in --scrape-worker; this is the sibling mode.
        argv[-1] = "--install-browsers"

        environment = QProcessEnvironment.systemEnvironment()
        environment.insert("PYTHONUNBUFFERED", "1")
        environment.insert("PLAYWRIGHT_BROWSERS_PATH", str(paths.browsers_dir()))

        process = QProcess(self)
        process.setProgram(argv[0])
        process.setArguments(argv[1:])
        process.setProcessEnvironment(environment)
        process.setProcessChannelMode(QProcess.MergedChannels)
        process.readyReadStandardOutput.connect(self._read)
        process.finished.connect(self._done)
        self._process = process
        process.start()

        self.exec()
        return self.succeeded

    def _read(self) -> None:
        text = bytes(self._process.readAllStandardOutput()).decode("utf-8", errors="replace")
        for line in text.splitlines():
            if line.strip():
                self.log.appendPlainText(line.rstrip())

    def _done(self, exit_code: int, _status) -> None:
        self._read()
        self.succeeded = exit_code == 0
        if self.succeeded:
            self.accept()
        else:
            self.progress.setRange(0, 1)
            self.progress.setValue(0)
            self.buttons.setStandardButtons(QDialogButtonBox.Close)
            self.buttons.rejected.connect(self.reject)
            self.log.appendPlainText(f"\nPreuzimanje nije uspjelo (kod {exit_code}).")


def ensure_chromium(parent: QWidget | None = None) -> bool:
    """True when HZZ can run. Offers to download Chromium if it is missing."""
    if chromium_installed():
        return True

    answer = QMessageBox.question(
        parent,
        "Nedostaje preglednik",
        "Za HZZ treba preglednik Chromium (~150 MB). Preuzeti ga sada?",
        QMessageBox.Yes | QMessageBox.No,
        QMessageBox.Yes,
    )
    if answer != QMessageBox.Yes:
        return False

    dialog = _InstallDialog(parent)
    if sys.platform == "darwin":
        dialog.setWindowModality(Qt.WindowModal)
    return dialog.run()
