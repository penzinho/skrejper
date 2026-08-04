"""GUI side of a scrape run: launch the worker, read its NDJSON, allow Stop."""

import json
import os
import sys
import tempfile
from pathlib import Path

from PySide6.QtCore import QObject, QProcess, QProcessEnvironment, Signal

from app.desktop import paths


def worker_argv() -> list[str]:
    """Command that re-enters this program in worker mode.

    In a PyInstaller bundle ``sys.executable`` *is* the app, so the app relaunches
    itself; from source it is the interpreter and the script path has to come too.
    """
    if paths.is_frozen():
        return [sys.executable, "--scrape-worker"]
    entry = Path(__file__).resolve().parents[2] / "skrejper_gui.py"
    return [sys.executable, "-u", str(entry), "--scrape-worker"]


class ScrapeProcess(QObject):
    """Runs one scrape job in a child process and re-emits its events as signals."""

    started = Signal()
    logged = Signal(str)
    rowArrived = Signal(dict)
    progressed = Signal(dict)
    targetStarted = Signal(int, int, str)
    targetDone = Signal(dict)
    succeeded = Signal(dict)
    cancelled = Signal(dict)
    failed = Signal(str, str)
    finished = Signal()

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._process: QProcess | None = None
        self._buffer = ""
        self._config_path: Path | None = None
        self._stopping = False
        self._saw_terminal_event = False

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.state() != QProcess.NotRunning

    def start(self, config: dict) -> None:
        if self.running:
            raise RuntimeError("Scrape already running")

        self._buffer = ""
        self._stopping = False
        self._saw_terminal_event = False

        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".json", prefix="skrejper-", delete=False, encoding="utf-8"
        )
        # Passed as a file path, never inline on argv: Windows has both a command
        # line length limit and its own quoting rules.
        with handle:
            json.dump(config, handle, ensure_ascii=False)
        self._config_path = Path(handle.name)

        argv = worker_argv()
        environment = QProcessEnvironment.systemEnvironment()
        # hzz.py prints without flush=True, so without this the log would arrive
        # in bursts instead of live.
        environment.insert("PYTHONUNBUFFERED", "1")
        environment.insert("PLAYWRIGHT_BROWSERS_PATH", str(paths.browsers_dir()))
        environment.insert("SKREJPER_STATE_DIR", str(paths.state_dir()))
        environment.insert("PYTHONIOENCODING", "utf-8")

        process = QProcess(self)
        process.setProgram(argv[0])
        process.setArguments(argv[1:] + [str(self._config_path)])
        process.setProcessEnvironment(environment)
        process.setWorkingDirectory(str(paths.project_root()))
        process.readyReadStandardOutput.connect(self._drain_stdout)
        process.readyReadStandardError.connect(self._drain_stderr)
        process.finished.connect(self._on_finished)
        process.errorOccurred.connect(self._on_error)
        self._process = process
        process.start()
        self.started.emit()

    def stop(self) -> None:
        """Ask the worker to stop, then insist."""
        if not self.running:
            return
        self._stopping = True
        self._process.terminate()
        if not self._process.waitForFinished(3000):
            self._process.kill()

    def _drain_stdout(self) -> None:
        data = bytes(self._process.readAllStandardOutput()).decode("utf-8", errors="replace")
        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                self._dispatch(line)

    def _drain_stderr(self) -> None:
        data = bytes(self._process.readAllStandardError()).decode("utf-8", errors="replace")
        for line in data.splitlines():
            if line.strip():
                self.logged.emit(line.rstrip())

    def _dispatch(self, line: str) -> None:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            # Anything that is not an event is still worth showing.
            self.logged.emit(line)
            return

        kind = event.get("event")
        if kind == "log":
            self.logged.emit(event.get("line", ""))
        elif kind == "row":
            self.rowArrived.emit(event.get("row") or {})
        elif kind == "progress":
            self.progressed.emit(event.get("stats") or {})
        elif kind == "target_start":
            self.targetStarted.emit(
                int(event.get("index", 0)), int(event.get("total", 0)), event.get("label", "")
            )
        elif kind == "target_done":
            self.targetDone.emit(event)
        elif kind == "done":
            self._saw_terminal_event = True
            self.succeeded.emit(event)
        elif kind == "cancelled":
            self._saw_terminal_event = True
            self.cancelled.emit(event)
        elif kind == "error":
            self._saw_terminal_event = True
            self.failed.emit(event.get("message", "Nepoznata greška"), event.get("traceback", ""))

    def _on_error(self, error: QProcess.ProcessError) -> None:
        if error == QProcess.FailedToStart:
            self._saw_terminal_event = True
            self.failed.emit("Ne mogu pokrenuti radni proces.", "")

    def _on_finished(self, exit_code: int, exit_status: QProcess.ExitStatus) -> None:
        self._drain_stdout()
        self._drain_stderr()

        if not self._saw_terminal_event:
            if self._stopping or exit_status == QProcess.CrashExit:
                # Killed mid-run: the CSV is written incrementally, so whatever
                # was collected before the Stop is already on disk.
                self.cancelled.emit({"files": [], "rows": 0})
            else:
                self.failed.emit(f"Radni proces je završio s kodom {exit_code}.", "")

        if self._config_path is not None:
            try:
                os.unlink(self._config_path)
            except OSError:
                pass
            self._config_path = None

        self._process = None
        self.finished.emit()
