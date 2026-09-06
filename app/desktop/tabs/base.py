"""Shared shell for a source page: header, form, actions, live results."""

from pathlib import Path

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from app.desktop import paths
from app.desktop.process import ScrapeProcess

MAX_TABLE_ROWS = 5000
MAX_LOG_LINES = 2000


class BaseScrapeTab(QWidget):
    """Subclasses provide the form, the config and the result columns."""

    source = ""
    settings_section = ""
    page_title = ""
    page_subtitle = ""
    empty_message = "Rezultati će se pojaviti ovdje čim krene skrejpanje."

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._process = ScrapeProcess(self)
        self._files: list[str] = []
        self._truncated = False
        self._build()
        self._connect()
        self.load_settings()

    # ---- subclass API ---------------------------------------------------

    def build_form(self) -> QWidget:
        raise NotImplementedError

    def build_config(self) -> dict:
        """Return the worker config, or raise ValueError with a message for the user."""
        raise NotImplementedError

    def table_columns(self) -> list[tuple[str, str]]:
        raise NotImplementedError

    def column_widths(self) -> list[int]:
        """Starting width per column; the last one stretches to fill the rest.

        Company and e-mail are what you actually read while a run is going, so
        they get the room — without this the header shares width evenly and both
        end up elided.
        """
        return [230, 210, 130, 150]

    def load_settings(self) -> None:
        pass

    def save_settings(self) -> None:
        pass

    def preflight(self) -> bool:
        """Last chance to block a run (e.g. missing browser). True means go."""
        return True

    # ---- construction ---------------------------------------------------

    def _build(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(22, 20, 22, 18)
        layout.setSpacing(14)

        layout.addLayout(self._build_header())
        # The form is the part that must not be cut; the result list can start
        # short and be dragged bigger by growing the window.
        layout.addWidget(self._build_form_area(), 5)
        layout.addWidget(self._divider())
        layout.addLayout(self._build_actions())
        layout.addWidget(self._build_results(), 2)

    def _build_header(self) -> QVBoxLayout:
        header = QVBoxLayout()
        header.setSpacing(2)
        title = QLabel(self.page_title or self.source.upper())
        title.setObjectName("title")
        subtitle = QLabel(self.page_subtitle)
        subtitle.setObjectName("subtitle")
        subtitle.setWordWrap(True)
        header.addWidget(title)
        header.addWidget(subtitle)
        return header

    def _divider(self) -> QFrame:
        line = QFrame()
        line.setObjectName("divider")
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        return line

    def _build_form_area(self) -> QWidget:
        self._form_scroll = QScrollArea()
        self._form_scroll.setWidgetResizable(True)
        self._form_scroll.setFrameShape(QScrollArea.NoFrame)
        self._form_scroll.setWidget(self.build_form())
        self._form_scroll.setMinimumHeight(300)
        return self._form_scroll

    def _build_actions(self) -> QVBoxLayout:
        actions = QVBoxLayout()
        actions.setSpacing(10)

        output_row = QHBoxLayout()
        output_row.setSpacing(8)
        label = QLabel("Spremi u")
        label.setObjectName("hint")
        output_row.addWidget(label)
        self.output_edit = QLineEdit(str(paths.default_output_dir()))
        output_row.addWidget(self.output_edit, 1)
        self.browse_button = QPushButton("Odaberi…")
        output_row.addWidget(self.browse_button)
        actions.addLayout(output_row)

        # Exposed so a tab can slot in its own action (see the Arbeitsagentur probe).
        self.button_row = button_row = QHBoxLayout()
        button_row.setSpacing(8)
        self.start_button = QPushButton("Pokreni skrejpanje")
        self.start_button.setObjectName("primary")
        self.stop_button = QPushButton("Zaustavi")
        self.stop_button.setObjectName("danger")
        self.stop_button.setEnabled(False)
        self.open_button = QPushButton("Otvori mapu")
        self.open_button.setEnabled(False)
        button_row.addWidget(self.start_button)
        button_row.addWidget(self.stop_button)
        button_row.addStretch(1)
        button_row.addWidget(self.open_button)
        actions.addLayout(button_row)

        # Status on one side, counters on the other — crammed onto the button row
        # they left the running label with no room to say anything.
        status_row = QHBoxLayout()
        status_row.setSpacing(6)
        self.status_label = QLabel("Spremno.")
        self.status_label.setObjectName("hint")
        status_row.addWidget(self.status_label, 1)

        self.chips = {
            "seen": QLabel(),
            "kept": QLabel(),
            "missing": QLabel(),
            "duplicates": QLabel(),
        }
        for key, chip in self.chips.items():
            chip.setObjectName("chipAccent" if key == "kept" else "chip")
            chip.setVisible(False)
            status_row.addWidget(chip)
        actions.addLayout(status_row)

        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.setRange(0, 1)
        self.progress.setValue(0)
        # Shown only while a run is going; an idle track just reads as a stray
        # grey bar under the buttons.
        self.progress.setVisible(False)
        actions.addWidget(self.progress)
        return actions

    def _build_results(self) -> QWidget:
        self.table = QTableWidget(0, len(self.table_columns()))
        self.table.setHorizontalHeaderLabels([header for _, header in self.table_columns()])
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(False)
        self.table.horizontalHeader().setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        for index, width in enumerate(self.column_widths()[: self.table.columnCount()]):
            self.table.setColumnWidth(index, width)

        empty = QLabel(self.empty_message)
        empty.setObjectName("empty")
        empty.setAlignment(Qt.AlignCenter)
        empty.setWordWrap(True)

        # An empty grid with headers and nothing under them reads as broken;
        # a sentence explains itself.
        self._results_stack = QStackedWidget()
        self._results_stack.addWidget(empty)
        self._results_stack.addWidget(self.table)

        self.console = QPlainTextEdit()
        self.console.setObjectName("console")
        self.console.setReadOnly(True)
        self.console.setMaximumBlockCount(MAX_LOG_LINES)

        results = QTabWidget()
        results.addTab(self._results_stack, "Rezultati")
        results.addTab(self.console, "Log")
        results.setMinimumHeight(170)
        return results

    def _connect(self) -> None:
        self.start_button.clicked.connect(self.start)
        self.stop_button.clicked.connect(self._request_stop)
        self.browse_button.clicked.connect(self._choose_output_dir)
        self.open_button.clicked.connect(self._open_output_dir)

        self._process.stopping.connect(self._on_stopping)
        self._process.logged.connect(self._append_log)
        self._process.rowArrived.connect(self._append_row)
        self._process.progressed.connect(self._update_stats)
        self._process.targetStarted.connect(self._on_target_started)
        self._process.targetDone.connect(self._on_target_done)
        self._process.succeeded.connect(self._on_succeeded)
        self._process.cancelled.connect(self._on_cancelled)
        self._process.failed.connect(self._on_failed)
        self._process.finished.connect(self._on_finished)

    # ---- actions --------------------------------------------------------

    @property
    def running(self) -> bool:
        return self._process.running

    def stop(self) -> None:
        self._process.stop()

    def wait_for_exit(self, timeout_ms: int = 5000) -> bool:
        return self._process.wait_for_exit(timeout_ms)

    def start(self) -> None:
        if self._process.running:
            return
        try:
            config = self.build_config()
        except ValueError as exc:
            QMessageBox.warning(self, "Provjeri postavke", str(exc))
            return

        if not self.preflight():
            return

        output_dir = Path(self.output_edit.text().strip() or str(paths.default_output_dir()))
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            QMessageBox.critical(self, "Mapa nije dostupna", f"Ne mogu pisati u {output_dir}:\n{exc}")
            return
        config["output_dir"] = str(output_dir)

        self.save_settings()
        self.table.setRowCount(0)
        self._results_stack.setCurrentIndex(0)
        self.console.clear()
        self._files = []
        self._truncated = False
        for chip in self.chips.values():
            chip.setVisible(False)
        self.progress.setVisible(True)
        self.progress.setRange(0, 0)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.open_button.setEnabled(False)
        self.status_label.setText("Pokrećem…")
        self._process.start(config)

    def _request_stop(self) -> None:
        self._process.stop()

    def _on_stopping(self) -> None:
        # Immediate feedback: the worker may take a moment to unwind (Playwright
        # has a browser to close), and a button that still says "Zaustavi"
        # invites a second, pointless click.
        self.stop_button.setEnabled(False)
        self.stop_button.setText("Zaustavljam…")
        self.progress.setRange(0, 0)
        self.status_label.setText("Zaustavljam — spremam ono što je prikupljeno…")

    def _choose_output_dir(self) -> None:
        chosen = QFileDialog.getExistingDirectory(self, "Mapa za spremanje", self.output_edit.text())
        if chosen:
            self.output_edit.setText(chosen)

    def _open_output_dir(self) -> None:
        target = Path(self._files[0]).parent if self._files else Path(self.output_edit.text())
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    # ---- event handling -------------------------------------------------

    def _append_log(self, line: str) -> None:
        self.console.appendPlainText(line)

    def _append_row(self, row: dict) -> None:
        if self.table.rowCount() >= MAX_TABLE_ROWS:
            if not self._truncated:
                self._truncated = True
                self._append_log(
                    f"[gui] Tablica prikazuje prvih {MAX_TABLE_ROWS} redova; "
                    "cijeli rezultat je u datoteci."
                )
            return
        self._results_stack.setCurrentIndex(1)
        index = self.table.rowCount()
        self.table.insertRow(index)
        for column, (field, _header) in enumerate(self.table_columns()):
            self.table.setItem(index, column, QTableWidgetItem(str(row.get(field) or "")))
        self.table.scrollToBottom()

    def _update_stats(self, stats: dict) -> None:
        duplicates = stats.get("duplicate_email", 0) + stats.get("duplicate_company", 0)
        values = {
            "seen": f"{stats.get('seen', 0)} pregledano",
            "kept": f"{stats.get('kept', 0)} s e-mailom",
            "missing": f"{stats.get('missing', 0)} bez e-maila",
            "duplicates": f"{duplicates} duplikata",
        }
        for key, chip in self.chips.items():
            chip.setText(values[key])
            # Only a source that keeps address-less entries has anything to say here.
            chip.setVisible(key != "missing" or bool(stats.get("missing")))

    def _on_target_started(self, index: int, total: int, label: str) -> None:
        self.progress.setRange(0, total)
        self.progress.setValue(index - 1)
        self.status_label.setText(f"({index}/{total})  {label}")

    def _on_target_done(self, event: dict) -> None:
        self.progress.setValue(int(event.get("index", 0)))
        self._update_stats(event.get("stats") or {})
        for key in ("csv", "xlsx", "missing_csv", "missing_xlsx"):
            value = event.get(key)
            if value:
                self._files.append(value)
        self._append_log(f"[gui] {event.get('label', '')}: {event.get('rows', 0)} redova → {event.get('csv')}")

    def _on_succeeded(self, event: dict) -> None:
        rows = event.get("rows", 0)
        self.status_label.setText(f"Gotovo — {rows} redova s e-mailom.")
        self.open_button.setEnabled(bool(self._files))

    def _on_cancelled(self, event: dict) -> None:
        self.status_label.setText("Zaustavljeno — spremljeno je ono što je do tada prikupljeno.")
        self.open_button.setEnabled(bool(self._files))

    def _on_failed(self, message: str, tb: str) -> None:
        self.status_label.setText("Greška.")
        if tb:
            self._append_log(tb)
        self.open_button.setEnabled(bool(self._files))
        QMessageBox.critical(self, "Skrejpanje nije uspjelo", message)

    def _on_finished(self) -> None:
        self.progress.setVisible(False)
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.stop_button.setText("Zaustavi")
