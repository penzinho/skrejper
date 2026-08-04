"""Shared shell for a source tab: form on top, live results and log below."""

from pathlib import Path

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
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
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        form_scroll = QScrollArea()
        form_scroll.setWidgetResizable(True)
        form_scroll.setFrameShape(QScrollArea.NoFrame)
        form_scroll.setWidget(self.build_form())

        controls = QWidget()
        controls_layout = QVBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(8)

        output_row = QHBoxLayout()
        output_row.addWidget(QLabel("Spremi u:"))
        self.output_edit = QLineEdit(str(paths.default_output_dir()))
        output_row.addWidget(self.output_edit, 1)
        self.browse_button = QPushButton("Odaberi…")
        output_row.addWidget(self.browse_button)
        controls_layout.addLayout(output_row)

        button_row = QHBoxLayout()
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
        controls_layout.addLayout(button_row)

        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.setRange(0, 1)
        self.progress.setValue(0)
        controls_layout.addWidget(self.progress)

        self.status_label = QLabel("Spremno.")
        self.status_label.setObjectName("hint")
        self.stats_label = QLabel("")
        self.stats_label.setObjectName("stat")
        status_row = QHBoxLayout()
        status_row.addWidget(self.status_label, 1)
        status_row.addWidget(self.stats_label)
        controls_layout.addLayout(status_row)

        self.table = QTableWidget(0, len(self.table_columns()))
        self.table.setHorizontalHeaderLabels([header for _, header in self.table_columns()])
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

        self.console = QPlainTextEdit()
        self.console.setObjectName("console")
        self.console.setReadOnly(True)
        self.console.setMaximumBlockCount(MAX_LOG_LINES)

        results = QTabWidget()
        results.addTab(self.table, "Rezultati")
        results.addTab(self.console, "Log")

        splitter = QSplitter(Qt.Vertical)
        top = QWidget()
        top_layout = QVBoxLayout(top)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.addWidget(form_scroll)
        top_layout.addWidget(controls)
        splitter.addWidget(top)
        splitter.addWidget(results)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        # The form needs real room or the options group collapses to a sliver on
        # first show; the results pane can start small and grow as rows arrive.
        form_scroll.setMinimumHeight(300)
        results.setMinimumHeight(180)
        splitter.setSizes([560, 240])

        layout.addWidget(splitter)

    def _connect(self) -> None:
        self.start_button.clicked.connect(self.start)
        self.stop_button.clicked.connect(self._process.stop)
        self.browse_button.clicked.connect(self._choose_output_dir)
        self.open_button.clicked.connect(self._open_output_dir)

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
        self.console.clear()
        self._files = []
        self._truncated = False
        self.stats_label.setText("")
        self.progress.setRange(0, 0)
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.open_button.setEnabled(False)
        self.status_label.setText("Pokrećem…")
        self._process.start(config)

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
        index = self.table.rowCount()
        self.table.insertRow(index)
        for column, (field, _header) in enumerate(self.table_columns()):
            self.table.setItem(index, column, QTableWidgetItem(str(row.get(field) or "")))
        self.table.scrollToBottom()

    def _update_stats(self, stats: dict) -> None:
        self.stats_label.setText(
            f"pregledano {stats.get('seen', 0)}  ·  "
            f"s e-mailom {stats.get('kept', 0)}  ·  "
            f"duplikati {stats.get('duplicate_email', 0) + stats.get('duplicate_company', 0)}"
        )

    def _on_target_started(self, index: int, total: int, label: str) -> None:
        self.progress.setRange(0, total)
        self.progress.setValue(index - 1)
        self.status_label.setText(f"({index}/{total}) {label}")

    def _on_target_done(self, event: dict) -> None:
        self.progress.setValue(int(event.get("index", 0)))
        self._update_stats(event.get("stats") or {})
        for key in ("csv", "xlsx"):
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
        self.progress.setRange(0, 1)
        self.progress.setValue(1)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
