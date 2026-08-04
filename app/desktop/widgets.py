"""Small reusable widgets."""

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class CheckList(QWidget):
    """A checkbox list with select-all / clear shortcuts.

    Checkboxes rather than ctrl-click multi-selection: the selection survives a
    stray click, and it is obvious at a glance what a run will cover.
    """

    changed = Signal()

    def __init__(self, extra_buttons: list[tuple[str, str]] | None = None, parent=None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.list = QListWidget()
        self.list.setSelectionMode(QListWidget.NoSelection)
        # Capped so whatever sits below the list stays on screen; the list
        # scrolls internally. A partly visible last row is the point — it is what
        # tells you there is more.
        self.list.setMinimumHeight(150)
        self.list.setMaximumHeight(188)
        self.list.itemChanged.connect(lambda _item: self._on_changed())
        layout.addWidget(self.list)

        row = QHBoxLayout()
        row.setSpacing(6)
        self.buttons: dict[str, QPushButton] = {}
        for key, text in (extra_buttons or []):
            button = QPushButton(text)
            self.buttons[key] = button
            row.addWidget(button)
        self.all_button = QPushButton("Sve")
        self.none_button = QPushButton("Ništa")
        self.all_button.clicked.connect(lambda: self.set_all(True))
        self.none_button.clicked.connect(lambda: self.set_all(False))
        row.addWidget(self.all_button)
        row.addWidget(self.none_button)
        row.addStretch(1)
        self.count_label = QLabel("")
        self.count_label.setObjectName("hint")
        row.addWidget(self.count_label)
        layout.addLayout(row)

    def set_items(self, items: list[tuple[str, str]]) -> None:
        """items is a list of (value, label)."""
        self.list.blockSignals(True)
        self.list.clear()
        for value, label in items:
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, value)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list.addItem(item)
        self.list.blockSignals(False)
        self._on_changed()

    def set_all(self, checked: bool) -> None:
        state = Qt.Checked if checked else Qt.Unchecked
        self.list.blockSignals(True)
        for index in range(self.list.count()):
            self.list.item(index).setCheckState(state)
        self.list.blockSignals(False)
        self._on_changed()

    def set_checked(self, values: list[str]) -> None:
        wanted = set(values)
        self.list.blockSignals(True)
        for index in range(self.list.count()):
            item = self.list.item(index)
            item.setCheckState(Qt.Checked if item.data(Qt.UserRole) in wanted else Qt.Unchecked)
        self.list.blockSignals(False)
        self._on_changed()

    def checked(self) -> list[str]:
        return [
            self.list.item(index).data(Qt.UserRole)
            for index in range(self.list.count())
            if self.list.item(index).checkState() == Qt.Checked
        ]

    def _on_changed(self) -> None:
        self.count_label.setText(f"{len(self.checked())} / {self.list.count()} odabrano")
        self.changed.emit()
