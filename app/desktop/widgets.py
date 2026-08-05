"""Small reusable widgets."""

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
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


class TreeCheckList(QWidget):
    """A two-level checkbox tree: groups with individually checkable children.

    Checking a collapsed group checks everything under it (Qt's auto-tristate),
    so the common "whole category" case stays one click; expanding a group is
    how you narrow a run down to specific sub-fields.
    """

    changed = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.setSelectionMode(QTreeWidget.NoSelection)
        self.tree.setMinimumHeight(150)
        self.tree.setMaximumHeight(320)
        self.tree.itemChanged.connect(lambda _item, _col: self._on_changed())
        layout.addWidget(self.tree)

        row = QHBoxLayout()
        row.setSpacing(6)
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

        self._total_children = 0

    def set_groups(self, groups: list[tuple[str, str, list[tuple[str, str]]]]) -> None:
        """groups: (group_value, group_label, [(child_value, child_label), ...])."""
        self.tree.blockSignals(True)
        self.tree.clear()
        self._total_children = 0
        for value, label, children in groups:
            parent = QTreeWidgetItem([label])
            parent.setData(0, Qt.UserRole, value)
            parent.setFlags(
                parent.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsAutoTristate
            )
            parent.setCheckState(0, Qt.Unchecked)
            for child_value, child_label in children:
                child = QTreeWidgetItem([child_label])
                child.setData(0, Qt.UserRole, child_value)
                child.setToolTip(0, child_value)
                child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
                child.setCheckState(0, Qt.Unchecked)
                parent.addChild(child)
                self._total_children += 1
            self.tree.addTopLevelItem(parent)
        self.tree.collapseAll()
        self.tree.blockSignals(False)
        self._on_changed()

    def set_all(self, checked: bool) -> None:
        state = Qt.Checked if checked else Qt.Unchecked
        self.tree.blockSignals(True)
        for index in range(self.tree.topLevelItemCount()):
            self.tree.topLevelItem(index).setCheckState(0, state)
        self.tree.blockSignals(False)
        self._on_changed()

    def selection(self) -> dict[str, list[str]]:
        """Checked children per group, only for groups with at least one."""
        out: dict[str, list[str]] = {}
        for index in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(index)
            values = [
                parent.child(i).data(0, Qt.UserRole)
                for i in range(parent.childCount())
                if parent.child(i).checkState(0) == Qt.Checked
            ]
            if values:
                out[parent.data(0, Qt.UserRole)] = values
        return out

    def group_size(self, group_value: str) -> int:
        for index in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(index)
            if parent.data(0, Qt.UserRole) == group_value:
                return parent.childCount()
        return 0

    def set_selection(self, selection: dict[str, list[str] | str]) -> None:
        """selection: group -> list of child values, or "all" for the whole group."""
        self.tree.blockSignals(True)
        for index in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(index)
            wanted = selection.get(parent.data(0, Qt.UserRole))
            for i in range(parent.childCount()):
                child = parent.child(i)
                checked = wanted == "all" or (
                    isinstance(wanted, list) and child.data(0, Qt.UserRole) in wanted
                )
                child.setCheckState(0, Qt.Checked if checked else Qt.Unchecked)
        self.tree.blockSignals(False)
        self._on_changed()

    def _on_changed(self) -> None:
        checked = sum(len(v) for v in self.selection().values())
        self.count_label.setText(f"{checked} / {self._total_children} odabrano")
        self.changed.emit()
