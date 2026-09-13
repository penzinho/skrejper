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


class CheckTree(QWidget):
    """Two-level checkbox tree: a category, and the fields it is made of.

    Ticking the category ticks every field under it; unticking single fields
    narrows the run without turning the category into a dozen separate ones. The
    parent shows partially-checked, so a narrowed category is visible while
    collapsed.
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
        # Same reasoning as CheckList: capped height, scrolls internally, and a
        # partly visible last row is what says there is more below.
        # Taller than the flat list it replaced: an expanded category has to show
        # a useful number of its fields without scrolling away the categories.
        self.tree.setMinimumHeight(230)
        self.tree.setMaximumHeight(420)
        self.tree.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.tree)

        row = QHBoxLayout()
        row.setSpacing(6)
        self.all_button = QPushButton("Sve")
        self.none_button = QPushButton("Ništa")
        self.expand_button = QPushButton("Razgrani")
        self.all_button.clicked.connect(lambda: self.set_all(True))
        self.none_button.clicked.connect(lambda: self.set_all(False))
        self.expand_button.clicked.connect(self._toggle_expanded)
        for button in (self.expand_button, self.all_button, self.none_button):
            row.addWidget(button)
        row.addStretch(1)
        self.count_label = QLabel("")
        self.count_label.setObjectName("hint")
        row.addWidget(self.count_label)
        layout.addLayout(row)

    # ---- contents --------------------------------------------------------

    def set_groups(self, groups: list[tuple[str, str, list[tuple[str, str]]]]) -> None:
        """groups is a list of (key, label, [(value, label), ...])."""
        self.tree.blockSignals(True)
        self.tree.clear()
        for key, label, children in groups:
            parent = QTreeWidgetItem([label])
            # Names are long and the column is not: the tooltip is where the full
            # German value stays readable, and that is the half being sent.
            parent.setToolTip(0, label)
            parent.setData(0, Qt.UserRole, key)
            parent.setFlags(parent.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsAutoTristate)
            parent.setCheckState(0, Qt.Unchecked)
            for value, child_label in children:
                child = QTreeWidgetItem([child_label])
                child.setToolTip(0, child_label)
                child.setData(0, Qt.UserRole, value)
                child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
                child.setCheckState(0, Qt.Unchecked)
                parent.addChild(child)
            self.tree.addTopLevelItem(parent)
        self.tree.blockSignals(False)
        self._on_changed()

    def _groups(self):
        return [self.tree.topLevelItem(i) for i in range(self.tree.topLevelItemCount())]

    @staticmethod
    def _children(group):
        return [group.child(i) for i in range(group.childCount())]

    # ---- selection -------------------------------------------------------

    def set_all(self, checked: bool) -> None:
        state = Qt.Checked if checked else Qt.Unchecked
        self.tree.blockSignals(True)
        for group in self._groups():
            group.setCheckState(0, state)
            for child in self._children(group):
                child.setCheckState(0, state)
        self.tree.blockSignals(False)
        self._on_changed()

    def set_checked(self, groups: list[str], fields: list[str] | None = None) -> None:
        """Restore a selection: whole groups by key, or single fields by value.

        A group key with no fields listed for it means the whole group — that is
        also what a settings file written before fields existed looks like.
        """
        wanted_groups = set(groups)
        wanted_fields = set(fields or [])
        self.tree.blockSignals(True)
        for group in self._groups():
            key = group.data(0, Qt.UserRole)
            children = self._children(group)
            per_field = [c for c in children if c.data(0, Qt.UserRole) in wanted_fields]
            if per_field:
                for child in children:
                    child.setCheckState(0, Qt.Checked if child in per_field else Qt.Unchecked)
            else:
                state = Qt.Checked if key in wanted_groups else Qt.Unchecked
                for child in children:
                    child.setCheckState(0, state)
            if not children:
                group.setCheckState(0, Qt.Checked if key in wanted_groups else Qt.Unchecked)
        self.tree.blockSignals(False)
        self._on_changed()

    def selection(self) -> list[dict]:
        """One entry per group with at least one field checked.

        `fields` is None when the whole group is checked, so the caller can keep
        passing the group on its own and let the scraper resolve it.
        """
        out = []
        for group in self._groups():
            children = self._children(group)
            checked = [c.data(0, Qt.UserRole) for c in children if c.checkState(0) == Qt.Checked]
            if not checked:
                continue
            out.append(
                {
                    "key": group.data(0, Qt.UserRole),
                    "fields": None if len(checked) == len(children) else checked,
                }
            )
        return out

    def checked_groups(self) -> list[str]:
        return [entry["key"] for entry in self.selection()]

    def checked_fields(self) -> list[str]:
        """Every checked field, flat — what gets written to the settings file."""
        return [
            child.data(0, Qt.UserRole)
            for group in self._groups()
            for child in self._children(group)
            if child.checkState(0) == Qt.Checked
        ]

    # ---- internals -------------------------------------------------------

    def _toggle_expanded(self) -> None:
        expanded = any(group.isExpanded() for group in self._groups())
        for group in self._groups():
            group.setExpanded(not expanded)
        self.expand_button.setText("Skupi" if not expanded else "Razgrani")

    def _on_item_changed(self, _item, _column) -> None:
        # Qt's auto-tristate keeps parent and children in step for us; all this
        # has to do is report the new count.
        self._on_changed()

    def _on_changed(self) -> None:
        fields = len(self.checked_fields())
        total = sum(group.childCount() for group in self._groups())
        groups = len(self.selection())
        self.count_label.setText(f"{groups} kategorija, {fields} / {total} polja")
        self.changed.emit()
