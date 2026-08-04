"""HZZ (burzarada.hzz.hr) — Croatian public job board. Needs Chromium."""

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from app.desktop import browsers, settings
from app.desktop.presets import preset_groups
from app.desktop.tabs.base import BaseScrapeTab
from app.desktop.widgets import CheckList
from app.scrapers.hzz import HZZ_CATEGORIES, HZZ_CATEGORY_GROUPS

DEFAULTS = {
    "category": "hospitality_tourism",
    "max_pages": 999,
    "results_per_page": 75,
    "start_page": 1,
    "company_limit": 0,
    "skip_seen": True,
    "exclude_public_sector": True,
    "dedupe_company": True,
    "write_xlsx": True,
    "output_dir": "",
}


class HzzTab(BaseScrapeTab):
    source = "hzz"
    settings_section = "hzz"

    def build_form(self) -> QWidget:
        container = QWidget()
        # Two columns rather than one tall stack: the window is wide, and this
        # keeps the options visible without scrolling.
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        selection = QGroupBox("Što skrejpamo")
        selection_layout = QVBoxLayout(selection)

        form = QFormLayout()
        form.setLabelAlignment(form.labelAlignment())
        self.category_combo = QComboBox()
        for key, label in HZZ_CATEGORIES.items():
            self.category_combo.addItem(label, key)
        self.category_combo.currentIndexChanged.connect(self._on_category_changed)
        form.addRow("Kategorija:", self.category_combo)
        selection_layout.addLayout(form)

        self.groups = CheckList(extra_buttons=[("preset", "Preporučene")])
        self.groups.buttons["preset"].clicked.connect(self._apply_preset)
        selection_layout.addWidget(QLabel("Podkategorije (zanimanja):"))
        selection_layout.addWidget(self.groups)

        hint = QLabel(
            "Svaka podkategorija je zasebna pretraga. „Preporučene” su ona zanimanja "
            "koja se skrejpaju u produkciji (scripts/run_hzz_*.py)."
        )
        hint.setObjectName("hint")
        hint.setWordWrap(True)
        selection_layout.addWidget(hint)
        selection_layout.addStretch(1)
        layout.addWidget(selection, 3)

        options = QGroupBox("Opcije")
        options_form = QFormLayout(options)

        self.max_pages = QSpinBox()
        self.max_pages.setRange(1, 999)
        self.max_pages.setToolTip("Broj stranica rezultata po podkategoriji. 999 = dok ima oglasa.")
        options_form.addRow("Maks. stranica:", self.max_pages)

        self.results_per_page = QComboBox()
        for value in (10, 25, 50, 75):
            self.results_per_page.addItem(str(value), value)
        options_form.addRow("Rezultata po stranici:", self.results_per_page)

        self.start_page = QSpinBox()
        self.start_page.setRange(1, 999)
        options_form.addRow("Počni od stranice:", self.start_page)

        self.company_limit = QSpinBox()
        self.company_limit.setRange(0, 100000)
        self.company_limit.setSpecialValueText("bez ograničenja")
        options_form.addRow("Maks. firmi:", self.company_limit)

        self.skip_seen = QCheckBox("Preskoči oglase koje sam već skrejpao")
        self.skip_seen.setToolTip(
            "Pamti se u korisničkoj mapi aplikacije. Bez ovoga se svaki put ide ispočetka."
        )
        self.exclude_public_sector = QCheckBox("Izbaci vrtiće, škole i općine")
        self.dedupe_company = QCheckBox("Samo jedan oglas po firmi")
        self.write_xlsx = QCheckBox("Spremi i Excel (.xlsx)")
        for box in (self.skip_seen, self.exclude_public_sector, self.dedupe_company, self.write_xlsx):
            options_form.addRow("", box)

        layout.addWidget(options, 2)
        return container

    def table_columns(self) -> list[tuple[str, str]]:
        return [
            ("company", "Firma"),
            ("email", "E-mail"),
            ("city", "Grad"),
            ("phone", "Telefon"),
            ("title", "Oglas"),
        ]

    # ---- form logic -----------------------------------------------------

    def _current_category(self) -> str:
        return self.category_combo.currentData()

    def _on_category_changed(self) -> None:
        category = self._current_category()
        self.groups.set_items([(g, g) for g in HZZ_CATEGORY_GROUPS.get(category, [])])
        self._apply_preset()

    def _apply_preset(self) -> None:
        preset = preset_groups(self._current_category())
        if preset:
            self.groups.set_checked(preset)
        else:
            self.groups.set_all(True)

    # ---- settings -------------------------------------------------------

    def load_settings(self) -> None:
        values = settings.load(self.settings_section, DEFAULTS)
        index = self.category_combo.findData(values["category"])
        self.category_combo.setCurrentIndex(index if index >= 0 else 0)
        self._on_category_changed()
        self.max_pages.setValue(values["max_pages"])
        rpp = self.results_per_page.findData(values["results_per_page"])
        self.results_per_page.setCurrentIndex(rpp if rpp >= 0 else self.results_per_page.count() - 1)
        self.start_page.setValue(values["start_page"])
        self.company_limit.setValue(values["company_limit"])
        self.skip_seen.setChecked(values["skip_seen"])
        self.exclude_public_sector.setChecked(values["exclude_public_sector"])
        self.dedupe_company.setChecked(values["dedupe_company"])
        self.write_xlsx.setChecked(values["write_xlsx"])
        if values["output_dir"]:
            self.output_edit.setText(values["output_dir"])

    def save_settings(self) -> None:
        settings.save(
            self.settings_section,
            {
                "category": self._current_category(),
                "max_pages": self.max_pages.value(),
                "results_per_page": self.results_per_page.currentData(),
                "start_page": self.start_page.value(),
                "company_limit": self.company_limit.value(),
                "skip_seen": self.skip_seen.isChecked(),
                "exclude_public_sector": self.exclude_public_sector.isChecked(),
                "dedupe_company": self.dedupe_company.isChecked(),
                "write_xlsx": self.write_xlsx.isChecked(),
                "output_dir": self.output_edit.text().strip(),
            },
        )

    # ---- run ------------------------------------------------------------

    def preflight(self) -> bool:
        return browsers.ensure_chromium(self)

    def build_config(self) -> dict:
        selected = self.groups.checked()
        if not selected:
            raise ValueError("Odaberi barem jednu podkategoriju.")

        category = self._current_category()
        return {
            "source": "hzz",
            "skip_seen": self.skip_seen.isChecked(),
            "dedupe_company": self.dedupe_company.isChecked(),
            "exclude_public_sector": self.exclude_public_sector.isChecked(),
            "write_xlsx": self.write_xlsx.isChecked(),
            "targets": [
                {
                    "category": category,
                    "label": HZZ_CATEGORIES[category],
                    "groups": selected,
                }
            ],
            "options": {
                "max_pages": self.max_pages.value(),
                "results_per_page": self.results_per_page.currentData(),
                "start_page": self.start_page.value(),
                "company_limit": self.company_limit.value() or None,
            },
        }
