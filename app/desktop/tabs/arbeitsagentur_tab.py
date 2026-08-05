"""Arbeitsagentur — the German federal job board's public JSON API.

Pure HTTP: no browser, no download, works the moment the app opens.
"""

from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from app.desktop import settings
from app.desktop.tabs.base import BaseScrapeTab
from app.desktop.widgets import CheckList
from app.scrapers.arbeitsagentur import get_arbeitsagentur_categories

DEFAULTS = {
    "categories": [],
    "keyword": "",
    "location": "",
    "radius": 0,
    "max_pages": 5,
    "results_per_page": 100,
    "listing_limit": 0,
    "company_limit": 0,
    "skip_seen": True,
    "dedupe_company": True,
    "write_xlsx": True,
    "output_dir": "",
}


class ArbeitsagenturTab(BaseScrapeTab):
    source = "arbeitsagentur"
    settings_section = "arbeitsagentur"
    page_title = "Arbeitsagentur — Jobsuche"
    page_subtitle = (
        "Njemački javni portal, preko službenog API-ja. Bez preglednika, "
        "radi odmah."
    )

    def __init__(self, parent=None) -> None:
        self._categories = get_arbeitsagentur_categories()
        super().__init__(parent)

        # A full run takes minutes before it can tell you the board is
        # unreachable; this answers the same question with one request.
        self.probe_button = QPushButton("Provjeri vezu")
        self.probe_button.setToolTip(
            "Jedan upit prema Arbeitsagenturu — javlja radi li veza i koji je endpoint živ."
        )
        self.button_row.insertWidget(2, self.probe_button)
        self.probe_button.clicked.connect(self._probe)
        self._process.probed.connect(self._on_probed)
        self._process.finished.connect(lambda: self.probe_button.setEnabled(True))

    def _probe(self) -> None:
        if self._process.running:
            return
        self.console.clear()
        self.probe_button.setEnabled(False)
        self.start_button.setEnabled(False)
        self.status_label.setText("Provjeravam vezu…")
        self._process.start(
            {
                "source": "arbeitsagentur",
                "mode": "probe",
                "keyword": self.keyword.text().strip() or None,
            }
        )

    def _on_probed(self, event: dict) -> None:
        if event.get("ok"):
            endpoint = (event.get("endpoint") or "").rsplit("/jobsuche-service", 1)[-1]
            total = event.get("total")
            found = f", {total} oglasa" if total is not None else ""
            self.status_label.setText(f"Veza radi — endpoint {endpoint}{found}.")
        else:
            self.status_label.setText(event.get("message") or "Veza ne radi.")

    def build_form(self) -> QWidget:
        container = QWidget()
        # Two columns rather than one tall stack: the window is wide, and this
        # keeps the options visible without scrolling.
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        right = QVBoxLayout()
        right.setSpacing(10)

        selection = QGroupBox("Što skrejpamo")
        selection_layout = QVBoxLayout(selection)
        selection_layout.addWidget(QLabel("Kategorije (Berufsfelder):"))
        self.categories = CheckList()
        self.categories.set_items(
            [
                (item["key"], f"{item['label']}  ({len(item['berufsfelder'])} polja)")
                for item in self._categories
            ]
        )
        selection_layout.addWidget(self.categories)

        hint = QLabel(
            "Svaka kategorija je grupa njemačkih Berufsfelder polja i piše se u svoju "
            "datoteku. Bez odabrane kategorije pretražuje se cijeli portal po pojmu."
        )
        hint.setObjectName("hint")
        hint.setWordWrap(True)
        selection_layout.addWidget(hint)

        # These live under the category list rather than in "Opcije": the right
        # column is the taller of the two, and this is where the spare room is.
        self.skip_seen = QCheckBox("Preskoči oglase koje sam već skrejpao")
        self.skip_seen.setToolTip(
            "Pamti se u korisničkoj mapi aplikacije. Bez ovoga se svaki put ide ispočetka."
        )
        self.dedupe_company = QCheckBox("Samo jedan oglas po firmi")
        self.write_xlsx = QCheckBox("Spremi i Excel (.xlsx)")
        selection_layout.addSpacing(4)
        for box in (self.skip_seen, self.dedupe_company, self.write_xlsx):
            selection_layout.addWidget(box)

        selection_layout.addStretch(1)
        layout.addWidget(selection, 3)

        search = QGroupBox("Pretraga (opcionalno)")
        search_form = QFormLayout(search)
        self.keyword = QLineEdit()
        self.keyword.setPlaceholderText("npr. Schweisser")
        search_form.addRow("Pojam:", self.keyword)
        self.location = QLineEdit()
        self.location.setPlaceholderText("npr. München")
        search_form.addRow("Mjesto:", self.location)
        self.radius = QSpinBox()
        self.radius.setRange(0, 200)
        self.radius.setSuffix(" km")
        self.radius.setSpecialValueText("zadano (25 km)")
        search_form.addRow("Radius:", self.radius)
        right.addWidget(search)

        options = QGroupBox("Opcije")
        options_form = QFormLayout(options)

        self.max_pages = QSpinBox()
        self.max_pages.setRange(1, 999)
        self.max_pages.setToolTip(
            "Broj stranica po Berufsfeldu, ne po kategoriji — kategorija s 19 polja "
            "napravi 19 × ovoliko pretraga."
        )
        options_form.addRow("Maks. stranica (po polju):", self.max_pages)

        self.results_per_page = QSpinBox()
        self.results_per_page.setRange(1, 100)
        options_form.addRow("Rezultata po stranici:", self.results_per_page)

        self.listing_limit = QSpinBox()
        self.listing_limit.setRange(0, 100000)
        self.listing_limit.setSpecialValueText("bez ograničenja")
        self.listing_limit.setToolTip("Koliko oglasa najviše otvoriti u detalje po kategoriji.")
        options_form.addRow("Maks. detalja:", self.listing_limit)

        self.company_limit = QSpinBox()
        self.company_limit.setRange(0, 100000)
        self.company_limit.setSpecialValueText("bez ograničenja")
        options_form.addRow("Maks. firmi:", self.company_limit)

        right.addWidget(options)
        right.addStretch(1)
        layout.addLayout(right, 2)
        return container

    def table_columns(self) -> list[tuple[str, str]]:
        return [
            ("company", "Firma"),
            ("email", "E-mail"),
            ("city", "Grad"),
            ("category", "Kategorija"),
            ("title", "Oglas"),
        ]

    # ---- settings -------------------------------------------------------

    def load_settings(self) -> None:
        values = settings.load(self.settings_section, DEFAULTS)
        if values["categories"]:
            self.categories.set_checked([str(v) for v in values["categories"]])
        self.keyword.setText(values["keyword"])
        self.location.setText(values["location"])
        self.radius.setValue(values["radius"])
        self.max_pages.setValue(values["max_pages"])
        self.results_per_page.setValue(values["results_per_page"])
        self.listing_limit.setValue(values["listing_limit"])
        self.company_limit.setValue(values["company_limit"])
        self.skip_seen.setChecked(values["skip_seen"])
        self.dedupe_company.setChecked(values["dedupe_company"])
        self.write_xlsx.setChecked(values["write_xlsx"])
        if values["output_dir"]:
            self.output_edit.setText(values["output_dir"])

    def save_settings(self) -> None:
        settings.save(
            self.settings_section,
            {
                "categories": self.categories.checked(),
                "keyword": self.keyword.text().strip(),
                "location": self.location.text().strip(),
                "radius": self.radius.value(),
                "max_pages": self.max_pages.value(),
                "results_per_page": self.results_per_page.value(),
                "listing_limit": self.listing_limit.value(),
                "company_limit": self.company_limit.value(),
                "skip_seen": self.skip_seen.isChecked(),
                "dedupe_company": self.dedupe_company.isChecked(),
                "write_xlsx": self.write_xlsx.isChecked(),
                "output_dir": self.output_edit.text().strip(),
            },
        )

    # ---- run ------------------------------------------------------------

    def build_config(self) -> dict:
        selected = self.categories.checked()
        keyword = self.keyword.text().strip()
        if not selected and not keyword:
            raise ValueError("Odaberi barem jednu kategoriju ili upiši pojam za pretragu.")

        labels = {item["key"]: item["label"] for item in self._categories}
        if selected:
            targets = [{"category": key, "label": labels.get(key, key)} for key in selected]
        else:
            targets = [{"category": None, "label": keyword}]

        return {
            "source": "arbeitsagentur",
            "skip_seen": self.skip_seen.isChecked(),
            "dedupe_company": self.dedupe_company.isChecked(),
            "exclude_public_sector": False,
            "write_xlsx": self.write_xlsx.isChecked(),
            "targets": targets,
            "options": {
                "max_pages": self.max_pages.value(),
                "results_per_page": self.results_per_page.value(),
                "listing_limit": self.listing_limit.value() or None,
                "company_limit": self.company_limit.value() or None,
                "keyword": keyword or None,
                "location": self.location.text().strip() or None,
                "radius": self.radius.value() or None,
            },
        }
