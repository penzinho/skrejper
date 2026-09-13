"""Arbeitsagentur — the German federal job board's public JSON API.

Pure HTTP: no browser, no download, works the moment the app opens.
"""

from pathlib import Path

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
from app.desktop.widgets import CheckTree
from app.scrapers.arbeitsagentur import get_arbeitsagentur_categories
from app.scrapers.arbeitsagentur_labels import bilingual

DEFAULTS = {
    "categories": [],
    "berufsfelder": [],
    "keyword": "",
    "beruf": "",
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

        # Test with a Berufsfeld from whatever is ticked, so the probe reflects
        # the run the user was about to start — and so its occupation list is the
        # one they need.
        berufsfeld = None
        fields = self.categories.checked_fields()
        if fields:
            berufsfeld = fields[0]

        self._process.start(
            {
                "source": "arbeitsagentur",
                "mode": "probe",
                "keyword": self.keyword.text().strip() or None,
                "berufsfeld": berufsfeld,
                "output_dir": self.output_edit.text().strip(),
            }
        )

    def _on_probed(self, event: dict) -> None:
        if not event.get("ok"):
            self.status_label.setText(event.get("message") or "Veza ne radi.")
            return

        endpoint = (event.get("endpoint") or "").rsplit("/jobsuche-service", 1)[-1]
        total = event.get("total")
        field, field_total = event.get("berufsfeld"), event.get("berufsfeld_total")

        if total and not event.get("parsed"):
            # The board answers and counts postings, but we could not read a
            # single one out of the payload — its schema moved under us. Without
            # this the probe would report a healthy connection while every
            # export came out empty.
            message = (
                f"Veza radi ({total} oglasa), ali nijedan oglas nije pročitan iz "
                "odgovora — format odgovora se promijenio."
            )
            self._files = [event["dump"]] if event.get("dump") else []
            self.open_button.setEnabled(bool(self._files))
        elif field and not field_total:
            # The interesting failure: the board answers, but our category names
            # no longer match anything it indexes.
            message = (
                f"Veza radi ({total} oglasa), ali kategorija „{field}” vraća 0 — "
                "oznake kategorija su zastarjele."
            )
            self._files = [event["dump"]] if event.get("dump") else []
            self.open_button.setEnabled(bool(self._files))
        else:
            found = f" — {total} oglasa" if total is not None else ""
            extra = f", „{field}” {field_total}" if field else ""
            message = f"Veza radi{found}{extra}. Endpoint {endpoint}."

        if event.get("dump"):
            message += f"  Podaci: {Path(event['dump']).name}"
        self.status_label.setText(message)

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
        selection_layout.addWidget(QLabel("Kategorije i podkategorije (Berufsfelder):"))
        self.categories = CheckTree()
        self.categories.set_groups(
            [
                (
                    item["key"],
                    f"{bilingual(item['label'], item['label_hr'])}  "
                    f"({len(item['berufsfelder'])} polja)",
                    [
                        (field["name"], bilingual(field["name"], field["hr"]))
                        for field in item["fields"]
                    ],
                )
                for item in self._categories
            ]
        )
        selection_layout.addWidget(self.categories)

        hint = QLabel(
            "Hrvatski naziv je samo prijevod — portalu se šalje njemački. Otvori "
            "kategoriju („Razgrani”) i odznači polja koja ne trebaš; svaka kategorija "
            "piše u svoju datoteku. Bez odabira pretražuje se cijeli portal po pojmu "
            "ili zanimanju."
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
        self.keyword.setToolTip(
            "Slobodan tekst — traži po tekstu oglasa, pa hvata i nevezane oglase."
        )
        search_form.addRow("Pojam:", self.keyword)
        self.beruf = QLineEdit()
        self.beruf.setPlaceholderText("npr. Gesundheits- und Krankenpfleger/in")
        self.beruf.setToolTip(
            "Točno zanimanje s portala (Beruf) — uže i preciznije od pojma. Više njih "
            "odvoji znakom ; . Naziv mora biti točno kako ga portal piše: „Provjeri "
            "vezu” ispiše zanimanja odabrane kategorije s prijevodom.\n"
            "Npr. medicinske sestre: Gesundheits- und Krankenpfleger/in"
        )
        search_form.addRow("Zanimanje:", self.beruf)
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
        if values["categories"] or values["berufsfelder"]:
            self.categories.set_checked(
                [str(v) for v in values["categories"]],
                [str(v) for v in values["berufsfelder"]],
            )
        self.keyword.setText(values["keyword"])
        self.beruf.setText(values["beruf"])
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
                "categories": self.categories.checked_groups(),
                "berufsfelder": self.categories.checked_fields(),
                "keyword": self.keyword.text().strip(),
                "beruf": self.beruf.text().strip(),
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
        selected = self.categories.selection()
        keyword = self.keyword.text().strip()
        beruf = self.beruf.text().strip()
        if not selected and not keyword and not beruf:
            raise ValueError(
                "Odaberi barem jednu kategoriju ili upiši pojam / zanimanje za pretragu."
            )

        group_labels = {item["key"]: item["label"] for item in self._categories}
        if selected:
            targets = [
                {
                    "category": entry["key"],
                    "label": group_labels.get(entry["key"], entry["key"]),
                    # None means the whole category; a list narrows it to these
                    # fields while the export keeps the category's name.
                    "berufsfelder": entry["fields"],
                }
                for entry in selected
            ]
        else:
            targets = [{"category": None, "label": beruf or keyword}]

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
                "beruf": beruf or None,
                "location": self.location.text().strip() or None,
                "radius": self.radius.value() or None,
            },
        }
