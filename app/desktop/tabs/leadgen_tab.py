"""Leadovi (BiH / Srbija) — employer-centric lead generation.

Different from the HZZ/AA pages on purpose: the product of a run is not
"postings with an e-mail" but a *ranked employer list* built from posting
history in the local database. Rows streaming into the table during a run are
the postings being collected; the file that lands in the output folder is the
scored, deduplicated lead list.
"""

from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from app.desktop import settings
from app.desktop.tabs.base import BaseScrapeTab
from app.leadgen import registry

DEFAULTS = {
    "sources": [],
    "max_pages": 0,          # 0 = bez limita
    "full": False,
    "fetch_details": False,
    "include_public_sector": False,
    "min_ads_24m": 2,
    "write_xlsx": True,
    "output_dir": "",
}


class LeadgenTab(BaseScrapeTab):
    source = "leadgen"
    settings_section = "leadgen"
    page_title = "Leadovi — BiH i Srbija"
    page_subtitle = (
        "Poslodavci koji stalno zapošljavaju, rangirani po broju oglasa u zadnja "
        "24 mjeseca. Sve se skuplja u lokalnu bazu, pa svaki idući run samo dodaje novo."
    )
    empty_message = "Novi oglasi i firme pojavit će se ovdje tijekom skrejpanja."

    def build_form(self) -> QWidget:
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        sources_box = QGroupBox("Izvori")
        sources_layout = QVBoxLayout(sources_box)
        self.source_checks: dict[str, QCheckBox] = {}
        for entry in registry.available():
            check = QCheckBox(entry["label"])
            check.setChecked(entry["default_enabled"])
            if not entry["default_enabled"]:
                check.setToolTip(
                    "Izvor je iza flaga (uvjeti korištenja / robots) i ne ulazi u "
                    "zadani run — uključi ga svjesno."
                )
            self.source_checks[entry["name"]] = check
            sources_layout.addWidget(check)
        hint = QLabel(
            "Klix daje JIB, adresu, web i kompletnu povijest oglasa po firmi — "
            "on je temelj bodovanja. Ostali izvori stižu u sljedećim fazama."
        )
        hint.setObjectName("hint")
        hint.setWordWrap(True)
        sources_layout.addWidget(hint)
        sources_layout.addStretch(1)
        layout.addWidget(sources_box, 3)

        right = QVBoxLayout()
        options = QGroupBox("Opcije")
        options_form = QFormLayout(options)

        self.max_pages = QSpinBox()
        self.max_pages.setRange(0, 999)
        self.max_pages.setSpecialValueText("bez limita")
        self.max_pages.setToolTip("Limit stranica listinga — korisno za brzu probu.")
        options_form.addRow("Maks. stranica:", self.max_pages)

        self.full = QCheckBox("Puni prolaz direktorija poslodavaca")
        self.full.setToolTip(
            "Prvi run ga radi automatski. Uključi ručno kad želiš osvježiti "
            "povijest svih firmi (Klix, ~3000 stranica uz pauzu između zahtjeva)."
        )
        options_form.addRow(self.full)

        self.fetch_details = QCheckBox("Dohvati i detalj svakog oglasa")
        self.fetch_details.setToolTip(
            "Puni opis i kontakt iz samog oglasa. Sporije; povijest i bodovanje "
            "rade i bez toga."
        )
        options_form.addRow(self.fetch_details)
        right.addWidget(options)

        export_box = QGroupBox("Izvoz leadova")
        export_form = QFormLayout(export_box)
        self.min_ads = QSpinBox()
        self.min_ads.setRange(0, 99)
        self.min_ads.setToolTip("Firme s manje oglasa u zadnja 24 mjeseca ne ulaze u izvoz.")
        export_form.addRow("Min. oglasa u 24 mj.:", self.min_ads)
        self.include_public_sector = QCheckBox("Uključi javni sektor (škole, bolnice…)")
        export_form.addRow(self.include_public_sector)
        self.write_xlsx = QCheckBox("Spremi i Excel (.xlsx)")
        export_form.addRow(self.write_xlsx)
        right.addWidget(export_box)

        right.addStretch(1)
        layout.addLayout(right, 2)
        return container

    def build_config(self) -> dict:
        chosen = [name for name, check in self.source_checks.items() if check.isChecked()]
        if not chosen:
            raise ValueError("Odaberi barem jedan izvor.")
        return {
            "source": "leadgen",
            "sources": chosen,
            "max_pages": self.max_pages.value() or None,
            "full": self.full.isChecked(),
            "fetch_details": self.fetch_details.isChecked(),
            "include_public_sector": self.include_public_sector.isChecked(),
            "min_ads_24m": self.min_ads.value(),
            "write_xlsx": self.write_xlsx.isChecked(),
        }

    def table_columns(self) -> list[tuple[str, str]]:
        return [
            ("company", "Poslodavac"),
            ("title", "Pozicija"),
            ("city", "Grad"),
            ("published_at", "Datum"),
            ("source", "Izvor"),
        ]

    def column_widths(self) -> list[int]:
        return [240, 260, 120, 100, 80]

    def _update_stats(self, stats: dict) -> None:
        # Leadgen progress counts employers/postings, not e-mails.
        values = {
            "seen": f"{stats.get('requests', 0)} zahtjeva",
            "kept": f"{stats.get('postings', 0)} oglasa",
            "duplicates": f"{stats.get('employers', 0)} firmi",
        }
        for key, chip in self.chips.items():
            chip.setText(values[key])
            chip.setVisible(True)

    def _on_succeeded(self, event: dict) -> None:
        rows = event.get("rows", 0)
        self.status_label.setText(f"Gotovo — {rows} leadova u izvozu.")
        self.open_button.setEnabled(bool(self._files))

    def load_settings(self) -> None:
        values = settings.load(self.settings_section, DEFAULTS)
        chosen = set(values["sources"])
        if chosen:
            for name, check in self.source_checks.items():
                check.setChecked(name in chosen)
        self.max_pages.setValue(values["max_pages"])
        self.full.setChecked(values["full"])
        self.fetch_details.setChecked(values["fetch_details"])
        self.include_public_sector.setChecked(values["include_public_sector"])
        self.min_ads.setValue(values["min_ads_24m"])
        self.write_xlsx.setChecked(values["write_xlsx"])
        if values["output_dir"]:
            self.output_edit.setText(values["output_dir"])

    def save_settings(self) -> None:
        settings.save(self.settings_section, {
            "sources": [name for name, check in self.source_checks.items() if check.isChecked()],
            "max_pages": self.max_pages.value(),
            "full": self.full.isChecked(),
            "fetch_details": self.fetch_details.isChecked(),
            "include_public_sector": self.include_public_sector.isChecked(),
            "min_ads_24m": self.min_ads.value(),
            "write_xlsx": self.write_xlsx.isChecked(),
            "output_dir": self.output_edit.text().strip(),
        })
