"""GVP — the German staffing industry association's member directory.

Not a job board: one page listing every member agency (head offices and
branches). Pure HTTP, no browser. The point of this page is *all* the
companies — the ones without an e-mail go to a second file, to be looked up.
"""

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from app.desktop import settings
from app.desktop.tabs.base import BaseScrapeTab
from app.scrapers.gvp import BUSINESS_AREAS, QUALITY_STANDARDS

DEFAULTS = {
    "search": "",
    "city": "",
    "zip": "",
    "business_area": "",
    "quality": "",
    "branches": False,
    "max_pages": 0,
    "member_limit": 0,
    "enrich": True,
    "enrich_pages": 4,
    "skip_seen": True,
    "dedupe_company": True,
    "write_xlsx": True,
    "output_dir": "",
}


class GvpTab(BaseScrapeTab):
    source = "gvp"
    settings_section = "gvp"
    page_title = "GVP — Mitgliederverzeichnis"
    page_subtitle = (
        "Imenik članova njemačkog udruženja agencija za zapošljavanje "
        "(personaldienstleister.de). Skupe se sve firme: one s e-mailom idu u glavnu "
        "datoteku, one bez u zasebnu „-missing-emails” datoteku za obogaćivanje."
    )
    empty_message = "Firme će se pojaviti ovdje čim krene skrejpanje — i one bez e-maila."

    def __init__(self, parent=None) -> None:
        self._missing_total = 0
        super().__init__(parent)

    def build_form(self) -> QWidget:
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        filters = QGroupBox("Filtri (isti kao na stranici)")
        filters_form = QFormLayout(filters)

        self.search = QLineEdit()
        self.search.setPlaceholderText("dio naziva firme")
        filters_form.addRow("Firma:", self.search)
        self.city = QLineEdit()
        self.city.setPlaceholderText("npr. München")
        filters_form.addRow("Mjesto:", self.city)
        self.zip = QLineEdit()
        self.zip.setPlaceholderText("npr. 80331")
        filters_form.addRow("PLZ:", self.zip)

        self.business_area = QComboBox()
        self.business_area.addItem("sva", "")
        for area in BUSINESS_AREAS:
            self.business_area.addItem(area, area)
        filters_form.addRow("Geschäftsfeld:", self.business_area)

        self.quality = QComboBox()
        self.quality.addItem("svi", "")
        for standard in QUALITY_STANDARDS:
            self.quality.addItem(standard, standard)
        filters_form.addRow("Qualitätsstandard:", self.quality)

        self.branches = QCheckBox("Podružnice (Niederlassung) umjesto centrala (Hauptstelle)")
        self.branches.setToolTip("Prekidač „Hauptstelle / Niederlassung” iznad liste na stranici.")
        filters_form.addRow("", self.branches)

        hint = QLabel(
            "Bez filtera se prolazi cijeli imenik — oko 8.700 unosa po 10 na stranici, "
            "što je desetak minuta. Traženje e-maila po webovima traje puno dulje; "
            "Zaustavi u svakom trenutku ostavlja obje datoteke spremljene."
        )
        hint.setObjectName("hint")
        hint.setWordWrap(True)
        filters_form.addRow(hint)
        layout.addWidget(filters, 3)

        options = QGroupBox("Opcije")
        options_form = QFormLayout(options)

        self.max_pages = QSpinBox()
        self.max_pages.setRange(0, 5000)
        self.max_pages.setSpecialValueText("sve")
        self.max_pages.setToolTip("Stranica imenika po 10 članova. 0 = dok ima članova.")
        options_form.addRow("Maks. stranica:", self.max_pages)

        self.member_limit = QSpinBox()
        self.member_limit.setRange(0, 100000)
        self.member_limit.setSpecialValueText("bez ograničenja")
        options_form.addRow("Maks. firmi:", self.member_limit)

        self.enrich = QCheckBox("Potraži e-mail na webu firme (Impressum / Kontakt)")
        self.enrich.setToolTip(
            "Za članove bez e-maila u imeniku otvori njihovu web stranicu i Impressum ili "
            "Kontakt. Nekoliko sekundi po firmi; neuspjeli pokušaji se pamte pa se ne ponavljaju."
        )
        options_form.addRow("", self.enrich)

        self.enrich_pages = QSpinBox()
        self.enrich_pages.setRange(1, 10)
        self.enrich_pages.setToolTip("Koliko stranica najviše otvoriti na jednom webu.")
        options_form.addRow("Maks. stranica po webu:", self.enrich_pages)
        self.enrich.toggled.connect(self.enrich_pages.setEnabled)

        self.skip_seen = QCheckBox("Preskoči firme koje sam već skrejpao")
        self.skip_seen.setToolTip(
            "Pamti se samo firma iz koje je izvučen e-mail; firme bez e-maila se "
            "provjeravaju ponovno (osim webova na kojima e-mail već nije nađen)."
        )
        self.dedupe_company = QCheckBox("Samo jedan unos po firmi")
        self.dedupe_company.setToolTip(
            "Centrala i podružnice su zasebni unosi. Uključeno: jedan red po firmi, "
            "a podružnice bez e-maila otpadaju ako je centrala dala e-mail."
        )
        self.write_xlsx = QCheckBox("Spremi i Excel (.xlsx)")
        for box in (self.skip_seen, self.dedupe_company, self.write_xlsx):
            options_form.addRow("", box)

        layout.addWidget(options, 2)
        return container

    def table_columns(self) -> list[tuple[str, str]]:
        return [
            ("company", "Firma"),
            ("email", "E-mail"),
            ("city", "Grad"),
            ("phone", "Telefon"),
            ("website", "Web"),
            ("email_source", "Izvor e-maila"),
        ]

    def column_widths(self) -> list[int]:
        return [230, 210, 120, 130, 180]

    # ---- settings -------------------------------------------------------

    def load_settings(self) -> None:
        values = settings.load(self.settings_section, DEFAULTS)
        self.search.setText(values["search"])
        self.city.setText(values["city"])
        self.zip.setText(values["zip"])
        for combo, value in ((self.business_area, values["business_area"]), (self.quality, values["quality"])):
            index = combo.findData(value)
            combo.setCurrentIndex(index if index >= 0 else 0)
        self.branches.setChecked(values["branches"])
        self.max_pages.setValue(values["max_pages"])
        self.member_limit.setValue(values["member_limit"])
        self.enrich.setChecked(values["enrich"])
        self.enrich_pages.setValue(values["enrich_pages"])
        self.enrich_pages.setEnabled(values["enrich"])
        self.skip_seen.setChecked(values["skip_seen"])
        self.dedupe_company.setChecked(values["dedupe_company"])
        self.write_xlsx.setChecked(values["write_xlsx"])
        if values["output_dir"]:
            self.output_edit.setText(values["output_dir"])

    def save_settings(self) -> None:
        settings.save(
            self.settings_section,
            {
                "search": self.search.text().strip(),
                "city": self.city.text().strip(),
                "zip": self.zip.text().strip(),
                "business_area": self.business_area.currentData() or "",
                "quality": self.quality.currentData() or "",
                "branches": self.branches.isChecked(),
                "max_pages": self.max_pages.value(),
                "member_limit": self.member_limit.value(),
                "enrich": self.enrich.isChecked(),
                "enrich_pages": self.enrich_pages.value(),
                "skip_seen": self.skip_seen.isChecked(),
                "dedupe_company": self.dedupe_company.isChecked(),
                "write_xlsx": self.write_xlsx.isChecked(),
                "output_dir": self.output_edit.text().strip(),
            },
        )

    # ---- run ------------------------------------------------------------

    def build_config(self) -> dict:
        filters = {
            "search": self.search.text().strip(),
            "city": self.city.text().strip(),
            "zip": self.zip.text().strip(),
            "business_area": self.business_area.currentData() or "",
            "quality": self.quality.currentData() or "",
        }
        # The filters end up in the file name, so two filtered runs on the same
        # day do not overwrite each other.
        parts = ["mitglieder", *[value for value in filters.values() if value]]
        if self.branches.isChecked():
            parts.append("niederlassungen")
        label = "GVP Mitglieder" + (f" ({', '.join(parts[1:])})" if len(parts) > 1 else "")

        return {
            "source": "gvp",
            "keep_without_email": True,
            "skip_seen": self.skip_seen.isChecked(),
            "dedupe_company": self.dedupe_company.isChecked(),
            "exclude_public_sector": False,
            "write_xlsx": self.write_xlsx.isChecked(),
            "targets": [{"category": "-".join(parts), "label": label}],
            "options": {
                **filters,
                "branches": self.branches.isChecked(),
                "max_pages": self.max_pages.value() or None,
                "member_limit": self.member_limit.value() or None,
                "enrich": self.enrich.isChecked(),
                "enrich_pages": self.enrich_pages.value(),
            },
        }

    def start(self) -> None:
        self._missing_total = 0
        super().start()

    def _on_target_done(self, event: dict) -> None:
        super()._on_target_done(event)
        self._missing_total += int(event.get("missing_rows") or 0)

    def _on_succeeded(self, event: dict) -> None:
        rows = event.get("rows", 0)
        self.status_label.setText(
            f"Gotovo — {rows} firmi s e-mailom, {self._missing_total} bez "
            "(u datoteci „-missing-emails”)."
        )
        self.open_button.setEnabled(bool(self._files))
