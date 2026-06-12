import unittest

from app.scrapers.meinestadt import (
    _build_paginated_url,
    _build_search_url,
    _extract_email_from_sources,
    _extract_external_website,
    _parse_listing_card_text,
    _parse_pagination_state,
    _resolve_category,
)
from scripts.dedupe_csv_by_company import dedupe_rows


class MeinestadtScraperTests(unittest.TestCase):
    def test_resolve_category_accepts_key_and_label_slug(self):
        by_key = _resolve_category("it_data_processing")
        by_label = _resolve_category("IT Datenverarbeitung")

        self.assertEqual(by_key["path"], "/deutschland/jk/0-15711")
        self.assertEqual(by_label["key"], "it_data_processing")

    def test_parse_listing_card_text_extracts_company_location_and_date(self):
        parsed = _parse_listing_card_text(
            "\n".join(
                [
                    "Systemadministrator (m/w/d)",
                    "Clarins GmbH",
                    "Sofort-Bewerbung",
                    "Muenchen",
                    "17.04.2026",
                    "Vollzeit",
                ]
            ),
            "Systemadministrator (m/w/d)",
        )

        self.assertEqual(parsed["company"], "Clarins GmbH")
        self.assertEqual(parsed["location"], "Muenchen")
        self.assertEqual(parsed["published_at"], "17.04.2026")

    def test_extract_email_prefers_application_email_over_site_email(self):
        email = _extract_email_from_sources(
            '<a href="mailto:myjob@clarins.com">E-Mail</a>',
            "E-Mail: myjob@clarins.com",
            "kontakt@meinestadt.de",
        )

        self.assertEqual(email, "myjob@clarins.com")

    def test_extract_email_ignores_asset_filenames(self):
        email = _extract_email_from_sources(
            'src="jobs_premium_detail_960x378@2x.jpg" Kontakt: bewerbung@firma.de',
        )

        self.assertEqual(email, "bewerbung@firma.de")

    def test_extract_external_website_ignores_meinestadt_links(self):
        website = _extract_external_website(
            """
            <a href="https://jobs.meinestadt.de/deutschland">meinestadt</a>
            <a href="https://www.clarins.de/">Clarins</a>
            """
        )

        self.assertEqual(website, "https://www.clarins.de/")

    def test_build_search_url_matches_expected_category_path(self):
        category = _resolve_category("logistics_transport")

        self.assertEqual(
            _build_search_url(category),
            "https://jobs.meinestadt.de/deutschland/jk/0-15237",
        )

    def test_build_paginated_url_adds_or_replaces_page_query(self):
        self.assertEqual(
            _build_paginated_url("https://jobs.meinestadt.de/deutschland/jk/0-15237", 2),
            "https://jobs.meinestadt.de/deutschland/jk/0-15237?page=2",
        )
        self.assertEqual(
            _build_paginated_url("https://jobs.meinestadt.de/deutschland/jk/0-15237?order=search(stelle%2Ctrue)&page=4", 5),
            "https://jobs.meinestadt.de/deutschland/jk/0-15237?order=search%28stelle%2Ctrue%29&page=5",
        )

    def test_parse_pagination_state_reads_current_and_total_pages(self):
        self.assertEqual(_parse_pagination_state("Seite 2 von 35"), (2, 35))
        self.assertEqual(_parse_pagination_state("\n  Seite 1 von 35\n"), (1, 35))
        self.assertIsNone(_parse_pagination_state("Keine Ergebnisse"))
        self.assertIsNone(_parse_pagination_state(""))

    def test_dedupe_rows_keeps_one_row_per_company(self):
        rows = [
            {"email": "first@example.com", "company": "Clarins GmbH"},
            {"email": "second@example.com", "company": "Clarins GmbH"},
            {"email": "third@example.com", "company": "Example AG"},
        ]

        deduped_rows, stats = dedupe_rows(rows)

        self.assertEqual(len(deduped_rows), 2)
        self.assertEqual(stats["skipped_duplicate_company"], 1)
        self.assertEqual(deduped_rows[0]["email"], "first@example.com")
        self.assertEqual(deduped_rows[1]["company"], "Example AG")


if __name__ == "__main__":
    unittest.main()
