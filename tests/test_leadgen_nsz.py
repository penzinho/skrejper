"""NSZ parser tests on saved live fixtures (2026-08-05)."""

import unittest
from pathlib import Path

from app.leadgen.sources import nsz

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nsz"


@unittest.skipUnless(FIXTURES.exists(), "nema nsz fixtureova")
class NszParserTest(unittest.TestCase):
    def _read(self, name):
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_newest_id_from_search(self):
        top = nsz.newest_id(self._read("search.html"))
        self.assertGreater(top, 100000)

    def test_parse_preview(self):
        parsed = nsz.parse_preview(
            self._read("preview-1.html"),
            "https://www.nsz.gov.rs/employee/jobs/preview/102240",
        )
        self.assertIsNotNone(parsed)
        employer, posting = parsed
        # Cyrillic body, Latin company name — kept as published.
        self.assertEqual(employer["name"], "CHEMCO doo")
        self.assertEqual(employer["city"], "Крагујевац")
        self.assertEqual(employer["country"], "RS")
        # Contact phone is in the ad body, not behind a portal.
        self.assertIn("063", employer["phone"])
        self.assertEqual(employer["public_sector_term"], "")

        self.assertEqual(posting["source_id"], "102240")
        self.assertIn("производњи", posting["title"])
        self.assertEqual(posting["published_at"], "2026-08-05")
        self.assertEqual(posting["expires_at"], "2026-09-04")
        self.assertEqual(posting["workers_count"], "1")
        self.assertIn("063", posting["contact_phone"])

    def test_empty_preview_is_none(self):
        self.assertIsNone(nsz.parse_preview(
            self._read("preview-empty.html"),
            "https://www.nsz.gov.rs/employee/jobs/preview/1",
        ))

    def test_public_sector_flagged(self):
        # A school ad would be flagged, not dropped, at parse time.
        html = self._read("preview-1.html").replace("CHEMCO doo", "ОШ Вук Караџић")
        employer, _ = nsz.parse_preview(html, "https://www.nsz.gov.rs/employee/jobs/preview/102240")
        self.assertTrue(employer["public_sector_term"])


if __name__ == "__main__":
    unittest.main()
