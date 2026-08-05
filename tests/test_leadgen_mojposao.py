"""MojPosao.ba parser tests, on saved live fixtures (2026-08-05)."""

import unittest
from pathlib import Path

from app.leadgen.sources import mojposao_ba

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "mojposao_ba"


@unittest.skipUnless(FIXTURES.exists(), "nema fixtureova — vidi scripts/leadgen.py fetch-fixtures")
class SearchApiTest(unittest.TestCase):
    def test_parse_search_page(self):
        parsed = mojposao_ba.parse_search_page(
            (FIXTURES / "search-p1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(parsed["total_pages"], 12)
        self.assertGreater(len(parsed["postings"]), 50)
        self.assertGreater(len(parsed["employers"]), 10)
        for posting in parsed["postings"]:
            self.assertTrue(posting["source_id"])
            self.assertTrue(posting["title"])
            self.assertTrue(posting["detail_url"].startswith("https://www.mojposao.ba/posao/"))
        # UUID ids, dates cut to ISO days.
        sample = parsed["postings"][0]
        self.assertRegex(sample["source_id"], r"^[0-9a-f-]{36}$")
        self.assertRegex(sample["published_at"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertTrue(sample["employer_source_id"])


@unittest.skipUnless(FIXTURES.exists(), "nema fixtureova")
class EmployerPageTest(unittest.TestCase):
    def test_parse_employer_page(self):
        url = "https://www.mojposao.ba/poslodavac/7aad0535-6c21-11ef-b01f-02798cc052cf/globalna-hrana-d-o-o"
        employer = mojposao_ba.parse_employer_page(
            (FIXTURES / "poslodavac-1.html").read_text(encoding="utf-8"),
            url, "7aad0535-6c21-11ef-b01f-02798cc052cf",
        )
        self.assertEqual(employer["name"], "Globalna hrana d.o.o.")
        self.assertEqual(employer["source_id"], "7aad0535-6c21-11ef-b01f-02798cc052cf")
        self.assertEqual(employer["country"], "BA")
        self.assertEqual(employer["public_sector_term"], "")


if __name__ == "__main__":
    unittest.main()
