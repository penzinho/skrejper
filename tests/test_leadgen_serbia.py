"""LakoDoPosla + Poslovi.rs parser tests on saved live fixtures (2026-08-05)."""

import unittest
from pathlib import Path

from app.leadgen.sources import lakodoposla, poslovi_rs

FIXTURES = Path(__file__).resolve().parent / "fixtures"
LDP = FIXTURES / "lakodoposla"
PRS = FIXTURES / "poslovi_rs"


@unittest.skipUnless(LDP.exists(), "nema lakodoposla fixtureova")
class LakoDoPoslaTest(unittest.TestCase):
    def test_parse_postings_page(self):
        parsed = lakodoposla.parse_postings_page(
            (LDP / "postings-p1.json").read_text(encoding="utf-8")
        )
        self.assertGreater(parsed["last_page"], 1)
        self.assertGreater(len(parsed["postings"]), 10)
        self.assertGreater(len(parsed["employers"]), 5)

        # The API embeds the full employer: this is the source with no
        # enrichment step needed.
        rich = [e for e in parsed["employers"] if e["tax_id"]]
        self.assertTrue(rich, "expected at least one employer with a PIB")
        sample = rich[0]
        self.assertRegex(sample["tax_id"], r"^\d{6,}$")
        self.assertTrue(sample["city"])
        self.assertEqual(sample["country"], "RS")

        posting = parsed["postings"][0]
        self.assertTrue(posting["source_id"])
        self.assertTrue(posting["title"])
        self.assertRegex(posting["expires_at"], r"^\d{4}-\d{2}-\d{2}$")


@unittest.skipUnless(PRS.exists(), "nema poslovi.rs fixtureova")
class PosloviRsTest(unittest.TestCase):
    def test_parse_sitemap(self):
        ads = poslovi_rs.parse_sitemap_ads((PRS / "sitemap-sample.xml").read_text(encoding="utf-8"))
        self.assertTrue(ads)
        for ad in ads:
            self.assertTrue(ad["id"].isdigit())
            self.assertTrue(ad["employer_slug"])
        # Sorted newest-first.
        ids = [int(ad["id"]) for ad in ads]
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_parse_ad_page(self):
        html = (PRS / "posao-1.html").read_text(encoding="utf-8")
        url = "https://www.poslovi.rs/posao/general-transport-doo/vozaci-mz-237896"
        parsed = poslovi_rs.parse_ad_page(html, url)
        self.assertIsNotNone(parsed)
        employer, posting = parsed
        self.assertEqual(employer["source_id"], "general-transport-doo")
        self.assertEqual(employer["name"], "General transport d.o.o.")
        self.assertEqual(employer["city"], "Kula")
        self.assertEqual(employer["country"], "RS")
        self.assertEqual(posting["source_id"], "237896")
        self.assertIn("Vozač", posting["title"])
        self.assertEqual(posting["expires_at"], "2026-09-05")

    def test_non_ad_url_rejected(self):
        self.assertIsNone(poslovi_rs.parse_ad_page("<title>x</title>", "https://www.poslovi.rs/o-nama"))


if __name__ == "__main__":
    unittest.main()
