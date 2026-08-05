"""Infostud parser tests on saved live fixtures (2026-08-05).

Napomena: adapter je iza flaga (DEFAULT_ENABLED=False, ToS) — testovi parsera
svejedno postoje da kod ostane ispravan za svjesno uključivanje.
"""

import unittest
from pathlib import Path

from app.leadgen import registry
from app.leadgen.sources import infostud

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "infostud"


class FlagTest(unittest.TestCase):
    def test_not_in_default_run(self):
        self.assertFalse(infostud.DEFAULT_ENABLED)
        self.assertNotIn("infostud", registry.default_sources())


@unittest.skipUnless(FIXTURES.exists(), "nema infostud fixtureova")
class ParserTest(unittest.TestCase):
    def _read(self, name):
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_jobs_sitemap(self):
        ads = infostud.parse_jobs_sitemap(self._read("jobs-sample.xml"))
        self.assertTrue(ads)
        for ad in ads:
            self.assertTrue(ad["id"].isdigit())
            self.assertTrue(ad["company_slug"])

    def test_profiles_sitemap(self):
        profiles = infostud.parse_profiles_sitemap(self._read("profiles-sample.xml"))
        self.assertGreater(len(profiles), 5)
        self.assertIn("ac-broker", profiles)

    def test_job_page_json_ld(self):
        parsed = infostud.parse_job_page(
            self._read("posao-1.html"),
            "https://poslovi.infostud.com/posao/prodavac-beograd/lidl-srbija-kd/680704",
        )
        self.assertIsNotNone(parsed)
        employer, posting = parsed
        self.assertEqual(employer["source_id"], "lidl-srbija-kd")
        self.assertEqual(employer["name"], "Lidl Srbija KD")
        self.assertEqual(employer["city"], "Nova Pazova")
        self.assertEqual(posting["source_id"], "680704")
        self.assertEqual(posting["published_at"], "2026-08-01")
        self.assertEqual(posting["expires_at"], "2026-08-31")
        self.assertEqual(posting["workers_count"], "1")
        self.assertNotIn("Lidl", posting["title"])  # employer suffix stripped

    def test_profile_page(self):
        profile = infostud.parse_profile_page(self._read("profil-1.html"))
        self.assertEqual(profile["tax_id"], "100285416")
        self.assertIn("Knez Mihailova", profile["address"])
        self.assertTrue(profile["website"].startswith("http"))


if __name__ == "__main__":
    unittest.main()
