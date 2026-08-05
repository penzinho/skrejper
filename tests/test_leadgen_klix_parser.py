"""Klix parser tests.

Two layers:

* synthetic HTML that exercises the label-based extraction logic — always runs;
* real saved pages under ``tests/fixtures/klix/`` — skipped until someone runs
  ``python scripts/leadgen.py fetch-fixtures`` from a machine with normal
  internet access and commits the files. The real-fixture tests assert only
  invariants (links found, ids numeric), not exact values.
"""

import unittest
from pathlib import Path

from app.leadgen.sources import klix

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "klix"

EMPLOYER_HTML = """
<html><body>
<h1>Drvo-Stil</h1>
<div class="info">
  <ul>
    <li><strong>Puni naziv:</strong> Drvo-Stil d.o.o. Sarajevo</li>
    <li><strong>JIB:</strong> 4200000110005</li>
    <li><strong>PDV broj:</strong> 200000110005</li>
    <li><strong>Adresa:</strong> Ferhadija 12, 71000 Sarajevo</li>
    <li><strong>Web:</strong> <a href="https://drvostil.ba">drvostil.ba</a></li>
  </ul>
</div>
<h2>Oglasi poslodavca</h2>
<ul class="ads">
  <li><a href="/oglasi/stolar-m-z/5501">Stolar (m/ž)</a> <span>Ističe: 15.08.2026.</span></li>
  <li><a href="/oglasi/pomocni-radnik/4200">Pomoćni radnik</a> <span>Istekao: 01.03.2025.</span></li>
</ul>
</body></html>
"""

AD_LISTING_HTML = """
<html><body>
<ul>
  <li class="ad">
    <a href="/oglasi/vozac-kamiona/6001">Vozač kamiona (m/ž)</a>
    <a href="/poslodavci/eurotrans/312">Eurotrans d.o.o.</a>
    <span>Objavljen: 01.08.2026. Ističe: 30.08.2026.</span>
  </li>
  <li class="ad">
    <a href="/oglasi/konobar/6002">Konobar</a>
    <span>Ističe: 20.08.2026.</span>
  </li>
</ul>
<a rel="next" href="/oglasi?page=2">Sljedeća</a>
</body></html>
"""

AD_DETAIL_HTML = """
<html><body>
<h1>Vozač kamiona (m/ž)</h1>
<div><span>Mjesto rada:</span> <span>Banja Luka</span></div>
<p>Objavljen: 01.08.2026.</p>
<p>Ističe: 30.08.2026.</p>
<p>Broj izvršilaca: 3</p>
<a href="/poslodavci/eurotrans/312">Eurotrans d.o.o.</a>
<div class="body">Potrebni vozači C kategorije. Prijave na posao@eurotrans.ba ili 065 123 456.</div>
</body></html>
"""


class EmployerParserTest(unittest.TestCase):
    def test_parse_employer_page(self):
        employer, postings = klix.parse_employer_page(
            EMPLOYER_HTML, "https://posao.klix.ba/poslodavci/drvo-stil/42"
        )
        self.assertEqual(employer["source_id"], "42")
        self.assertEqual(employer["name"], "Drvo-Stil")
        self.assertEqual(employer["legal_name"], "Drvo-Stil d.o.o. Sarajevo")
        self.assertEqual(employer["tax_id"], "4200000110005")
        self.assertEqual(employer["vat_id"], "200000110005")
        self.assertEqual(employer["address"], "Ferhadija 12, 71000 Sarajevo")
        self.assertEqual(employer["website"], "https://drvostil.ba")
        self.assertEqual(employer["country"], "BA")
        self.assertEqual(employer["public_sector_term"], "")

        self.assertEqual(len(postings), 2)
        by_id = {p["source_id"]: p for p in postings}
        self.assertEqual(by_id["5501"]["title"], "Stolar (m/ž)")
        self.assertEqual(by_id["5501"]["expires_at"], "2026-08-15")
        self.assertEqual(by_id["5501"]["employer_source_id"], "42")
        self.assertEqual(by_id["4200"]["expires_at"], "2025-03-01")


class AdListingParserTest(unittest.TestCase):
    def test_parse_ad_cards(self):
        cards = klix.parse_ad_cards(AD_LISTING_HTML)
        self.assertEqual(len(cards), 2)
        first = cards[0]
        self.assertEqual(first["id"], "6001")
        self.assertEqual(first["title"], "Vozač kamiona (m/ž)")
        self.assertEqual(first["employer_id"], "312")
        self.assertEqual(first["employer_name"], "Eurotrans d.o.o.")
        self.assertEqual(first["published_at"], "2026-08-01")
        self.assertEqual(first["expires_at"], "2026-08-30")
        # Second card has no employer link and only an expiry date.
        self.assertEqual(cards[1]["employer_id"], "")
        self.assertEqual(cards[1]["expires_at"], "2026-08-20")


class AdDetailParserTest(unittest.TestCase):
    def test_parse_ad_page(self):
        posting = klix.parse_ad_page(
            AD_DETAIL_HTML, "https://posao.klix.ba/oglasi/vozac-kamiona/6001"
        )
        self.assertEqual(posting["source_id"], "6001")
        self.assertEqual(posting["title"], "Vozač kamiona (m/ž)")
        self.assertEqual(posting["employer_source_id"], "312")
        self.assertEqual(posting["city"], "Banja Luka")
        self.assertEqual(posting["published_at"], "2026-08-01")
        self.assertEqual(posting["expires_at"], "2026-08-30")
        self.assertEqual(posting["workers_count"], "3")
        self.assertEqual(posting["contact_email"], "posao@eurotrans.ba")
        self.assertIn("employer_url", posting)


@unittest.skipUnless(FIXTURES.exists(), "nema živih fixtureova — vidi scripts/leadgen.py fetch-fixtures")
class RealFixtureTest(unittest.TestCase):
    def _read(self, name):
        path = FIXTURES / name
        if not path.exists():
            self.skipTest(f"fixture {name} nije spremljen")
        return path.read_text(encoding="utf-8")

    def test_employer_listing_has_links(self):
        html = self._read("poslodavci-list.html")
        entries = klix.parse_entity_links(html, klix.EMPLOYER_URL_RE)
        self.assertGreater(len(entries), 0)
        for entry in entries:
            self.assertTrue(entry["id"].isdigit())

    def test_ad_listing_has_links(self):
        html = self._read("oglasi-list.html")
        cards = klix.parse_ad_cards(html)
        self.assertGreater(len(cards), 0)

    def test_employer_page_parses(self):
        html = self._read("poslodavac-1.html")
        employer, postings = klix.parse_employer_page(
            html, "https://posao.klix.ba/poslodavci/x/1"
        )
        self.assertTrue(employer["name"])


if __name__ == "__main__":
    unittest.main()
