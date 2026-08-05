import tempfile
import unittest
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.dedupe import dedupe_employers
from app.leadgen.schema import make_employer


class DedupeTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db = LeadDb(Path(self._dir.name) / "test.db")

    def tearDown(self):
        self.db.close()
        self._dir.cleanup()

    def _add(self, source, source_id, **fields):
        self.db.upsert_employer(make_employer(source=source, source_id=source_id, **fields))

    def _canonical(self, source, source_id):
        return self.db.get_employer(source, source_id)["canonical_key"]

    def test_tax_id_match_across_sources(self):
        self._add("klix", "1", name="Drvo-Stil d.o.o.", tax_id="4200000110005", city="Sarajevo")
        self._add("mojposao_ba", "abc", name="DRVO STIL", tax_id="JIB 4200000110005")
        dedupe_employers(self.db)
        self.assertEqual(self._canonical("klix", "1"), self._canonical("mojposao_ba", "abc"))
        # Canonical is the richer record (the one with the website/tax data).
        self.assertEqual(self._canonical("klix", "1"), "klix:1")

    def test_fuzzy_name_and_city(self):
        self._add("klix", "2", name="Eurotrans d.o.o. Banja Luka", city="Banja Luka")
        self._add("nsz", "77", name="ЕУРОТРАНС ДОО", city="Бања Лука")
        dedupe_employers(self.db)
        self.assertEqual(self._canonical("klix", "2"), self._canonical("nsz", "77"))

    def test_different_city_blocks_fuzzy_match(self):
        self._add("klix", "3", name="Metalac d.o.o.", city="Mostar")
        self._add("posao_rs", "9", name="Metalac d.o.o.", city="Kragujevac")
        dedupe_employers(self.db)
        self.assertNotEqual(self._canonical("klix", "3"), self._canonical("posao_rs", "9"))

    def test_unrelated_names_stay_apart(self):
        self._add("klix", "4", name="Alfa Gradnja d.o.o.", city="Tuzla")
        self._add("klix", "5", name="Beta Promet d.o.o.", city="Tuzla")
        dedupe_employers(self.db)
        self.assertNotEqual(self._canonical("klix", "4"), self._canonical("klix", "5"))


if __name__ == "__main__":
    unittest.main()
