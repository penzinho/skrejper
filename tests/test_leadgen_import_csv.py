import csv
import tempfile
import unittest
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.import_csv import import_csv_files

HZZ_FIELDS = [
    "email", "phone", "company", "city", "employer_address", "title", "group",
    "employment_type", "working_hours", "valid_from", "valid_to", "detail_url", "source",
]
AA_FIELDS = [
    "email", "company", "city", "title", "category", "published_at",
    "detail_url", "employer_website", "refnr", "source",
]


class ImportCsvTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db = LeadDb(Path(self._dir.name) / "test.db")

    def tearDown(self):
        self.db.close()
        self._dir.cleanup()

    def _write(self, name, fields, rows):
        path = Path(self._dir.name) / name
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_imports_both_formats(self):
        hzz = self._write("hzz.csv", HZZ_FIELDS, [{
            "email": "info@gradnja.hr", "phone": "091 111 222", "company": "Gradnja d.o.o.",
            "city": "Zagreb", "employer_address": "Ilica 1, 10000 Zagreb",
            "title": "Zidar", "group": "construction", "employment_type": "",
            "working_hours": "", "valid_from": "01.07.2026", "valid_to": "01.09.2026",
            "detail_url": "https://burzarada.hzz.hr/RadnoMjesto_Ispis.aspx?WebSifra=123",
            "source": "hzz",
        }])
        aa = self._write("aa.csv", AA_FIELDS, [{
            "email": "jobs@bau.de", "company": "Bau GmbH", "city": "München",
            "title": "Maurer", "category": "bau_ausbau", "published_at": "2026-07-15",
            "detail_url": "https://www.arbeitsagentur.de/jobsuche/jobdetail/x",
            "employer_website": "https://bau.de", "refnr": "10001-ABC-S", "source": "arbeitsagentur",
        }])

        stats = import_csv_files(self.db, [hzz, aa], log=lambda *_: None)
        self.assertEqual(stats["imported"], 2)
        self.assertEqual(len(self.db.employers("hzz")), 1)
        self.assertEqual(len(self.db.employers("arbeitsagentur")), 1)
        self.assertEqual(self.db.known_posting_ids("arbeitsagentur"), {"10001-ABC-S"})

        hzz_employer = self.db.employers("hzz")[0]
        self.assertEqual(hzz_employer["country"], "HR")
        self.assertEqual(hzz_employer["email"], "info@gradnja.hr")

        posting = self.db.postings("hzz")[0]
        self.assertEqual(posting["published_at"], "2026-07-01")
        self.assertEqual(posting["expires_at"], "2026-09-01")

    def test_reimport_is_idempotent(self):
        aa = self._write("aa.csv", AA_FIELDS, [{
            "email": "jobs@bau.de", "company": "Bau GmbH", "city": "München",
            "title": "Maurer", "category": "", "published_at": "2026-07-15",
            "detail_url": "", "employer_website": "", "refnr": "10001-ABC-S",
            "source": "arbeitsagentur",
        }])
        import_csv_files(self.db, [aa], log=lambda *_: None)
        stats = import_csv_files(self.db, [aa], log=lambda *_: None)
        self.assertEqual(stats["imported"], 0)


if __name__ == "__main__":
    unittest.main()
