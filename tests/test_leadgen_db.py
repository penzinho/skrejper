import tempfile
import unittest
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.schema import make_employer, make_posting


def _employer(**overrides):
    values = {
        "source": "klix", "source_id": "42", "name": "Drvo-Stil",
        "city": "Sarajevo", "country": "BA", "updated_at": "2026-01-01T00:00:00Z",
    }
    values.update(overrides)
    return make_employer(**values)


def _posting(**overrides):
    values = {
        "source": "klix", "source_id": "1001", "employer_source": "klix",
        "employer_source_id": "42", "title": "Vozač", "published_at": "2026-01-10",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    values.update(overrides)
    return make_posting(**values)


class DbTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db = LeadDb(Path(self._dir.name) / "test.db")

    def tearDown(self):
        self.db.close()
        self._dir.cleanup()

    def test_upsert_merges_fields(self):
        self.db.upsert_employer(_employer())
        # A later scrape adds the JIB but not the city; the city must survive.
        changed = self.db.upsert_employer(_employer(
            city="", tax_id="4200000110005", updated_at="2026-02-01T00:00:00Z"
        ))
        self.assertTrue(changed)
        employer = self.db.get_employer("klix", "42")
        self.assertEqual(employer["tax_id"], "4200000110005")
        self.assertEqual(employer["city"], "Sarajevo")

    def test_upsert_older_record_never_wins(self):
        self.db.upsert_employer(_employer(name="Novo Ime", updated_at="2026-03-01T00:00:00Z"))
        self.db.upsert_employer(_employer(name="Staro Ime", updated_at="2025-01-01T00:00:00Z"))
        self.assertEqual(self.db.get_employer("klix", "42")["name"], "Novo Ime")

    def test_no_change_returns_false(self):
        self.db.upsert_employer(_employer())
        self.assertFalse(self.db.upsert_employer(_employer()))

    def test_known_ids(self):
        self.db.upsert_posting(_posting())
        self.assertEqual(self.db.known_posting_ids("klix"), {"1001"})
        self.assertEqual(self.db.known_posting_ids("nsz"), set())

    def test_cursors(self):
        self.assertEqual(self.db.get_cursor("klix", "x", "def"), "def")
        self.db.set_cursor("klix", "x", "123")
        self.assertEqual(self.db.get_cursor("klix", "x"), "123")

    def test_ndjson_roundtrip_is_union(self):
        # Machine A knows employer 42 + posting 1001, machine B posting 1002.
        self.db.upsert_employer(_employer())
        self.db.upsert_posting(_posting())
        dump = Path(self._dir.name) / "a.ndjson"
        self.db.export_ndjson(dump)

        other = LeadDb(Path(self._dir.name) / "b.db")
        other.upsert_posting(_posting(source_id="1002", title="Skladištar"))
        changed = other.import_ndjson(dump)
        self.assertEqual(changed, 2)  # employer + posting 1001
        self.assertEqual(other.known_posting_ids("klix"), {"1001", "1002"})
        self.assertEqual(other.get_employer("klix", "42")["name"], "Drvo-Stil")
        # Importing the same dump again is a no-op.
        self.assertEqual(other.import_ndjson(dump), 0)
        other.close()


if __name__ == "__main__":
    unittest.main()
