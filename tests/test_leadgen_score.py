import tempfile
import unittest
from datetime import date
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.schema import make_employer, make_posting
from app.leadgen.score import compute_scores

TODAY = date(2026, 8, 1)


class ScoreTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db = LeadDb(Path(self._dir.name) / "test.db")
        self.db.upsert_employer(make_employer(source="klix", source_id="1", name="Prevoz X"))

    def tearDown(self):
        self.db.close()
        self._dir.cleanup()

    def _post(self, source_id, title, published):
        self.db.upsert_posting(make_posting(
            source="klix", source_id=source_id, employer_source="klix",
            employer_source_id="1", title=title, published_at=published,
        ))

    def test_frequency_metrics(self):
        # 3 driver ads within 24 months (repeated position), one old ad outside.
        self._post("a", "Vozač kamiona", "2026-07-01")
        self._post("b", "Vozač kamiona", "2026-01-15")
        self._post("c", "Vozac kamiona", "2025-03-01")   # same title, no diacritics
        self._post("d", "Skladištar", "2022-01-01")       # outside the window

        compute_scores(self.db, today=TODAY)
        row = self.db.scores()[0]
        self.assertEqual(row["total_ads"], 4)
        self.assertEqual(row["ads_24m"], 3)
        self.assertEqual(row["distinct_titles_24m"], 1)   # normalized titles match
        self.assertEqual(row["repeated_titles_24m"], 1)
        self.assertEqual(row["last_ad"], "2026-07-01")
        self.assertEqual(row["first_ad"], "2022-01-01")
        # 3 ads + 4×1 repeated + 4 recency (last ad ~1 mjesec prije TODAY,
        # tj. taman preko granice od 1 mjeseca -> bonus 4, ne 6)
        self.assertEqual(row["score"], 11.0)

    def test_expiry_date_counts_when_publish_missing(self):
        self.db.upsert_posting(make_posting(
            source="klix", source_id="e", employer_source="klix",
            employer_source_id="1", title="Konobar", expires_at="2026-06-01",
        ))
        compute_scores(self.db, today=TODAY)
        self.assertEqual(self.db.scores()[0]["ads_24m"], 1)


if __name__ == "__main__":
    unittest.main()
