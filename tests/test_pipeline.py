import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.desktop.pipeline import (
    EXCLUDED_COMPANY_TERMS,
    LeadCollector,
    is_excluded_company,
    normalize_key,
)


def aa_job(**overrides):
    job = {
        "title": "Schweisser",
        "company": "Muster GmbH",
        "location": "München",
        "published_at": "2026-08-01",
        "detail_url": "https://example.invalid/1",
        "category": "Metall",
        "email": "info@muster.de",
        "employer_website": "https://muster.de",
        "refnr": "REF-1",
        "source": "arbeitsagentur",
    }
    job.update(overrides)
    return job


def hzz_job(**overrides):
    job = {
        "title": "Konobar",
        "company": "Hotel Adria d.o.o.",
        "location": "Split",
        "employer_address": "Obala 1, 21000 Split",
        "email": "posao@adria.hr",
        "phone": "021 123 456",
        "employment_type": "stalni",
        "working_hours": "puno",
        "valid_from": "2026-08-01",
        "valid_to": "2026-09-01",
        "detail_url": "https://burzarada.invalid/1",
        "source": "hzz",
    }
    job.update(overrides)
    return job


class NormalizeTests(unittest.TestCase):
    def test_strips_diacritics_and_case(self):
        self.assertEqual(normalize_key("Dječji Vrtić  Sunce"), "djecji vrtic sunce")

    def test_excluded_company_matches_accented_names(self):
        self.assertTrue(is_excluded_company("Dječji vrtić Sunce"))
        self.assertTrue(is_excluded_company("OSNOVNA ŠKOLA Petra Krešimira"))
        self.assertFalse(is_excluded_company("Hotel Adria d.o.o."))

    def test_no_terms_means_nothing_excluded(self):
        self.assertFalse(is_excluded_company("Dječji vrtić", terms=()))


class LeadCollectorTests(unittest.TestCase):
    def test_keeps_a_lead_and_maps_arbeitsagentur_columns(self):
        collector = LeadCollector("arbeitsagentur")
        row = collector.add(aa_job())

        self.assertIsNotNone(row)
        self.assertEqual(list(row), collector.fields)
        self.assertEqual(row["company"], "Muster GmbH")
        self.assertEqual(row["city"], "München")
        self.assertEqual(row["refnr"], "REF-1")
        self.assertEqual(collector.stats.kept, 1)

    def test_maps_hzz_columns_including_group_label(self):
        collector = LeadCollector("hzz")
        row = collector.add(hzz_job(), "Konobari/konobarice")

        self.assertEqual(list(row), collector.fields)
        self.assertEqual(row["group"], "Konobari/konobarice")
        self.assertEqual(row["city"], "Split")
        self.assertEqual(row["phone"], "021 123 456")

    def test_job_key_group_wins_over_label(self):
        collector = LeadCollector("hzz")
        row = collector.add(hzz_job(group="Iz oglasa"), "Iz taba")
        self.assertEqual(row["group"], "Iz oglasa")

    def test_posting_without_email_is_dropped_and_id_not_burned(self):
        collector = LeadCollector("arbeitsagentur")

        self.assertIsNone(collector.add(aa_job(email="")))
        self.assertEqual(collector.new_ids, [])
        self.assertEqual(collector.stats.without_email, 1)

    def test_id_is_remembered_even_when_the_row_is_deduped_away(self):
        # The detail fetch already happened and did yield an e-mail, so there is
        # no reason to ever pay for that posting again.
        collector = LeadCollector("arbeitsagentur")
        collector.add(aa_job(refnr="REF-1"))
        collector.add(aa_job(refnr="REF-2", company="Druga GmbH"))

        self.assertEqual(collector.new_ids, ["REF-1", "REF-2"])
        self.assertEqual(collector.stats.duplicate_email, 1)
        self.assertEqual(len(collector.rows), 1)

    def test_email_seen_in_a_previous_run_is_skipped(self):
        collector = LeadCollector("arbeitsagentur", seen_emails={"info@muster.de"})

        self.assertIsNone(collector.add(aa_job()))
        self.assertEqual(collector.stats.duplicate_email, 1)
        self.assertEqual(collector.new_emails, [])

    def test_email_dedupe_is_case_insensitive(self):
        collector = LeadCollector("arbeitsagentur", seen_emails={"info@muster.de"})
        self.assertIsNone(collector.add(aa_job(email="INFO@Muster.de")))

    def test_second_posting_from_the_same_company_is_dropped(self):
        collector = LeadCollector("arbeitsagentur")
        collector.add(aa_job(email="a@muster.de", refnr="REF-1"))
        result = collector.add(aa_job(email="b@muster.de", refnr="REF-2"))

        self.assertIsNone(result)
        self.assertEqual(collector.stats.duplicate_company, 1)

    def test_company_dedupe_can_be_turned_off(self):
        collector = LeadCollector("arbeitsagentur", dedupe_company=False)
        collector.add(aa_job(email="a@muster.de"))

        self.assertIsNotNone(collector.add(aa_job(email="b@muster.de")))
        self.assertEqual(len(collector.rows), 2)

    def test_excluded_company_is_dropped_only_when_terms_are_active(self):
        without = LeadCollector("hzz")
        self.assertIsNotNone(without.add(hzz_job(company="Dječji vrtić Sunce")))

        with_terms = LeadCollector("hzz", exclude_terms=EXCLUDED_COMPANY_TERMS)
        self.assertIsNone(with_terms.add(hzz_job(company="Dječji vrtić Sunce")))
        self.assertEqual(with_terms.stats.excluded_company, 1)

    def test_stats_add_up(self):
        collector = LeadCollector("arbeitsagentur")
        collector.add(aa_job(email=""))
        collector.add(aa_job(email="a@x.de", company="A", refnr="1"))
        collector.add(aa_job(email="a@x.de", company="B", refnr="2"))
        collector.add(aa_job(email="c@x.de", company="A", refnr="3"))

        stats = collector.stats.as_dict()
        self.assertEqual(stats["seen"], 4)
        self.assertEqual(stats["without_email"], 1)
        self.assertEqual(stats["duplicate_email"], 1)
        self.assertEqual(stats["duplicate_company"], 1)
        self.assertEqual(stats["kept"], 1)

    def test_unknown_source_is_rejected(self):
        with self.assertRaises(ValueError):
            LeadCollector("gelbeseiten")


class ColumnParityTests(unittest.TestCase):
    """The desktop app and the cron runners must keep writing the same columns.

    Otherwise files from the two paths stop being interchangeable in the mail tool.
    """

    def setUp(self):
        scripts_dir = PROJECT_ROOT / "scripts"
        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))

    def test_hzz_columns_match_scrape_hzz_category(self):
        from app.desktop.pipeline import HZZ_FIELDS

        import scrape_hzz_category

        self.assertEqual(HZZ_FIELDS, scrape_hzz_category.FIELDS)

    def test_arbeitsagentur_columns_match_scrape_category(self):
        from app.desktop.pipeline import ARBEITSAGENTUR_FIELDS

        import scrape_category

        self.assertEqual(ARBEITSAGENTUR_FIELDS, scrape_category.FIELDS)


if __name__ == "__main__":
    unittest.main()
