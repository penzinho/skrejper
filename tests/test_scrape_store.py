import unittest

from app.services.scrape_store import normalize_hzz_job, normalize_mojposao_job, scrape_and_store_hzz


class FakeStorage:
    def __init__(self):
        self.created_runs = []
        self.upserted_jobs = []
        self.inserted_snapshots = []
        self.completed_runs = []
        self.failed_runs = []

    def create_scrape_run(self, source, filters):
        self.created_runs.append({"source": source, "filters": filters})
        return "run-123"

    def upsert_jobs(self, jobs):
        self.upserted_jobs.extend(jobs)
        return len(jobs)

    def insert_job_snapshots(self, snapshots):
        self.inserted_snapshots.extend(snapshots)
        return len(snapshots)

    def complete_scrape_run(self, run_id, **payload):
        self.completed_runs.append({"run_id": run_id, **payload})

    def fail_scrape_run(self, run_id, **payload):
        self.failed_runs.append({"run_id": run_id, **payload})


class ScrapeStoreTests(unittest.TestCase):
    def test_normalize_hzz_job_maps_contacts_and_published_date(self):
        normalized = normalize_hzz_job(
            {
                "title": "Skladistar",
                "company": "Caritas",
                "location": "Zagreb",
                "detail_url": "https://burzarada.hzz.hr/job/1",
                "valid_from": "09. 04. 2026.",
                "email": "kontakt@example.com",
                "phone": "+385 1 555 000",
                "employer_address": "Ulica Stjepana Babonica 121, 10000 Zagreb",
            },
            category="hospitality_tourism",
            run_id="run-1",
        )

        self.assertEqual(normalized["published_at"], "2026-04-09")
        self.assertEqual(normalized["employer_email"], "kontakt@example.com")
        self.assertEqual(normalized["employer_phone"], "+385 1 555 000")
        self.assertEqual(normalized["employer_address"], "Ulica Stjepana Babonica 121, 10000 Zagreb")
        self.assertEqual(normalized["category"], "hospitality_tourism")
        self.assertIsNone(normalized["employer_website"])

    def test_normalize_mojposao_job_leaves_contact_fields_empty(self):
        normalized = normalize_mojposao_job(
            {
                "title": "Arhitekt",
                "company": "Studio",
                "location": "Zagreb",
                "detail_url": "https://mojposao.hr/job/1",
                "published_at": "08. 04. 2026.",
                "category": "Arhitektura",
                "employer_website": "https://studio.example.com",
                "email": "ignored@example.com",
                "phone": "12345",
            },
            run_id="run-2",
        )

        self.assertEqual(normalized["published_at"], "2026-04-08")
        self.assertEqual(normalized["employer_website"], "https://studio.example.com")
        self.assertIsNone(normalized["employer_email"])
        self.assertIsNone(normalized["employer_phone"])
        self.assertIsNone(normalized["employer_address"])

    def test_scrape_and_store_hzz_upserts_and_snapshots(self):
        storage = FakeStorage()

        def fake_scraper(max_pages, category):
            self.assertEqual(max_pages, 2)
            self.assertEqual(category, "hospitality_tourism")
            return [
                {
                    "title": "Skladistar",
                    "company": "Caritas",
                    "location": "Zagreb",
                    "detail_url": "https://burzarada.hzz.hr/job/1",
                    "valid_from": "09.04.2026.",
                    "valid_to": "20.04.2026.",
                    "email": "kontakt@example.com",
                    "phone": "+385 1 555 000",
                    "employer_address": "Ulica Stjepana Babonica 121, 10000 Zagreb",
                }
            ]

        summary = scrape_and_store_hzz(
            max_pages=2,
            category="hospitality_tourism",
            storage=storage,
            scraper=fake_scraper,
        )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["scraped_count"], 1)
        self.assertEqual(summary["upserted_count"], 1)
        self.assertEqual(summary["snapshot_count"], 1)
        self.assertEqual(summary["failed_count"], 0)
        self.assertEqual(len(storage.upserted_jobs), 1)
        self.assertEqual(storage.upserted_jobs[0]["published_at"], "2026-04-09")
        self.assertEqual(storage.upserted_jobs[0]["source"], "hzz")
        self.assertEqual(len(storage.inserted_snapshots), 1)
        self.assertEqual(storage.inserted_snapshots[0]["job_payload"]["valid_to"], "20.04.2026.")
        self.assertEqual(len(storage.completed_runs), 1)
        self.assertEqual(len(storage.failed_runs), 0)

    def test_scrape_and_store_filters_school_and_kindergarten_employers(self):
        storage = FakeStorage()

        def fake_scraper(max_pages, category):
            return [
                {
                    "title": "Ucitelj",
                    "company": "Osnovna skola Vladimira Nazora",
                    "location": "Zagreb",
                    "detail_url": "https://burzarada.hzz.hr/job/blocked-school",
                    "valid_from": "09.04.2026.",
                },
                {
                    "title": "Odgajatelj",
                    "company": "Djecji vrtic Tratinčica",
                    "location": "Split",
                    "detail_url": "https://burzarada.hzz.hr/job/blocked-kindergarten",
                    "valid_from": "09.04.2026.",
                },
                {
                    "title": "Prodajni predstavnik",
                    "company": "Caritas",
                    "location": "Zagreb",
                    "detail_url": "https://burzarada.hzz.hr/job/allowed",
                    "valid_from": "09.04.2026.",
                },
            ]

        summary = scrape_and_store_hzz(
            max_pages=1,
            category=None,
            storage=storage,
            scraper=fake_scraper,
        )

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["scraped_count"], 3)
        self.assertEqual(summary["upserted_count"], 1)
        self.assertEqual(summary["snapshot_count"], 1)
        self.assertEqual(len(storage.upserted_jobs), 1)
        self.assertEqual(storage.upserted_jobs[0]["company"], "Caritas")
        self.assertEqual(len(storage.inserted_snapshots), 1)
        self.assertEqual(storage.inserted_snapshots[0]["detail_url"], "https://burzarada.hzz.hr/job/allowed")


if __name__ == "__main__":
    unittest.main()
