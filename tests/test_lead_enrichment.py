import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from app.services.lead_enrichment import (
    FirecrawlLeadEnricher,
    enrich_scrape_run_emails,
    schedule_scrape_run_email_enrichment,
)


class FakeStorage:
    def __init__(self, jobs):
        self.jobs = {job["id"]: dict(job) for job in jobs}

    def list_jobs_pending_email_enrichment(self, *, run_id):
        return [
            job
            for job in self.jobs.values()
            if job.get("last_run_id") == run_id and not job.get("employer_email") and not job.get("email_enrichment_unusable", False)
        ]

    def schedule_jobs_email_enrichment(self, job_ids, *, scheduled_for):
        for job_id in job_ids:
            self.jobs[job_id]["email_enrichment_next_attempt_at"] = scheduled_for

    def update_jobs_employer_email(self, job_ids, employer_email):
        for job_id in job_ids:
            self.jobs[job_id]["employer_email"] = employer_email
            self.jobs[job_id]["email_enrichment_next_attempt_at"] = None
            self.jobs[job_id]["email_enrichment_unusable"] = False

    def list_jobs_for_company_names(self, company_names):
        wanted = {name for name in company_names if name}
        return [job for job in self.jobs.values() if job.get("company") in wanted]

    def mark_jobs_email_enrichment_unusable(self, job_ids):
        for job_id in job_ids:
            self.jobs[job_id]["email_enrichment_unusable"] = True
            self.jobs[job_id]["email_enrichment_next_attempt_at"] = None

    def update_job_email_enrichment_state(
        self,
        job_id,
        *,
        attempt_count,
        last_attempt_at,
        next_attempt_at,
        unusable,
    ):
        job = self.jobs[job_id]
        job["email_enrichment_attempt_count"] = attempt_count
        job["email_enrichment_last_attempt_at"] = last_attempt_at
        job["email_enrichment_next_attempt_at"] = next_attempt_at
        job["email_enrichment_unusable"] = unusable


class FakeEnricher:
    def __init__(self, result_by_company):
        self.result_by_company = result_by_company

    @property
    def is_configured(self):
        return True

    def find_company_email(self, job):
        return self.result_by_company.get(job.get("company"))


class LeadEnrichmentTests(unittest.TestCase):
    def test_firecrawl_enricher_prefers_contact_page_email_with_small_page_budget(self):
        class StubEnricher(FirecrawlLeadEnricher):
            def __init__(self):
                super().__init__(api_key="test-key", page_limit=3)
                self.scraped_urls = []

            def _scrape_page(self, url):
                self.scraped_urls.append(url)
                pages = {
                    "https://example.com": {
                        "markdown": "Welcome to Example",
                        "links": [
                            "https://example.com/about",
                            "https://example.com/contact",
                            "https://example.com/jobs",
                            "https://other.test/contact",
                        ],
                    },
                    "https://example.com/contact": {
                        "markdown": "For jobs and applications contact contact@example.com or press@example.com",
                        "links": [],
                    },
                    "https://example.com/about": {
                        "markdown": "About Example",
                        "links": [],
                    },
                }
                return pages[url]

        enricher = StubEnricher()

        email = enricher.find_company_email(
            {
                "company": "Example",
                "employer_website": "https://example.com",
            }
        )

        self.assertEqual(email, "contact@example.com")
        self.assertEqual(
            enricher.scraped_urls,
            ["https://example.com", "https://example.com/contact"],
        )

    def test_firecrawl_enricher_accepts_contextual_gmail_on_official_contact_page(self):
        class StubEnricher(FirecrawlLeadEnricher):
            def __init__(self):
                super().__init__(api_key="test-key", page_limit=2)

            def _scrape_page(self, url):
                pages = {
                    "https://example.com": {
                        "markdown": "Welcome to Example",
                        "links": ["https://example.com/contact"],
                    },
                    "https://example.com/contact": {
                        "markdown": "Example careers: send your application to example.hr@gmail.com",
                        "links": [],
                    },
                }
                return pages[url]

        enricher = StubEnricher()
        email = enricher.find_company_email(
            {
                "company": "Example",
                "employer_website": "https://example.com",
            }
        )
        self.assertEqual(email, "example.hr@gmail.com")

    def test_firecrawl_enricher_prefers_kontakt_over_info_hr(self):
        class StubEnricher(FirecrawlLeadEnricher):
            def __init__(self):
                super().__init__(api_key="test-key", page_limit=1)

            def _scrape_page(self, url):
                return {
                    "markdown": "Kontakt: kontakt@ophirum.hr Info HR: info.hr@ophirum.com",
                    "links": [],
                }

        enricher = StubEnricher()
        email = enricher.find_company_email(
            {
                "company": "Ophirum",
                "employer_website": "https://www.ophirum.hr/",
            }
        )
        self.assertEqual(email, "kontakt@ophirum.hr")

    def test_firecrawl_enricher_deprioritizes_onlineshop_email(self):
        class StubEnricher(FirecrawlLeadEnricher):
            def __init__(self):
                super().__init__(api_key="test-key", page_limit=1)

            def _scrape_page(self, url):
                return {
                    "markdown": "Contact us at onlineshop@wuerth.com.hr or wuerth@wuerth.com.hr",
                    "links": [],
                }

        enricher = StubEnricher()
        email = enricher.find_company_email(
            {
                "company": "Wurth Hrvatska",
                "employer_website": "https://eshop.wuerth.com.hr/wuerth/home/index",
            }
        )
        self.assertEqual(email, "wuerth@wuerth.com.hr")

    def test_firecrawl_enricher_returns_none_for_weakly_contextual_third_party_email(self):
        class StubEnricher(FirecrawlLeadEnricher):
            def __init__(self):
                super().__init__(api_key="test-key", page_limit=2)

            def _scrape_page(self, url):
                pages = {
                    "https://example.com": {
                        "markdown": "Welcome to Example",
                        "links": ["https://example.com/contact"],
                    },
                    "https://example.com/contact": {
                        "markdown": "Contact our external partner at hello@agency.test for more information.",
                        "links": [],
                    },
                }
                return pages[url]

        enricher = StubEnricher()
        email = enricher.find_company_email(
            {
                "company": "Example",
                "employer_website": "https://example.com",
            }
        )
        self.assertIsNone(email)

    def test_reuses_existing_company_email_without_calling_firecrawl(self):
        now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
        storage = FakeStorage(
            [
                {
                    "id": "job-existing",
                    "company": "Alpha",
                    "last_run_id": "old-run",
                    "employer_email": "kontakt@alpha.test",
                    "email_enrichment_attempt_count": 1,
                    "email_enrichment_unusable": False,
                },
                {
                    "id": "job-new",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 0,
                    "email_enrichment_next_attempt_at": (now - timedelta(minutes=1)).isoformat(),
                    "email_enrichment_unusable": False,
                },
            ]
        )

        class ExplodingEnricher(FakeEnricher):
            def find_company_email(self, job):
                raise AssertionError("Firecrawl should not be called when company email already exists")

        result = enrich_scrape_run_emails(
            run_id="run-1",
            storage=storage,
            enricher=ExplodingEnricher({}),
            now=now,
        )

        self.assertEqual(result["enriched_count"], 1)
        self.assertEqual(result["reused_company_email_count"], 1)
        self.assertEqual(result["skipped_known_company_count"], 0)
        self.assertEqual(storage.jobs["job-new"]["employer_email"], "kontakt@alpha.test")
        self.assertEqual(storage.jobs["job-new"]["email_enrichment_attempt_count"], 0)

    def test_skips_company_when_previous_attempt_already_failed(self):
        now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
        storage = FakeStorage(
            [
                {
                    "id": "job-existing",
                    "company": "Alpha",
                    "last_run_id": "old-run",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 1,
                    "email_enrichment_last_attempt_at": (now - timedelta(days=1)).isoformat(),
                    "email_enrichment_unusable": False,
                },
                {
                    "id": "job-new",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 0,
                    "email_enrichment_next_attempt_at": (now - timedelta(minutes=1)).isoformat(),
                    "email_enrichment_unusable": False,
                },
            ]
        )

        class ExplodingEnricher(FakeEnricher):
            def find_company_email(self, job):
                raise AssertionError("Firecrawl should not be called for a previously failed company")

        result = enrich_scrape_run_emails(
            run_id="run-1",
            storage=storage,
            enricher=ExplodingEnricher({}),
            now=now,
        )

        self.assertEqual(result["enriched_count"], 0)
        self.assertEqual(result["reused_company_email_count"], 0)
        self.assertEqual(result["skipped_known_company_count"], 1)
        self.assertEqual(result["unusable_count"], 1)
        self.assertTrue(storage.jobs["job-new"]["email_enrichment_unusable"])
        self.assertEqual(storage.jobs["job-new"]["email_enrichment_attempt_count"], 0)

    def test_schedule_scrape_run_email_enrichment_marks_due_time_and_enqueues_task(self):
        storage = FakeStorage(
            [
                {
                    "id": "job-1",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_unusable": False,
                }
            ]
        )

        with patch("app.services.lead_enrichment.enqueue_task") as enqueue_task_mock:
            result = schedule_scrape_run_email_enrichment(run_id="run-1", storage=storage, delay_hours=3)

        self.assertTrue(result["scheduled"])
        self.assertIsNotNone(storage.jobs["job-1"]["email_enrichment_next_attempt_at"])
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.enrich_scrape_run_emails",
            countdown_seconds=3 * 3600,
            run_id="run-1",
        )

    def test_first_attempt_schedules_retry_when_no_email_is_found(self):
        now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
        storage = FakeStorage(
            [
                {
                    "id": "job-1",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 0,
                    "email_enrichment_next_attempt_at": (now - timedelta(minutes=1)).isoformat(),
                    "email_enrichment_unusable": False,
                }
            ]
        )

        with patch("app.services.lead_enrichment.enqueue_task") as enqueue_task_mock:
            result = enrich_scrape_run_emails(
                run_id="run-1",
                storage=storage,
                enricher=FakeEnricher({"Alpha": None}),
                now=now,
            )

        self.assertEqual(result["attempted_count"], 1)
        self.assertEqual(result["retry_scheduled_count"], 1)
        self.assertEqual(result["unusable_count"], 0)
        self.assertEqual(storage.jobs["job-1"]["email_enrichment_attempt_count"], 1)
        self.assertFalse(storage.jobs["job-1"]["email_enrichment_unusable"])
        self.assertIsNotNone(storage.jobs["job-1"]["email_enrichment_next_attempt_at"])
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.enrich_scrape_run_emails",
            countdown_seconds=48 * 3600,
            run_id="run-1",
        )

    def test_second_attempt_flags_job_unusable_when_email_is_still_missing(self):
        now = datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
        storage = FakeStorage(
            [
                {
                    "id": "job-1",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 1,
                    "email_enrichment_next_attempt_at": (now - timedelta(minutes=1)).isoformat(),
                    "email_enrichment_unusable": False,
                }
            ]
        )

        with patch("app.services.lead_enrichment.enqueue_task") as enqueue_task_mock:
            result = enrich_scrape_run_emails(
                run_id="run-1",
                storage=storage,
                enricher=FakeEnricher({"Alpha": None}),
                now=now,
            )

        self.assertEqual(result["attempted_count"], 1)
        self.assertEqual(result["retry_scheduled_count"], 0)
        self.assertEqual(result["unusable_count"], 1)
        self.assertEqual(storage.jobs["job-1"]["email_enrichment_attempt_count"], 2)
        self.assertTrue(storage.jobs["job-1"]["email_enrichment_unusable"])
        self.assertIsNone(storage.jobs["job-1"]["email_enrichment_next_attempt_at"])
        enqueue_task_mock.assert_not_called()

    def test_successful_attempt_persists_email_and_clears_retry(self):
        now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
        storage = FakeStorage(
            [
                {
                    "id": "job-1",
                    "company": "Alpha",
                    "last_run_id": "run-1",
                    "employer_email": None,
                    "email_enrichment_attempt_count": 0,
                    "email_enrichment_next_attempt_at": (now - timedelta(minutes=1)).isoformat(),
                    "email_enrichment_unusable": False,
                }
            ]
        )

        with patch("app.services.lead_enrichment.enqueue_task") as enqueue_task_mock:
            result = enrich_scrape_run_emails(
                run_id="run-1",
                storage=storage,
                enricher=FakeEnricher({"Alpha": "hello@alpha.test"}),
                now=now,
            )

        self.assertEqual(result["enriched_count"], 1)
        self.assertEqual(storage.jobs["job-1"]["employer_email"], "hello@alpha.test")
        self.assertEqual(storage.jobs["job-1"]["email_enrichment_attempt_count"], 1)
        self.assertIsNone(storage.jobs["job-1"]["email_enrichment_next_attempt_at"])
        self.assertFalse(storage.jobs["job-1"]["email_enrichment_unusable"])
        enqueue_task_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
