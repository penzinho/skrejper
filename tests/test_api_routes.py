import unittest
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
    from app.api.main import app
except ModuleNotFoundError:
    TestClient = None
    app = None


@unittest.skipUnless(TestClient is not None and app is not None, "FastAPI dependencies are not installed")
class ApiRoutesTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("app.api.main.scrape_and_store_hzz")
    def test_run_hzz_scraper_route_calls_store_service(self, scrape_and_store_hzz_mock):
        scrape_and_store_hzz_mock.return_value = {
            "run_id": "run-hzz-1",
            "source": "hzz",
            "status": "completed",
            "scraped_count": 12,
            "upserted_count": 10,
            "snapshot_count": 10,
            "failed_count": 2,
            "error": None,
        }

        response = self.client.post(
            "/scrapers/hzz",
            json={"max_pages": 4, "category": "hospitality_tourism"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-hzz-1")
        scrape_and_store_hzz_mock.assert_called_once_with(max_pages=4, category="hospitality_tourism")

    @patch("app.api.main.scrape_and_store_mojposao")
    def test_run_mojposao_scraper_route_calls_store_service(self, scrape_and_store_mojposao_mock):
        scrape_and_store_mojposao_mock.return_value = {
            "run_id": "run-mp-1",
            "source": "mojposao",
            "status": "completed",
            "scraped_count": 7,
            "upserted_count": 7,
            "snapshot_count": 7,
            "failed_count": 0,
            "error": None,
        }

        response = self.client.post(
            "/scrapers/mojposao",
            json={"keyword": "python", "max_clicks": 3, "category": "it_telecommunications"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-mp-1")
        scrape_and_store_mojposao_mock.assert_called_once_with(
            keyword="python",
            max_clicks=3,
            category="it_telecommunications",
        )

    @patch("app.api.main.scrape_and_store_mojposao")
    @patch("app.api.main.scrape_and_store_hzz")
    def test_run_all_scrapers_route_calls_both_store_services(
        self,
        scrape_and_store_hzz_mock,
        scrape_and_store_mojposao_mock,
    ):
        scrape_and_store_hzz_mock.return_value = {
            "run_id": "run-hzz-2",
            "source": "hzz",
            "status": "completed",
            "scraped_count": 4,
            "upserted_count": 4,
            "snapshot_count": 4,
            "failed_count": 0,
            "error": None,
        }
        scrape_and_store_mojposao_mock.return_value = {
            "run_id": "run-mp-2",
            "source": "mojposao",
            "status": "completed",
            "scraped_count": 9,
            "upserted_count": 8,
            "snapshot_count": 8,
            "failed_count": 1,
            "error": None,
        }

        response = self.client.post(
            "/scrapers/run-all",
            json={
                "hzz": {"max_pages": 2, "category": "it"},
                "mojposao": {"keyword": "backend", "max_clicks": 2, "category": "it_telecommunications"},
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["results"]), 2)
        scrape_and_store_hzz_mock.assert_called_once_with(max_pages=2, category="it")
        scrape_and_store_mojposao_mock.assert_called_once_with(
            keyword="backend",
            max_clicks=2,
            category="it_telecommunications",
        )

    @patch("app.api.main.scrape_and_store_hzz")
    def test_failed_summary_returns_http_error(self, scrape_and_store_hzz_mock):
        scrape_and_store_hzz_mock.return_value = {
            "run_id": "run-hzz-failed",
            "source": "hzz",
            "status": "failed",
            "scraped_count": 0,
            "upserted_count": 0,
            "snapshot_count": 0,
            "failed_count": 0,
            "error": "Unknown HZZ category 'invalid'",
        }

        response = self.client.post("/scrapers/hzz", json={"max_pages": 1, "category": "invalid"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["status"], "failed")

    @patch("app.api.main.create_email_campaign")
    def test_create_email_campaign_route_calls_service(self, create_email_campaign_mock):
        create_email_campaign_mock.return_value = {
            "campaign_id": "campaign-1",
            "name": "April Outreach",
            "status": "draft",
            "scheduled_for": None,
            "total_recipients": 2,
            "sent_count": 0,
            "failed_count": 0,
            "queued_count": 2,
            "warmup_remaining_today": None,
        }

        response = self.client.post(
            "/email/campaigns",
            json={
                "name": "April Outreach",
                "target": {"job_ids": ["job-1", "job-2"], "only_not_emailed": True},
                "subject": "Hi {{company}}",
                "html_content": "<p>Hello {{company}}</p>",
                "sender_email": "sales@example.com",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["campaign_id"], "campaign-1")
        create_email_campaign_mock.assert_called_once_with(
            name="April Outreach",
            source=None,
            run_id=None,
            job_ids=["job-1", "job-2"],
            only_not_emailed=True,
            require_email=True,
            template_id=None,
            subject="Hi {{company}}",
            html_content="<p>Hello {{company}}</p>",
            text_content=None,
            sender_email="sales@example.com",
            reply_to_email=None,
            created_by=None,
            scheduled_for=None,
            send_now=False,
        )

    @patch("app.api.main.get_email_warmup_status")
    def test_get_email_warmup_status_route_returns_service_payload(self, get_email_warmup_status_mock):
        get_email_warmup_status_mock.return_value = {
            "settings": {"enabled": True, "initial_daily_limit": 10},
            "effective_daily_limit": 15,
            "sent_today": 3,
            "remaining_today": 12,
        }

        response = self.client.get("/email/warmup")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["effective_daily_limit"], 15)
        self.assertEqual(response.json()["remaining_today"], 12)


if __name__ == "__main__":
    unittest.main()
