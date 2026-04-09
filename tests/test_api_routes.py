import os
import unittest
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
    from app.api.main import app, _get_allowed_origins
    from app.rate_limit import rate_limiter
except ModuleNotFoundError:
    TestClient = None
    app = None
    _get_allowed_origins = None
    rate_limiter = None


@unittest.skipUnless(TestClient is not None and app is not None, "FastAPI dependencies are not installed")
class ApiRoutesTests(unittest.TestCase):
    def setUp(self):
        self.api_key = "test-scraper-api-key"
        self.env_patcher = patch.dict(os.environ, {"SCRAPER_API_KEY": self.api_key}, clear=False)
        self.env_patcher.start()
        rate_limiter.reset()
        self.client = TestClient(app)

    def tearDown(self):
        self.env_patcher.stop()

    def _auth_headers(self) -> dict[str, str]:
        return {"x-api-key": self.api_key}

    def _headers_for_ip(self, ip_address: str, include_api_key: bool = False) -> dict[str, str]:
        headers = {"x-forwarded-for": ip_address}
        if include_api_key:
            headers["x-api-key"] = self.api_key
        return headers

    def test_requests_from_allowed_origin_are_accepted(self):
        response = self.client.get("/health", headers={"Origin": "https://flow.protalent.hr"})

        self.assertEqual(response.status_code, 200)

    def test_requests_from_disallowed_origin_are_rejected(self):
        response = self.client.get("/health", headers={"Origin": "https://evil.example"})

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"], "Origin not allowed")

    def test_allowed_origins_are_configurable_via_env(self):
        with patch.dict(os.environ, {"CORS_ALLOW_ORIGINS": "https://flow.protalent.hr,https://portal.example"}, clear=False):
            self.assertEqual(
                _get_allowed_origins(),
                ["https://flow.protalent.hr", "https://portal.example"],
            )

    def test_health_route_returns_429_after_rate_limit_is_exceeded(self):
        headers = self._headers_for_ip("198.51.100.10")

        for _ in range(120):
            response = self.client.get("/health", headers=headers)
            self.assertEqual(response.status_code, 200)

        limited_response = self.client.get("/health", headers=headers)

        self.assertEqual(limited_response.status_code, 429)
        self.assertEqual(limited_response.json()["detail"], "Rate limit exceeded")
        self.assertIn("Retry-After", limited_response.headers)

    def test_rate_limits_are_scoped_per_endpoint(self):
        headers = self._headers_for_ip("198.51.100.20")

        for _ in range(120):
            response = self.client.get("/health", headers=headers)
            self.assertEqual(response.status_code, 200)

        limited_response = self.client.get("/health", headers=headers)
        categories_response = self.client.get("/scrapers/hzz/categories", headers=headers)

        self.assertEqual(limited_response.status_code, 429)
        self.assertEqual(categories_response.status_code, 200)

    @patch("app.api.main.enqueue_task")
    def test_scraper_route_returns_429_after_ip_rate_limit_is_exceeded(self, enqueue_task_mock):
        enqueue_task_mock.return_value = {
            "task_id": "task-rate-limit",
            "task_name": "app.tasks.scrape_hzz",
            "status": "queued",
            "queued_at": "2026-04-09T10:00:00+00:00",
        }
        headers = self._headers_for_ip("198.51.100.30", include_api_key=True)

        for _ in range(10):
            response = self.client.post("/scrapers/hzz", json={"max_pages": 1}, headers=headers)
            self.assertEqual(response.status_code, 202)

        limited_response = self.client.post("/scrapers/hzz", json={"max_pages": 1}, headers=headers)

        self.assertEqual(limited_response.status_code, 429)
        self.assertEqual(limited_response.json()["detail"], "Rate limit exceeded")

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
            json={"max_pages": 4, "category": "hospitality_tourism", "async_job": False},
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-hzz-1")
        scrape_and_store_hzz_mock.assert_called_once_with(max_pages=4, category="hospitality_tourism")

    @patch("app.api.main.enqueue_task")
    def test_run_hzz_scraper_route_queues_by_default(self, enqueue_task_mock):
        enqueue_task_mock.return_value = {
            "task_id": "task-default-1",
            "task_name": "app.tasks.scrape_hzz",
            "status": "queued",
            "queued_at": "2026-04-09T10:00:00+00:00",
        }

        response = self.client.post(
            "/scrapers/hzz",
            json={"max_pages": 4, "category": "hospitality_tourism"},
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["task_id"], "task-default-1")
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.scrape_hzz",
            max_pages=4,
            category="hospitality_tourism",
        )

    @patch("app.api.main.enqueue_task")
    def test_run_hzz_scraper_route_can_enqueue_async_task(self, enqueue_task_mock):
        enqueue_task_mock.return_value = {
            "task_id": "task-1",
            "task_name": "app.tasks.scrape_hzz",
            "status": "queued",
            "queued_at": "2026-04-09T10:00:00+00:00",
        }

        response = self.client.post(
            "/scrapers/hzz",
            json={"max_pages": 4, "category": "hospitality_tourism", "async_job": True},
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["task_id"], "task-1")
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.scrape_hzz",
            max_pages=4,
            category="hospitality_tourism",
        )

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
            json={"keyword": "python", "max_clicks": 3, "category": "it_telecommunications", "async_job": False},
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-mp-1")
        scrape_and_store_mojposao_mock.assert_called_once_with(
            keyword="python",
            max_clicks=3,
            category="it_telecommunications",
        )

    @patch("app.api.main.scrape_and_store_mojposao")
    def test_run_mojposao_scraper_route_can_still_run_sync(self, scrape_and_store_mojposao_mock):
        scrape_and_store_mojposao_mock.return_value = {
            "run_id": "run-mp-sync-1",
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
            json={
                "keyword": "python",
                "max_clicks": 3,
                "category": "it_telecommunications",
                "async_job": False,
            },
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["run_id"], "run-mp-sync-1")

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
                "async_job": False,
                "hzz": {"max_pages": 2, "category": "it"},
                "mojposao": {
                    "keyword": "backend",
                    "max_clicks": 2,
                    "category": "it_telecommunications",
                    "async_job": False,
                },
            },
            headers=self._auth_headers(),
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

        response = self.client.post(
            "/scrapers/hzz",
            json={"max_pages": 1, "category": "invalid", "async_job": False},
            headers=self._auth_headers(),
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["status"], "failed")

    def test_scraper_routes_require_api_key_header(self):
        response = self.client.post("/scrapers/hzz", json={"max_pages": 1, "async_job": False})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Invalid API key")

    def test_scraper_routes_reject_invalid_api_key(self):
        response = self.client.post(
            "/scrapers/hzz",
            json={"max_pages": 1, "async_job": False},
            headers={"x-api-key": "wrong-key"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Invalid API key")

    def test_non_scraper_routes_do_not_require_api_key(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)

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
                "async_job": False,
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

    @patch("app.api.main.enqueue_task")
    def test_create_email_campaign_route_queues_by_default(self, enqueue_task_mock):
        enqueue_task_mock.return_value = {
            "task_id": "task-campaign-default-1",
            "task_name": "app.tasks.create_email_campaign",
            "status": "queued",
            "queued_at": "2026-04-09T10:00:00+00:00",
        }

        response = self.client.post(
            "/email/campaigns",
            json={
                "name": "Queued By Default",
                "target": {"job_ids": ["job-1"], "only_not_emailed": True},
                "subject": "Hi {{company}}",
                "html_content": "<p>Hello {{company}}</p>",
                "sender_email": "sales@example.com",
            },
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["task_id"], "task-campaign-default-1")
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.create_email_campaign",
            name="Queued By Default",
            source=None,
            run_id=None,
            job_ids=["job-1"],
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

    @patch("app.api.main.enqueue_task")
    def test_create_email_campaign_route_can_enqueue_async_task(self, enqueue_task_mock):
        enqueue_task_mock.return_value = {
            "task_id": "task-campaign-1",
            "task_name": "app.tasks.create_email_campaign",
            "status": "queued",
            "queued_at": "2026-04-09T10:00:00+00:00",
        }

        response = self.client.post(
            "/email/campaigns",
            json={
                "name": "Queued Outreach",
                "target": {"job_ids": ["job-1"], "only_not_emailed": True},
                "subject": "Hi {{company}}",
                "html_content": "<p>Hello {{company}}</p>",
                "sender_email": "sales@example.com",
                "send_now": True,
                "async_job": True,
            },
        )

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["task_id"], "task-campaign-1")
        enqueue_task_mock.assert_called_once_with(
            "app.tasks.create_email_campaign",
            name="Queued Outreach",
            source=None,
            run_id=None,
            job_ids=["job-1"],
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
            send_now=True,
        )

    @patch("app.api.main.create_email_campaign")
    def test_create_email_campaign_route_can_still_run_sync(self, create_email_campaign_mock):
        create_email_campaign_mock.return_value = {
            "campaign_id": "campaign-sync-1",
            "name": "Sync Outreach",
            "status": "draft",
            "scheduled_for": None,
            "total_recipients": 1,
            "sent_count": 0,
            "failed_count": 0,
            "queued_count": 1,
            "warmup_remaining_today": None,
        }

        response = self.client.post(
            "/email/campaigns",
            json={
                "name": "Sync Outreach",
                "target": {"job_ids": ["job-1"], "only_not_emailed": True},
                "subject": "Hi {{company}}",
                "html_content": "<p>Hello {{company}}</p>",
                "sender_email": "sales@example.com",
                "async_job": False,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["campaign_id"], "campaign-sync-1")

    @patch("app.api.main.get_task_status")
    def test_get_queue_task_status_route_returns_payload(self, get_task_status_mock):
        get_task_status_mock.return_value = {
            "task_id": "task-1",
            "status": "success",
            "ready": True,
            "successful": True,
            "result": {"run_id": "run-hzz-1"},
            "error": None,
        }

        response = self.client.get("/queue/tasks/task-1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "success")
        self.assertTrue(response.json()["successful"])

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
