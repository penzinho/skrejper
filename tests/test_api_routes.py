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


if __name__ == "__main__":
    unittest.main()
