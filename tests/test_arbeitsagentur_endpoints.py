"""Endpoint resolution for the Arbeitsagentur board.

The board retires API paths without notice — /pc/v4/jobs answered for a long
time and then started 404ing, which produced empty exports and a log that said
only "No response". These tests pin both halves of the fix: try the documented
paths, and never let a 404 on a *search* pass quietly.

No network: urlopen is replaced with a fake router.
"""

import json
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers import arbeitsagentur as aa

SEARCH_PAYLOAD = {"stellenangebote": [], "maxErgebnisse": 0}
DETAIL_PAYLOAD = {"stellenangebotsBeschreibung": "Bewerbung an info@muster.de", "firma": "Muster"}


class FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")
        self.status = 200

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def router(live_paths: dict, calls: list):
    """urlopen stand-in: serves live_paths, 404s everything else."""

    def _urlopen(request, timeout=None):
        url = request.full_url
        calls.append(url)
        for fragment, payload in live_paths.items():
            if fragment in url:
                return FakeResponse(payload)
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    return _urlopen


class EndpointTestCase(unittest.TestCase):
    def setUp(self):
        # Resolution is cached in module state for the life of a run.
        aa._search_url = None
        aa._detail_url = None
        self.addCleanup(setattr, aa, "_search_url", None)
        self.addCleanup(setattr, aa, "_detail_url", None)

        self.logs = []
        patcher = mock.patch.object(aa, "_log", self.logs.append)
        patcher.start()
        self.addCleanup(patcher.stop)


class SearchEndpointTests(EndpointTestCase):
    def search(self, live_paths):
        calls = []
        with mock.patch.object(aa.urllib.request, "urlopen", router(live_paths, calls)):
            payload = aa._search_json("Hochbau", None, None, None, 1, 100, 10)
        return payload, calls

    def test_falls_through_a_retired_path_to_one_that_answers(self):
        payload, calls = self.search({"/pc/v4/app/jobs": SEARCH_PAYLOAD})

        self.assertEqual(payload, SEARCH_PAYLOAD)
        self.assertTrue(calls[0].startswith(f"{aa.API_BASE}/pc/v6/jobs"), calls[0])
        self.assertIn("/pc/v4/app/jobs", calls[1])
        self.assertEqual(aa._search_url, f"{aa.API_BASE}/pc/v4/app/jobs")
        self.assertTrue(any("Search endpoint" in line for line in self.logs), self.logs)

    def test_the_first_working_path_is_used_without_probing(self):
        payload, calls = self.search({"/pc/v6/jobs": SEARCH_PAYLOAD})

        self.assertEqual(payload, SEARCH_PAYLOAD)
        self.assertEqual(len(calls), 1)

    def test_resolution_is_reused_for_later_pages(self):
        self.search({"/pc/v4/app/jobs": SEARCH_PAYLOAD})
        _payload, calls = self.search({"/pc/v4/app/jobs": SEARCH_PAYLOAD})

        self.assertEqual(len(calls), 1, "should not re-probe once resolved")
        self.assertIn("/pc/v4/app/jobs", calls[0])

    def test_every_path_gone_is_reported_loudly(self):
        # The regression that started this: empty results and a log that only
        # said "No response".
        payload, calls = self.search({})

        self.assertIsNone(payload)
        self.assertEqual(len(calls), len(aa.SEARCH_PATHS))
        self.assertTrue(
            any("has moved" in line for line in self.logs),
            f"a dead API must not be silent, got: {self.logs}",
        )

    def test_an_auth_error_stops_instead_of_trying_other_paths(self):
        def unauthorized(request, timeout=None):
            raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

        with mock.patch.object(aa.urllib.request, "urlopen", unauthorized):
            payload = aa._search_json("Hochbau", None, None, None, 1, 100, 10)

        self.assertIsNone(payload)
        self.assertTrue(any("HTTP 403" in line for line in self.logs), self.logs)
        self.assertIsNone(aa._search_url)

    def test_query_parameters_survive_the_indirection(self):
        calls = []
        with mock.patch.object(
            aa.urllib.request, "urlopen", router({"/pc/v6/jobs": SEARCH_PAYLOAD}, calls)
        ):
            aa._search_json("Hochbau", "Schweisser", "München", 50, 2, 25, 10)

        url = calls[0]
        self.assertIn("berufsfeld=Hochbau", url)
        self.assertIn("was=Schweisser", url)
        self.assertIn("umkreis=50", url)
        self.assertIn("page=2", url)
        self.assertIn("size=25", url)


class DetailEndpointTests(EndpointTestCase):
    def test_falls_through_to_the_older_detail_path(self):
        calls = []
        with mock.patch.object(
            aa.urllib.request, "urlopen", router({"/pc/v3/jobdetails": DETAIL_PAYLOAD}, calls)
        ):
            detail = aa._detail_json("enc", timeout=10)

        self.assertEqual(detail, DETAIL_PAYLOAD)
        self.assertIn("/pc/v4/jobdetails", calls[0])
        self.assertIn("/pc/v3/jobdetails", calls[1])
        self.assertEqual(aa._detail_url, f"{aa.API_BASE}{aa.DETAIL_PATHS[1]}")

    def test_an_expired_posting_stays_quiet(self):
        # 404 on a detail is routine; it must not produce log noise per posting.
        calls = []
        with mock.patch.object(aa.urllib.request, "urlopen", router({}, calls)):
            detail = aa._detail_json("enc", timeout=10)

        self.assertIsNone(detail)
        self.assertEqual(self.logs, [])

    def test_resolution_is_reused(self):
        with mock.patch.object(
            aa.urllib.request, "urlopen", router({"/pc/v3/jobdetails": DETAIL_PAYLOAD}, [])
        ):
            aa._detail_json("enc", timeout=10)

        calls = []
        with mock.patch.object(
            aa.urllib.request, "urlopen", router({"/pc/v3/jobdetails": DETAIL_PAYLOAD}, calls)
        ):
            aa._detail_json("enc2", timeout=10)

        self.assertEqual(len(calls), 1)


class BerufsfeldFallbackTests(EndpointTestCase):
    """The board renames Berufsfeld values, and a renamed one filters every
    posting out while still answering 200 — which looked exactly like "the
    scraper is broken": connection fine, zero results, nothing in the log."""

    def setUp(self):
        super().setUp()
        aa._search_url = f"{aa.API_BASE}/pc/v6/jobs"  # skip resolution
        self.listing = {
            "refnr": "REF-1",
            "titel": "Maurer",
            "arbeitgeber": "Bau GmbH",
            "arbeitsort": {"ort": "Berlin"},
        }

    def scrape(self, responses, **kwargs):
        """responses: callable(berufsfeld, keyword) -> payload."""
        calls = []

        def fake_search(berufsfeld, keyword, location, radius, page, size, timeout):
            calls.append({"berufsfeld": berufsfeld, "keyword": keyword, "page": page})
            return responses(berufsfeld, keyword, page)

        with mock.patch.object(aa, "_search_json", fake_search), mock.patch.object(
            aa, "_detail_json", return_value=DETAIL_PAYLOAD
        ):
            jobs = aa.scrape_arbeitsagentur(category="Hochbau", max_pages=2, **kwargs)
        return jobs, calls

    def test_a_berufsfeld_that_matches_nothing_is_retried_as_free_text(self):
        def responses(berufsfeld, keyword, page):
            if berufsfeld:
                return {"stellenangebote": [], "maxErgebnisse": 0}
            return {"stellenangebote": [self.listing] if page == 1 else [], "maxErgebnisse": 1}

        jobs, calls = self.scrape(responses)

        self.assertEqual(calls[0]["berufsfeld"], "Hochbau")
        self.assertEqual(calls[1], {"berufsfeld": None, "keyword": "Hochbau", "page": 1})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["email"], "info@muster.de")

    def test_the_switch_is_announced(self):
        # A wider, less precise search must never happen silently.
        self.scrape(lambda b, k, p: {"stellenangebote": [], "maxErgebnisse": 0})

        self.assertTrue(
            any("ponavljam kao was=" in line for line in self.logs),
            f"the fallback must be visible in the log, got: {self.logs}",
        )

    def test_later_pages_stay_on_free_text(self):
        def responses(berufsfeld, keyword, page):
            if berufsfeld:
                return {"stellenangebote": [], "maxErgebnisse": 0}
            listing = dict(self.listing, refnr=f"REF-{page}")
            # Comfortably above one page, or the scraper stops after page 1.
            return {"stellenangebote": [listing], "maxErgebnisse": 5000}

        _jobs, calls = self.scrape(responses)

        self.assertTrue(all(call["berufsfeld"] is None for call in calls[1:]), calls)
        self.assertEqual([call["keyword"] for call in calls[1:]], ["Hochbau", "Hochbau"])

    def test_a_berufsfeld_that_works_is_left_alone(self):
        def responses(berufsfeld, keyword, page):
            listing = dict(self.listing, refnr=f"REF-{page}")
            return {"stellenangebote": [listing] if page == 1 else [], "maxErgebnisse": 1}

        _jobs, calls = self.scrape(responses)

        self.assertTrue(all(call["berufsfeld"] == "Hochbau" for call in calls), calls)
        self.assertEqual(self.logs, [])

    def test_an_explicit_keyword_is_not_overwritten(self):
        # The user asked for this term; a category that matches nothing must not
        # silently replace it.
        def responses(berufsfeld, keyword, page):
            return {"stellenangebote": [], "maxErgebnisse": 0}

        _jobs, calls = self.scrape(responses, keyword="Schweisser")

        self.assertTrue(all(call["keyword"] == "Schweisser" for call in calls), calls)
        self.assertEqual(len(calls), 1, "no fallback when the user supplied a keyword")

    def test_no_fallback_when_the_first_page_had_results(self):
        def responses(berufsfeld, keyword, page):
            listing = dict(self.listing, refnr=f"REF-{page}")
            return {"stellenangebote": [listing] if page == 1 else [], "maxErgebnisse": 5000}

        _jobs, calls = self.scrape(responses)

        self.assertEqual([call["page"] for call in calls], [1, 2])
        self.assertTrue(all(call["keyword"] is None for call in calls), calls)


class V6ResponseShapeTests(EndpointTestCase):
    """v6 kept `maxErgebnisse` but renamed the result list to `ergebnisliste`
    and every listing field with it — which made the probe say "924 oglasa"
    while the scraper exported zero rows."""

    V6_LISTING = {
        "referenznummer": "10001-1003492538-S",
        "stellenangebotsTitel": "Technischer Assistent (m/w/d)",
        "firma": "CeGaT GmbH",
        "hauptberuf": "Biologisch-technische/r Assistent/in",
        "stellenlokationen": [
            {"adresse": {"ort": "Tübingen", "region": "BADEN_WUERTTEMBERG"}}
        ],
        "datumErsteVeroeffentlichung": "2026-08-05",
    }

    def test_the_ergebnisliste_key_is_read(self):
        payload = {"ergebnisliste": [self.V6_LISTING], "maxErgebnisse": 924}
        self.assertEqual(aa._listings(payload), [self.V6_LISTING])
        self.assertEqual(aa._listings({"stellenangebote": [self.V6_LISTING]}), [self.V6_LISTING])
        self.assertEqual(aa._listings(None), [])

    def test_a_scrape_against_a_v6_search_response_produces_rows(self):
        live = {
            "/pc/v6/jobs": {"ergebnisliste": [self.V6_LISTING], "maxErgebnisse": 1},
            "/pc/v4/jobdetails": DETAIL_PAYLOAD,
        }
        with mock.patch.object(aa.urllib.request, "urlopen", router(live, [])):
            jobs = aa.scrape_arbeitsagentur(category="Biologie", max_pages=1)

        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["refnr"], "10001-1003492538-S")
        self.assertEqual(job["title"], "Technischer Assistent (m/w/d)")
        self.assertEqual(job["location"], "Tübingen")
        self.assertEqual(job["published_at"], "2026-08-05")
        self.assertEqual(job["email"], "info@muster.de")
        # No fallback chatter: the Berufsfeld search matched.
        self.assertFalse(any("ponavljam kao was=" in line for line in self.logs), self.logs)

    def test_a_v6_listing_without_arbeitsort_still_gets_a_location(self):
        self.assertEqual(aa._listing_location(self.V6_LISTING), "Tübingen")
        self.assertEqual(aa._listing_location({"arbeitsort": {"ort": "Berlin"}}), "Berlin")
        self.assertEqual(aa._listing_location({"stellenlokationen": []}), "")


class ScrapeIntegrationTests(EndpointTestCase):
    def test_a_whole_scrape_works_against_the_surviving_endpoints(self):
        listing = {
            "refnr": "REF-1",
            "titel": "Schweisser",
            "arbeitgeber": "Muster GmbH",
            "arbeitsort": {"ort": "München"},
            "aktuelleVeroeffentlichungsdatum": "2026-08-01",
        }
        live = {
            "/pc/v4/app/jobs": {"stellenangebote": [listing], "maxErgebnisse": 1},
            "/pc/v3/jobdetails": DETAIL_PAYLOAD,
        }
        seen = []
        with mock.patch.object(aa.urllib.request, "urlopen", router(live, [])):
            jobs = aa.scrape_arbeitsagentur(
                category="Hochbau", max_pages=1, on_job=seen.append
            )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["email"], "info@muster.de")
        self.assertEqual(jobs[0]["company"], "Muster")
        self.assertEqual(jobs[0]["refnr"], "REF-1")
        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
