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

    def test_a_forbidden_path_is_skipped_like_a_retired_one(self):
        # A retired path does not stay a 404 forever: /pc/v4/jobs now answers
        # 403. Stopping there would hide a working path further down the list.
        payload, calls = self.search({"/pc/v4/app/jobs": SEARCH_PAYLOAD})
        self.assertEqual(payload, SEARCH_PAYLOAD)

        def forbidden(request, timeout=None):
            raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, None)

        self.setUp()  # forget the resolved endpoint
        with mock.patch.object(aa.urllib.request, "urlopen", forbidden):
            payload = aa._search_json("Hochbau", None, None, None, 1, 100, 10)

        self.assertIsNone(payload)
        self.assertTrue(any("HTTP 403" in line for line in self.logs), self.logs)
        self.assertTrue(any("has moved" in line for line in self.logs), self.logs)
        self.assertIsNone(aa._search_url)

    def test_a_resolved_path_that_dies_mid_run_falls_through(self):
        # The endpoint can be retired between one page and the next; the rest of
        # the known paths must still be there to catch it.
        self.search({"/pc/v6/jobs": SEARCH_PAYLOAD})
        self.assertEqual(aa._search_url, f"{aa.API_BASE}/pc/v6/jobs")

        payload, calls = self.search({"/pc/v4/app/jobs": SEARCH_PAYLOAD})

        self.assertEqual(payload, SEARCH_PAYLOAD)
        self.assertEqual(aa._search_url, f"{aa.API_BASE}/pc/v4/app/jobs")
        self.assertIn("/pc/v6/jobs", calls[0])

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

        def fake_search(berufsfeld, keyword, location, radius, page, size, timeout, beruf=None):
            calls.append(
                {"berufsfeld": berufsfeld, "keyword": keyword, "page": page, "beruf": beruf}
            )
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
        self.assertEqual(
            calls[1], {"berufsfeld": None, "keyword": "Hochbau", "page": 1, "beruf": None}
        )
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


class BerufFilterTests(EndpointTestCase):
    """`beruf` is the board's occupation facet, one level below Berufsfeld:
    "Gesundheits- und Krankenpfleger/in" rather than all of "Krankenpflege,
    Rettungsdienst und Geburtshilfe"."""

    LISTING = {"referenznummer": "REF-1", "stellenangebotsTitel": "Pflegefachkraft"}

    def scrape(self, **kwargs):
        calls = []

        def fake_search(berufsfeld, keyword, location, radius, page, size, timeout, beruf=None):
            calls.append({"berufsfeld": berufsfeld, "keyword": keyword, "beruf": beruf})
            return {"ergebnisliste": [self.LISTING] if page == 1 else [], "maxErgebnisse": 1}

        with mock.patch.object(aa, "_search_json", fake_search), mock.patch.object(
            aa, "_detail_json", return_value=DETAIL_PAYLOAD
        ):
            jobs = aa.scrape_arbeitsagentur(max_pages=1, **kwargs)
        return jobs, calls

    def test_the_occupation_reaches_the_query(self):
        _jobs, calls = self.scrape(beruf="Gesundheits- und Krankenpfleger/in")

        self.assertEqual(calls, [{"berufsfeld": None, "keyword": None,
                                  "beruf": "Gesundheits- und Krankenpfleger/in"}])

    def test_several_occupations_are_queried_one_by_one(self):
        # The board answers a repeated `beruf` parameter with nothing at all, so
        # each occupation gets its own search.
        _jobs, calls = self.scrape(beruf="Krankenschwester/-pfleger; Pflegeassistent/in")

        self.assertEqual(
            [call["beruf"] for call in calls],
            ["Krankenschwester/-pfleger", "Pflegeassistent/in"],
        )

    def test_a_list_works_as_well_as_a_string(self):
        _jobs, calls = self.scrape(beruf=["Altenpfleger/in", "Altenpflegehelfer/in"])

        self.assertEqual([call["beruf"] for call in calls],
                         ["Altenpfleger/in", "Altenpflegehelfer/in"])

    def test_the_occupation_replaces_the_category(self):
        # Each occupation sits in exactly one Berufsfeld, so intersecting the two
        # would only add queries that match nothing.
        _jobs, calls = self.scrape(category="gesundheit_pflege", beruf="Altenpfleger/in")

        self.assertEqual(len(calls), 1, calls)
        self.assertIsNone(calls[0]["berufsfeld"])
        self.assertTrue(
            any("Zanimanja su uža" in line for line in self.logs),
            f"narrowing to the occupation must be visible, got: {self.logs}",
        )

    def test_the_category_label_still_lands_on_the_row(self):
        jobs, _calls = self.scrape(category="gesundheit_pflege", beruf="Altenpfleger/in")

        self.assertEqual(jobs[0]["category"], "Gesundheit, Medizin & Pflege")

    def test_a_misspelled_occupation_says_so_instead_of_widening_the_search(self):
        calls = []

        def empty(berufsfeld, keyword, location, radius, page, size, timeout, beruf=None):
            calls.append(beruf)
            return {"ergebnisliste": [], "maxErgebnisse": 0}

        with mock.patch.object(aa, "_search_json", empty):
            jobs = aa.scrape_arbeitsagentur(max_pages=1, beruf="Krankenpflegerin")

        self.assertEqual(jobs, [])
        self.assertEqual(calls, ["Krankenpflegerin"], "no free-text retry for an occupation")
        self.assertTrue(
            any("provjerite točan naziv" in line for line in self.logs), self.logs
        )

    def test_the_occupation_is_sent_as_one_parameter(self):
        url = aa._build_search_url(
            "https://example.invalid/jobs", None, None, None, None, 1, 100,
            "Gesundheits- und Krankenpfleger/in",
        )

        self.assertIn("beruf=Gesundheits-+und+Krankenpfleger%2Fin", url)
        self.assertEqual(url.count("beruf="), 1)


class NarrowedCategoryTests(EndpointTestCase):
    """A category can be run whole or narrowed to some of its Berufsfelder —
    "Gesundheit" has eleven fields and a run for nurses needs one of them."""

    def scrape(self, **kwargs):
        calls = []

        def fake_search(berufsfeld, keyword, location, radius, page, size, timeout, beruf=None):
            calls.append(berufsfeld)
            # One result per field: an empty field would trigger the free-text
            # retry and add calls that say nothing about the narrowing.
            listing = {"referenznummer": f"REF-{len(calls)}"}
            return {"ergebnisliste": [listing], "maxErgebnisse": 1}

        with mock.patch.object(aa, "_search_json", fake_search), mock.patch.object(
            aa, "_detail_json", return_value=DETAIL_PAYLOAD
        ):
            jobs = aa.scrape_arbeitsagentur(max_pages=1, **kwargs)
        return jobs, calls

    def test_only_the_listed_fields_are_queried(self):
        _jobs, calls = self.scrape(
            category="gesundheit_pflege",
            berufsfelder=["Krankenpflege, Rettungsdienst und Geburtshilfe", "Altenpflege"],
        )

        self.assertEqual(
            calls, ["Krankenpflege, Rettungsdienst und Geburtshilfe", "Altenpflege"]
        )

    def test_the_whole_category_still_runs_without_a_narrowing(self):
        _jobs, calls = self.scrape(category="gesundheit_pflege")

        self.assertEqual(len(calls), 11, calls)

    def test_an_empty_narrowing_means_the_whole_category(self):
        _jobs, calls = self.scrape(category="gesundheit_pflege", berufsfelder=[])

        self.assertEqual(len(calls), 11, calls)


class PayloadSchemaTests(EndpointTestCase):
    """/pc/v6 answers 200 with a payload shaped differently from /pc/v4: the
    postings live under `ergebnisliste`, and their fields are spelled
    differently. Reading only the old names turned a healthy board into empty
    exports — every category "returned 0 oglasa" and fell back to a free-text
    search that also read as empty."""

    V6_LISTING = {
        "referenznummer": "REF-V6",
        "stellenangebotsTitel": "Pflegefachkraft (m/w/d) Altenpflege",
        "hauptberuf": "Altenpfleger/in",
        "firma": "Pflege GmbH",
        "stellenlokationen": [
            {"adresse": {"plz": "32545", "ort": "Bad Oeynhausen", "region": "NORDRHEIN_WESTFALEN"}}
        ],
        "datumErsteVeroeffentlichung": "2026-09-06",
        "externeURL": "https://pflege-gmbh.example/karriere",
    }

    def test_v6_postings_are_found_in_the_payload(self):
        payload = {"ergebnisliste": [self.V6_LISTING], "maxErgebnisse": 30971}

        self.assertEqual(aa._listings(payload), [self.V6_LISTING])

    def test_the_older_payload_shape_still_reads(self):
        payload = {"stellenangebote": [{"refnr": "REF-1"}], "maxErgebnisse": 1}

        self.assertEqual(aa._listings(payload), [{"refnr": "REF-1"}])

    def test_a_v6_listing_fills_every_column(self):
        with mock.patch.object(aa, "_detail_json", return_value=DETAIL_PAYLOAD):
            job = aa._enrich_listing(dict(self.V6_LISTING), "Zdravstvo", 10)

        self.assertEqual(job["refnr"], "REF-V6")
        self.assertEqual(job["title"], "Pflegefachkraft (m/w/d) Altenpflege")
        self.assertEqual(job["company"], "Muster")
        self.assertEqual(job["location"], "Bad Oeynhausen")
        self.assertEqual(job["published_at"], "2026-09-06")
        self.assertEqual(job["email"], "info@muster.de")
        self.assertEqual(job["employer_website"], "https://pflege-gmbh.example/karriere")
        self.assertIn("REF-V6", job["detail_url"])

    def test_the_employer_falls_back_to_the_listing(self):
        # Expired postings answer no detail at all; the search result still
        # names the employer.
        with mock.patch.object(aa, "_detail_json", return_value=None):
            job = aa._enrich_listing(dict(self.V6_LISTING), "Zdravstvo", 10)

        self.assertEqual(job["company"], "Pflege GmbH")

    def test_a_listing_without_a_town_falls_back_to_the_state(self):
        listing = dict(
            self.V6_LISTING,
            stellenlokationen=[{"adresse": {"region": "NORDRHEIN_WESTFALEN"}}],
        )
        with mock.patch.object(aa, "_detail_json", return_value=DETAIL_PAYLOAD):
            job = aa._enrich_listing(listing, "Zdravstvo", 10)

        # The enum constant is what v6 reports; the CSV carries prose.
        self.assertEqual(job["location"], "Nordrhein-Westfalen")

    def test_the_publication_date_falls_back_to_the_period(self):
        listing = dict(self.V6_LISTING)
        del listing["datumErsteVeroeffentlichung"]
        listing["veroeffentlichungszeitraum"] = {"von": "2026-09-01"}
        with mock.patch.object(aa, "_detail_json", return_value=DETAIL_PAYLOAD):
            job = aa._enrich_listing(listing, "Zdravstvo", 10)

        self.assertEqual(job["published_at"], "2026-09-01")

    def test_a_v6_search_is_scraped_end_to_end(self):
        live = {
            "/pc/v6/jobs": {"ergebnisliste": [self.V6_LISTING], "maxErgebnisse": 1},
            "/pc/v4/jobdetails": DETAIL_PAYLOAD,
        }
        with mock.patch.object(aa.urllib.request, "urlopen", router(live, [])):
            jobs = aa.scrape_arbeitsagentur(category="Altenpflege", max_pages=1)

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["refnr"], "REF-V6")
        self.assertEqual(
            self.logs,
            [f"[arbeitsagentur] Search endpoint: {aa.API_BASE}/pc/v6/jobs",
             f"[arbeitsagentur] Detail endpoint: {aa.API_BASE}{aa.DETAIL_PATHS[0]}"],
            "a v6 payload must not look like an empty category",
        )


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
