import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.desktop import runner


def job(refnr, email="info@muster.de", company="Muster GmbH"):
    return {
        "title": "Schweisser",
        "company": company,
        "location": "München",
        "published_at": "2026-08-01",
        "detail_url": f"https://example.invalid/{refnr}",
        "category": "Metall",
        "email": email,
        "employer_website": "",
        "refnr": refnr,
        "source": "arbeitsagentur",
    }


class RunnerTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.root = Path(self._dir.name)
        self.output = self.root / "out"
        self.state = self.root / "state"
        # Overwrite, not setdefault: runner.configure_environment may already have
        # pointed this at the real user directory when the module was imported.
        os.environ["SKREJPER_STATE_DIR"] = str(self.state)
        self.addCleanup(os.environ.pop, "SKREJPER_STATE_DIR", None)

        self.stream = io.StringIO()
        self.events = runner._EventWriter(self.stream)

    def emitted(self):
        return [json.loads(line) for line in self.stream.getvalue().splitlines() if line.strip()]

    def events_of(self, kind):
        return [event for event in self.emitted() if event["event"] == kind]

    def config(self, **overrides):
        config = {
            "source": "arbeitsagentur",
            "output_dir": str(self.output),
            "skip_seen": True,
            "dedupe_company": True,
            "exclude_public_sector": False,
            "write_xlsx": False,
            "targets": [{"category": "bau_ausbau", "label": "Bau, Ausbau & Gebäudetechnik"}],
            "options": {"max_pages": 1},
        }
        config.update(overrides)
        return config


class ArbeitsagenturRunTests(RunnerTestCase):
    def test_streams_rows_and_writes_the_csv(self):
        def fake_scrape(**kwargs):
            kwargs["on_job"](job("REF-1"))
            kwargs["on_job"](job("REF-2", email="zwei@muster.de", company="Zweite GmbH"))
            return []

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", fake_scrape):
            self.assertEqual(runner.run(self.config(), self.events), 0)

        rows = self.events_of("row")
        self.assertEqual([r["row"]["email"] for r in rows], ["info@muster.de", "zwei@muster.de"])

        done = self.events_of("done")[0]
        self.assertEqual(done["rows"], 2)

        csv_path = Path(self.events_of("target_done")[0]["csv"])
        self.assertTrue(csv_path.name.startswith("arbeitsagentur-bau_ausbau-"))
        text = csv_path.read_text(encoding="utf-8-sig")
        self.assertIn('"info@muster.de"', text)
        self.assertIn('"Zweite GmbH"', text)

    def test_remembers_seen_ids_and_skips_them_next_time(self):
        def fake_scrape(**kwargs):
            kwargs["on_job"](job("REF-1"))
            return []

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", fake_scrape):
            runner.run(self.config(), self.events)

        self.assertEqual(
            (self.state / "seen-arbeitsagentur.txt").read_text(encoding="utf-8").split(),
            ["REF-1"],
        )
        self.assertEqual(
            (self.state / "seen-arbeitsagentur-emails.txt").read_text(encoding="utf-8").split(),
            ["info@muster.de"],
        )

        captured = {}

        def capture(**kwargs):
            captured.update(kwargs)
            return []

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", capture):
            runner.run(self.config(), self.events)
        self.assertEqual(captured["skip_ids"], {"REF-1"})

    def test_skip_seen_off_does_not_touch_the_store(self):
        def fake_scrape(**kwargs):
            kwargs["on_job"](job("REF-1"))
            return []

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", fake_scrape):
            runner.run(self.config(skip_seen=False), self.events)

        self.assertFalse((self.state / "seen-arbeitsagentur.txt").exists())

    def test_one_file_per_target(self):
        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", lambda **k: []):
            runner.run(
                self.config(
                    targets=[
                        {"category": "bau_ausbau", "label": "Bau"},
                        {"category": "it", "label": "IT"},
                    ]
                ),
                self.events,
            )

        files = sorted(p.name for p in self.output.glob("*.csv"))
        self.assertEqual(len(files), 2)
        self.assertTrue(any(name.startswith("arbeitsagentur-bau_ausbau-") for name in files))
        self.assertTrue(any(name.startswith("arbeitsagentur-it-") for name in files))

    def test_keyword_only_target_names_the_file_after_the_keyword(self):
        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", lambda **k: []):
            runner.run(
                self.config(targets=[{"category": None, "label": "Schweisser München"}]),
                self.events,
            )
        names = [p.name for p in self.output.glob("*.csv")]
        self.assertTrue(any(n.startswith("arbeitsagentur-schweisser-munchen-") for n in names), names)

    def test_a_crash_mid_run_still_leaves_the_rows_collected_so_far(self):
        def exploding(**kwargs):
            kwargs["on_job"](job("REF-1"))
            raise RuntimeError("API je pao")

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", exploding):
            self.assertEqual(runner.run(self.config(), self.events), 1)

        error = self.events_of("error")[0]
        self.assertIn("API je pao", error["message"])

        csv_path = next(self.output.glob("*.csv"))
        self.assertIn('"info@muster.de"', csv_path.read_text(encoding="utf-8-sig"))

    def test_stop_is_reported_as_cancelled(self):
        def interrupted(**kwargs):
            kwargs["on_job"](job("REF-1"))
            raise KeyboardInterrupt

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", interrupted):
            self.assertEqual(runner.run(self.config(), self.events), 130)

        self.assertEqual(len(self.events_of("cancelled")), 1)
        csv_path = next(self.output.glob("*.csv"))
        self.assertIn('"info@muster.de"', csv_path.read_text(encoding="utf-8-sig"))

    def test_xlsx_is_written_next_to_the_csv(self):
        from app.desktop.exporters import xlsx_available

        if not xlsx_available():
            self.skipTest("openpyxl not installed")

        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", lambda **k: []):
            runner.run(self.config(write_xlsx=True), self.events)

        target = self.events_of("target_done")[0]
        self.assertTrue(Path(target["xlsx"]).exists())


class VersionStampTests(RunnerTestCase):
    def test_every_run_starts_by_naming_the_build(self):
        # Without this a pasted log cannot be attributed to a binary, which is
        # how a fixed bug gets reported again from a stale build.
        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", lambda **k: []):
            runner.run(self.config(), self.events)

        first = self.emitted()[0]
        self.assertEqual(first["event"], "log")
        self.assertIn("Skrejper", first["line"])
        self.assertIn(runner.paths.version_string(), first["line"])


class ProbeTests(RunnerTestCase):
    """The "Provjeri vezu" button: one request, a definite answer."""

    def setUp(self):
        super().setUp()
        from app.scrapers import arbeitsagentur

        self.aa = arbeitsagentur
        arbeitsagentur._search_url = None
        self.addCleanup(setattr, arbeitsagentur, "_search_url", None)

    def probe_config(self, **overrides):
        config = {"source": "arbeitsagentur", "mode": "probe"}
        config.update(overrides)
        return config

    def test_reports_the_live_endpoint_and_result_count(self):
        payload = {"stellenangebote": [], "maxErgebnisse": 4321}
        with mock.patch.object(self.aa, "_search_json", return_value=payload):
            self.aa._search_url = f"{self.aa.API_BASE}/pc/v6/jobs"
            self.assertEqual(runner.run(self.probe_config(), self.events), 0)

        probe = self.events_of("probe")[0]
        self.assertTrue(probe["ok"])
        self.assertEqual(probe["total"], 4321)
        self.assertIn("/pc/v6/jobs", probe["endpoint"])

    def test_reports_failure_without_writing_any_files(self):
        with mock.patch.object(self.aa, "_search_json", return_value=None):
            self.assertEqual(runner.run(self.probe_config(), self.events), 1)

        probe = self.events_of("probe")[0]
        self.assertFalse(probe["ok"])
        self.assertIn("endpoint", probe["message"])
        self.assertFalse(self.output.exists(), "a probe must not create output files")

    def test_compares_an_unfiltered_search_against_a_berufsfeld_one(self):
        # The difference between the two is what separates "the board is down"
        # from "our category names no longer match the board's".
        calls = []

        def capture(berufsfeld, keyword, *rest):
            calls.append((berufsfeld, keyword))
            return {"maxErgebnisse": 0 if berufsfeld else 1_200_000}

        with mock.patch.object(self.aa, "_search_json", capture):
            runner.run(self.probe_config(berufsfeld="Hochbau"), self.events)

        self.assertEqual(calls, [(None, None), ("Hochbau", None)])

        probe = self.events_of("probe")[0]
        self.assertTrue(probe["ok"])
        self.assertEqual(probe["total"], 1_200_000)
        self.assertEqual(probe["berufsfeld"], "Hochbau")
        self.assertEqual(probe["berufsfeld_total"], 0)

    def test_passes_the_keyword_through_on_the_unfiltered_search(self):
        calls = []

        def capture(berufsfeld, keyword, *rest):
            calls.append((berufsfeld, keyword))
            return {"maxErgebnisse": 5}

        with mock.patch.object(self.aa, "_search_json", capture):
            runner.run(self.probe_config(keyword="Schweisser", berufsfeld="Hochbau"), self.events)

        self.assertEqual(calls[0], (None, "Schweisser"))

    def test_falls_back_to_a_known_berufsfeld_when_none_is_selected(self):
        calls = []

        def capture(berufsfeld, keyword, *rest):
            calls.append(berufsfeld)
            return {"maxErgebnisse": 1}

        with mock.patch.object(self.aa, "_search_json", capture):
            runner.run(self.probe_config(), self.events)

        first_known = self.aa.get_arbeitsagentur_categories()[0]["berufsfelder"][0]
        self.assertEqual(calls[1], first_known)

    def test_writes_the_raw_response_for_diagnosis(self):
        payload = {
            "maxErgebnisse": 7,
            "facetten": {"berufsfeld": {"counts": {"Hochbau": 12, "Tiefbau": 5}}},
        }
        with mock.patch.object(self.aa, "_search_json", return_value=payload):
            runner.run(self.probe_config(output_dir=str(self.output)), self.events)

        probe = self.events_of("probe")[0]
        dump = Path(probe["dump"])
        self.assertTrue(dump.exists())
        self.assertEqual(json.loads(dump.read_text(encoding="utf-8"))["unfiltered"], payload)
        self.assertFalse(list(self.output.glob("*.csv")), "a probe must not write exports")

    def test_reports_the_live_facet_values(self):
        payload = {
            "maxErgebnisse": 7,
            "facetten": {"berufsfeld": {"counts": {"Hochbau": 12, "Tiefbau": 5}}},
        }
        with mock.patch.object(self.aa, "_search_json", return_value=payload):
            runner.run(self.probe_config(), self.events)

        logged = " ".join(event["line"] for event in self.events_of("log"))
        self.assertIn("facetten: berufsfeld", logged)
        self.assertIn("Hochbau (12)", logged)

    def test_survives_an_unexpected_facet_shape(self):
        # The response shape is not pinned down anywhere we control.
        for facets in ({"berufsfeld": ["Hochbau"]}, {"berufsfeld": None}, [], "nope", None):
            with self.subTest(facets=facets):
                with mock.patch.object(
                    self.aa, "_search_json", return_value={"maxErgebnisse": 1, "facetten": facets}
                ):
                    self.assertEqual(runner.run(self.probe_config(), self.events), 0)

    def test_an_unexpected_failure_is_reported_as_an_error(self):
        with mock.patch.object(self.aa, "_search_json", side_effect=RuntimeError("boom")):
            self.assertEqual(runner.run(self.probe_config(), self.events), 1)

        self.assertIn("boom", self.events_of("error")[0]["message"])


class HzzRunTests(RunnerTestCase):
    def hzz_config(self, **overrides):
        config = self.config(
            source="hzz",
            exclude_public_sector=True,
            targets=[
                {
                    "category": "hospitality_tourism",
                    "label": "Ugostitelji",
                    "groups": ["Konobari/konobarice", "Kuhari/kuharice"],
                }
            ],
            options={"max_pages": 2, "results_per_page": 75, "start_page": 1},
        )
        config.update(overrides)
        return config

    def test_one_scraper_call_per_subgroup_with_the_group_label_on_the_row(self):
        calls = []

        def fake_scrape(**kwargs):
            calls.append(kwargs)
            kwargs["on_job"](
                {
                    "company": f"Firma {kwargs['group']}",
                    "email": f"{len(calls)}@firma.hr",
                    "location": "Split",
                    "detail_url": f"https://burzarada.invalid/{len(calls)}",
                    "title": "Oglas",
                }
            )
            return []

        with mock.patch("app.scrapers.hzz.scrape_hzz", fake_scrape):
            self.assertEqual(runner.run(self.hzz_config(), self.events), 0)

        self.assertEqual([c["group"] for c in calls], ["Konobari/konobarice", "Kuhari/kuharice"])
        self.assertTrue(all(c["use_subgroups"] is False for c in calls))
        self.assertEqual([c["max_pages"] for c in calls], [2, 2])

        rows = [event["row"] for event in self.events_of("row")]
        self.assertEqual([r["group"] for r in rows], ["Konobari/konobarice", "Kuhari/kuharice"])
        self.assertEqual(rows[0]["source"], "hzz")

    def test_no_groups_falls_back_to_automatic_subgroup_discovery(self):
        calls = []

        with mock.patch("app.scrapers.hzz.scrape_hzz", lambda **k: calls.append(k) or []):
            runner.run(
                self.hzz_config(
                    targets=[{"category": "hospitality_tourism", "label": "Ugostitelji", "groups": []}]
                ),
                self.events,
            )

        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0]["use_subgroups"])
        self.assertIsNone(calls[0].get("group"))

    def test_public_sector_employers_are_filtered_out(self):
        def fake_scrape(**kwargs):
            kwargs["on_job"](
                {
                    "company": "Dječji vrtić Sunce",
                    "email": "vrtic@sunce.hr",
                    "location": "Zagreb",
                    "detail_url": "https://burzarada.invalid/9",
                }
            )
            return []

        with mock.patch("app.scrapers.hzz.scrape_hzz", fake_scrape):
            runner.run(
                self.hzz_config(
                    targets=[
                        {"category": "hospitality_tourism", "label": "U", "groups": ["Konobari/konobarice"]}
                    ]
                ),
                self.events,
            )

        self.assertEqual(self.events_of("row"), [])
        self.assertEqual(self.events_of("target_done")[0]["stats"]["excluded_company"], 1)


class GvpRunTests(RunnerTestCase):
    """A directory source: every member is wanted, with or without an e-mail."""

    def member(self, title, company, email="", website="https://firma.de", **extra):
        job = {
            "member": title,
            "company": company,
            "city": title.split(" - ")[-1].split(" (")[0],
            "plz": "10115",
            "street": "Weg 1",
            "country": "Deutschland",
            "phone": "030 1",
            "website": website,
            "managing_director": "",
            "business_fields": "Zeitarbeit",
            "email": email,
            "email_source": "gvp" if email else "",
            "source": "gvp",
        }
        job.update(extra)
        return job

    def gvp_config(self, **overrides):
        config = self.config(
            source="gvp",
            keep_without_email=True,
            targets=[{"category": "mitglieder", "label": "GVP Mitglieder"}],
            options={"enrich": True, "enrich_pages": 4, "branches": False},
        )
        config.update(overrides)
        return config

    def run_with(self, members, config=None):
        def fake_scrape(**kwargs):
            for member in members:
                kwargs["on_member"](member)
            return []

        with mock.patch("app.scrapers.gvp.scrape_gvp", fake_scrape):
            return runner.run(config or self.gvp_config(), self.events)

    def test_members_with_email_go_to_the_main_file_the_rest_to_missing_emails(self):
        code = self.run_with(
            [
                self.member("Alpha GmbH - Berlin (10115)", "Alpha GmbH", "info@alpha.de"),
                self.member("Beta GmbH - Bonn (53111)", "Beta GmbH"),
                self.member("Gamma GmbH - Kiel (24103)", "Gamma GmbH", "post@gamma.de"),
            ]
        )
        self.assertEqual(code, 0)

        done = self.events_of("target_done")[0]
        self.assertEqual(done["rows"], 2)
        self.assertEqual(done["missing_rows"], 1)
        main = Path(done["csv"])
        missing = Path(done["missing_csv"])
        self.assertTrue(main.name.startswith("gvp-mitglieder-"))
        self.assertEqual(missing.name, main.stem + "-missing-emails.csv")

        main_text = main.read_text(encoding="utf-8-sig")
        self.assertIn('"info@alpha.de"', main_text)
        self.assertIn('"post@gamma.de"', main_text)
        self.assertNotIn("Beta", main_text)

        missing_text = missing.read_text(encoding="utf-8-sig")
        self.assertIn('"Beta GmbH"', missing_text)
        self.assertIn('"https://firma.de"', missing_text)
        self.assertNotIn("Alpha", missing_text)
        # Same columns in both files, so they can be merged after enrichment.
        self.assertEqual(main_text.splitlines()[0], missing_text.splitlines()[0])

        # Address-less members are streamed to the table too, just later.
        rows = [event["row"] for event in self.events_of("row")]
        self.assertEqual([r["company"] for r in rows], ["Alpha GmbH", "Gamma GmbH", "Beta GmbH"])
        self.assertEqual(done["stats"]["missing"], 1)

    def test_a_branch_without_email_is_dropped_when_the_head_office_has_one(self):
        self.run_with(
            [
                self.member("Alpha GmbH - Berlin (10115)", "Alpha GmbH"),
                self.member("Alpha GmbH - Bonn (53111)", "Alpha GmbH", "info@alpha.de"),
                self.member("Alpha GmbH - Kiel (24103)", "Alpha GmbH"),
                self.member("Beta GmbH - Bonn (53111)", "Beta GmbH"),
                self.member("Beta GmbH - Kiel (24103)", "Beta GmbH"),
            ]
        )
        done = self.events_of("target_done")[0]
        self.assertEqual(done["rows"], 1)
        # Beta: two branches, no address anywhere -> one worklist entry.
        self.assertEqual(done["missing_rows"], 1)
        missing_text = Path(done["missing_csv"]).read_text(encoding="utf-8-sig")
        self.assertIn('"Beta GmbH - Bonn (53111)"', missing_text)
        self.assertNotIn("Alpha", missing_text)

    def test_only_members_with_an_email_are_remembered(self):
        self.run_with(
            [
                self.member("Alpha GmbH - Berlin (10115)", "Alpha GmbH", "info@alpha.de"),
                self.member("Beta GmbH - Bonn (53111)", "Beta GmbH"),
            ]
        )
        self.assertEqual(
            (self.state / "seen-gvp.txt").read_text(encoding="utf-8").split("\n")[:-1],
            ["Alpha GmbH - Berlin (10115)"],
        )
        self.assertFalse((self.state / "seen-gvp-enriched.txt").exists())

    def test_failed_website_lookups_are_remembered_and_passed_back(self):
        self.run_with(
            [
                self.member("Beta GmbH - Bonn (53111)", "Beta GmbH", enrich_tried=True),
                self.member("Gamma GmbH - Kiel (24103)", "Gamma GmbH", "post@gamma.de", email_source="website"),
            ]
        )
        self.assertEqual(
            (self.state / "seen-gvp-enriched.txt").read_text(encoding="utf-8").split("\n")[:-1],
            ["Beta GmbH - Bonn (53111)"],
        )

        captured = {}

        def capture(**kwargs):
            captured.update(kwargs)
            return []

        with mock.patch("app.scrapers.gvp.scrape_gvp", capture):
            runner.run(self.gvp_config(), self.events)
        self.assertEqual(captured["skip_enrich_ids"], {"Beta GmbH - Bonn (53111)"})
        self.assertEqual(captured["skip_ids"], {"Gamma GmbH - Kiel (24103)"})
        self.assertTrue(captured["enrich"])
        self.assertEqual(captured["enrich_pages"], 4)

    def test_options_reach_the_scraper(self):
        captured = {}

        with mock.patch("app.scrapers.gvp.scrape_gvp", lambda **k: captured.update(k) or []):
            runner.run(
                self.gvp_config(
                    options={
                        "search": "Muster", "city": "Kleve", "zip": "47533",
                        "business_area": "Zeitarbeit", "quality": "QS Pflege", "branches": True,
                        "max_pages": 3, "member_limit": 20, "enrich": False, "enrich_pages": 2,
                    }
                ),
                self.events,
            )
        self.assertEqual(captured["search"], "Muster")
        self.assertEqual(captured["city"], "Kleve")
        self.assertEqual(captured["zip_code"], "47533")
        self.assertEqual(captured["business_area"], "Zeitarbeit")
        self.assertEqual(captured["quality"], "QS Pflege")
        self.assertTrue(captured["branches"])
        self.assertEqual((captured["max_pages"], captured["member_limit"]), (3, 20))
        self.assertFalse(captured["enrich"])

    def test_stop_mid_run_keeps_both_files(self):
        def interrupted(**kwargs):
            kwargs["on_member"](self.member("Alpha GmbH - Berlin (10115)", "Alpha GmbH", "info@alpha.de"))
            kwargs["on_member"](self.member("Beta GmbH - Bonn (53111)", "Beta GmbH"))
            raise KeyboardInterrupt

        with mock.patch("app.scrapers.gvp.scrape_gvp", interrupted):
            self.assertEqual(runner.run(self.gvp_config(), self.events), 130)

        files = {p.name: p for p in self.output.glob("*.csv")}
        main = next(p for name, p in files.items() if "missing" not in name)
        missing = next(p for name, p in files.items() if "missing" in name)
        self.assertIn("info@alpha.de", main.read_text(encoding="utf-8-sig"))
        # Held back until the next company — the Stop releases it.
        self.assertIn("Beta GmbH", missing.read_text(encoding="utf-8-sig"))

    def test_other_sources_do_not_get_a_missing_emails_file(self):
        with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", lambda **k: []):
            runner.run(self.config(), self.events)
        self.assertEqual([p.name for p in self.output.glob("*missing*")], [])
        self.assertNotIn("missing_csv", self.events_of("target_done")[0])


class LogRedirectionTests(RunnerTestCase):
    def test_scraper_prints_become_log_events(self):
        stream = io.StringIO()
        events = runner._EventWriter(stream)
        redirector = runner._LogRedirector(events)

        redirector.write("[hzz] Scraping listing page 3: 75 rows\n")
        redirector.write("partial ")
        redirector.write("line\n")
        redirector.write("no newline yet")
        redirector.flush()

        lines = [json.loads(l)["line"] for l in stream.getvalue().splitlines()]
        self.assertEqual(
            lines,
            ["[hzz] Scraping listing page 3: 75 rows", "partial line", "no newline yet"],
        )

    def test_a_bad_config_path_is_reported_not_raised(self):
        stream = io.StringIO()
        with mock.patch.object(sys, "stdout", stream):
            code = runner.main([str(self.root / "nope.json")])
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
