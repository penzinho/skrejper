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

    def test_makes_exactly_one_request(self):
        calls = []

        def once(*args, **kwargs):
            calls.append(args)
            return {"maxErgebnisse": 1}

        with mock.patch.object(self.aa, "_search_json", once):
            runner.run(self.probe_config(), self.events)

        self.assertEqual(len(calls), 1)

    def test_passes_the_keyword_through(self):
        seen = {}

        def capture(berufsfeld, keyword, *rest):
            seen["berufsfeld"] = berufsfeld
            seen["keyword"] = keyword
            return {"maxErgebnisse": 0}

        with mock.patch.object(self.aa, "_search_json", capture):
            runner.run(self.probe_config(keyword="Schweisser"), self.events)

        self.assertIsNone(seen["berufsfeld"])
        self.assertEqual(seen["keyword"], "Schweisser")

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
