"""The GUI side: NDJSON lines in, Qt signals out.

Runs without a display (QObject signals need no QApplication) and without
spawning a worker — the subprocess itself is covered by tests/test_runner.py.
"""

import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from app.desktop.process import ScrapeProcess, worker_argv
except ImportError as exc:  # pragma: no cover - PySide6 missing
    raise unittest.SkipTest(f"PySide6 unavailable: {exc}")


class Recorder:
    def __init__(self, process: ScrapeProcess) -> None:
        self.logs: list[str] = []
        self.rows: list[dict] = []
        self.stats: list[dict] = []
        self.targets: list[tuple] = []
        self.done: list[dict] = []
        self.cancelled: list[dict] = []
        self.failures: list[tuple] = []

        process.logged.connect(self.logs.append)
        process.rowArrived.connect(self.rows.append)
        process.progressed.connect(self.stats.append)
        process.targetStarted.connect(lambda *a: self.targets.append(a))
        process.succeeded.connect(self.done.append)
        process.cancelled.connect(self.cancelled.append)
        process.failed.connect(lambda *a: self.failures.append(a))


class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.process = ScrapeProcess()
        self.recorder = Recorder(self.process)

    def feed(self, event: dict) -> None:
        self.process._dispatch(json.dumps(event, ensure_ascii=False))

    def test_row_event_becomes_a_row_signal(self):
        self.feed({"event": "row", "row": {"email": "a@b.hr", "company": "Firma"}})
        self.assertEqual(self.recorder.rows, [{"email": "a@b.hr", "company": "Firma"}])

    def test_log_and_progress_events(self):
        self.feed({"event": "log", "line": "[hzz] Scraping listing page 3: 75 rows"})
        self.feed({"event": "progress", "stats": {"seen": 10, "kept": 4}})

        self.assertEqual(self.recorder.logs, ["[hzz] Scraping listing page 3: 75 rows"])
        self.assertEqual(self.recorder.stats, [{"seen": 10, "kept": 4}])

    def test_target_start_carries_position_and_label(self):
        self.feed({"event": "target_start", "index": 2, "total": 5, "label": "Kuhari"})
        self.assertEqual(self.recorder.targets, [(2, 5, "Kuhari")])

    def test_terminal_events(self):
        self.feed({"event": "done", "rows": 7, "results": []})
        self.assertEqual(self.recorder.done[0]["rows"], 7)
        self.assertTrue(self.process._saw_terminal_event)

    def test_error_event_carries_message_and_traceback(self):
        self.feed({"event": "error", "message": "boom", "traceback": "Traceback…"})
        self.assertEqual(self.recorder.failures, [("boom", "Traceback…")])

    def test_non_json_output_is_shown_as_a_log_line_instead_of_crashing(self):
        # A stray print that escapes the worker's redirection must not kill the run.
        self.process._dispatch("Traceback (most recent call last):")
        self.assertEqual(self.recorder.logs, ["Traceback (most recent call last):"])

    def test_unknown_event_is_ignored(self):
        self.feed({"event": "something-new", "x": 1})
        self.assertEqual(self.recorder.logs, [])
        self.assertEqual(self.recorder.rows, [])

    def test_utf8_survives_the_round_trip(self):
        self.feed({"event": "row", "row": {"city": "München", "company": "Dječji vrtić"}})
        self.assertEqual(self.recorder.rows[0]["city"], "München")
        self.assertEqual(self.recorder.rows[0]["company"], "Dječji vrtić")


class WorkerArgvTests(unittest.TestCase):
    def test_from_source_it_runs_the_entry_script_unbuffered(self):
        argv = worker_argv()

        self.assertEqual(argv[0], sys.executable)
        self.assertIn("-u", argv)
        self.assertTrue(argv[-2].endswith("skrejper_gui.py"))
        self.assertEqual(argv[-1], "--scrape-worker")
        self.assertTrue(Path(argv[-2]).exists())

    def test_frozen_bundle_relaunches_itself(self):
        from app.desktop import paths

        original = paths.is_frozen
        paths.is_frozen = lambda: True
        try:
            self.assertEqual(worker_argv(), [sys.executable, "--scrape-worker"])
        finally:
            paths.is_frozen = original


if __name__ == "__main__":
    unittest.main()
