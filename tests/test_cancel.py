"""Stop, end to end: a real worker process, a real signal, a real CSV.

The scraper is faked (slow, endless) but everything around it is the production
path — app/desktop/runner.py in a child process, the SIGTERM handler, the NDJSON
pipe and the Qt signals. That is the part worth testing: Stop is the only way
out of a run, because neither scraper can be interrupted from the inside.
"""

import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PySide6.QtCore import QCoreApplication, QElapsedTimer, QEventLoop, QTimer

    from app.desktop import process as process_module
    from app.desktop.process import ScrapeProcess
except ImportError as exc:  # pragma: no cover - PySide6 missing
    raise unittest.SkipTest(f"PySide6 unavailable: {exc}")

# A worker that streams a row every 50 ms and never finishes on its own.
FAKE_WORKER = textwrap.dedent(
    """
    import sys, time
    sys.path.insert(0, {root!r})
    from unittest import mock
    from app.desktop import runner

    def endless(**kwargs):
        index = 0
        while True:
            index += 1
            kwargs["on_job"]({{
                "company": f"Firma {{index}}",
                "email": f"kontakt{{index}}@firma.de",
                "location": "München",
                "refnr": f"REF-{{index}}",
                "title": "Oglas",
            }})
            time.sleep(0.05)

    with mock.patch("app.scrapers.arbeitsagentur.scrape_arbeitsagentur", endless):
        sys.exit(runner.main(sys.argv[2:]))
    """
)


@unittest.skipIf(os.name == "nt", "SIGTERM-based graceful stop is POSIX-only")
class CancelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QCoreApplication.instance() or QCoreApplication([])

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.root = Path(self._dir.name)
        self.output = self.root / "out"

        os.environ["SKREJPER_STATE_DIR"] = str(self.root / "state")
        self.addCleanup(os.environ.pop, "SKREJPER_STATE_DIR", None)

        worker = self.root / "fake_worker.py"
        worker.write_text(FAKE_WORKER.format(root=str(PROJECT_ROOT)), encoding="utf-8")

        original = process_module.worker_argv
        process_module.worker_argv = lambda: [sys.executable, "-u", str(worker), "--scrape-worker"]
        self.addCleanup(setattr, process_module, "worker_argv", original)

        self.process = ScrapeProcess()
        self.rows = []
        self.cancelled = []
        self.stopping = []
        self.process.rowArrived.connect(self.rows.append)
        self.process.cancelled.connect(self.cancelled.append)
        self.process.stopping.connect(lambda: self.stopping.append(True))

    def config(self):
        return {
            "source": "arbeitsagentur",
            "output_dir": str(self.output),
            "skip_seen": True,
            "dedupe_company": True,
            "exclude_public_sector": False,
            "write_xlsx": False,
            "targets": [{"category": "it", "label": "IT"}],
            "options": {"max_pages": 1},
        }

    def pump_until(self, predicate, timeout_ms=15000):
        """Spin the Qt event loop until predicate holds or we give up."""
        clock = QElapsedTimer()
        clock.start()
        loop = QEventLoop()
        while not predicate() and clock.elapsed() < timeout_ms:
            QTimer.singleShot(20, loop.quit)
            loop.exec()
        return predicate()

    def test_stop_ends_the_run_and_keeps_what_was_collected(self):
        self.process.start(self.config())
        self.assertTrue(self.pump_until(lambda: len(self.rows) >= 3), "worker never produced rows")

        collected = len(self.rows)
        self.process.stop()

        self.assertTrue(self.stopping, "stopping signal not emitted")
        self.assertTrue(self.pump_until(lambda: not self.process.running), "worker did not exit")
        self.assertTrue(self.cancelled, "cancelled signal not emitted")

        csv_path = next(self.output.glob("*.csv"))
        body = csv_path.read_text(encoding="utf-8-sig").splitlines()
        self.assertGreaterEqual(len(body) - 1, collected)
        self.assertIn("kontakt1@firma.de", body[1])

    def test_stop_does_not_block_the_caller(self):
        self.process.start(self.config())
        self.assertTrue(self.pump_until(lambda: len(self.rows) >= 1))

        clock = QElapsedTimer()
        clock.start()
        self.process.stop()
        elapsed = clock.elapsed()

        # The old implementation sat in waitForFinished for up to 3s here, which
        # froze the window while the user waited for a button to respond.
        self.assertLess(elapsed, 500, f"stop() blocked for {elapsed} ms")
        self.pump_until(lambda: not self.process.running)

    def test_ids_harvested_before_the_stop_are_remembered(self):
        self.process.start(self.config())
        self.assertTrue(self.pump_until(lambda: len(self.rows) >= 2))
        self.process.stop()
        self.assertTrue(self.pump_until(lambda: not self.process.running))

        seen = (self.root / "state" / "seen-arbeitsagentur.txt").read_text(encoding="utf-8").split()
        self.assertIn("REF-1", seen)

    def test_a_second_run_can_start_after_a_stop(self):
        self.process.start(self.config())
        self.assertTrue(self.pump_until(lambda: len(self.rows) >= 1))
        self.process.stop()
        self.assertTrue(self.pump_until(lambda: not self.process.running))

        self.rows.clear()
        self.process.start(self.config())
        self.assertTrue(self.pump_until(lambda: len(self.rows) >= 1), "could not restart")
        self.process.stop()
        self.pump_until(lambda: not self.process.running)

    def test_a_worker_that_ignores_sigterm_is_killed(self):
        stubborn = self.root / "stubborn_worker.py"
        stubborn.write_text(
            textwrap.dedent(
                """
                import signal, sys, time
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                sys.stdout.write('{"event": "start", "targets": 1}\\n')
                sys.stdout.flush()
                while True:
                    time.sleep(0.1)
                """
            ),
            encoding="utf-8",
        )
        process_module.worker_argv = lambda: [sys.executable, "-u", str(stubborn), "--scrape-worker"]

        process = ScrapeProcess()
        cancelled = []
        process.cancelled.connect(cancelled.append)
        process.KILL_GRACE_MS = 300  # don't make the test wait the real grace period
        process.start(self.config())

        self.assertTrue(self.pump_until(lambda: process.running))
        process.stop()

        clock = QElapsedTimer()
        clock.start()
        loop = QEventLoop()
        while process.running and clock.elapsed() < 10000:
            QTimer.singleShot(20, loop.quit)
            loop.exec()

        self.assertFalse(process.running, "stubborn worker survived the kill")
        self.assertTrue(cancelled, "kill did not report a cancelled run")


if __name__ == "__main__":
    unittest.main()
