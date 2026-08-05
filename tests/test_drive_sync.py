"""Google Drive sync of the seen-id store.

Only the pure parts run here — merging and the sync decision loop, with the
Drive REST layer faked. The OAuth flow itself needs a browser and a Google
account, so it stays untested by design.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import drive_sync, seen_store


class MergeTests(unittest.TestCase):
    def test_union_of_both_sides(self):
        merged = drive_sync.merge_lines("a\nb\n", "b\nc\n")
        self.assertEqual(merged, "a\nb\nc\n")

    def test_blank_lines_and_whitespace_disappear(self):
        self.assertEqual(drive_sync.merge_lines("a\n\n  \n", "  b \n"), "a\nb\n")

    def test_empty_everything_stays_empty(self):
        self.assertEqual(drive_sync.merge_lines("", ""), "")


class SyncTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        patcher = mock.patch.object(seen_store, "state_dir", return_value=Path(self._tmp.name))
        patcher.start()
        self.addCleanup(patcher.stop)

        self.remote: dict[str, str] = {}  # name -> content
        self.uploads: list[str] = []

        mock.patch.object(
            drive_sync,
            "_remote_files",
            side_effect=lambda: {name: name for name in self.remote},
        ).start()
        mock.patch.object(
            drive_sync, "_download", side_effect=lambda file_id: self.remote[file_id]
        ).start()

        def upload(name, content, file_id):
            self.remote[name] = content
            self.uploads.append(name)

        mock.patch.object(drive_sync, "_upload", side_effect=upload).start()
        self.addCleanup(mock.patch.stopall)

    def local(self, name: str, content: str) -> Path:
        path = Path(self._tmp.name) / name
        path.write_text(content, encoding="utf-8")
        return path

    def test_both_sides_end_up_with_the_union(self):
        path = self.local("seen-arbeitsagentur.txt", "A\nB\n")
        self.remote["seen-arbeitsagentur.txt"] = "B\nC\n"

        touched = drive_sync.sync(log=lambda _line: None)

        self.assertEqual(touched, 1)
        self.assertEqual(path.read_text(encoding="utf-8"), "A\nB\nC\n")
        self.assertEqual(self.remote["seen-arbeitsagentur.txt"], "A\nB\nC\n")

    def test_a_remote_only_file_is_pulled_down(self):
        self.remote["seen-hzz.txt"] = "X\n"

        drive_sync.sync(log=lambda _line: None)

        pulled = Path(self._tmp.name) / "seen-hzz.txt"
        self.assertEqual(pulled.read_text(encoding="utf-8"), "X\n")
        self.assertEqual(self.uploads, [], "nothing new locally, so no upload")

    def test_a_local_only_file_is_pushed_up(self):
        self.local("seen-arbeitsagentur-emails.txt", "a@b.hr\n")

        drive_sync.sync(log=lambda _line: None)

        self.assertEqual(self.remote["seen-arbeitsagentur-emails.txt"], "a@b.hr\n")

    def test_identical_sides_touch_nothing(self):
        self.local("seen-hzz.txt", "X\n")
        self.remote["seen-hzz.txt"] = "X\n"

        touched = drive_sync.sync(log=lambda _line: None)

        self.assertEqual(touched, 0)
        self.assertEqual(self.uploads, [])

    def test_the_token_file_is_never_synced(self):
        self.local(drive_sync.TOKEN_FILE, '{"refresh_token": "secret"}')
        self.local("seen-hzz.txt", "X\n")

        drive_sync.sync(log=lambda _line: None)

        self.assertNotIn(drive_sync.TOKEN_FILE, self.remote)


if __name__ == "__main__":
    unittest.main()
