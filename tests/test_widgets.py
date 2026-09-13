"""The category tree: picking a whole category, or only some of its fields.

A run used to be all-or-nothing per category — ticking "Gesundheit, Medizin &
Pflege" meant all eleven of its Berufsfelder, eleven searches deep, when the ask
was nurses. These pin the narrowing, and the round trip through the settings
file that has to survive a restart.
"""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from app.desktop.widgets import CheckTree
    from tests.qt_app import widget_app
except ImportError as exc:  # pragma: no cover - PySide6 missing
    raise unittest.SkipTest(f"PySide6 unavailable: {exc}")

GROUPS = [
    ("gesundheit", "Zdravstvo — Gesundheit", [
        ("Altenpflege", "njega starijih — Altenpflege"),
        ("Krankenpflege", "medicinske sestre — Krankenpflege"),
        ("Pharmazie", "farmacija — Pharmazie"),
    ]),
    ("bau", "Građevina — Bau", [
        ("Hochbau", "visokogradnja — Hochbau"),
        ("Tiefbau", "niskogradnja — Tiefbau"),
    ]),
]


class CheckTreeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No display and no QT_QPA_PLATFORM: Qt cannot open a window at all, and
        # creating one anyway takes the whole run down with it.
        cls.app = widget_app()
        if cls.app is None:
            raise unittest.SkipTest("no Qt platform for widgets (set QT_QPA_PLATFORM=offscreen)")

    def tree(self):
        widget = CheckTree()
        widget.set_groups(GROUPS)
        self.addCleanup(widget.deleteLater)
        return widget

    def test_nothing_is_selected_to_begin_with(self):
        tree = self.tree()

        self.assertEqual(tree.selection(), [])
        self.assertEqual(tree.checked_fields(), [])

    def test_a_whole_category_reports_no_narrowing(self):
        # `fields: None` is what lets the runner pass the category on its own and
        # have the scraper resolve it, exactly as before the tree existed.
        tree = self.tree()
        tree.set_checked(["gesundheit"])

        self.assertEqual(tree.selection(), [{"key": "gesundheit", "fields": None}])
        self.assertEqual(
            tree.checked_fields(), ["Altenpflege", "Krankenpflege", "Pharmazie"]
        )

    def test_single_fields_narrow_the_category(self):
        tree = self.tree()
        tree.set_checked([], ["Krankenpflege"])

        self.assertEqual(
            tree.selection(), [{"key": "gesundheit", "fields": ["Krankenpflege"]}]
        )
        self.assertEqual(tree.checked_groups(), ["gesundheit"])

    def test_every_field_ticked_one_by_one_is_the_whole_category(self):
        tree = self.tree()
        tree.set_checked([], ["Altenpflege", "Krankenpflege", "Pharmazie"])

        self.assertEqual(tree.selection(), [{"key": "gesundheit", "fields": None}])

    def test_fields_from_two_categories_stay_apart(self):
        tree = self.tree()
        tree.set_checked([], ["Krankenpflege", "Hochbau", "Tiefbau"])

        self.assertEqual(
            tree.selection(),
            [
                {"key": "gesundheit", "fields": ["Krankenpflege"]},
                {"key": "bau", "fields": None},
            ],
        )

    def test_a_selection_survives_the_settings_round_trip(self):
        tree = self.tree()
        tree.set_checked([], ["Krankenpflege", "Pharmazie"])
        saved_groups, saved_fields = tree.checked_groups(), tree.checked_fields()

        restored = self.tree()
        restored.set_checked(saved_groups, saved_fields)

        self.assertEqual(restored.selection(), tree.selection())

    def test_settings_written_before_fields_existed_still_load(self):
        # Older settings files hold category keys and nothing else; those mean
        # the whole category, not an empty selection.
        tree = self.tree()
        tree.set_checked(["bau"], [])

        self.assertEqual(tree.selection(), [{"key": "bau", "fields": None}])

    def test_select_all_and_clear(self):
        tree = self.tree()
        tree.set_all(True)

        self.assertEqual(
            tree.selection(),
            [{"key": "gesundheit", "fields": None}, {"key": "bau", "fields": None}],
        )

        tree.set_all(False)
        self.assertEqual(tree.selection(), [])

    def test_the_count_says_categories_and_fields(self):
        tree = self.tree()
        tree.set_checked([], ["Krankenpflege"])

        self.assertEqual(tree.count_label.text(), "1 kategorija, 1 / 5 polja")


if __name__ == "__main__":
    unittest.main()
