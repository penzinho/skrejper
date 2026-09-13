"""The Croatian names for the board's German taxonomy.

The point of these is upkeep: the board renames its Berufsfelder (that is what
broke a run once already), and a renamed field must not silently lose its
translation and turn back into a German riddle in the category list.
"""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers import arbeitsagentur as aa
from app.scrapers import arbeitsagentur_labels as labels


class TranslationCoverageTests(unittest.TestCase):
    def test_every_berufsfeld_has_a_croatian_name(self):
        missing = [name for name in aa.BERUFSFELDER if not labels.berufsfeld_hr(name)]

        self.assertEqual(missing, [], "add these to BERUFSFELD_HR")

    def test_no_translation_for_a_field_the_board_no_longer_has(self):
        # A leftover entry is a rename we missed, so it is worth failing on.
        stale = [name for name in labels.BERUFSFELD_HR if name not in aa.BERUFSFELDER]

        self.assertEqual(stale, [], "these are not in BERUFSFELDER any more")

    def test_every_group_has_a_croatian_name(self):
        missing = [
            item["key"] for item in aa.get_arbeitsagentur_categories() if not item["label_hr"]
        ]

        self.assertEqual(missing, [], "add these to GROUP_LABELS_HR")

    def test_categories_carry_the_translated_fields(self):
        health = next(
            item
            for item in aa.get_arbeitsagentur_categories()
            if item["key"] == "gesundheit_pflege"
        )

        self.assertEqual(health["label_hr"], "Zdravstvo, medicina i njega")
        self.assertEqual(
            [field["name"] for field in health["fields"]], health["berufsfelder"]
        )
        nursing = next(
            field
            for field in health["fields"]
            if field["name"] == "Krankenpflege, Rettungsdienst und Geburtshilfe"
        )
        self.assertEqual(nursing["hr"], "medicinske sestre, hitna pomoć i babice")

    def test_the_german_value_stays_visible_in_a_label(self):
        # The German half is what gets sent to the board, so a label may never
        # replace it — only add to it.
        label = labels.bilingual("Altenpflege", labels.berufsfeld_hr("Altenpflege"))

        self.assertIn("Altenpflege", label)
        self.assertIn("njega starijih osoba", label)

    def test_an_untranslated_value_is_shown_as_it_is(self):
        self.assertEqual(labels.bilingual("Neues Berufsfeld", ""), "Neues Berufsfeld")
        self.assertEqual(labels.beruf_hr("Etwas Unbekanntes"), "")


if __name__ == "__main__":
    unittest.main()
