import unittest

from app.leadgen.public_sector import public_sector_term


class PublicSectorTest(unittest.TestCase):
    def test_flags_public_sector(self):
        for name in (
            "JU Osnovna škola Vuk Karadžić",
            "Dom zdravlja Banja Luka",
            "ЈЗУ Дом здравља Бијељина",
            "Opštinska uprava Prijedor",
            "Univerzitet u Sarajevu",
            "JKP Vodovod i kanalizacija",
            "Ministarstvo unutrašnjih poslova",
            "Centar za socijalni rad Tuzla",
        ):
            self.assertTrue(public_sector_term(name), name)

    def test_private_firms_pass(self):
        for name in (
            "Drvo-Stil d.o.o.",
            "Jupiter transport d.o.o.",   # "ju" inside a word must not match
            "Gradnja-Mont s.p.",           # "grad" inside a word must not match
            "Adriatic Marinas d.o.o.",
            "Školjka restoran",            # "skola" only as a whole word
        ):
            self.assertEqual(public_sector_term(name), "", name)


if __name__ == "__main__":
    unittest.main()
