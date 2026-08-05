import unittest

from app.leadgen.normalize import (
    extract_email,
    norm_city,
    norm_company,
    norm_tax_id,
    norm_text,
    parse_date,
    translit,
)


class TranslitTest(unittest.TestCase):
    def test_cyrillic_to_latin(self):
        self.assertEqual(translit("Београд"), "Beograd")
        self.assertEqual(translit("Џудо клуб Љубљана"), "Džudo klub Ljubljana")
        self.assertEqual(translit("НАЦИОНАЛНА СЛУЖБА ЗА ЗАПОШЉАВАЊЕ"), "NACIONALNA SLUŽBA ZA ZAPOŠLJAVANJE")

    def test_latin_passthrough(self):
        self.assertEqual(translit("Sarajevo d.o.o."), "Sarajevo d.o.o.")


class NormTest(unittest.TestCase):
    def test_norm_text_mixes_scripts(self):
        self.assertEqual(norm_text("Никола Тесла"), norm_text("Nikola Tesla"))

    def test_norm_company_strips_legal_forms(self):
        self.assertEqual(norm_company("Drvo-Stil d.o.o. Sarajevo"), norm_company("DRVO STIL doo Sarajevo"))
        self.assertEqual(norm_company("Metalac a.d."), "metalac")
        self.assertEqual(norm_company("Prevoz Marković PR"), "prevoz markovic")

    def test_norm_company_keeps_real_words(self):
        # "ad" inside a word must survive the legal-form stripping.
        self.assertIn("adria", norm_company("Adria Media d.o.o."))

    def test_norm_city(self):
        self.assertEqual(norm_city("71000 Sarajevo"), "sarajevo")
        self.assertEqual(norm_city("Нови Сад"), "novi sad")

    def test_norm_tax_id(self):
        self.assertEqual(norm_tax_id("JIB: 4200000110000"), "4200000110000")
        self.assertEqual(norm_tax_id("123"), "")
        self.assertEqual(norm_tax_id(""), "")


class DateTest(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(parse_date("15.08.2026"), "2026-08-15")
        self.assertEqual(parse_date("15.08.2026."), "2026-08-15")
        self.assertEqual(parse_date("2026-08-15"), "2026-08-15")
        self.assertEqual(parse_date("Objavljen: 3.2.2025"), "2025-02-03")
        self.assertEqual(parse_date("15. 08. 2026"), "2026-08-15")

    def test_garbage(self):
        self.assertEqual(parse_date("uskoro"), "")
        self.assertEqual(parse_date(""), "")


class EmailTest(unittest.TestCase):
    def test_extracts_and_filters(self):
        self.assertEqual(extract_email("pišite na Posao@Firma.BA odmah"), "posao@firma.ba")
        self.assertEqual(extract_email("logo@2x.png"), "")


if __name__ == "__main__":
    unittest.main()
