import tempfile
import unittest
from pathlib import Path

from app.leadgen.db import LeadDb
from app.leadgen.enrich import RegistryLookup, enrich
from app.leadgen.schema import make_employer


class _FakeHttp:
    """Stands in for app.leadgen.http.Http: serves canned pages by URL."""

    def __init__(self, pages):
        self.pages = pages
        self.requests_made = 0
        self.cache_hits = 0

    def get_or_none(self, url, **kwargs):
        self.requests_made += 1
        for needle, body in self.pages.items():
            if needle in url:
                return body
        return None


class _FakeRegistry(RegistryLookup):
    def by_name(self, name, city, country):
        if "eurotrans" in name.casefold():
            return {"tax_id": "108462825", "address": "Bulevar 1"}
        return {}


class EnrichTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.db = LeadDb(Path(self._dir.name) / "test.db")

    def tearDown(self):
        self.db.close()
        self._dir.cleanup()

    def test_klix_cross_fill(self):
        # Klix knows the JIB + website; the MojPosao record does not.
        self.db.upsert_employer(make_employer(
            source="klix", source_id="1", name="Drvo-Stil d.o.o.",
            tax_id="4200000110005", website="https://drvostil.ba", address="Ferhadija 12",
        ))
        self.db.upsert_employer(make_employer(
            source="mojposao_ba", source_id="uuid-1", name="DRVO STIL doo",
        ))
        stats = enrich(self.db, _FakeHttp({}), do_website_email=False)
        self.assertEqual(stats["klix_filled"], 1)
        target = self.db.get_employer("mojposao_ba", "uuid-1")
        self.assertEqual(target["tax_id"], "4200000110005")
        self.assertEqual(target["website"], "https://drvostil.ba")

    def test_website_to_email(self):
        self.db.upsert_employer(make_employer(
            source="poslovi_rs", source_id="firma", name="Firma doo",
            website="https://firma.rs",
        ))
        http = _FakeHttp({
            "firma.rs/kontakt": '<a href="mailto:info@firma.rs">piši nam</a>',
            "firma.rs": "<html>homepage bez kontakta</html>",
        })
        stats = enrich(self.db, http, do_klix=False)
        self.assertEqual(stats["emails_found"], 1)
        self.assertEqual(self.db.get_employer("poslovi_rs", "firma")["email"], "info@firma.rs")

    def test_does_not_overwrite_existing(self):
        self.db.upsert_employer(make_employer(
            source="poslovi_rs", source_id="x", name="X doo",
            website="https://x.rs", email="already@x.rs",
        ))
        http = _FakeHttp({"x.rs": '<a href="mailto:other@x.rs">x</a>'})
        stats = enrich(self.db, http, do_klix=False)
        self.assertEqual(stats["emails_found"], 0)
        self.assertEqual(self.db.get_employer("poslovi_rs", "x")["email"], "already@x.rs")

    def test_registry_lookup(self):
        self.db.upsert_employer(make_employer(
            source="nsz", source_id="eurotrans", name="Eurotrans doo", city="Beograd", country="RS",
        ))
        stats = enrich(self.db, _FakeHttp({}), do_klix=False, do_website_email=False,
                       registry=_FakeRegistry())
        self.assertEqual(stats["registry_filled"], 1)
        self.assertEqual(self.db.get_employer("nsz", "eurotrans")["tax_id"], "108462825")

    def test_generic_email_rejected(self):
        self.db.upsert_employer(make_employer(
            source="poslovi_rs", source_id="y", name="Y doo", website="https://y.rs",
        ))
        http = _FakeHttp({"y.rs": '<a href="mailto:noreply@y.rs">x</a>'})
        stats = enrich(self.db, http, do_klix=False)
        self.assertEqual(stats["emails_found"], 0)


if __name__ == "__main__":
    unittest.main()
