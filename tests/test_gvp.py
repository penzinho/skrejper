"""The GVP member directory scraper — parsing, paging and the website lookup.

No network: the REST route and the company websites are both stubbed.
"""

import json
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers import gvp


def entry(title, *, business="", director="", phone="", email="", website="", street="", plz="", city="", country="Deutschland"):
    """One accordion entry, in the exact markup the theme renders."""

    def element(cls, label, value):
        if not value:
            return ""
        return (
            f'<div class="memberlist__element {cls}">\n'
            f'        <span class="label">{label}:</span>\n'
            f'        <span class="entry">\n                        {value}        </span>\n'
            f"    </div>\n"
        )

    website_html = (
        f'<a class="member-link" href="{website}" target="_blank" rel="noopener">'
        f'{website.split("//", 1)[-1].rstrip("/")}</a>'
        if website
        else ""
    )
    business_html = f"<h4>\n          Geschäftsfeld: <span>{business}</span>\n        </h4>" if business else ""
    return (
        '<div class="accordion__entry memberlist__entry" data-state="closed">\n'
        '  <div class="accordion__head memberlist__head">\n'
        '          <div class="accordion__title memberlist__title">\n'
        f"        <h4>\n          {title}        </h4>\n      </div>\n"
        '        <svg viewBox="0 0 27 27" class="accordion__indicator"><path d="M0 0L27 13.5L0 27L0 0Z" /></svg>\n'
        "  </div>\n"
        '  <div class="accordion__body memberlist__body">\n'
        '    <div class="accordion__content">\n'
        f"{business_html}"
        '      <div class="accordion-column-container">\n'
        '        <div class="accordion-column">\n'
        + element("geschaeftsfuehrer", "Geschäftsführer", director)
        + element("telefon", "Telefon", phone)
        + element("email", "E-Mail", email)
        + element("website", "Website", website_html)
        + "        </div>\n"
        '        <div class="accordion-column">\n'
        + element("strasse", "Straße", street)
        + element("plz", "PLZ", plz)
        + element("stadt", "Stadt", city)
        + element("land", "Land", country)
        + "        </div>\n      </div>\n    </div>\n  </div>\n</div>\n"
    )


HQ = entry(
    "Muster Personal GmbH - Kleve (47533)",
    business="Zeitarbeit,  Personalvermittlung",
    director="Erika Muster",
    phone="02821-719340",
    email="info@muster-personal.de",
    website="http://www.muster-personal.de",
    street="Lohengrinstr. 3",
    plz="47533",
    city="Kleve",
)
BRANCH = entry(
    "Muster Personal GmbH - Erfurt (99096)",
    phone="0361 123",
    website="http://www.muster-personal.de",
    street="Am Hopfenberg 2",
    plz="99096",
    city="Erfurt",
)
DASHED = entry(
    "A - B Personal GmbH &amp; Co. KG - Zahna-Elster (06895)",
    business="Zeitarbeit",
    director="Björn Beispiel",
    phone="+4935383989070",
    website="https://ab-personal.de/",
    street="Meltendorfer Straße 10",
    plz="06895",
    city="Zahna-Elster",
)


class ParseTests(unittest.TestCase):
    def test_reads_every_field_of_an_entry(self):
        (member,) = gvp.parse_members(HQ)
        self.assertEqual(member["member"], "Muster Personal GmbH - Kleve (47533)")
        self.assertEqual(member["company"], "Muster Personal GmbH")
        self.assertEqual(member["business_fields"], "Zeitarbeit, Personalvermittlung")
        self.assertEqual(member["managing_director"], "Erika Muster")
        self.assertEqual(member["phone"], "02821-719340")
        self.assertEqual(member["email"], "info@muster-personal.de")
        self.assertEqual(member["email_source"], "gvp")
        self.assertEqual(member["website"], "http://www.muster-personal.de")
        self.assertEqual(member["street"], "Lohengrinstr. 3")
        self.assertEqual((member["plz"], member["city"], member["country"]), ("47533", "Kleve", "Deutschland"))
        self.assertEqual(member["source"], "gvp")

    def test_a_branch_has_empty_fields_not_missing_keys(self):
        (member,) = gvp.parse_members(BRANCH)
        self.assertEqual(member["email"], "")
        self.assertEqual(member["email_source"], "")
        self.assertEqual(member["business_fields"], "")
        self.assertEqual(member["managing_director"], "")
        self.assertEqual(member["company"], "Muster Personal GmbH")

    def test_company_names_with_a_dash_stay_whole(self):
        # The title is "Firma - Stadt (PLZ)"; the city and PLZ fields are what
        # lets a " - " inside the company name survive.
        (member,) = gvp.parse_members(DASHED)
        self.assertEqual(member["company"], "A - B Personal GmbH & Co. KG")
        self.assertEqual(member["city"], "Zahna-Elster")

    def test_parses_a_full_fragment_in_order(self):
        members = gvp.parse_members(HQ + BRANCH + DASHED)
        self.assertEqual([m["city"] for m in members], ["Kleve", "Erfurt", "Zahna-Elster"])

    def test_website_without_link_gets_a_scheme(self):
        block = HQ.replace(
            '<a class="member-link" href="http://www.muster-personal.de" target="_blank" rel="noopener">www.muster-personal.de</a>',
            "muster-personal.de",
        )
        (member,) = gvp.parse_members(block)
        self.assertEqual(member["website"], "https://muster-personal.de")

    def test_member_key_is_case_and_accent_insensitive(self):
        self.assertEqual(
            gvp.normalize_member_key("Müller  Personal GmbH - Köln (50667)"),
            "muller personal gmbh - koln (50667)",
        )


class EmailExtractionTests(unittest.TestCase):
    def test_mailto_beats_text_and_own_domain_beats_foreign(self):
        page = (
            '<a href="mailto:Bewerbung@Firma.de">Bewerben</a> '
            "Presse: presse@agentur-extern.de, allgemein info@firma.de"
        )
        self.assertEqual(
            gvp.extract_emails(page, "https://www.firma.de"),
            ["info@firma.de", "bewerbung@firma.de", "presse@agentur-extern.de"],
        )

    def test_reads_obfuscated_impressum_addresses(self):
        page = "E-Mail: info (at) firma (dot) de &middot; kontakt [at] firma.de &middot; post&#64;firma.de"
        self.assertEqual(
            gvp.extract_emails(page, "https://firma.de"),
            ["info@firma.de", "kontakt@firma.de", "post@firma.de"],
        )

    def test_decodes_cloudflare_email_protection(self):
        # "info@firma.de" XOR-ed with key 0x5a, the way Cloudflare rewrites it.
        encoded = "5a" + bytes(b ^ 0x5A for b in b"info@firma.de").hex()
        page = (
            f'<a href="/cdn-cgi/l/email-protection#{encoded}">'
            f'<span class="__cf_email__" data-cfemail="{encoded}">[email&#160;protected]</span></a>'
        )
        self.assertEqual(gvp.extract_emails(page, "https://firma.de"), ["info@firma.de"])

    def test_ignores_assets_placeholders_and_noreply(self):
        page = "logo@2x.png icon@3x.svg user@example.com noreply@firma.de me@sentry.io"
        self.assertEqual(gvp.extract_emails(page, "https://firma.de"), [])

    def test_directory_field_uses_the_same_rules(self):
        block = HQ.replace("info@muster-personal.de", "info (at) muster-personal.de")
        self.assertEqual(gvp.parse_members(block)[0]["email"], "info@muster-personal.de")


class WebsiteLookupTests(unittest.TestCase):
    def lookup(self, pages: dict, url="https://firma.de", max_pages=4):
        calls = []

        def fetch(page_url, _timeout):
            calls.append(page_url)
            if page_url not in pages:
                raise urllib.error.URLError("no such page")
            return pages[page_url]

        return gvp.find_email_on_website(url, max_pages=max_pages, fetch=fetch), calls

    def test_homepage_footer_is_enough(self):
        email, calls = self.lookup({"https://firma.de": "<footer>info@firma.de</footer>"})
        self.assertEqual(email, "info@firma.de")
        self.assertEqual(calls, ["https://firma.de"])

    def test_follows_the_impressum_link_first(self):
        email, calls = self.lookup(
            {
                "https://firma.de": '<a href="/ueber-uns">Über uns</a> <a href="/rechtliches/impressum.html">Impressum</a>',
                "https://firma.de/rechtliches/impressum.html": "Vertreten durch ... E-Mail: kontakt@firma.de",
            }
        )
        self.assertEqual(email, "kontakt@firma.de")
        self.assertEqual(calls, ["https://firma.de", "https://firma.de/rechtliches/impressum.html"])

    def test_tries_the_usual_paths_when_nothing_is_linked(self):
        email, calls = self.lookup(
            {"https://firma.de": "<p>Willkommen</p>", "https://firma.de/impressum": "post@firma.de"},
        )
        self.assertEqual(email, "post@firma.de")
        self.assertEqual(calls, ["https://firma.de", "https://firma.de/impressum"])

    def test_www_variant_when_the_bare_domain_is_dead(self):
        email, calls = self.lookup({"https://www.firma.de": "info@firma.de"})
        self.assertEqual(email, "info@firma.de")
        self.assertEqual(calls, ["https://firma.de", "https://www.firma.de"])

    def test_stays_within_the_request_budget(self):
        email, calls = self.lookup({"https://firma.de": "<p>nichts</p>"}, max_pages=2)
        self.assertEqual(email, "")
        self.assertEqual(len(calls), 2)

    def test_a_dead_site_is_an_empty_answer_not_an_error(self):
        email, calls = self.lookup({})
        self.assertEqual(email, "")

    def test_no_website_means_no_requests(self):
        email, calls = self.lookup({}, url="")
        self.assertEqual((email, calls), ("", []))


class PagingTests(unittest.TestCase):
    """The REST route walk, with the route stubbed out."""

    def setUp(self):
        patcher = mock.patch.object(gvp, "_fetch_nonce", return_value="abc123")
        patcher.start()
        self.addCleanup(patcher.stop)
        # No sleeping between pages in tests.
        self.env = mock.patch.dict("os.environ", {"GVP_PAGE_DELAY_MS": "0", "GVP_SITE_DELAY_MS": "0"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def route(self, pages: list[str], total: int | None = None):
        """A fake filter route: page N -> pages[N-1], with the site's counts."""
        calls = []
        total = len(pages) * 10 if total is None else total

        def fake(page, filters, nonce, timeout, attempts=3):
            calls.append((page, dict(filters), nonce))
            fragment = pages[page - 1] if page - 1 < len(pages) else ""
            return {"data": fragment, "current_count": fragment.count("memberlist__entry"), "total_count": total}

        return fake, calls

    def page_of(self, start: int, count: int = 10) -> str:
        return "".join(
            entry(f"Firma {index:04d} GmbH - Stadt ({index:05d})", plz=f"{index:05d}", city="Stadt")
            for index in range(start, start + count)
        )

    def test_walks_pages_until_the_total_is_reached(self):
        pages = [self.page_of(1), self.page_of(11), self.page_of(21, 5)]
        fake, calls = self.route(pages, total=25)
        with mock.patch.object(gvp, "_filter_page", fake):
            members = gvp.scrape_gvp()

        self.assertEqual(len(members), 25)
        self.assertEqual([c[0] for c in calls], [1, 2, 3])
        self.assertEqual(calls[0][2], "abc123")

    def test_a_short_page_is_the_last_one(self):
        # total_count unreliable (too high): the short page still ends the walk.
        fake, calls = self.route([self.page_of(1), self.page_of(11, 3)], total=999)
        with mock.patch.object(gvp, "_filter_page", fake):
            self.assertEqual(len(gvp.scrape_gvp()), 13)
        self.assertEqual([c[0] for c in calls], [1, 2])

    def test_stops_when_the_site_keeps_serving_the_same_page(self):
        # The route ignores `page` in a query string and returns page 1 forever;
        # this must not turn into 900 identical requests.
        first = self.page_of(1)
        fake, calls = self.route([first, first, first, first], total=8763)
        with mock.patch.object(gvp, "_filter_page", fake):
            members = gvp.scrape_gvp()
        self.assertEqual(len(members), 10)
        self.assertEqual([c[0] for c in calls], [1, 2])

    def test_filters_and_limits_reach_the_route(self):
        fake, calls = self.route([self.page_of(1)])
        with mock.patch.object(gvp, "_filter_page", fake):
            members = gvp.scrape_gvp(
                search="Muster", city="Kleve", zip_code="47533",
                business_area="Zeitarbeit", quality="QS Pflege", branches=True, member_limit=3,
            )
        self.assertEqual(len(members), 3)
        self.assertEqual(
            calls[0][1],
            {"search": "Muster", "city": "Kleve", "zip": "47533", "area": "Zeitarbeit",
             "quality": "QS Pflege", "branches": True},
        )

    def test_max_pages_caps_the_walk(self):
        fake, calls = self.route([self.page_of(1), self.page_of(11), self.page_of(21)])
        with mock.patch.object(gvp, "_filter_page", fake):
            self.assertEqual(len(gvp.scrape_gvp(max_pages=2)), 20)
        self.assertEqual([c[0] for c in calls], [1, 2])

    def test_skip_ids_are_not_returned_but_still_count_towards_paging(self):
        fake, calls = self.route([self.page_of(1), self.page_of(11, 2)], total=12)
        known = {gvp.normalize_member_key(f"Firma {i:04d} GmbH - Stadt ({i:05d})") for i in range(1, 11)}
        streamed = []
        with mock.patch.object(gvp, "_filter_page", fake):
            members = gvp.scrape_gvp(skip_ids=known, on_member=streamed.append)
        self.assertEqual([m["plz"] for m in members], ["00011", "00012"])
        self.assertEqual(len(streamed), 2)
        self.assertEqual([c[0] for c in calls], [1, 2])

    def test_enrichment_fills_the_gaps_and_marks_the_failures(self):
        fragment = HQ + BRANCH + DASHED
        fake, _calls = self.route([fragment], total=3)
        looked_up = []

        def lookup(site, timeout, max_pages):
            looked_up.append((site, max_pages))
            return "kontakt@ab-personal.de" if "ab-personal" in site else ""

        with mock.patch.object(gvp, "_filter_page", fake), mock.patch.object(
            gvp, "find_email_on_website", lookup
        ):
            members = gvp.scrape_gvp(enrich=True, enrich_pages=3)

        by_city = {m["city"]: m for m in members}
        # Listed in the directory: untouched, and not looked up.
        self.assertEqual(by_city["Kleve"]["email_source"], "gvp")
        # Found on the website.
        self.assertEqual(by_city["Zahna-Elster"]["email"], "kontakt@ab-personal.de")
        self.assertEqual(by_city["Zahna-Elster"]["email_source"], "website")
        self.assertNotIn("enrich_tried", by_city["Zahna-Elster"])
        # Looked up in vain: flagged so the next run can skip it.
        self.assertEqual(by_city["Erfurt"]["email"], "")
        self.assertTrue(by_city["Erfurt"]["enrich_tried"])
        self.assertEqual(looked_up, [("http://www.muster-personal.de", 3), ("https://ab-personal.de/", 3)])

    def test_enrichment_skips_websites_already_searched(self):
        fake, _calls = self.route([BRANCH], total=1)
        key = gvp.normalize_member_key("Muster Personal GmbH - Erfurt (99096)")
        with mock.patch.object(gvp, "_filter_page", fake), mock.patch.object(
            gvp, "find_email_on_website", side_effect=AssertionError("must not be called")
        ):
            (member,) = gvp.scrape_gvp(enrich=True, skip_enrich_ids={key})
        self.assertEqual(member["email"], "")
        self.assertNotIn("enrich_tried", member)

    def test_no_response_ends_the_walk_with_what_was_collected(self):
        fake, _calls = self.route([self.page_of(1)], total=50)
        with mock.patch.object(gvp, "_filter_page", lambda page, *a, **k: fake(page, *a, **k) if page == 1 else None):
            self.assertEqual(len(gvp.scrape_gvp()), 10)


class RouteTests(unittest.TestCase):
    """What actually goes over the wire to the filter route."""

    def test_posts_the_form_the_page_javascript_posts(self):
        captured = {}

        def fetch(url, timeout, data=None, headers=None):
            captured.update(url=url, data=data, headers=headers)
            return json.dumps({"data": "", "current_count": 0, "total_count": 0})

        with mock.patch.object(gvp, "_fetch_text", fetch):
            payload = gvp._filter_page(3, {"search": "Muster", "branches": True}, "n0nce", 10)

        self.assertEqual(payload["total_count"], 0)
        self.assertEqual(captured["url"], gvp.FILTER_URL)
        form = dict(pair.split("=") for pair in captured["data"].decode().split("&"))
        self.assertEqual(form["page"], "3")
        self.assertEqual(form["search"], "Muster")
        self.assertEqual(form["listtype"], "mitglieder")
        self.assertEqual(form["branchswitch"], "true")
        self.assertEqual(captured["headers"]["X-WP-Nonce"], "n0nce")
        self.assertEqual(captured["headers"]["X-Requested-With"], "XMLHttpRequest")

    def test_survives_a_double_encoded_body(self):
        body = json.dumps(json.dumps({"data": "<p>x</p>", "current_count": 1, "total_count": 1}))
        with mock.patch.object(gvp, "_fetch_text", return_value=body):
            self.assertEqual(gvp._filter_page(1, {}, "", 10)["current_count"], 1)

    def test_a_firewall_block_is_a_clear_error_not_a_retry_loop(self):
        blocked = urllib.error.HTTPError(
            gvp.FILTER_URL, 403, "Forbidden", {}, None
        )
        blocked.read = lambda: b"<title>Sucuri WebSite Firewall - Access Denied</title>"
        with mock.patch.object(gvp, "_fetch_text", side_effect=blocked), mock.patch.object(gvp.time, "sleep"):
            with self.assertRaises(gvp.GvpBlockedError) as caught:
                gvp._filter_page(1, {}, "", 10)
        self.assertIn("Sucuri", str(caught.exception))

    def test_transient_errors_are_retried_then_given_up_quietly(self):
        with mock.patch.object(gvp, "_fetch_text", side_effect=OSError("reset")), mock.patch.object(
            gvp.time, "sleep"
        ) as sleep:
            self.assertIsNone(gvp._filter_page(1, {}, "", 10))
        self.assertEqual(sleep.call_count, 2)

    def test_nonce_is_read_from_the_page_and_optional(self):
        with mock.patch.object(gvp, "_fetch_text", return_value="const gvp_config = { rest_url: 'x', nonce: '2f8e5e3eb8' }"):
            self.assertEqual(gvp._fetch_nonce(10), "2f8e5e3eb8")
        with mock.patch.object(gvp, "_fetch_text", side_effect=OSError("blocked")):
            self.assertEqual(gvp._fetch_nonce(10), "")


if __name__ == "__main__":
    unittest.main()
