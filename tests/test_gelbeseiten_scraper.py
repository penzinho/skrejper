import unittest
from unittest.mock import patch

from app.scrapers.gelbeseiten import (
    _extract_card_from_html,
    _extract_detail_fields_from_html,
    _extract_email_candidates,
    _extract_email_from_website,
    _merge_detail_fields,
)


class FakeResponse:
    def __init__(self, url, body, ok=True, headers=None):
        self.url = url
        self._body = body
        self.ok = ok
        self.headers = headers or {"content-type": "text/html; charset=utf-8"}

    def text(self):
        return self._body


class FakeRequestContext:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout=30000):
        self.calls.append(url)
        response = self.responses.get(url)
        if response is None:
            raise RuntimeError(f"unexpected url: {url}")
        return response


class GelbeseitenScraperTests(unittest.TestCase):
    def test_extract_card_from_html_uses_list_page_fields(self):
        card_html = """
        <article class="mod mod-Treffer">
          <a href="https://www.gelbeseiten.de/gsbiz/example-id"></a>
          <h2 class="mod-Treffer__name">Example GmbH</h2>
          <button
            class="contains-icon-chat"
            data-parameters="{&quot;inboxConfig&quot;:{&quot;organizationQuery&quot;:{&quot;generic&quot;:{&quot;street&quot;:&quot;Musterstr. 1&quot;,&quot;city&quot;:&quot;Berlin&quot;,&quot;phones&quot;:[&quot;030 123456&quot;],&quot;email&quot;:&quot;kontakt@example.com&quot;}}}}">
          </button>
          <address class="mod-AdresseKompakt">
            <div class="mod-AdresseKompakt__adress-text">
              Musterstr. 1,
              <span class="nobr mod-AdresseKompakt__adress__ort">10115 Berlin</span>
            </div>
          </address>
          <a class="mod-TelefonnummerKompakt__phoneNumber">030 123456</a>
          <span class="mod-WebseiteKompakt__text" data-webseiteLink="aHR0cHM6Ly9leGFtcGxlLmNvbQ==">Webseite</span>
        </article>
        """

        item = _extract_card_from_html(card_html)

        self.assertEqual(item["company"], "Example GmbH")
        self.assertEqual(item["address"], "Musterstr. 1")
        self.assertEqual(item["city"], "Berlin")
        self.assertEqual(item["phone"], "030 123456")
        self.assertEqual(item["email"], "kontakt@example.com")
        self.assertEqual(item["website"], "https://example.com")
        self.assertEqual(item["detail_url"], "https://www.gelbeseiten.de/gsbiz/example-id")

    def test_extract_email_candidates_filters_asset_like_matches(self):
        body = """
        <a href="mailto:Sales@Example.com?subject=Hello">Mail</a>
        <div>info@example.com</div>
        <img src="/assets/favicon@128x.png" />
        """

        emails = _extract_email_candidates(body)

        self.assertEqual(emails, ["sales@example.com", "info@example.com"])

    def test_extract_detail_fields_from_html_reads_mailto_and_same_as(self):
        body = """
        <div id="email_versenden" data-link="mailto:jobs@example.com?subject=Hello"></div>
        <script type="application/ld+json">{"sameAs":"https://example.com/contact"}</script>
        """

        details = _extract_detail_fields_from_html(body)

        self.assertEqual(details["email"], "jobs@example.com")
        self.assertEqual(details["website"], "https://example.com/contact")

    def test_extract_email_from_website_uses_contact_page_fallback(self):
        request_context = FakeRequestContext(
            {
                "https://example.com": FakeResponse(
                    "https://example.com",
                    '<html><body><a href="/kontakt">Kontakt</a></body></html>',
                ),
                "https://example.com/kontakt": FakeResponse(
                    "https://example.com/kontakt",
                    '<html><body><a href="mailto:office@example.com">office@example.com</a></body></html>',
                ),
                "https://example.com/kontakt/": FakeResponse("https://example.com/kontakt/", "<html></html>", ok=False),
                "https://example.com/contact": FakeResponse("https://example.com/contact", "<html></html>", ok=False),
                "https://example.com/contact/": FakeResponse("https://example.com/contact/", "<html></html>", ok=False),
                "https://example.com/contact-us": FakeResponse("https://example.com/contact-us", "<html></html>", ok=False),
                "https://example.com/impressum": FakeResponse("https://example.com/impressum", "<html></html>", ok=False),
                "https://example.com/impressum/": FakeResponse("https://example.com/impressum/", "<html></html>", ok=False),
            }
        )

        email = _extract_email_from_website(request_context, "https://example.com")

        self.assertEqual(email, "office@example.com")
        self.assertEqual(request_context.calls[:2], ["https://example.com", "https://example.com/kontakt"])

    @patch("app.scrapers.gelbeseiten._extract_email_from_website")
    def test_merge_detail_fields_uses_card_website_for_missing_email(self, extract_email_from_website_mock):
        extract_email_from_website_mock.return_value = "fallback@example.com"
        item = {
            "company": "Example GmbH",
            "website": "https://example.com",
            "location": "Berlin",
            "email": "",
        }

        merged = _merge_detail_fields(item, {"email": "", "website": "", "address": "", "city": "", "phone": ""}, object())

        self.assertEqual(merged["email"], "fallback@example.com")
        extract_email_from_website_mock.assert_called_once_with(unittest.mock.ANY, "https://example.com")

    def test_merge_detail_fields_skips_website_fetch_when_request_context_missing(self):
        item = {
            "company": "Example GmbH",
            "website": "https://example.com",
            "location": "Berlin",
            "email": "",
        }

        merged = _merge_detail_fields(item, {"email": "", "website": "", "address": "", "city": "", "phone": ""}, None)

        self.assertEqual(merged["email"], "")


if __name__ == "__main__":
    unittest.main()
