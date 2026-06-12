import unittest
from unittest.mock import patch

from app.scrapers.omh import extract_contact_fields_from_html, fetch_hotel_posts


class OmhScraperTests(unittest.TestCase):
    def test_extract_contact_fields_from_detail_html(self):
        body = """
        <ul class="elementor-icon-list-items">
          <li class="elementor-icon-list-item">
            <span class="elementor-icon-list-text">Puntižela 40, Pula</span>
          </li>
          <li class="elementor-icon-list-item">
            <a href="tel:+385%2052%2033%2000%2060">
              <span class="elementor-icon-list-text">+385 52 33 00 60</span>
            </a>
          </li>
          <li class="elementor-icon-list-item">
            <a href="mailto:adrionaparthotel@gmail.com">
              <span class="elementor-icon-list-text">adrionaparthotel@gmail.com</span>
            </a>
          </li>
          <li class="elementor-icon-list-item">
            <a href="https://www.adrion-aparthotel.hr" target="_blank">
              <span class="elementor-icon-list-text">www.adrion-aparthotel.hr</span>
            </a>
          </li>
        </ul>
        """

        fields = extract_contact_fields_from_html(body)

        self.assertEqual(fields["address"], "Puntižela 40, Pula")
        self.assertEqual(fields["phone"], "+385 52 33 00 60")
        self.assertEqual(fields["email"], "adrionaparthotel@gmail.com")
        self.assertEqual(fields["website"], "https://www.adrion-aparthotel.hr")

    def test_extract_contact_fields_ignores_omh_footer_contact(self):
        body = """
        <ul class="elementor-icon-list-items">
          <li class="elementor-icon-list-item">
            <span class="elementor-icon-list-text">Obala hrvatskog narodnog preporoda 7/3</span>
          </li>
          <li class="elementor-icon-list-item">
            <a href="mailto:info@omh.hr">
              <span class="elementor-icon-list-text">info@omh.hr</span>
            </a>
          </li>
        </ul>
        """

        fields = extract_contact_fields_from_html(body)

        self.assertEqual(fields["email"], "")
        self.assertEqual(fields["website"], "")

    def test_extract_contact_fields_accepts_multiple_mailto_emails(self):
        body = """
        <ul class="elementor-icon-list-items">
          <li class="elementor-icon-list-item">
            <span class="elementor-icon-list-text">Bana Jelačića 8, Trilj</span>
          </li>
          <li class="elementor-icon-list-item">
            <a href="mailto:sime.klaric@gmail.com,sv.mihovil@inet.hr">
              <span class="elementor-icon-list-text">sime.klaric@gmail.com,sv.mihovil@inet.hr</span>
            </a>
          </li>
        </ul>
        """

        fields = extract_contact_fields_from_html(body)

        self.assertEqual(fields["email"], "sime.klaric@gmail.com, sv.mihovil@inet.hr")

    @patch("app.scrapers.omh._fetch_rest_pages")
    def test_fetch_hotel_posts_defaults_to_regular_hotels_with_region(self, fetch_rest_pages_mock):
        fetch_rest_pages_mock.return_value = [
            {"id": 1, "title": {"rendered": "Regular"}, "region": [1140]},
            {"id": 2, "title": {"rendered": "Associated"}, "region": []},
            {"id": 3, "title": {"rendered": ""}, "region": []},
        ]

        posts = fetch_hotel_posts()

        self.assertEqual([post["id"] for post in posts], [1])

    @patch("app.scrapers.omh._fetch_rest_pages")
    def test_fetch_hotel_posts_can_include_associated_hotels(self, fetch_rest_pages_mock):
        fetch_rest_pages_mock.return_value = [
            {"id": 1, "title": {"rendered": "Regular"}, "region": [1140]},
            {"id": 2, "title": {"rendered": "Associated"}, "region": []},
            {"id": 3, "title": {"rendered": ""}, "region": []},
        ]

        posts = fetch_hotel_posts(include_associated=True)

        self.assertEqual([post["id"] for post in posts], [1, 2])


if __name__ == "__main__":
    unittest.main()
