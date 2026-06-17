"""Scraper for the German Federal Employment Agency job board
(https://www.arbeitsagentur.de/jobsuche/).

Unlike the HZZ and Meinestadt scrapers, this one does **not** drive a browser.
The public `jobsuche` frontend (the "infinite scroll" list) is powered by a
documented REST API, so we talk to that directly:

* search   -> `GET .../pc/v4/jobs?berufsfeld=...&page=..&size=..`
* detail   -> `GET .../pc/v3/jobdetails/{base64(refnr)}`

The search response carries the employer name + location for every listing; the
detail response carries the free-text job description, which is where employer
contact e-mails live (applications on this board are otherwise routed through
the platform). We harvest the e-mail out of that description, mirroring the
e-mail-first shaping the other scrapers produce.

The API requires a fixed public key header (`X-API-Key: jobboerse-jobsuche`) —
the same one the frontend ships — and no other authentication.

"category" here maps to the board's own *Berufsfeld* (occupational field) facet,
which is exactly what the website's category filter uses.
"""

import base64
import html
import json
import os
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable

API_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{API_BASE}/pc/v4/jobs"
DETAIL_URL = f"{API_BASE}/pc/v3/jobdetails/{{enc}}"
# Public web URL for a posting, built from its reference number.
PUBLIC_DETAIL_URL = "https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}"
API_KEY = "jobboerse-jobsuche"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

# The board caps page size at 100; larger values are silently clamped.
MAX_PAGE_SIZE = 100

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
INVALID_EMAIL_DOMAIN_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".css", ".js",
)
# Generic / platform addresses that are never an employer contact.
EXCLUDED_EMAIL_DOMAINS = {"arbeitsagentur.de", "arbeitsagentur.com"}
# Listing fields point at these for "external" postings; they are job boards,
# not employer websites, so we don't treat them as a contact.
EXCLUDED_WEBSITE_HOST_SUFFIXES = (
    "arbeitsagentur.de",
    "facebook.com",
    "linkedin.com",
    "xing.com",
    "x.com",
    "twitter.com",
    "instagram.com",
    "wa.me",
    "whatsapp.com",
)

# The board's Berufsfeld (occupational field) facet — the values its own
# category filter exposes. Kept in sync with the `arbeitsagentur_categories`
# enum in `.actor/input_schema.json`. A category is passed verbatim as the
# `berufsfeld` query parameter, so this list is also accepted as free text.
BERUFSFELDER = (
    'Altenpflege',
    'Angehörige der regulären Streitkräfte in sonstigen Rängen',
    'Angehörige gesetzgebender Körperschaften',
    'Arzt- und Praxishilfe',
    'Aus- und Trockenbau, Isolierung, Zimmerei, Glaserei',
    'Bau- und Transportgeräteführung',
    'Bauplanung und -überwachung, Architektur',
    'Berg-, Tagebau und Sprengtechnik',
    'Bestattungswesen',
    'Biologie',
    'Bodenverlegung',
    'Buch- und Kunstantiquitäten, Musikfachhandel',
    'Bühnen- und Kostümbildnerei, Requisite',
    'Büro und Sekretariat',
    'Chemie',
    'Drucktechnik, Buchbinderei',
    'Einkauf und Vertrieb',
    'Elektrotechnik',
    'Energietechnik',
    'Ernährungs- und Gesundheitsberatung',
    'Erziehung, Sozialarbeit, Heilerziehungspflege',
    'Fahr- und Sportunterricht an außerschulischen Bildungseinrichtungen',
    'Fahrzeug-, Luft-, Raumfahrt- und Schiffbautechnik',
    'Fahrzeugführung im Eisenbahnverkehr',
    'Fahrzeugführung im Flugverkehr',
    'Fahrzeugführung im Schiffsverkehr',
    'Fahrzeugführung im Straßenverkehr',
    'Farb- und Lacktechnik',
    'Feinwerk- und Werkzeugtechnik',
    'Fischwirtschaft',
    'Floristik',
    'Forstwirtschaft, Jagdwirtschaft, Landschaftspflege',
    'Fototechnik und Fotografie',
    'Gartenbau',
    'Gastronomie',
    'Gebäudetechnik',
    'Geisteswissenschaften',
    'Geologie, Geografie und Meteorologie',
    'Geschäftsführung und Vorstand',
    'Gesellschaftswissenschaften',
    'Getränkeherstellung',
    'Gewerbe, Gesundheitsaufsicht, Desinfektion',
    'Handel',
    'Hauswirtschaft und Verbraucherberatung',
    'Hochbau',
    'Holzbe- und -verarbeitung',
    'Hotellerie',
    'Human- und Zahnmedizin',
    'IT-Netzwerktechnik, -Administration, -Organisation',
    'IT-Systemanalyse, -Anwendungsberatung und -Vertrieb',
    'Immobilienwirtschaft und Facility-Management',
    'Industrielle Glasherstellung',
    'Industrielle Keramikherstellung',
    'Informatik',
    'Innenarchitektur, Raumausstattung',
    'Kaufleute - Verkehr und Logistik',
    'Klempnerei, Sanitär-, Heizungs- und Klimatechnik',
    'Krankenpflege, Rettungsdienst und Geburtshilfe',
    'Kunsthandwerk und bildende Kunst',
    'Kunsthandwerkliche Keramik- und Glasgestaltung',
    'Kunsthandwerkliche Metallgestaltung',
    'Kunststoff- und Kautschukherstellung',
    'Körperpflege',
    'Lagerwirtschaft, Post und Zustellung, Güterumschlag',
    'Landwirtschaft',
    'Lebensmittel- und Genussmittelherstellung',
    'Leder- und Pelzherstellung',
    'Lehr- und Forschungstätigkeit an Hochschulen',
    'Lehrtätigkeit an allgemeinbildenden Schulen',
    'Lehrtätigkeit an außerschulischen Bildungseinrichtungen',
    'Lehrtätigkeit berufsbildender Fächer und betriebliche Ausbildung',
    'Maler, Stuckateure, Bautenschutz',
    'Maschinenbau- und Betriebstechnik',
    'Mathematik und Statistik',
    'Mechatronik und Automatisierungstechnik',
    'Medien-, Dokumentations- und Informationsdienste',
    'Medizin-, Orthopädie- und Rehatechnik',
    'Medizinisches Laboratorium',
    'Metallbau und Schweißtechnik',
    'Metallbearbeitung',
    'Metallerzeugung',
    'Metalloberflächenbehandlung',
    'Moderation und Unterhaltung',
    'Museumstechnik und -management',
    'Musik-, Gesang-, Dirigententätigkeiten',
    'Musikinstrumentenbau',
    'Naturstein- und Mineralaufbereitung, Baustoffherstellung',
    'Nichtärztliche Therapie und Heilkunde',
    'Objekt-, Personen-, Brandschutz, Arbeitssicherheit',
    'Offiziere',
    'Papier- und Verpackungstechnik',
    'Personalwesen und -dienstleistung',
    'Pferdewirtschaft',
    'Pharmazie',
    'Physik',
    'Polizei- und Kriminaldienst, Gerichts- und Justizvollzug',
    'Produkt- und Industriedesign',
    'Psychologie, nichtärztliche Psychotherapie',
    'Rechnungswesen, Controlling und Revision',
    'Rechtsberatung, -sprechung und -ordnung',
    'Redaktion und Journalismus',
    'Reinigung',
    'Schauspiel, Tanz und Bewegungskunst',
    'Servicekräfte im Personenverkehr',
    'Softwareentwicklung und Programmierung',
    'Speisenzubereitung',
    'Sprach- und Literaturwissenschaften',
    'Steuerberatung',
    'Technische Forschung und Entwicklung',
    'Technische Mediengestaltung',
    'Technische Produktionsplanung und -steuerung',
    'Technischer Betrieb des Eisenbahn-, Luft- und Schiffsverkehrs',
    'Technisches Zeichnen, Konstruktion und Modellbau',
    'Textiltechnik und -produktion',
    'Textilverarbeitung',
    'Theater-, Film- und Fernsehproduktion',
    'Theologie und Gemeindearbeit',
    'Tiefbau',
    'Tiermedizin und Tierheilkunde',
    'Tierpflege',
    'Tierwirtschaft',
    'Tourismus und Sport',
    'Umweltmanagement und -beratung',
    'Umweltschutztechnik',
    'Unternehmensorganisation und -strategie',
    'Unteroffiziere mit Portepee',
    'Unteroffiziere ohne Portepee',
    'Ver- und Entsorgung',
    'Veranstaltungs-, Kamera-, Tontechnik',
    'Veranstaltungsservice und -management',
    'Verkauf (ohne Produktspezialisierung)',
    'Verkauf Bekleidung, Elektro, KFZ, Hartwaren',
    'Verkauf von Lebensmitteln',
    'Verkauf von drogerie- und apothekenüblichen Waren',
    'Verlags- und Medienwirtschaft',
    'Vermessung und Kartografie',
    'Versicherungs- und Finanzdienstleistungen',
    'Verwaltung',
    'Weinbau',
    'Werbung und Marketing',
    'Wirtschaftswissenschaften',
    'Öffentlichkeitsarbeit',
    'Überwachung und Steuerung des Verkehrsbetriebs',
    'Überwachung, Wartung Verkehrsinfrastruktur',
)

# Broad, user-facing categories. Each maps to a set of Berufsfelder that are
# queried (and deduped) together. This is what the input form exposes; the raw
# Berufsfeld names above are still accepted as free text. Every Berufsfeld
# belongs to exactly one group (verified by `_check_groups.py`).
ARBEITSAGENTUR_GROUPS: dict[str, dict] = {
    "bau_ausbau": {
        "label": "Bau, Ausbau & Gebäudetechnik",
        "berufsfelder": [
            "Hochbau",
            "Tiefbau",
            "Aus- und Trockenbau, Isolierung, Zimmerei, Glaserei",
            "Bodenverlegung",
            "Maler, Stuckateure, Bautenschutz",
            "Klempnerei, Sanitär-, Heizungs- und Klimatechnik",
            "Gebäudetechnik",
            "Bauplanung und -überwachung, Architektur",
            "Bau- und Transportgeräteführung",
            "Naturstein- und Mineralaufbereitung, Baustoffherstellung",
            "Berg-, Tagebau und Sprengtechnik",
            "Holzbe- und -verarbeitung",
        ],
    },
    "metall_maschinen_elektro": {
        "label": "Metall, Maschinen, Elektro & Fahrzeugtechnik",
        "berufsfelder": [
            "Metallbau und Schweißtechnik",
            "Metallbearbeitung",
            "Metallerzeugung",
            "Metalloberflächenbehandlung",
            "Maschinenbau- und Betriebstechnik",
            "Mechatronik und Automatisierungstechnik",
            "Feinwerk- und Werkzeugtechnik",
            "Elektrotechnik",
            "Energietechnik",
            "Fahrzeug-, Luft-, Raumfahrt- und Schiffbautechnik",
            "Technische Produktionsplanung und -steuerung",
            "Technisches Zeichnen, Konstruktion und Modellbau",
            "Technische Forschung und Entwicklung",
            "Kunsthandwerkliche Metallgestaltung",
        ],
    },
    "produktion_fertigung": {
        "label": "Produktion & Fertigung",
        "berufsfelder": [
            "Kunststoff- und Kautschukherstellung",
            "Lebensmittel- und Genussmittelherstellung",
            "Getränkeherstellung",
            "Chemie",
            "Papier- und Verpackungstechnik",
            "Drucktechnik, Buchbinderei",
            "Textiltechnik und -produktion",
            "Textilverarbeitung",
            "Leder- und Pelzherstellung",
            "Industrielle Glasherstellung",
            "Industrielle Keramikherstellung",
            "Musikinstrumentenbau",
            "Farb- und Lacktechnik",
        ],
    },
    "logistik_verkehr": {
        "label": "Logistik, Lager & Verkehr",
        "berufsfelder": [
            "Lagerwirtschaft, Post und Zustellung, Güterumschlag",
            "Kaufleute - Verkehr und Logistik",
            "Fahrzeugführung im Straßenverkehr",
            "Fahrzeugführung im Eisenbahnverkehr",
            "Fahrzeugführung im Schiffsverkehr",
            "Fahrzeugführung im Flugverkehr",
            "Überwachung und Steuerung des Verkehrsbetriebs",
            "Überwachung, Wartung Verkehrsinfrastruktur",
            "Technischer Betrieb des Eisenbahn-, Luft- und Schiffsverkehrs",
            "Servicekräfte im Personenverkehr",
        ],
    },
    "handel_verkauf": {
        "label": "Handel, Einkauf & Verkauf",
        "berufsfelder": [
            "Verkauf (ohne Produktspezialisierung)",
            "Verkauf Bekleidung, Elektro, KFZ, Hartwaren",
            "Verkauf von Lebensmitteln",
            "Verkauf von drogerie- und apothekenüblichen Waren",
            "Handel",
            "Einkauf und Vertrieb",
            "Buch- und Kunstantiquitäten, Musikfachhandel",
        ],
    },
    "gastronomie_tourismus": {
        "label": "Gastronomie, Hotellerie & Tourismus",
        "berufsfelder": [
            "Gastronomie",
            "Speisenzubereitung",
            "Hotellerie",
            "Tourismus und Sport",
        ],
    },
    "gesundheit_pflege": {
        "label": "Gesundheit, Medizin & Pflege",
        "berufsfelder": [
            "Altenpflege",
            "Arzt- und Praxishilfe",
            "Human- und Zahnmedizin",
            "Krankenpflege, Rettungsdienst und Geburtshilfe",
            "Nichtärztliche Therapie und Heilkunde",
            "Medizinisches Laboratorium",
            "Medizin-, Orthopädie- und Rehatechnik",
            "Pharmazie",
            "Psychologie, nichtärztliche Psychotherapie",
            "Ernährungs- und Gesundheitsberatung",
            "Tiermedizin und Tierheilkunde",
        ],
    },
    "bildung_soziales": {
        "label": "Bildung, Soziales & Erziehung",
        "berufsfelder": [
            "Erziehung, Sozialarbeit, Heilerziehungspflege",
            "Lehrtätigkeit an allgemeinbildenden Schulen",
            "Lehrtätigkeit an außerschulischen Bildungseinrichtungen",
            "Lehrtätigkeit berufsbildender Fächer und betriebliche Ausbildung",
            "Lehr- und Forschungstätigkeit an Hochschulen",
            "Fahr- und Sportunterricht an außerschulischen Bildungseinrichtungen",
            "Theologie und Gemeindearbeit",
        ],
    },
    "it": {
        "label": "IT & Softwareentwicklung",
        "berufsfelder": [
            "Informatik",
            "Softwareentwicklung und Programmierung",
            "IT-Netzwerktechnik, -Administration, -Organisation",
            "IT-Systemanalyse, -Anwendungsberatung und -Vertrieb",
        ],
    },
    "buero_finanzen_recht": {
        "label": "Büro, Finanzen, Recht & Management",
        "berufsfelder": [
            "Büro und Sekretariat",
            "Verwaltung",
            "Rechnungswesen, Controlling und Revision",
            "Steuerberatung",
            "Versicherungs- und Finanzdienstleistungen",
            "Rechtsberatung, -sprechung und -ordnung",
            "Unternehmensorganisation und -strategie",
            "Personalwesen und -dienstleistung",
            "Geschäftsführung und Vorstand",
            "Immobilienwirtschaft und Facility-Management",
        ],
    },
    "marketing_medien_kunst": {
        "label": "Marketing, Medien, Design & Kunst",
        "berufsfelder": [
            "Werbung und Marketing",
            "Öffentlichkeitsarbeit",
            "Redaktion und Journalismus",
            "Verlags- und Medienwirtschaft",
            "Technische Mediengestaltung",
            "Medien-, Dokumentations- und Informationsdienste",
            "Theater-, Film- und Fernsehproduktion",
            "Veranstaltungs-, Kamera-, Tontechnik",
            "Veranstaltungsservice und -management",
            "Produkt- und Industriedesign",
            "Innenarchitektur, Raumausstattung",
            "Fototechnik und Fotografie",
            "Kunsthandwerk und bildende Kunst",
            "Schauspiel, Tanz und Bewegungskunst",
            "Musik-, Gesang-, Dirigententätigkeiten",
            "Moderation und Unterhaltung",
            "Bühnen- und Kostümbildnerei, Requisite",
            "Kunsthandwerkliche Keramik- und Glasgestaltung",
            "Museumstechnik und -management",
        ],
    },
    "reinigung_sicherheit_versorgung": {
        "label": "Reinigung, Sicherheit, Ver- & Entsorgung",
        "berufsfelder": [
            "Reinigung",
            "Objekt-, Personen-, Brandschutz, Arbeitssicherheit",
            "Ver- und Entsorgung",
            "Umweltschutztechnik",
            "Gewerbe, Gesundheitsaufsicht, Desinfektion",
            "Hauswirtschaft und Verbraucherberatung",
            "Bestattungswesen",
            "Körperpflege",
        ],
    },
    "landwirtschaft_natur": {
        "label": "Landwirtschaft, Tiere & Natur",
        "berufsfelder": [
            "Gartenbau",
            "Landwirtschaft",
            "Forstwirtschaft, Jagdwirtschaft, Landschaftspflege",
            "Tierwirtschaft",
            "Tierpflege",
            "Pferdewirtschaft",
            "Fischwirtschaft",
            "Floristik",
            "Weinbau",
        ],
    },
    "wissenschaft_forschung": {
        "label": "Naturwissenschaften & Forschung",
        "berufsfelder": [
            "Biologie",
            "Physik",
            "Mathematik und Statistik",
            "Geologie, Geografie und Meteorologie",
            "Geisteswissenschaften",
            "Gesellschaftswissenschaften",
            "Sprach- und Literaturwissenschaften",
            "Wirtschaftswissenschaften",
            "Vermessung und Kartografie",
            "Umweltmanagement und -beratung",
        ],
    },
    "oeffentlicher_dienst_sicherheit": {
        "label": "Öffentlicher Dienst, Sicherheit & Militär",
        "berufsfelder": [
            "Polizei- und Kriminaldienst, Gerichts- und Justizvollzug",
            "Angehörige der regulären Streitkräfte in sonstigen Rängen",
            "Offiziere",
            "Unteroffiziere mit Portepee",
            "Unteroffiziere ohne Portepee",
            "Angehörige gesetzgebender Körperschaften",
        ],
    },
}


def _log(message: str) -> None:
    # flush so progress is visible in piped/captured logs (Apify, docker, tee)
    print(message, flush=True)


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.casefold()).strip("_")


def get_arbeitsagentur_categories() -> list[dict]:
    """The broad, user-facing categories (groups of Berufsfelder)."""
    return [
        {"key": key, "label": value["label"], "berufsfelder": list(value["berufsfelder"])}
        for key, value in ARBEITSAGENTUR_GROUPS.items()
    ]


def _resolve_berufsfelder(category: str | None) -> tuple[list[str], str]:
    """Resolve an input category into (list of Berufsfelder, display label).

    Accepts, in order of preference:
      * a broad group key or label (e.g. ``bau_ausbau``) -> all its Berufsfelder,
      * an exact Berufsfeld name or its ASCII slug      -> just that one,
      * anything else                                   -> passed through as a
        single Berufsfeld (the API returns nothing for a bad value), so new
        fields the board adds keep working without a code change.

    Returns ([], "") when no category is given (board-wide / keyword search).
    """
    if not category:
        return [], ""

    candidate = _clean_text(category)
    candidate_slug = _slugify(candidate)

    # Broad group, by key or by label.
    if candidate in ARBEITSAGENTUR_GROUPS:
        group = ARBEITSAGENTUR_GROUPS[candidate]
        return list(group["berufsfelder"]), group["label"]
    for key, group in ARBEITSAGENTUR_GROUPS.items():
        if candidate_slug in (key, _slugify(group["label"])):
            return list(group["berufsfelder"]), group["label"]

    # Single Berufsfeld, by exact name or slug.
    if candidate in BERUFSFELDER:
        return [candidate], candidate
    for name in BERUFSFELDER:
        if candidate_slug == _slugify(name):
            return [name], name

    _log(f"[arbeitsagentur] Unknown category '{category}', passing through as Berufsfeld.")
    return [candidate], candidate


def _is_valid_email_candidate(value: str) -> bool:
    if not value or value.count("@") != 1:
        return False
    local_part, domain = value.split("@", 1)
    if not local_part or "." not in domain:
        return False
    normalized_domain = domain.casefold()
    if normalized_domain.endswith(INVALID_EMAIL_DOMAIN_SUFFIXES):
        return False
    return normalized_domain not in EXCLUDED_EMAIL_DOMAINS


def _extract_email(*sources: str) -> str:
    for source in sources:
        if not source:
            continue
        for match in EMAIL_RE.findall(html.unescape(source)):
            candidate = match.strip(" <>\"'(),;:").casefold()
            if _is_valid_email_candidate(candidate):
                return candidate
    return ""


def _clean_website(url: str) -> str:
    url = _clean_text(url)
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = parsed.netloc.casefold()
    if any(host == suffix or host.endswith("." + suffix) for suffix in EXCLUDED_WEBSITE_HOST_SUFFIXES):
        return ""
    return url


def _http_get_json(url: str, timeout: int, attempts: int = 3) -> dict | None:
    """GET a JSON document with the board's public API key, retrying transient
    errors. Returns None on a 404 (posting expired) or after exhausting retries.
    """
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            headers={"X-API-Key": API_KEY, "User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None  # posting gone; skip quietly
            last_error = exc
            if exc.code in (400, 401, 403):
                _log(f"[arbeitsagentur] HTTP {exc.code} for {url}")
                return None
        except Exception as exc:  # network / timeout / decode
            last_error = exc
        if attempt < attempts:
            time.sleep(2 * attempt)
    _log(f"[arbeitsagentur] Giving up on {url}: {last_error}")
    return None


def _build_search_url(
    berufsfeld: str | None,
    keyword: str | None,
    location: str | None,
    radius: int | None,
    page: int,
    size: int,
) -> str:
    params: list[tuple[str, str]] = [("page", str(page)), ("size", str(size))]
    if berufsfeld:
        params.append(("berufsfeld", berufsfeld))
    if keyword:
        params.append(("was", keyword))
    if location:
        params.append(("wo", location))
        params.append(("umkreis", str(radius if radius is not None else 25)))
    return f"{SEARCH_URL}?{urllib.parse.urlencode(params)}"


def _enrich_listing(listing: dict, category_label: str, detail_timeout: int) -> dict | None:
    refnr = (listing.get("refnr") or "").strip()
    if not refnr:
        return None

    arbeitsort = listing.get("arbeitsort") or {}
    ort = (arbeitsort.get("ort") or "").strip()
    region = (arbeitsort.get("region") or "").strip()
    location = ort or region

    enc = base64.b64encode(refnr.encode("utf-8")).decode("ascii")
    detail = _http_get_json(DETAIL_URL.format(enc=enc), timeout=detail_timeout)

    description = (detail.get("stellenangebotsBeschreibung") if detail else "") or ""
    company = (
        (detail.get("firma") if detail else "")
        or listing.get("arbeitgeber")
        or ""
    ).strip()

    email = _extract_email(description)
    website = _clean_website(listing.get("externeUrl") or "")

    return {
        "title": (listing.get("titel") or listing.get("beruf") or "").strip(),
        "company": company,
        "location": location,
        "published_at": (listing.get("aktuelleVeroeffentlichungsdatum") or "").strip(),
        "detail_url": PUBLIC_DETAIL_URL.format(refnr=urllib.parse.quote(refnr)),
        "category": category_label,
        "email": email,
        "employer_website": website,
        "refnr": refnr,
        "source": "arbeitsagentur",
    }


def scrape_arbeitsagentur(
    category: str | None = None,
    max_pages: int = 1,
    company_limit: int | None = None,
    results_per_page: int = MAX_PAGE_SIZE,
    keyword: str | None = None,
    location: str | None = None,
    radius: int | None = None,
    listing_limit: int | None = None,
    on_job: Callable[[dict], None] | None = None,
    skip_ids: set[str] | None = None,
) -> list[dict]:
    """Scrape postings from the arbeitsagentur.de job board via its public API.

    * `category`   -> a broad group key (see `ARBEITSAGENTUR_GROUPS`) or a single
      Berufsfeld; a group is scraped by querying each of its Berufsfelder in turn.
    * `keyword`    -> free-text search (`was`), e.g. a job title.
    * `location`   -> place/PLZ (`wo`); `radius` is the km search radius.
    * `max_pages`  -> result pages (of `results_per_page`) to walk *per Berufsfeld*.
    * `company_limit`  -> stop after this many distinct employers (across the group).
    * `listing_limit`  -> stop after processing this many listings (across the group);
      bounds the number of detail fetches, useful for large groups / smoke tests.
    * `skip_ids`   -> posting ``refnr``s already scraped on a previous run; these
      are skipped *before* the detail fetch, so an incremental daily run only pays
      for new postings. See `app.seen_store`.

    Returns one dict per posting (e-mail may be empty; the caller filters).
    """
    skip_ids = skip_ids or set()
    page_size = max(1, min(int(results_per_page or MAX_PAGE_SIZE), MAX_PAGE_SIZE))
    search_timeout = int(os.getenv("ARBEITSAGENTUR_SEARCH_TIMEOUT", "60"))
    detail_timeout = int(os.getenv("ARBEITSAGENTUR_DETAIL_TIMEOUT", "30"))
    detail_delay_ms = int(os.getenv("ARBEITSAGENTUR_DETAIL_DELAY_MS", "300"))
    debug_progress = os.getenv("ARBEITSAGENTUR_DEBUG_PROGRESS", "false") == "true"

    berufsfelder, category_label = _resolve_berufsfelder(category)
    # A keyword/board-wide sweep has no Berufsfeld filter; represent it as one
    # query with berufsfeld=None.
    queries: list[str | None] = berufsfelder or [None]

    jobs: list[dict] = []
    seen_refnrs: set[str] = set()
    seen_company_keys: set[str] = set()
    processed = 0  # listings whose detail we fetched (== detail requests)

    for berufsfeld in queries:
        total_results: int | None = None

        for page in range(1, max_pages + 1):
            url = _build_search_url(berufsfeld, keyword, location, radius, page, page_size)
            payload = _http_get_json(url, timeout=search_timeout)
            if payload is None:
                _log(f"[arbeitsagentur] No response for {berufsfeld!r} page {page}; skipping rest.")
                break

            if total_results is None:
                total_results = payload.get("maxErgebnisse")
            listings = payload.get("stellenangebote") or []

            if debug_progress:
                _log(
                    f"[arbeitsagentur] berufsfeld={berufsfeld!r} page {page} "
                    f"listings={len(listings)} max={total_results} "
                    f"processed={processed} jobs={len(jobs)}"
                )

            if not listings:
                break

            for listing in listings:
                refnr = (listing.get("refnr") or "").strip()
                if not refnr or refnr in seen_refnrs:
                    continue
                seen_refnrs.add(refnr)
                # Already scraped on a previous run -> skip the detail fetch.
                if refnr in skip_ids:
                    continue

                job = _enrich_listing(listing, category_label, detail_timeout)
                processed += 1
                if job is None:
                    continue

                if company_limit is not None:
                    company_key = _slugify(job.get("company", "")) or refnr
                    if company_key in seen_company_keys:
                        continue
                    seen_company_keys.add(company_key)

                jobs.append(job)
                if on_job is not None:
                    try:
                        on_job(job)
                    except Exception as exc:
                        _log(f"[arbeitsagentur] on_job callback failed: {exc}")

                if company_limit is not None and len(seen_company_keys) >= company_limit:
                    _log(f"[arbeitsagentur] Reached company_limit={company_limit}.")
                    return jobs
                if listing_limit is not None and processed >= listing_limit:
                    _log(f"[arbeitsagentur] Reached listing_limit={listing_limit}.")
                    return jobs

                if detail_delay_ms:
                    time.sleep(detail_delay_ms / 1000)

            # Stop once we've walked past the last result page of this Berufsfeld.
            if total_results is not None and page * page_size >= total_results:
                break

    return jobs
