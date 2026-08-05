"""Scraper for the German Federal Employment Agency job board
(https://www.arbeitsagentur.de/jobsuche/).

Unlike the HZZ and Meinestadt scrapers, this one does **not** drive a browser.
The public `jobsuche` frontend (the "infinite scroll" list) is powered by a
documented REST API, so we talk to that directly:

* search   -> `GET .../pc/v6/jobs?berufsfeld=...&page=..&size=..`
* detail   -> `GET .../pc/v4/jobdetails/{base64(refnr)}`

The API renames things between versions: v6 keeps `maxErgebnisse` but moved the
result list from `stellenangebote` to `ergebnisliste` and renamed the listing
fields (`refnr` -> `referenznummer`, `titel` -> `stellenangebotsTitel`,
`arbeitgeber` -> `firma`, `arbeitsort` -> `stellenlokationen[].adresse`). Both
shapes are read, so whichever version answers, the export is not empty.

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

# The board versions its paths and retires old ones without notice: /pc/v4/jobs
# answered for a long time and now 404s. Both documented search paths are tried
# in turn and the one that answers is remembered for the rest of the run, so a
# future retirement costs a redundant request rather than a silent empty export.
SEARCH_PATHS = (
    "/pc/v6/jobs",
    "/pc/v4/app/jobs",
    "/pc/v4/jobs",
)
DETAIL_PATHS = (
    "/pc/v4/jobdetails/{enc}",
    "/pc/v3/jobdetails/{enc}",
)
SEARCH_URL = f"{API_BASE}{SEARCH_PATHS[0]}"
DETAIL_URL = f"{API_BASE}{DETAIL_PATHS[0]}"

# Resolved on first use, then reused.
_search_url: str | None = None
_detail_url: str | None = None
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

# Croatian display names for the German Berufsfeld values above — GUI text
# only. Searches always send the German name; every Berufsfeld must have a
# translation (verified by tests).
BERUFSFELD_HR = {
    'Altenpflege': 'Njega starijih osoba',
    'Angehörige der regulären Streitkräfte in sonstigen Rängen': 'Vojno osoblje ostalih činova',
    'Angehörige gesetzgebender Körperschaften': 'Članovi zakonodavnih tijela',
    'Arzt- und Praxishilfe': 'Pomoćno osoblje u ordinacijama',
    'Aus- und Trockenbau, Isolierung, Zimmerei, Glaserei': 'Suha gradnja, izolacija, tesarstvo, staklarstvo',
    'Bau- und Transportgeräteführung': 'Rukovanje građevinskim i transportnim strojevima',
    'Bauplanung und -überwachung, Architektur': 'Projektiranje i nadzor gradnje, arhitektura',
    'Berg-, Tagebau und Sprengtechnik': 'Rudarstvo, površinski kopovi i miniranje',
    'Bestattungswesen': 'Pogrebna djelatnost',
    'Biologie': 'Biologija',
    'Bodenverlegung': 'Polaganje podova',
    'Buch- und Kunstantiquitäten, Musikfachhandel': 'Antikvarijati i prodaja glazbene opreme',
    'Bühnen- und Kostümbildnerei, Requisite': 'Scenografija, kostimografija, rekviziti',
    'Büro und Sekretariat': 'Ured i tajništvo',
    'Chemie': 'Kemija',
    'Drucktechnik, Buchbinderei': 'Tiskarstvo i knjigoveštvo',
    'Einkauf und Vertrieb': 'Nabava i prodaja',
    'Elektrotechnik': 'Elektrotehnika',
    'Energietechnik': 'Energetika',
    'Ernährungs- und Gesundheitsberatung': 'Savjetovanje o prehrani i zdravlju',
    'Erziehung, Sozialarbeit, Heilerziehungspflege': 'Odgoj, socijalni rad, njega osoba s invaliditetom',
    'Fahr- und Sportunterricht an außerschulischen Bildungseinrichtungen': 'Autoškole i sportska poduka',
    'Fahrzeug-, Luft-, Raumfahrt- und Schiffbautechnik': 'Tehnika vozila, zrakoplova i brodogradnja',
    'Fahrzeugführung im Eisenbahnverkehr': 'Upravljanje vlakovima',
    'Fahrzeugführung im Flugverkehr': 'Piloti i upravljanje zrakoplovima',
    'Fahrzeugführung im Schiffsverkehr': 'Upravljanje plovilima',
    'Fahrzeugführung im Straßenverkehr': 'Vozači u cestovnom prometu',
    'Farb- und Lacktechnik': 'Bojenje i lakiranje',
    'Feinwerk- und Werkzeugtechnik': 'Precizna mehanika i alatničarstvo',
    'Fischwirtschaft': 'Ribarstvo',
    'Floristik': 'Cvjećarstvo',
    'Forstwirtschaft, Jagdwirtschaft, Landschaftspflege': 'Šumarstvo, lovstvo, njega krajobraza',
    'Fototechnik und Fotografie': 'Fotografija i fototehnika',
    'Gartenbau': 'Vrtlarstvo i hortikultura',
    'Gastronomie': 'Ugostiteljstvo — posluživanje (konobari)',
    'Gebäudetechnik': 'Tehnika i održavanje zgrada (domari)',
    'Geisteswissenschaften': 'Humanističke znanosti',
    'Geologie, Geografie und Meteorologie': 'Geologija, geografija i meteorologija',
    'Geschäftsführung und Vorstand': 'Uprava i direktori',
    'Gesellschaftswissenschaften': 'Društvene znanosti',
    'Getränkeherstellung': 'Proizvodnja pića',
    'Gewerbe, Gesundheitsaufsicht, Desinfektion': 'Sanitarni nadzor i dezinfekcija',
    'Handel': 'Trgovina',
    'Hauswirtschaft und Verbraucherberatung': 'Domaćinstvo i savjetovanje potrošača',
    'Hochbau': 'Visokogradnja (zidari, fasaderi)',
    'Holzbe- und -verarbeitung': 'Obrada drva (stolari)',
    'Hotellerie': 'Hotelijerstvo (recepcija, sobarice)',
    'Human- und Zahnmedizin': 'Liječnici i stomatolozi',
    'IT-Netzwerktechnik, -Administration, -Organisation': 'IT mreže i administracija',
    'IT-Systemanalyse, -Anwendungsberatung und -Vertrieb': 'IT analiza, savjetovanje i prodaja',
    'Immobilienwirtschaft und Facility-Management': 'Nekretnine i upravljanje objektima',
    'Industrielle Glasherstellung': 'Industrijska proizvodnja stakla',
    'Industrielle Keramikherstellung': 'Industrijska proizvodnja keramike',
    'Informatik': 'Informatika',
    'Innenarchitektur, Raumausstattung': 'Unutarnje uređenje i opremanje prostora',
    'Kaufleute - Verkehr und Logistik': 'Komercijalisti u prometu i logistici',
    'Klempnerei, Sanitär-, Heizungs- und Klimatechnik': 'Vodoinstalacije, grijanje i klimatizacija',
    'Krankenpflege, Rettungsdienst und Geburtshilfe': 'Medicinske sestre, hitna pomoć i primaljstvo',
    'Kunsthandwerk und bildende Kunst': 'Umjetnički obrt i likovna umjetnost',
    'Kunsthandwerkliche Keramik- und Glasgestaltung': 'Umjetnička keramika i staklo',
    'Kunsthandwerkliche Metallgestaltung': 'Umjetnička obrada metala',
    'Kunststoff- und Kautschukherstellung': 'Proizvodnja plastike i gume',
    'Körperpflege': 'Frizeri, kozmetika i njega tijela',
    'Lagerwirtschaft, Post und Zustellung, Güterumschlag': 'Skladište, pošta i dostava',
    'Landwirtschaft': 'Poljoprivreda',
    'Lebensmittel- und Genussmittelherstellung': 'Proizvodnja hrane (pekari, mesari)',
    'Leder- und Pelzherstellung': 'Proizvodnja kože i krzna',
    'Lehr- und Forschungstätigkeit an Hochschulen': 'Nastava i istraživanje na fakultetima',
    'Lehrtätigkeit an allgemeinbildenden Schulen': 'Učitelji i nastavnici u školama',
    'Lehrtätigkeit an außerschulischen Bildungseinrichtungen': 'Predavači u izvanškolskom obrazovanju',
    'Lehrtätigkeit berufsbildender Fächer und betriebliche Ausbildung': 'Strukovni nastavnici i mentori',
    'Maler, Stuckateure, Bautenschutz': 'Soboslikari, žbukeri, zaštita građevina',
    'Maschinenbau- und Betriebstechnik': 'Strojarstvo i pogonska tehnika',
    'Mathematik und Statistik': 'Matematika i statistika',
    'Mechatronik und Automatisierungstechnik': 'Mehatronika i automatizacija',
    'Medien-, Dokumentations- und Informationsdienste': 'Mediji, dokumentacija i informacijske službe',
    'Medizin-, Orthopädie- und Rehatechnik': 'Medicinska i ortopedska tehnika',
    'Medizinisches Laboratorium': 'Medicinski laboratorij',
    'Metallbau und Schweißtechnik': 'Metalne konstrukcije i zavarivanje',
    'Metallbearbeitung': 'Obrada metala (CNC, tokari)',
    'Metallerzeugung': 'Proizvodnja metala (ljevaonice)',
    'Metalloberflächenbehandlung': 'Površinska obrada metala',
    'Moderation und Unterhaltung': 'Voditeljstvo i zabava',
    'Museumstechnik und -management': 'Muzejska tehnika i upravljanje',
    'Musik-, Gesang-, Dirigententätigkeiten': 'Glazbenici, pjevači, dirigenti',
    'Musikinstrumentenbau': 'Izrada glazbala',
    'Naturstein- und Mineralaufbereitung, Baustoffherstellung': 'Kamen, minerali i građevinski materijali',
    'Nichtärztliche Therapie und Heilkunde': 'Terapeuti (fizioterapija i sl.)',
    'Objekt-, Personen-, Brandschutz, Arbeitssicherheit': 'Zaštitari, protupožarna zaštita, zaštita na radu',
    'Offiziere': 'Časnici',
    'Papier- und Verpackungstechnik': 'Papir i ambalaža',
    'Personalwesen und -dienstleistung': 'Ljudski resursi (HR)',
    'Pferdewirtschaft': 'Konjogojstvo',
    'Pharmazie': 'Farmacija',
    'Physik': 'Fizika',
    'Polizei- und Kriminaldienst, Gerichts- und Justizvollzug': 'Policija, sudstvo i zatvorski sustav',
    'Produkt- und Industriedesign': 'Produktni i industrijski dizajn',
    'Psychologie, nichtärztliche Psychotherapie': 'Psihologija i psihoterapija',
    'Rechnungswesen, Controlling und Revision': 'Računovodstvo, kontroling i revizija',
    'Rechtsberatung, -sprechung und -ordnung': 'Pravno savjetovanje i pravosuđe',
    'Redaktion und Journalismus': 'Novinarstvo i uredništvo',
    'Reinigung': 'Čišćenje',
    'Schauspiel, Tanz und Bewegungskunst': 'Gluma, ples i pokret',
    'Servicekräfte im Personenverkehr': 'Osoblje u putničkom prometu',
    'Softwareentwicklung und Programmierung': 'Razvoj softvera i programiranje',
    'Speisenzubereitung': 'Kuhari i priprema hrane',
    'Sprach- und Literaturwissenschaften': 'Jezici i književnost',
    'Steuerberatung': 'Porezno savjetovanje',
    'Technische Forschung und Entwicklung': 'Tehničko istraživanje i razvoj',
    'Technische Mediengestaltung': 'Grafičko i medijsko oblikovanje',
    'Technische Produktionsplanung und -steuerung': 'Planiranje i vođenje proizvodnje',
    'Technischer Betrieb des Eisenbahn-, Luft- und Schiffsverkehrs': 'Tehničke službe željeznice, zračnog i brodskog prometa',
    'Technisches Zeichnen, Konstruktion und Modellbau': 'Tehničko crtanje i konstrukcija',
    'Textiltechnik und -produktion': 'Tekstilna proizvodnja',
    'Textilverarbeitung': 'Šivanje i prerada tekstila',
    'Theater-, Film- und Fernsehproduktion': 'Kazališna, filmska i TV produkcija',
    'Theologie und Gemeindearbeit': 'Teologija i rad u župi',
    'Tiefbau': 'Niskogradnja (ceste, cjevovodi)',
    'Tiermedizin und Tierheilkunde': 'Veterina',
    'Tierpflege': 'Njega životinja',
    'Tierwirtschaft': 'Stočarstvo',
    'Tourismus und Sport': 'Turizam i sport',
    'Umweltmanagement und -beratung': 'Upravljanje okolišem i savjetovanje',
    'Umweltschutztechnik': 'Tehnika zaštite okoliša',
    'Unternehmensorganisation und -strategie': 'Organizacija i strategija poduzeća',
    'Unteroffiziere mit Portepee': 'Dočasnici višeg ranga',
    'Unteroffiziere ohne Portepee': 'Dočasnici nižeg ranga',
    'Ver- und Entsorgung': 'Opskrba i zbrinjavanje otpada',
    'Veranstaltungs-, Kamera-, Tontechnik': 'Tehnika događanja, kamera i ton',
    'Veranstaltungsservice und -management': 'Organizacija događanja',
    'Verkauf (ohne Produktspezialisierung)': 'Prodaja — opća (blagajnici, trgovci)',
    'Verkauf Bekleidung, Elektro, KFZ, Hartwaren': 'Prodaja odjeće, elektronike i vozila',
    'Verkauf von Lebensmitteln': 'Prodaja hrane (pekarnice, mesnice)',
    'Verkauf von drogerie- und apothekenüblichen Waren': 'Prodaja u drogerijama i ljekarnama',
    'Verlags- und Medienwirtschaft': 'Izdavaštvo i medijska industrija',
    'Vermessung und Kartografie': 'Geodezija i kartografija',
    'Versicherungs- und Finanzdienstleistungen': 'Osiguranje i financijske usluge',
    'Verwaltung': 'Uprava i administracija',
    'Weinbau': 'Vinogradarstvo',
    'Werbung und Marketing': 'Oglašavanje i marketing',
    'Wirtschaftswissenschaften': 'Ekonomske znanosti',
    'Öffentlichkeitsarbeit': 'Odnosi s javnošću (PR)',
    'Überwachung und Steuerung des Verkehrsbetriebs': 'Nadzor i upravljanje prometom',
    'Überwachung, Wartung Verkehrsinfrastruktur': 'Održavanje prometne infrastrukture',
}


# Broad, user-facing categories. Each maps to a set of Berufsfelder that are
# queried (and deduped) together. This is what the input form exposes; the raw
# Berufsfeld names above are still accepted as free text. Every Berufsfeld
# belongs to exactly one group (verified by `_check_groups.py`).
# Labels are Croatian — they are display + export text only. The Berufsfelder
# themselves stay German because they are sent verbatim to the board's API.
ARBEITSAGENTUR_GROUPS: dict[str, dict] = {
    "bau_ausbau": {
        "label": "Građevina, završni radovi i instalacije",
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
        "label": "Metal, strojarstvo, elektrotehnika i vozila",
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
        "label": "Proizvodnja i prerada",
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
        "label": "Logistika, skladište i promet",
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
        "label": "Trgovina, nabava i prodaja",
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
        "label": "Gastronomija, hotelijerstvo i turizam",
        "berufsfelder": [
            "Gastronomie",
            "Speisenzubereitung",
            "Hotellerie",
            "Tourismus und Sport",
        ],
    },
    "gesundheit_pflege": {
        "label": "Zdravstvo, medicina i njega",
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
        "label": "Obrazovanje, socijalni rad i odgoj",
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
        "label": "IT i razvoj softvera",
        "berufsfelder": [
            "Informatik",
            "Softwareentwicklung und Programmierung",
            "IT-Netzwerktechnik, -Administration, -Organisation",
            "IT-Systemanalyse, -Anwendungsberatung und -Vertrieb",
        ],
    },
    "buero_finanzen_recht": {
        "label": "Ured, financije, pravo i menadžment",
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
        "label": "Marketing, mediji, dizajn i umjetnost",
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
        "label": "Čišćenje, sigurnost, opskrba i zbrinjavanje",
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
        "label": "Poljoprivreda, životinje i priroda",
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
        "label": "Prirodne znanosti i istraživanje",
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
        "label": "Javna služba, sigurnost i vojska",
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


def _listings(payload: dict | None) -> list[dict]:
    """The result list, wherever this API version keeps it.

    v4 called it `stellenangebote`; v6 renamed it to `ergebnisliste` while
    keeping `maxErgebnisse` — so a probe that only reads the total says
    "924 oglasa" while a scraper reading the old key exports nothing.
    """
    if not payload:
        return []
    return payload.get("stellenangebote") or payload.get("ergebnisliste") or []


def _listing_refnr(listing: dict) -> str:
    # v4: refnr; v6: referenznummer. Same value, and the v4 detail endpoint
    # accepts it base64-encoded either way.
    return (listing.get("refnr") or listing.get("referenznummer") or "").strip()


def _listing_location(listing: dict) -> str:
    # v4: arbeitsort {ort, region}; v6: stellenlokationen [{adresse: {ort, region}}].
    arbeitsort = listing.get("arbeitsort") or {}
    ort = (arbeitsort.get("ort") or "").strip()
    region = (arbeitsort.get("region") or "").strip()
    if ort or region:
        return ort or region

    lokationen = listing.get("stellenlokationen") or []
    adresse = (lokationen[0].get("adresse") or {}) if lokationen else {}
    return (adresse.get("ort") or "").strip() or (adresse.get("region") or "").strip()


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


def _request_json(url: str, timeout: int, attempts: int = 3) -> tuple[dict | None, int | None]:
    """GET a JSON document with the board's public API key, retrying transient
    errors. Returns (payload, status): status is the HTTP code when the server
    answered, None when it never did.
    """
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            headers={"X-API-Key": API_KEY, "User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8")), response.status
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (400, 401, 403, 404):
                return None, exc.code  # the caller decides how loud to be
        except Exception as exc:  # network / timeout / decode
            last_error = exc
        if attempt < attempts:
            time.sleep(2 * attempt)
    _log(f"[arbeitsagentur] Giving up on {url}: {last_error}")
    return None, None


def _http_get_json(url: str, timeout: int, attempts: int = 3) -> dict | None:
    """Single-URL fetch. A 404 here means "posting gone" and stays quiet."""
    payload, status = _request_json(url, timeout, attempts)
    if payload is None and status in (400, 401, 403):
        _log(f"[arbeitsagentur] HTTP {status} for {url}")
    return payload


def _search_json(
    berufsfeld: str | None,
    keyword: str | None,
    location: str | None,
    radius: int | None,
    page: int,
    size: int,
    timeout: int,
) -> dict | None:
    """Run a search, resolving which API path this board still answers on.

    A 404 on a *search* is not an expired posting — it means the endpoint moved,
    which is exactly what happened to /pc/v4/jobs. So it is never swallowed: we
    move on to the next documented path, and if none answers, we say so.
    """
    global _search_url

    candidates = [_search_url] if _search_url else [f"{API_BASE}{path}" for path in SEARCH_PATHS]
    for base in candidates:
        payload, status = _request_json(
            _build_search_url(base, berufsfeld, keyword, location, radius, page, size), timeout
        )
        if payload is not None:
            if _search_url != base:
                _search_url = base
                _log(f"[arbeitsagentur] Search endpoint: {base}")
            return payload
        if status == 404:
            continue  # path retired; try the next one
        if status in (400, 401, 403):
            _log(f"[arbeitsagentur] HTTP {status} from {base} — check the API key or parameters.")
        return None

    _log(
        "[arbeitsagentur] None of the known search endpoints answered "
        f"({', '.join(SEARCH_PATHS)}). The board's API has moved."
    )
    return None


def _detail_json(enc: str, timeout: int) -> dict | None:
    """Fetch one posting's detail, resolving the detail path the same way.

    Unlike search, a 404 here is routine — postings expire — so the path is only
    treated as wrong while it has never once succeeded.
    """
    global _detail_url

    if _detail_url:
        return _http_get_json(_detail_url.format(enc=enc), timeout=timeout)

    for template in DETAIL_PATHS:
        url = f"{API_BASE}{template}"
        payload, status = _request_json(url.format(enc=enc), timeout)
        if payload is not None:
            _detail_url = url
            _log(f"[arbeitsagentur] Detail endpoint: {url}")
            return payload
        if status in (400, 401, 403):
            _log(f"[arbeitsagentur] HTTP {status} for {url.format(enc=enc)}")
            return None
    return None  # 404 everywhere: this posting is gone, or every path is


def _build_search_url(
    base: str,
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
    return f"{base}?{urllib.parse.urlencode(params)}"


def _enrich_listing(listing: dict, category_label: str, detail_timeout: int) -> dict | None:
    refnr = _listing_refnr(listing)
    if not refnr:
        return None

    location = _listing_location(listing)

    enc = base64.b64encode(refnr.encode("utf-8")).decode("ascii")
    detail = _detail_json(enc, timeout=detail_timeout)

    description = (detail.get("stellenangebotsBeschreibung") if detail else "") or ""
    company = (
        (detail.get("firma") if detail else "")
        or listing.get("arbeitgeber")
        or listing.get("firma")
        or ""
    ).strip()

    email = _extract_email(description)
    website = _clean_website(listing.get("externeUrl") or "")

    return {
        "title": (
            listing.get("titel")
            or listing.get("stellenangebotsTitel")
            or listing.get("beruf")
            or listing.get("hauptberuf")
            or ""
        ).strip(),
        "company": company,
        "location": location,
        "published_at": (
            listing.get("aktuelleVeroeffentlichungsdatum")
            or listing.get("datumErsteVeroeffentlichung")
            or ""
        ).strip(),
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
    berufsfelder: list[str] | None = None,
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
    * `berufsfelder` -> explicit subset of German Berufsfeld names to query,
      overriding whatever `category` resolves to. This is how the GUI scrapes a
      partially-checked group; `category` still supplies the display label.

    Returns one dict per posting (e-mail may be empty; the caller filters).
    """
    skip_ids = skip_ids or set()
    page_size = max(1, min(int(results_per_page or MAX_PAGE_SIZE), MAX_PAGE_SIZE))
    search_timeout = int(os.getenv("ARBEITSAGENTUR_SEARCH_TIMEOUT", "60"))
    detail_timeout = int(os.getenv("ARBEITSAGENTUR_DETAIL_TIMEOUT", "30"))
    detail_delay_ms = int(os.getenv("ARBEITSAGENTUR_DETAIL_DELAY_MS", "300"))
    debug_progress = os.getenv("ARBEITSAGENTUR_DEBUG_PROGRESS", "false") == "true"

    resolved, category_label = _resolve_berufsfelder(category)
    if berufsfelder:
        resolved = list(berufsfelder)
        category_label = category_label or ", ".join(berufsfelder)
    # A keyword/board-wide sweep has no Berufsfeld filter; represent it as one
    # query with berufsfeld=None.
    queries: list[str | None] = resolved or [None]

    jobs: list[dict] = []
    seen_refnrs: set[str] = set()
    seen_company_keys: set[str] = set()
    processed = 0  # listings whose detail we fetched (== detail requests)

    for berufsfeld in queries:
        total_results: int | None = None
        # True once the Berufsfeld matched nothing and we switched to free text.
        fell_back = False

        for page in range(1, max_pages + 1):
            payload = _search_json(
                None if fell_back else berufsfeld,
                berufsfeld if fell_back else keyword,
                location,
                radius,
                page,
                page_size,
                search_timeout,
            )
            if payload is None:
                _log(f"[arbeitsagentur] No response for {berufsfeld!r} page {page}; skipping rest.")
                break

            if total_results is None:
                total_results = payload.get("maxErgebnisse")
            listings = _listings(payload)

            # The board renames its Berufsfeld values without notice, and a
            # renamed one filters every posting out while still answering 200.
            # Retry the same term as a free-text search rather than reporting an
            # empty category — but say so, because `was` matches the advert text
            # instead of the structured occupation, so the scope is wider.
            if not listings and page == 1 and berufsfeld and not keyword and not fell_back:
                _log(
                    f"[arbeitsagentur] berufsfeld={berufsfeld!r} vratio 0 oglasa; "
                    f"ponavljam kao was={berufsfeld!r} (šira, manje precizna pretraga)."
                )
                fell_back = True
                payload = _search_json(
                    None, berufsfeld, location, radius, page, page_size, search_timeout
                )
                if payload is None:
                    break
                total_results = payload.get("maxErgebnisse")
                listings = _listings(payload)

            if debug_progress:
                _log(
                    f"[arbeitsagentur] berufsfeld={berufsfeld!r} page {page} "
                    f"listings={len(listings)} max={total_results} "
                    f"processed={processed} jobs={len(jobs)}"
                )

            if not listings:
                break

            for listing in listings:
                refnr = _listing_refnr(listing)
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
