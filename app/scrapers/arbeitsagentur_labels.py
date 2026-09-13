"""Croatian names for the board's German taxonomy.

The Arbeitsagentur speaks only German: its occupational fields (*Berufsfelder*)
and occupations (*Berufe*) are the values we have to send, so they are what the
app has always shown. That makes picking a category a guessing game for anyone
who does not read German, which is the whole reason this file exists — the
German value stays the key, the Croatian text is only there to read.

A translation is a gloss, not a dictionary entry: it says what kind of work the
field covers ("Krankenpflege, Rettungsdienst und Geburtshilfe" ->
"medicinske sestre, hitna pomoć i babice"), because that is the question being
asked when you tick a box.

`tests/test_arbeitsagentur_labels.py` fails if the board's taxonomy grows a
value this file does not translate, so a rename cannot quietly go untranslated.
"""

# Our own groups (see ARBEITSAGENTUR_GROUPS), keyed by group key.
GROUP_LABELS_HR = {
    "bau_ausbau": "Građevina, završni radovi i instalacije",
    "metall_maschinen_elektro": "Metal, strojevi, elektro i vozila",
    "produktion_fertigung": "Proizvodnja i prerada",
    "logistik_verkehr": "Logistika, skladište i transport",
    "handel_verkauf": "Trgovina, nabava i prodaja",
    "gastronomie_tourismus": "Gastronomija, hotelijerstvo i turizam",
    "gesundheit_pflege": "Zdravstvo, medicina i njega",
    "bildung_soziales": "Obrazovanje, socijalni rad i odgoj",
    "it": "IT i razvoj softvera",
    "buero_finanzen_recht": "Uredski posao, financije, pravo i menadžment",
    "marketing_medien_kunst": "Marketing, mediji, dizajn i umjetnost",
    "reinigung_sicherheit_versorgung": "Čišćenje, sigurnost, komunalne usluge",
    "landwirtschaft_natur": "Poljoprivreda, životinje i priroda",
    "wissenschaft_forschung": "Prirodne znanosti i istraživanje",
    "oeffentlicher_dienst_sicherheit": "Javna služba, sigurnost i vojska",
}

# The board's Berufsfeld facet — one entry per value in BERUFSFELDER.
BERUFSFELD_HR = {
    # Građevina
    "Hochbau": "visokogradnja",
    "Tiefbau": "niskogradnja",
    "Aus- und Trockenbau, Isolierung, Zimmerei, Glaserei":
        "unutarnji i suhi radovi, izolacija, tesarstvo, staklarstvo",
    "Bodenverlegung": "polaganje podova",
    "Maler, Stuckateure, Bautenschutz": "soboslikari, gipsari, zaštita građevina",
    "Klempnerei, Sanitär-, Heizungs- und Klimatechnik":
        "vodoinstalacije, grijanje i klimatizacija",
    "Gebäudetechnik": "tehnika zgrada (instalacije i održavanje)",
    "Bauplanung und -überwachung, Architektur": "projektiranje i nadzor gradnje, arhitektura",
    "Bau- und Transportgeräteführung": "upravljanje građevinskim i transportnim strojevima",
    "Naturstein- und Mineralaufbereitung, Baustoffherstellung":
        "obrada kamena i minerala, proizvodnja građevinskog materijala",
    "Berg-, Tagebau und Sprengtechnik": "rudarstvo, površinski kopovi i miniranje",
    "Holzbe- und -verarbeitung": "obrada i prerada drva",
    # Metal, strojevi, elektro
    "Metallbau und Schweißtechnik": "metalne konstrukcije i varenje",
    "Metallbearbeitung": "obrada metala",
    "Metallerzeugung": "proizvodnja metala",
    "Metalloberflächenbehandlung": "obrada površine metala (galvanizacija, lakiranje)",
    "Maschinenbau- und Betriebstechnik": "strojarstvo i održavanje pogona",
    "Mechatronik und Automatisierungstechnik": "mehatronika i automatizacija",
    "Feinwerk- und Werkzeugtechnik": "precizna mehanika i alatničarstvo",
    "Elektrotechnik": "elektrotehnika",
    "Energietechnik": "energetika",
    "Fahrzeug-, Luft-, Raumfahrt- und Schiffbautechnik":
        "vozila, zrakoplovstvo, svemirska tehnika i brodogradnja",
    "Technische Produktionsplanung und -steuerung":
        "tehničko planiranje i upravljanje proizvodnjom",
    "Technisches Zeichnen, Konstruktion und Modellbau":
        "tehničko crtanje, konstrukcija i modelarstvo",
    "Technische Forschung und Entwicklung": "tehničko istraživanje i razvoj",
    "Kunsthandwerkliche Metallgestaltung": "umjetnička obrada metala",
    # Proizvodnja
    "Kunststoff- und Kautschukherstellung": "proizvodnja plastike i gume",
    "Lebensmittel- und Genussmittelherstellung": "proizvodnja hrane, kave i tabaka",
    "Getränkeherstellung": "proizvodnja napitaka",
    "Chemie": "kemija",
    "Papier- und Verpackungstechnik": "papirna i ambalažna tehnika",
    "Drucktechnik, Buchbinderei": "tiskarstvo i knjigoveštvo",
    "Textiltechnik und -produktion": "tekstilna tehnika i proizvodnja",
    "Textilverarbeitung": "prerada tekstila (šivanje)",
    "Leder- und Pelzherstellung": "proizvodnja kože i krzna",
    "Industrielle Glasherstellung": "industrijska proizvodnja stakla",
    "Industrielle Keramikherstellung": "industrijska proizvodnja keramike",
    "Musikinstrumentenbau": "izrada glazbenih instrumenata",
    "Farb- und Lacktechnik": "boje i lakovi (proizvodnja i nanošenje)",
    # Logistika i transport
    "Lagerwirtschaft, Post und Zustellung, Güterumschlag":
        "skladište, pošta i dostava, pretovar robe",
    "Kaufleute - Verkehr und Logistik": "komercijalisti u transportu i logistici",
    "Fahrzeugführung im Straßenverkehr": "vozači u cestovnom prometu",
    "Fahrzeugführung im Eisenbahnverkehr": "strojovođe (željeznica)",
    "Fahrzeugführung im Schiffsverkehr": "upravljanje plovilima",
    "Fahrzeugführung im Flugverkehr": "piloti",
    "Überwachung und Steuerung des Verkehrsbetriebs": "nadzor i upravljanje prometom",
    "Überwachung, Wartung Verkehrsinfrastruktur": "nadzor i održavanje prometne infrastrukture",
    "Technischer Betrieb des Eisenbahn-, Luft- und Schiffsverkehrs":
        "tehnički pogon željezničkog, zračnog i pomorskog prometa",
    "Servicekräfte im Personenverkehr": "osoblje u putničkom prijevozu",
    # Trgovina
    "Verkauf (ohne Produktspezialisierung)": "prodaja (bez specijalizacije)",
    "Verkauf Bekleidung, Elektro, KFZ, Hartwaren":
        "prodaja odjeće, elektronike, auto-dijelova i tehničke robe",
    "Verkauf von Lebensmitteln": "prodaja prehrambenih proizvoda",
    "Verkauf von drogerie- und apothekenüblichen Waren":
        "prodaja drogerijske i ljekarničke robe",
    "Handel": "trgovina (općenito)",
    "Einkauf und Vertrieb": "nabava i prodaja (B2B)",
    "Buch- und Kunstantiquitäten, Musikfachhandel":
        "antikvarijati i specijalizirana prodaja glazbe",
    # Gastronomija i turizam
    "Gastronomie": "gastronomija (posluživanje)",
    "Speisenzubereitung": "priprema hrane (kuhari)",
    "Hotellerie": "hotelijerstvo",
    "Tourismus und Sport": "turizam i sport",
    # Zdravstvo i njega
    "Altenpflege": "njega starijih osoba",
    "Arzt- und Praxishilfe": "pomoćno osoblje u ordinacijama",
    "Human- und Zahnmedizin": "liječnici i stomatolozi",
    "Krankenpflege, Rettungsdienst und Geburtshilfe":
        "medicinske sestre, hitna pomoć i babice",
    "Nichtärztliche Therapie und Heilkunde": "fizioterapija, logopedija i druge terapije",
    "Medizinisches Laboratorium": "medicinski laboratorij",
    "Medizin-, Orthopädie- und Rehatechnik":
        "medicinska, ortopedska i rehabilitacijska tehnika",
    "Pharmazie": "farmacija",
    "Psychologie, nichtärztliche Psychotherapie": "psihologija i psihoterapija",
    "Ernährungs- und Gesundheitsberatung": "savjetovanje o prehrani i zdravlju",
    "Tiermedizin und Tierheilkunde": "veterina",
    # Obrazovanje i socijalni rad
    "Erziehung, Sozialarbeit, Heilerziehungspflege":
        "odgoj, socijalni rad, rad s osobama s invaliditetom",
    "Lehrtätigkeit an allgemeinbildenden Schulen": "nastava u općeobrazovnim školama",
    "Lehrtätigkeit an außerschulischen Bildungseinrichtungen":
        "nastava u izvanškolskim ustanovama",
    "Lehrtätigkeit berufsbildender Fächer und betriebliche Ausbildung":
        "nastava strukovnih predmeta i obuka u poduzeću",
    "Lehr- und Forschungstätigkeit an Hochschulen":
        "nastava i istraživanje na visokim školama",
    "Fahr- und Sportunterricht an außerschulischen Bildungseinrichtungen":
        "auto-škole i sportska poduka",
    "Theologie und Gemeindearbeit": "teologija i rad u vjerskoj zajednici",
    # IT
    "Informatik": "informatika",
    "Softwareentwicklung und Programmierung": "razvoj softvera i programiranje",
    "IT-Netzwerktechnik, -Administration, -Organisation":
        "IT mreže, administracija i organizacija",
    "IT-Systemanalyse, -Anwendungsberatung und -Vertrieb":
        "IT analiza sustava, savjetovanje i prodaja",
    # Ured, financije, pravo
    "Büro und Sekretariat": "uredski posao i tajništvo",
    "Verwaltung": "uprava i administracija",
    "Rechnungswesen, Controlling und Revision": "računovodstvo, kontroling i revizija",
    "Steuerberatung": "porezno savjetovanje",
    "Versicherungs- und Finanzdienstleistungen": "osiguranje i financijske usluge",
    "Rechtsberatung, -sprechung und -ordnung": "pravno savjetovanje i pravosuđe",
    "Unternehmensorganisation und -strategie": "organizacija i strategija poduzeća",
    "Personalwesen und -dienstleistung": "kadrovska služba i agencije za zapošljavanje",
    "Geschäftsführung und Vorstand": "uprava i direktori",
    "Immobilienwirtschaft und Facility-Management": "nekretnine i upravljanje objektima",
    # Marketing, mediji, umjetnost
    "Werbung und Marketing": "oglašavanje i marketing",
    "Öffentlichkeitsarbeit": "odnosi s javnošću",
    "Redaktion und Journalismus": "uredništvo i novinarstvo",
    "Verlags- und Medienwirtschaft": "izdavaštvo i medijska industrija",
    "Technische Mediengestaltung": "tehnički dizajn medija (DTP, montaža)",
    "Medien-, Dokumentations- und Informationsdienste":
        "arhivi, knjižnice i informacijske službe",
    "Theater-, Film- und Fernsehproduktion": "produkcija u teatru, filmu i na TV-u",
    "Veranstaltungs-, Kamera-, Tontechnik": "tehnika događanja, kamera i zvuk",
    "Veranstaltungsservice und -management": "organizacija događanja",
    "Produkt- und Industriedesign": "dizajn proizvoda i industrijski dizajn",
    "Innenarchitektur, Raumausstattung": "dizajn interijera i unutarnje opremanje",
    "Fototechnik und Fotografie": "fotografija i fototehnika",
    "Kunsthandwerk und bildende Kunst": "umjetničko obrtništvo i likovna umjetnost",
    "Schauspiel, Tanz und Bewegungskunst": "gluma, ples i scenski pokret",
    "Musik-, Gesang-, Dirigententätigkeiten": "glazba, pjevanje i dirigiranje",
    "Moderation und Unterhaltung": "voditeljstvo i zabava",
    "Bühnen- und Kostümbildnerei, Requisite": "scenografija, kostimografija i rekvizita",
    "Kunsthandwerkliche Keramik- und Glasgestaltung": "umjetnička keramika i staklo",
    "Museumstechnik und -management": "muzejska tehnika i upravljanje",
    # Čišćenje, sigurnost, komunalne usluge
    "Reinigung": "čišćenje",
    "Objekt-, Personen-, Brandschutz, Arbeitssicherheit":
        "zaštita objekata i osoba, protupožarna zaštita, zaštita na radu",
    "Ver- und Entsorgung": "voda, energija i odvoz otpada",
    "Umweltschutztechnik": "tehnika zaštite okoliša",
    "Gewerbe, Gesundheitsaufsicht, Desinfektion":
        "gospodarski i sanitarni nadzor, dezinfekcija",
    "Hauswirtschaft und Verbraucherberatung": "kućanstvo i savjetovanje potrošača",
    "Bestattungswesen": "pogrebne usluge",
    "Körperpflege": "njega tijela (frizeri, kozmetika)",
    # Poljoprivreda i priroda
    "Gartenbau": "vrtlarstvo",
    "Landwirtschaft": "poljoprivreda",
    "Forstwirtschaft, Jagdwirtschaft, Landschaftspflege":
        "šumarstvo, lovstvo i održavanje krajobraza",
    "Tierwirtschaft": "stočarstvo",
    "Tierpflege": "njega životinja",
    "Pferdewirtschaft": "konjogojstvo",
    "Fischwirtschaft": "ribarstvo",
    "Floristik": "cvjećarstvo",
    "Weinbau": "vinogradarstvo",
    # Znanost
    "Biologie": "biologija",
    "Physik": "fizika",
    "Mathematik und Statistik": "matematika i statistika",
    "Geologie, Geografie und Meteorologie": "geologija, geografija i meteorologija",
    "Geisteswissenschaften": "humanističke znanosti",
    "Gesellschaftswissenschaften": "društvene znanosti",
    "Sprach- und Literaturwissenschaften": "jezične i književne znanosti",
    "Wirtschaftswissenschaften": "ekonomske znanosti",
    "Vermessung und Kartografie": "geodezija i kartografija",
    "Umweltmanagement und -beratung": "upravljanje okolišem i savjetovanje",
    # Javna služba, sigurnost, vojska
    "Polizei- und Kriminaldienst, Gerichts- und Justizvollzug":
        "policija, kriminalistika, sudovi i zatvorski sustav",
    "Angehörige der regulären Streitkräfte in sonstigen Rängen": "vojnici (ostali činovi)",
    "Offiziere": "časnici",
    "Unteroffiziere mit Portepee": "podčasnici (viši)",
    "Unteroffiziere ohne Portepee": "podčasnici (niži)",
    "Angehörige gesetzgebender Körperschaften": "članovi zakonodavnih tijela",
}

# The board's Beruf facet is thousands of values deep and is read live off the
# API, so it is not translated wholesale — only the care and health occupations
# this app is actually pointed at. Anything missing stays German, which is
# exactly what has to be typed into the search anyway.
BERUF_HR = {
    "Gesundheits- und Krankenpfleger/in": "medicinska sestra / tehničar (opća njega)",
    "Pflegefachmann/-frau (Gesundheits- und Krankenpflege)":
        "medicinska sestra / tehničar (nova njemačka titula)",
    "Pflegefachmann/-frau (Altenpflege)": "njegovatelj/ica starijih (nova titula)",
    "Pflegefachmann/-frau (Gesundheits- und Kinderkrankenpflege)":
        "pedijatrijska sestra (nova titula)",
    "Pflegefachmann/-frau (Ausbildung)": "njega — naukovanje (Ausbildung)",
    "Krankenschwester/-pfleger": "medicinska sestra / tehničar (stari naziv)",
    "Gesundheits- und Kinderkrankenpfleger/in": "pedijatrijska sestra / tehničar",
    "Gesundheits- und Krankenpflegehelfer/in": "pomoćni/a njegovatelj/ica u bolnici",
    "Altenpfleger/in": "njegovatelj/ica starijih osoba",
    "Altenpflegehelfer/in": "pomoćni/a njegovatelj/ica starijih osoba",
    "Helfer/in - Altenpflege/Persönliche Assistenz":
        "pomoćnik/ca u njezi starijih / osobni asistent",
    "Helfer/in - stationäre Krankenpflege": "pomoćnik/ca u stacionarnoj njezi",
    "Pflegeassistent/in": "asistent/ica u njezi",
    "Pflegeassistent/in (Gesundheits- und Krankenpflege)": "asistent/ica u zdravstvenoj njezi",
    "Pflegeassistent/in (Altenpflege)": "asistent/ica u njezi starijih",
    "Ambulante/r Pfleger/in": "njegovatelj/ica u kućnoj (ambulantnoj) njezi",
    "Betreuungskraft / Alltagsbegleiter/in": "pratitelj/ica i pomoć u svakodnevici",
    "Pflegedienstleiter/in": "voditelj/ica službe njege",
    "Stationsleiter/in - Kranken-/Alten-/Kinderkrankenpflege": "glavna sestra odjela",
    "Leiter/in - Altenpflegeeinrichtung": "voditelj/ica doma za starije",
    "Fachkrankenpfleger/in - Intensivpflege/Anästhesie":
        "sestra specijalist — intenzivna njega / anestezija",
    "Fachkinderkrankenpfleger/in - Intensivpflege/Anästhesie":
        "pedijatrijska sestra specijalist — intenzivna njega / anestezija",
    "Fachkrankenschwester/-pfleger - Operationsdienst": "instrumentarka / sestra u operacijskoj",
    "Operationstechnische/r Assistent/in": "operacijski tehničar (OTA)",
    "Operationstechnische/r Angestellte/r": "operacijski tehničar (zaposlenik)",
    "Anästhesietechnische/r Assistent/in": "anesteziološki tehničar (ATA)",
    "Notfallsanitäter/in": "medicinski tehničar hitne pomoći (najviša razina)",
    "Rettungssanitäter/in": "spasilac / tehničar hitne pomoći",
    "Hebamme/Entbindungspfleger": "babica",
    "Heilerziehungspfleger/in": "njegovatelj/ica osoba s invaliditetom",
    "Medizinische/r Fachangestellte/r": "medicinski administrativni asistent u ordinaciji",
    "Zahnmedizinische/r Fachangestellte/r": "stomatološki asistent",
    "Arzthelfer/in": "pomoćnik/ca u ordinaciji",
    "Physiotherapeut/in": "fizioterapeut",
    "Ergotherapeut/in": "radni terapeut",
    "Masseur/in und medizinische/r Bademeister/in": "maser i medicinski kupeljar",
}


def berufsfeld_hr(name: str) -> str:
    """The Croatian gloss for a Berufsfeld, or "" when there is none."""
    return BERUFSFELD_HR.get(name, "")


def beruf_hr(name: str) -> str:
    """The Croatian gloss for a Beruf, or "" when it is not in the curated set."""
    return BERUF_HR.get(name, "")


def bilingual(german: str, croatian: str) -> str:
    """Croatian first, German after — the German half is what gets sent, so it
    has to stay visible and exact."""
    return f"{croatian} — {german}" if croatian else german
