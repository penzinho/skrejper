import os
import re
import time
from urllib.parse import urljoin

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


LANDING_URL = "https://burzarada.hzz.hr/Posloprimac_RadnaMjesta.aspx"
BASE_URL = "https://burzarada.hzz.hr/Posloprimac_RadnaMjesta.aspx?trazi=1"
DETAIL_URL_KEYWORD = "RadnoMjesto_Ispis.aspx"
EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
PHONE_RE = re.compile(
    r"(?:(?:\+|00)\s*385[\s/.-]*)?(?:0\d[\d\s/.-]{5,}\d|\d{2,3}[\s/.-]*\d{3}[\s/.-]*\d{3,4})"
)
ADDRESS_RE = re.compile(
    r"([A-ZČĆŠŽĐ][^,\n]{2,}?\d+[A-Za-z]?(?:/\d+[A-Za-z]?)?\s*,\s*\d{5}\s+[A-ZČĆŠŽĐ][^\n,]{2,})"
)
GENERIC_EMAILS = {"burzarada@hzz.hr"}
DETAIL_SECTION_LABELS = [
    "Mjesto rada:",
    "Broj traženih radnika:",
    "Vrsta zaposlenja:",
    "Radno vrijeme:",
    "Način rada:",
    "Smještaj:",
    "Naknada za prijevoz:",
    "Bruto plaća:",
    "Natječaj vrijedi od:",
    "Natječaj vrijedi do:",
    "Uvjeti na radnom mjestu:",
    "Posloprimac",
    "Poslodavac",
    "Kontakt:",
    "Potrebna zvanja:",
    "Razina obrazovanja:",
    "Vozački ispit:",
    "Radno iskustvo:",
    "Ostale informacije:",
    "Opis posla:",
    "Nudimo:",
    "Karakteristike prave osobe za nas:",
]
HZZ_CATEGORY_GROUPS: dict[str, list[str]] = {
    'it': [
        'Razvojni inženjeri i analitičari/razvojne inženjerke i analitičarke za razvoj programske podrške d. n.',
        'Administratori sustava/administratorice sustava',
        'Analitičari/analitičarke sustava',
        'Programeri/programerke za razvoj aplikacija',
        'Razvojni inženjeri/razvojne inženjerke za programsku podršku',
        'Tehničari/tehničarke informacijske i komunikacijske tehnologije za podršku korisnicima',
    ],
    'economy_admin': [
        'Bankarski službenici/bankarske službenice za šaltersko novčano poslovanje i srodna zanimanja',
        'Carinski inspektori/carinske inspektorice i granični inspektori/granične inspektorice',
        'Ekonomisti/ekonomistice',
        'Ekonomisti/ekonomistice i voditelji/voditeljice dijelova računovodstva',
        'Komercijalisti/komercijalistice u prodaji',
        'Operatori/operatorice na telefonskoj centrali',
        'Poslovni i organizacijski analitičari/poslovne i organizacijske analitičarke',
        'Pravni asistenti/pravne asistentice i srodna zanimanja',
        'Pravni stručnjaci/pravne stručnjakinje d. n.',
        'Pravni zastupnici/pravne zastupnice',
        'Računovodstveni stručnjaci/računovodstvene stručnjakinje',
        'Referenti/referentice za koncesije, dozvole i srodna zanimanja',
        'Službenici/službenice knjižnica i muzeja',
        'Službenici/službenice u knjigovodstvu',
        'Službenici/službenice za ljudske potencijale',
        'Službenici/službenice za pružanje informacija u kontakt centru',
        'Službenici/službenice za unos podataka',
        'Stručni suradnici/stručne suradnice na poslovima državne regulative d. n.',
        'Stručni suradnici/stručne suradnice za kredite i zajmove',
        'Tajnici/tajnice i srodna zanimanja',
        'Uredski službenici/uredske službenice za opće poslove',
        'Zastupnici osiguranja/zastupnice osiguranja',
    ],
    'arts_culture': [
        'Fotografi/fotografkinje',
        'Glazbeni umjetnici/glazbene umjetnice, pjevači/pjevačice i skladatelji/skladateljice',
        'Industrijski i modni dizajneri/industrijske i modne dizajnerice',
        'Likovni umjetnici/likovne umjetnice',
        'Novinari/novinarke',
        'Plesači/plesačice i koreografi/koreografkinje',
        'Stručni suradnici/stručne suradnice za djelatnosti u umjetnosti i kulturi d. n.',
        'Tehničari/tehničarke za emitiranje i audio-vizualne sustave',
    ],
    'construction': [
        'Rukovatelji/rukovateljice strojevima za proizvodnju cementnih, mineralnih proizvoda i ostalih proizvoda od kamena',
        'Arhitekti/arhitektice',
        'Armirači i betonirci/armiračice i betonirke',
        'Fasaderi i gipsari/fasaderke i gipsarice',
        'Graditelji/graditeljice kuća',
        'Građevinski nadzornici/građevinske nadzornice',
        'Inženjeri/inženjerke građevinarstva',
        'Izolateri/izolaterke',
        'Kamenoresci/kamenoreskinje, klesari/klesarice i srodna zanimanja',
        'Kartografi/kartografkinje i geodeti/geodetkinje',
        'Krovopokrivači/krovopokrivačice',
        'Podopolagači/podopolagačice i srodna zanimanja',
        'Radnici/radnice u niskogradnji',
        'Radnici/radnice u visokogradnji',
        'Rukovatelji/rukovateljice građevinskim i sličnim strojevima',
        'Soboslikari/soboslikarice, ličioci/ličiteljice i srodna zanimanja',
        'Tehničari/tehničarke za arhitekturu, građevinarstvo, geodeziju i srodna zanimanja',
        'Tesari i građevinski stolari/tesarice i građevinske stolarice',
        'Zidari/zidarice i srodna građevinska zanimanja d.n.',
        'Zidari/zidarke i srodna zanimanja',
    ],
    'trade_marketing': [
        'Agenti/agentice posredovanja u prometu nekretnina',
        'Blagajnici/blagajnice, prodavači/prodavačice ulaznica i srodna zanimanja',
        'Organizatori/organizatorice putovanja i službenici/službenice u turističkim agencijama',
        'Poslovođe/poslovotkinje u trgovinama',
        'Posrednici/posrednice za prijevoz, trgovinu i srodna zanimanja',
        'Prodavači/prodavačice u trgovinama',
        'Prodavači/prodavačice, d. n.',
        'Punjači/punjačice polica',
        'Stručnjaci/stručnjakinje za promidžbu i marketing',
        'Trgovci/trgovke u vlastitim trgovinama',
        'Zastupnici/zastupnice direktne prodaje',
    ],
    'hospitality_tourism': [
        'Barmeni/barmenice',
        'Domaćinska zanimanja u uredima, hotelima i ostalim objektima',
        'Dostavljači, nosači/dostavljačice, nosačice i srodna zanimanja',
        'Glavni kuhari/glavne kuharice',
        'Hotelski recepcionari/hotelske recepcionarke',
        'Konobari/konobarice',
        'Kuhari/kuharice',
        'Kuhinjski pomoćnik/kuhinjske pomoćnice',
        'Kušači i ocjenjivači/kušateljice i ocjenjivačice namirnica',
        'Priprematelji/pripremateljice brze hrane',
        'Recepcionari/recepcionarke (općenito)',
        'Službenici/službenice u kladionici, kasinu i srodna zanimanja',
        'Uslužna zanimanja, d. n.',
        'Vodiči/vodičice',
    ],
    'geology_mining_metallurgy': [
        'Bušači/bušačice bušotina i srodna zanimanja',
        'Geolozi/geologinje i geofizičari/geofizičarke',
        'Inženjeri/inženjerke rudarstva, metalurgije i srodni stručnjaci/stručnjakinje',
        'Ljevači/ljevačice metala i srodna zanimanja',
        'Rudarski nadzornici/rudarske nadzornice',
        'Rukovatelji/rukovateljice postrojenjima za proizvodnju metala',
        'Tehničari/tehničarke za rudarstvo, metalurgiju i srodna zanimanja',
    ],
    'mechanics_shipbuilding': [
        'Alatničari/alatničarke i srodna zanimanja',
        'Instalateri/instalaterke i monteri/monterke cjevovoda',
        'Instalateri/instalaterke i serviseri/serviserke klimatizacijskih uređaja',
        'Inženjeri/inženjerke strojarstva',
        'Kovinoglodači/kovinoglodačice i srodna zanimanja',
        'Limari/limarice i srodna zanimanja',
        'Mehaničari/mehaničarke i monteri/monterke industrijskih i ostalih strojeva i srodna zanimanja',
        'Mehaničari/mehaničarke i monteri/monterke motornih vozila',
        'Monteri/monterke metalnih konstrukcija i srodna zanimanja',
        'Polirači/poliračice, brusači/brusačice, oštrači/oštračice',
        'Precizni mehaničari/precizne mehaničarke',
        'Rukovatelji/rukovateljice dizalicama i sličnim uređajima',
        'Rukovatelji/rukovateljice strojevima i uređajima, d. n.',
        'Rukovoditelji/rukovoditeljice energetskim i srodnim postrojenjima',
        'Strojari/strojarke kotlovnica i srodna zanimanja',
        'Tehničari/tehničarke strojarstva, brodogradnje i srodna zanimanja',
        'Tehnički crtači/tehničke crtačice',
        'Zavarivači/zavarivačice i srodna zanimanja',
        'Zlatari/zlatarice, draguljari/draguljarke i srodna zanimanja',
    ],
    'electrical': [
        'Elektroinstalateri/elektroinstalaterke i srodna zanimanja',
        'Elektromehaničari/elektromehaničarke',
        'Inženjeri/inženjerke elektroenergetike',
        'Inženjeri/inženjerke elektronike',
        'Mehaničari/mehaničarke i serviseri/serviserke elektronike',
        'Monteri/monterke, serviseri/serviserke informacijsko-komunikacijskih uređaja',
        'Procesni tehničari/procesne tehničarke d. n.',
        'Sastavljači/sastavljačice električne i elektroničke opreme',
        'Tehničari/tehničarke za elektroniku',
        'Tehničari/tehničarke za elektrotehniku i srodna zanimanja',
        'Tehničari/tehničarke za telekomunikacije',
    ],
    'chemical_food': [
        'Inženjeri/inženjerke kemijske tehnologije',
        'Kemičari/kemičarke',
        'Kemijsko-procesni tehničari/kemijsko-procesne tehničarke',
        'Mesari/mesarice i srodna zanimanja',
        'Nadzornici/nadzornice u proizvodnim djelatnostima',
        'Pekari/pekarice, slastičari/slastičarke i srodna zanimanja',
        'Rukovatelji/rukovateljice strojevima za proizvodnju gumenih proizvoda',
        'Rukovatelji/rukovateljice strojevima za proizvodnju plastičnih proizvoda',
        'Rukovatelji/rukovateljice strojevima za proizvodnju prehrambenih i sličnih proizvoda',
        'Staklari/staklarke i srodna zanimanja',
        'Tehničari/tehničarke za fiziku, kemiju i srodna zanimanja',
        'Tehničari/tehničarke za kemijsku i prehrambenu tehnologiju i biotehnologiju',
    ],
    'wood_paper_graphics': [
        'Izrađivači/izrađivačice tradicijskih proizvoda od drva i srodnih materijala, pletači/pletačice košara',
        'Grafički dizajneri/grafičke dizajnerice i dizajneri/dizajnerice multimedijskih sadržaja',
        'Rukovatelji/rukovateljice postrojenjima i strojevima za preradu drva',
        'Rukovatelji/rukovateljice tiskarskim, knjigoveškim i sličnim strojevima',
        'Slagari/slagarke',
        'Stolari/stolarice i srodna zanimanja',
        'Tiskari/tiskarice',
    ],
    'textiles_leather': [
        'Krojači/krojačice, krznari/krznarice i klobučari/klobučarke',
        'Obućari/obućarke, kožni galanteristi/kožne galanteristice i srodna zanimanja',
        'Rukovatelji/rukovateljice šivaćim strojevima',
        'Sastavljači/sastavljačice strojeva, uređaja i opreme, d. n.',
        'Stručnjaci/stručnjakinje tehničko-tehnoloških znanosti (osim elektrotehnike) d. n.',
        'Šivač/šivačice, vezilje i srodna zanimanja',
        'Tapetar/tapetarke i srodna zanimanja',
        'Tehničko-tehnološki tehničari/tehničko-tehnološke tehničarke d. n.',
    ],
    'transport': [
        'Časnici stroja/časnice stroja i srodna zanimanja',
        'Inženjeri/inženjerke urbanizma i prometa',
        'Kapetani/kapetanice plovila i srodna zanimanja',
        'Kormilari/kormilarice, brodske strojovođe/brodske strojovotkinje i mornari/mornarke',
        'Poštari/poštarice',
        'Prometni uredski službenici/prometne uredske službenice',
        'Rukovatelji/rukovateljice pokretnim poljoprivrednim i šumarskim strojevima',
        'Rukovatelji/rukovateljice teretom',
        'Rukovatelji/rukovateljice teškim kamionima s dizalicom',
        'Skladišni službenici/skladišne službenice',
        'Špediteri/špediterke i srodna zanimanja',
        'Vozači/vozačice autobusa i tramvaja',
        'Vozači/vozačice osobnih vozila, taksija i lakih dostavnih vozila',
        'Vozači/vozačice teretnih vozila i kamiona',
    ],
    'education': [
        'Instruktori/instruktorice vožnje',
        'Nastavnici/nastavnice u srednjim školama',
        'Pomoćnici učitelja/pomoćnice učitelja',
        'Predškolski odgajatelji/predškolske odgajateljice',
        'Stručnjaci/stručnjakinje za obrazovne metode',
        'Stručnjaci/stručnjakinje za odgoj i obrazovanje, d. n.',
        'Strukovni nastavnici/strukovne nastavnice',
        'Sveučilišni i visokoškolski nastavnici/sveučilišne i visokoškolske nastavnice',
        'Učitelji/učiteljice razredne/predmetne nastave u osnovnim školama',
    ],
    'social_humanities': [
        'Arhivisti/arhivistice i stručno muzejsko osoblje',
        'Filozofi/filozofkinje, povjesničari/povjesničarke i politolozi/politologinje',
        'Knjižničari/knjižničarke i informacijski stručnjaci/informacijske stručnjakinje',
        'Prevoditelji/prevoditeljice, tumači/tumačiteljice i drugi jezikoslovci/druge jezikoslovke',
        'Psiholozi/psihologinje',
        'Sociolozi/sociologinje, antropolozi/antropologinje i srodni stručnjaci/srodne stručnjakinje',
    ],
    'science_math': [
        'Laboratorijski tehničari/laboratorijske tehničarke u biomedicini, biologiji, farmakologiji (osim medicinsko-laboratorijskih tehničara)',
        'Biolozi/biologinje, botaničari/botaničarke, zoolozi/zoologinje i srodni stručnjaci/stručnjakinje',
        'Fizičari/fizičarke i astronomi/astronomke',
        'Inženjeri/inženjerke industrijskog inženjerstva i proizvodnje',
    ],
    'health_social_care': [
        'Asistenti/asistentice zdravstvene njege',
        'Audiolozi/audiologinje i logopedi/logopedinje',
        'Dentalni asistenti i terapeuti/dentalne asistentice i terapeutkinje',
        'Dentalni i ortopedski tehničari/dentalne i ortopedske tehničarke',
        'Djelatnici/djelatnice za zdravstvenu i socijalnu skrb u kući',
        'Doktori/doktorice dentalne medicine',
        'Doktori/doktorice medicine rada',
        'Farmaceuti/farmaceutkinje',
        'Farmaceutski tehničari/farmaceutske tehničarke',
        'Fizioterapeuti/fizioterapeutkinje',
        'Fizioterapeutski tehničari/fizioterapeutske tehničarke',
        'Liječnici specijalisti/liječnice specijalistice',
        'Liječnici/liječnice opće prakse',
        'Medicinske sestre/medicinski tehničari',
        'Medicinske sestre-primalje',
        'Njegovatelji/njegovateljice djece',
        'Optičari/optičarke',
        'Socijalni radnici/socijalne radnice',
        'Stručnjaci/stručnjakinje edukacijsko-rehabilitacijskog profila',
        'Stručnjaci/stručnjakinje za socijalni rad i savjetovanje',
        'Tehničari/tehničarke medicinskih uređaja',
        'Zdravstveni stručnjaci/zdravstvene stručnjakinje d. n.',
        'Zdravstveno-laboratorijski i patološki tehničari/zdravstveno-laboratorijske i patološke tehničarke',
    ],
    'agriculture_forestry_fishing': [
        'Poljoprivredni tehničari/poljoprivredne tehničarke',
        'Radnici/radnice na poljoprivrednom gospodarstvu mješovite proizvodnje (biljne i stočarske)',
        'Radnici/radnice na ratarskoj farmi',
        'Radnici/radnice na stočarskoj farmi',
        'Radnici/radnice u ribarstvu i akvakulturi',
        'Radnici/radnice za jednostavne šumarske radove',
        'Radnici/radnice za jednostavne vrtlarske i hortikulturne radove',
        'Ribari/ribarke na otvorenom moru',
        'Savjetnici/savjetnice u poljoprivredi, šumarstvu i ribarstvu',
        'Stočari/stočarice',
        'Šumari/šumarke i srodna zanimanja',
        'Šumarski tehničari/šumarske tehničarke',
        'Veterinari/veterinarke',
        'Veterinarski tehničari, asistenti/veterinarske tehničarke, asistentice',
        'Voćari/voćarke, vinogradari/vinogradarke i srodna zanimanja',
        'Vrtlari/vrtlarke, hortikulturni djelatnici/hortikulturne djelatnice i srodna zanimanja',
    ],
    'home_communal_other': [
        'Čistači/čistačice i kućne pomoćnice i srodna zanimanja',
        'Čistači/čistačice ulica i srodna zanimanja',
        'Dimnjačari/dimnjačarke, perači/peračice fasada',
        'Djelatnici/djelatnice za zaštitu osoba i imovine, d.n.',
        'Frizeri/frizerke',
        'Grobari/grobarke, pogrebnici/pogrebnice i srodna zanimanja',
        'Jednostavna zanimanja u prerađivačkoj industriji, d. n.',
        'Kozmetičari/kozmetičarke, pedikeri/pedikerke i srodna zanimanja',
        'Kriminalisti/kriminalistkinje i srodna zanimanja',
        'Kućepazitelji/kućepaziteljice',
        'Lakireri/lakirerice i srodna zanimanja',
        'Ostali čistači/ostale čistačice',
        'Pralje i glačarice',
        'Pratitelj/pratiteljice i posluga',
        'Praznitelji/prazniteljice prodajnih automata, parkirališnih satova i sl.',
        'Radnici/radnice na razvrstavanju otpada',
        'Ronioci/roniteljice',
        'Ručni pakiratelji/ručne pakirateljice',
        'Ručni perači/ručne peračice automobila',
        'Ručni perači/ručne peračice prozora',
        'Rukovatelji/rukovateljice strojevima za pranje',
        'Športski treneri/trenerice; športski instruktori/instruktorice',
        'Vatrogasci/vatrogaskinje',
        'Zaštitari/zaštitarke',
    ],
    'directors_managers': [
        'Ravnatelji/ravnateljice osnovnih škola i srednjoškolskih ustanova te dekani/dekanice i rektori/rektorice visokih učilišta',
        'Direktori/direktorice ljudskih potencijala',
        'Direktori/direktorice prodaje i marketinga',
        'Direktori/direktorice restorana i srodnih ugostiteljskih objekata',
        'Direktori/direktorice športskih, rekreacijskih i kulturnih centara',
        'Dužnosnici/dužnosnice tijela državne vlasti',
        'Ravnatelji/ravnateljice ustanova za pružanje stručnih usluga d. n.',
        'Ravnatelji/ravnateljice ustanova za pružanje usluga njege starijim osobama',
        'Ravnatelji/ravnateljice zdravstvenih ustanova',
        'Vojna zanimanja, ostali činovi',
    ],
}

HZZ_CATEGORIES = {
    "it": "Informatički, računalni i stručnjaci za Internet",
    "economy_admin": "Ekonomisti, pravnici, administrativci",
    "arts_culture": "Umjetnici, dizajneri, stručnjaci u kulturi",
    "construction": "Građevinari, arhitekti, geodete",
    "trade_marketing": "Trgovci, promotori, oglašivači",
    "hospitality_tourism": "Ugostitelji, radnici u turizmu",
    "geology_mining_metallurgy": "Geolozi, rudari, metalurzi",
    "mechanics_shipbuilding": "Strojari i brodograditelji",
    "electrical": "Stručnjaci za elektrotehniku",
    "chemical_food": "Kemijski i prehrambeni stručnjaci",
    "wood_paper_graphics": "Obrađivači drva, proizvođači papira, grafičari",
    "textiles_leather": "Tekstilci i kožari",
    "transport": "Stručnjaci u prometu",
    "education": "Učitelji, nastavnici, profesori",
    "social_humanities": "Društveno-humanistički stručnjaci",
    "science_math": "Prirodoslovno-matematički stručnjaci",
    "health_social_care": "Zdravstvo, socijalna skrb, njega",
    "agriculture_forestry_fishing": "Poljoprivredni, šumarski, ribarski stručnjaci",
    "home_communal_other": "Usluge u kući, komunalne usluge i ostala zanimanja",
    "directors_managers": "Direktori, ravnatelji, čelnici",
}


def extract_email(text: str) -> str:
    for match in EMAIL_RE.finditer(text or ""):
        email = match.group(0).strip()
        if email.casefold() not in GENERIC_EMAILS:
            return email
    return ""


def extract_phone(text: str) -> str:
    match = PHONE_RE.search(text or "")
    if not match:
        return ""

    phone = re.sub(r"\s+", " ", match.group(0)).strip(" .,-;")
    digits = re.sub(r"\D", "", phone)
    return phone if len(digits) >= 6 else ""


def extract_address(text: str) -> str:
    for raw_line in (text or "").splitlines():
        line = _clean_text(raw_line)
        if not line or "mjesto rada" in line.casefold():
            continue
        if re.search(r"\d{5}\s+\S+", line):
            sanitized = re.sub(
                r"^(adresa|adresa poslodavca|sjedište|sjediste|kontakt|poslodavac)\s*:\s*",
                "",
                line,
                flags=re.IGNORECASE,
            )
            return sanitized

    match = ADDRESS_RE.search(text or "")
    return _clean_text(match.group(1)) if match else ""


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _strip_hzz_count(value: str) -> str:
    return re.sub(r"\s+\d+$", "", _clean_text(value))


def _slugify_category(value: str) -> str:
    normalized = (value or "").casefold()
    replacements = {
        "č": "c",
        "ć": "c",
        "š": "s",
        "ž": "z",
        "đ": "d",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def _company_limit_key(company: str, detail_url: str) -> str:
    normalized_company = _slugify_category(company or "")
    if normalized_company:
        return normalized_company
    return detail_url


def get_hzz_categories() -> list[dict[str, str]]:
    return [{"key": key, "label": label} for key, label in HZZ_CATEGORIES.items()]


def get_hzz_category_groups(category: str) -> list[str]:
    if category in HZZ_CATEGORY_GROUPS:
        return list(HZZ_CATEGORY_GROUPS[category])
    candidate_slug = _slugify_category(category)
    for key, groups in HZZ_CATEGORY_GROUPS.items():
        if candidate_slug in {key, _slugify_category(HZZ_CATEGORIES.get(key, ""))}:
            return list(groups)
    return []


def _resolve_category(category: str | None) -> str | None:
    if not category:
        return None

    candidate = category.strip()
    if candidate in HZZ_CATEGORIES:
        return HZZ_CATEGORIES[candidate]

    candidate_slug = _slugify_category(candidate)
    for key, label in HZZ_CATEGORIES.items():
        if candidate_slug in {key, _slugify_category(label)}:
            return label

    available = ", ".join(HZZ_CATEGORIES.keys())
    raise ValueError(f"Unknown HZZ category '{category}'. Available keys: {available}")


def _parse_row_text(text: str, title: str) -> tuple[str, str]:
    company = ""
    location = ""

    lines = [_clean_text(line) for line in text.splitlines() if _clean_text(line)]
    for line in lines:
        normalized = line.casefold()
        if normalized.startswith("poslodavac:"):
            company = line.split(":", 1)[1].strip()
        elif normalized.startswith("mjesto rada:"):
            location = line.split(":", 1)[1].strip()

    if company and location:
        return company, location

    flattened = " | ".join(lines)
    company_match = re.search(
        r"Poslodavac:\s*(.+?)(?=\s+\|\s+(?:Rok za prijavu|Mjesto rada|Traženo radnika)\s*:|$)",
        flattened,
        re.IGNORECASE,
    )
    location_match = re.search(
        r"Mjesto rada:\s*(.+?)(?=\s+\|\s+(?:Traženo radnika|Poslodavac|Rok za prijavu)\s*:|$)",
        flattened,
        re.IGNORECASE,
    )

    if company_match and not company:
        company = _clean_text(company_match.group(1))
    if location_match and not location:
        location = _clean_text(location_match.group(1))

    if not company or not location:
        filtered = [line for line in lines if line != title and ":" not in line]
        if filtered:
            if not location:
                location = filtered[0]
            if len(filtered) > 1 and not company:
                company = filtered[1]

    return company, location


def _collect_listing_rows(page: Page) -> list[dict]:
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    anchors = page.locator(f"a.TitleLink[href*='{DETAIL_URL_KEYWORD}']")
    total = anchors.count()

    for index in range(total):
        try:
            anchor = anchors.nth(index)
            title = _clean_text(anchor.inner_text(timeout=5000))
            href = anchor.get_attribute("href", timeout=5000)
            if not href:
                continue

            detail_url = urljoin(page.url, href)
            if not title or detail_url in seen_urls:
                continue

            container = anchor.locator(
                "xpath=ancestor::*[self::tr or self::article or self::li or self::div][.//a[contains(@href, 'RadnoMjesto_Ispis.aspx')]][1]"
            )
            row_text = container.inner_text(timeout=5000)
            company, location = _parse_row_text(row_text, title)

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "employees_needed": "",
                    "employment_type": "",
                    "working_hours": "",
                    "accommodation": "",
                    "valid_from": "",
                    "valid_to": "",
                    "jobseeker_info": "",
                    "detail_url": detail_url,
                }
            )
            seen_urls.add(detail_url)
        except Exception as exc:
            print(f"[hzz] Skipping broken row {index}: {exc}")

    return jobs


def _open_detail_page(detail_page: Page, detail_url: str) -> tuple[str, str]:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(300)
        text = detail_page.locator("body").inner_text(timeout=5000)
        email = extract_email(text)
        phone = extract_phone(text)
        return email, phone
    except Exception as exc:
        print(f"[hzz] Failed to scrape detail page {detail_url}: {exc}")
        return "", ""


def _extract_labeled_value(text: str, label: str, stop_labels: list[str] | None = None) -> str:
    stop_labels = stop_labels or []
    pattern = re.escape(label) + r"\s*(.+?)"
    if stop_labels:
        stop_pattern = "|".join(re.escape(stop_label) for stop_label in stop_labels)
        pattern += r"(?=\n(?:" + stop_pattern + r")|$)"
    else:
        pattern += r"(?=\n|$)"

    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _default_stop_labels(label: str, extra_stop_labels: list[str] | None = None) -> list[str]:
    extra_stop_labels = extra_stop_labels or []
    return [item for item in DETAIL_SECTION_LABELS if item != label] + extra_stop_labels


def _extract_hzz_value(text: str, label: str, extra_stop_labels: list[str] | None = None) -> str:
    return _extract_labeled_value(text, label, _default_stop_labels(label, extra_stop_labels))


def _extract_section(text: str, heading: str, stop_headings: list[str]) -> str:
    pattern = (
        re.escape(heading)
        + r"\s*(.+?)(?=\n(?:"
        + "|".join(re.escape(stop_heading) for stop_heading in stop_headings)
        + r")\s*$)"
    )
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _extract_detail_fields(text: str) -> dict:
    normalized_text = re.sub(r"\r\n?", "\n", text or "")
    posloprimac_info = _extract_section(
        normalized_text,
        "Posloprimac",
        ["Poslodavac"],
    )
    poslodavac_info = _extract_section(
        normalized_text,
        "Poslodavac",
        ["Kontakt:", "Uvjeti na radnom mjestu:", "Potrebna zvanja:", "Posloprimac"],
    )

    return {
        "employees_needed": _extract_hzz_value(normalized_text, "Broj traženih radnika:"),
        "employment_type": _extract_hzz_value(normalized_text, "Vrsta zaposlenja:"),
        "working_hours": _extract_hzz_value(normalized_text, "Radno vrijeme:"),
        "accommodation": _extract_hzz_value(normalized_text, "Smještaj:"),
        "valid_from": _extract_hzz_value(normalized_text, "Natječaj vrijedi od:"),
        "valid_to": _extract_hzz_value(normalized_text, "Natječaj vrijedi do:"),
        "jobseeker_info": posloprimac_info,
        "employer_address": extract_address(poslodavac_info or normalized_text),
    }


def _scrape_detail_page(detail_page: Page, detail_url: str) -> dict:
    try:
        detail_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
        detail_page.wait_for_timeout(300)
        text = detail_page.locator("body").inner_text(timeout=5000)
        details = _extract_detail_fields(text)
        details["email"] = extract_email(text)
        details["phone"] = extract_phone(text)
        return details
    except Exception as exc:
        print(f"[hzz] Failed to scrape detail page {detail_url}: {exc}")
        return {
            "email": "",
            "phone": "",
            "employer_address": "",
            "employees_needed": "",
            "employment_type": "",
            "working_hours": "",
            "accommodation": "",
            "valid_from": "",
            "valid_to": "",
            "jobseeker_info": "",
        }


def _select_category(page: Page, category_label: str) -> None:
    page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1000)

    category_link = page.locator("a").filter(has_text=category_label).first
    if category_link.count() == 0:
        raise ValueError(f"HZZ category link not found for '{category_label}'")

    category_link.click(timeout=10000)
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    page.wait_for_timeout(1000)


def _discover_category_group_links(page: Page, category_label: str) -> list[dict[str, str]]:
    groups = page.evaluate(
        """(categoryLabel) => {
            const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
            const categoryText = normalize(categoryLabel).toLocaleLowerCase("hr-HR");
            const anchors = Array.from(document.querySelectorAll("a"));
            const categoryAnchor = anchors.find((anchor) => {
                const text = normalize(anchor.textContent).toLocaleLowerCase("hr-HR");
                const id = anchor.id || "";
                return text.startsWith(categoryText) && id.includes("DataList1");
            });
            if (!categoryAnchor || !categoryAnchor.id) {
                return [];
            }

            const categoryPrefix = categoryAnchor.id.replace(/_(?:lnkKategorija|LinkButton1)$/, "");
            const seen = new Set();
            return anchors
                .filter((anchor) => {
                    const id = anchor.id || "";
                    const href = anchor.getAttribute("href") || "";
                    return id.startsWith(`${categoryPrefix}_Skupine_`) && href.includes("__doPostBack");
                })
                .map((anchor) => ({
                    label: normalize(anchor.textContent).replace(/\\s+\\d+$/, ""),
                    href: anchor.getAttribute("href") || "",
                }))
                .filter((group) => {
                    if (!group.label || seen.has(group.label)) {
                        return false;
                    }
                    seen.add(group.label);
                    return true;
                });
        }""",
        category_label,
    )
    return [
        {"label": _strip_hzz_count(item.get("label", "")), "href": item.get("href", "")}
        for item in groups
        if item.get("label") and item.get("href")
    ]


def _run_postback_href(page: Page, href: str) -> bool:
    match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href or "")
    if not match:
        return False

    page.evaluate(
        """({target, argument}) => {
            if (typeof window.__doPostBack !== "function") {
                return false;
            }
            window.__doPostBack(target, argument);
            return true;
        }""",
        {"target": match.group(1), "argument": match.group(2)},
    )
    return True


def _select_category_group(page: Page, category_label: str, group_label: str) -> None:
    page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1000)

    groups = _discover_category_group_links(page, category_label)
    group_slug = _slugify_category(group_label)
    group_href = next(
        (
            item["href"]
            for item in groups
            if _slugify_category(item["label"]) == group_slug
        ),
        "",
    )
    if not group_href:
        raise ValueError(
            f"HZZ group link not found for '{group_label}' in category '{category_label}'"
        )

    if not _run_postback_href(page, group_href):
        raise ValueError(
            f"HZZ group link has unsupported navigation for '{group_label}'"
        )

    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    page.wait_for_timeout(1000)


def _wait_after_listing_change(page: Page) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except PlaywrightTimeoutError:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    try:
        page.locator(f"a.TitleLink[href*='{DETAIL_URL_KEYWORD}']").first.wait_for(timeout=7000)
    except PlaywrightTimeoutError:
        pass
    page.wait_for_timeout(1000)


def _listing_row_count(page: Page) -> int:
    try:
        return page.locator(f"a.TitleLink[href*='{DETAIL_URL_KEYWORD}']").count()
    except Exception:
        return 0


def _set_results_per_page(page: Page, per_page: int = 75) -> bool:
    target = str(per_page)
    selects = page.locator("select")

    for index in range(selects.count()):
        try:
            select = selects.nth(index)
            candidate = select.evaluate(
                """(element, target) => {
                    const rect = element.getBoundingClientRect();
                    if (!rect.width || !rect.height) {
                        return null;
                    }

                    const options = Array.from(element.options || []);
                    const numericTexts = new Set(
                        options
                            .map((item) => (item.textContent || "").trim())
                            .filter((text) => /^\\d+$/.test(text))
                    );
                    for (const expected of ["10", "25", "50", "75"]) {
                        if (!numericTexts.has(expected)) {
                            return null;
                        }
                    }

                    const option = options.find((item) => {
                        const text = (item.textContent || "").trim();
                        return text === target || item.value === target;
                    });
                    return option ? {value: option.value, previous: element.value} : null;
                }""",
                target,
            )
            if not candidate:
                continue

            option_value = candidate["value"]
            previous_value = candidate["previous"]
            if select.input_value(timeout=2000) == option_value:
                return True

            select.select_option(value=option_value, timeout=5000)
            _wait_after_listing_change(page)

            if _listing_row_count(page) > 0:
                print(f"[hzz] Results per page set to {per_page}")
                return True

            print(
                f"[hzz] Results per page {per_page} returned no rows; "
                "reverting to previous page size"
            )
            select.select_option(value=previous_value, timeout=5000)
            _wait_after_listing_change(page)
            return False
        except Exception as exc:
            print(f"[hzz] Failed to set results per page on select {index}: {exc}")

    return False


def _page_identity(page: Page) -> str:
    try:
        anchors = page.locator(f"a.TitleLink[href*='{DETAIL_URL_KEYWORD}']")
        if anchors.count() == 0:
            return ""
        return anchors.first.get_attribute("href", timeout=5000) or ""
    except Exception:
        return ""


def _find_next_page_link(page: Page, current_page: int):
    target_page = current_page + 1
    candidates: list[tuple[int, int]] = []
    links = page.locator("a")

    for index in range(links.count()):
        try:
            link = links.nth(index)
            href = link.get_attribute("href", timeout=1000) or ""
            match = re.search(r"Page\$(\d+)", href)
            if match:
                page_number = int(match.group(1))
            else:
                link_text = _clean_text(link.inner_text(timeout=1000))
                if not link_text.isdigit() or "__doPostBack" not in href:
                    continue
                page_number = int(link_text)

            if page_number < target_page:
                continue

            candidates.append((page_number, index))
        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0] != target_page, item[0]))
    return links.nth(candidates[0][1])


def _postback_to_listing_page(page: Page, target_page: int) -> bool:
    identity_before = _page_identity(page)
    try:
        page.evaluate(
            """(targetPage) => {
                if (typeof window.__doPostBack !== "function") {
                    return false;
                }
                window.__doPostBack("ctl00$MainContent$gwSearch", `Page$${targetPage}`);
                return true;
            }""",
            target_page,
        )
        _wait_after_listing_change(page)
    except Exception as exc:
        print(f"[hzz] Direct pagination to page {target_page} failed: {exc}")
        return False

    identity_after = _page_identity(page)
    return bool(identity_after and identity_after != identity_before)


def _go_to_next_page(page: Page, current_page: int) -> bool:
    try:
        target_page = current_page + 1
        next_button = page.locator(
            "ul.pagination a:has-text('Sljede'), ul.pagination a[aria-label*='Sljede'], "
            "a:has-text('Sljede'), button:has-text('Sljede'), input[value*='Sljede']"
        ).first
        if next_button.count() == 0:
            next_button = _find_next_page_link(page, current_page)

        if next_button is None or next_button.count() == 0:
            for attempt in range(1, 4):
                if _postback_to_listing_page(page, target_page):
                    return True
                print(
                    f"[hzz] Direct pagination to page {target_page} "
                    f"did not change listings; retry {attempt}/3"
                )
                page.wait_for_timeout(1000)
            return False

        identity_before = _page_identity(page)
        next_button.click(timeout=10000)
        _wait_after_listing_change(page)

        identity_after = _page_identity(page)
        if identity_after and identity_after != identity_before:
            return True

        print(
            f"[hzz] Pagination click to page {target_page} "
            "did not change listings; trying direct pagination"
        )
        for attempt in range(1, 4):
            if _postback_to_listing_page(page, target_page):
                return True
            print(
                f"[hzz] Direct pagination to page {target_page} "
                f"did not change listings; retry {attempt}/3"
            )
            page.wait_for_timeout(1000)

        return False
    except PlaywrightTimeoutError:
        print(f"[hzz] Timeout while moving to page {current_page + 1}")
        return False
    except Exception as exc:
        print(f"[hzz] Failed to paginate to page {current_page + 1}: {exc}")
        return False


def scrape_hzz(
    max_pages: int = 3,
    category: str | None = None,
    group: str | None = None,
    company_limit: int | None = None,
    start_page: int = 1,
    results_per_page: int | None = 75,
    use_subgroups: bool = True,
) -> list[dict]:
    headless = os.getenv("HEADLESS", "true") == "true"
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    seen_company_keys: set[str] = set()
    resolved_category = _resolve_category(category)
    start_page = max(1, start_page)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        detail_page = context.new_page()

        try:
            def scrape_selected_listing(listing_label: str = "") -> None:
                if results_per_page is not None:
                    _set_results_per_page(page, results_per_page)

                if start_page > 1 and not _postback_to_listing_page(page, start_page):
                    raise RuntimeError(f"Could not move to HZZ listing page {start_page}")

                end_page = start_page + max_pages - 1
                for page_number in range(start_page, end_page + 1):
                    try:
                        page_jobs = _collect_listing_rows(page)
                        if not page_jobs and page_number > 1:
                            print(f"[hzz] Listing page {page_number} returned no rows; retrying")
                            if _postback_to_listing_page(page, page_number):
                                page_jobs = _collect_listing_rows(page)

                        prefix = f"[{listing_label}] " if listing_label else ""
                        print(
                            f"[hzz] {prefix}Scraping listing page {page_number}: "
                            f"{len(page_jobs)} rows"
                        )
                        for job in page_jobs:
                            detail_url = job["detail_url"]
                            if detail_url in seen_urls:
                                continue

                            detail_fields = _scrape_detail_page(detail_page, detail_url)
                            job.update(detail_fields)
                            job["source"] = "hzz"
                            if listing_label:
                                job["group"] = listing_label

                            if company_limit is not None:
                                company_key = _company_limit_key(job.get("company", ""), detail_url)
                                if company_key in seen_company_keys:
                                    continue
                                seen_company_keys.add(company_key)

                            jobs.append(job)
                            seen_urls.add(detail_url)
                            time.sleep(0.2)

                            if company_limit is not None and len(seen_company_keys) >= company_limit:
                                break
                    except Exception as exc:
                        print(f"[hzz] Failed while scraping listing page {page_number}: {exc}")

                    if company_limit is not None and len(seen_company_keys) >= company_limit:
                        break

                    if page_number >= end_page:
                        break

                    if not _go_to_next_page(page, page_number):
                        break

            if resolved_category and group:
                _select_category_group(page, resolved_category, group)
                scrape_selected_listing(group)
            elif resolved_category and use_subgroups:
                page.goto(LANDING_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1000)
                groups = _discover_category_group_links(page, resolved_category)
                if groups:
                    print(f"[hzz] Found {len(groups)} subgroups for category '{resolved_category}'")
                    for group_item in groups:
                        group_label = group_item["label"]
                        print(f"[hzz] Scraping subgroup: {group_label}")
                        _select_category_group(page, resolved_category, group_label)
                        scrape_selected_listing(group_label)
                        if company_limit is not None and len(seen_company_keys) >= company_limit:
                            break
                else:
                    print(
                        f"[hzz] No subgroups found for category '{resolved_category}', "
                        "scraping category directly"
                    )
                    _select_category(page, resolved_category)
                    scrape_selected_listing()
            else:
                if resolved_category:
                    _select_category(page, resolved_category)
                else:
                    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(1000)
                scrape_selected_listing()
        finally:
            context.close()
            browser.close()

    return jobs


if __name__ == "__main__":
    jobs = scrape_hzz(max_pages=2, category="hospitality_tourism")
    print(len(jobs))
    print(jobs[:3])
