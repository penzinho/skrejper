import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("home_communal_other", [
    "Čistači/čistačice i kućne pomoćnice i srodna zanimanja",
    "Čistači/čistačice ulica i srodna zanimanja",
    "Dimnjačari/dimnjačarke, perači/peračice fasada",
    "Frizeri/frizerke",
    "Grobari/grobarke, pogrebnici/pogrebnice i srodna zanimanja",
    "Jednostavna zanimanja u prerađivačkoj industriji, d. n.",
    "Kozmetičari/kozmetičarke, pedikeri/pedikerke i srodna zanimanja",
    "Kriminalisti/kriminalistkinje i srodna zanimanja",
    "Kućepazitelji/kućepaziteljice",
    "Lakireri/lakirerice i srodna zanimanja",
    "Ostali čistači/ostale čistačice",
    "Pralje i glačarice",
    "Pratitelj/pratiteljice i posluga",
    "Praznitelji/prazniteljice prodajnih automata, parkirališnih satova i sl.",
    "Radnici/radnice na razvrstavanju otpada",
    "Ronioci/roniteljice",
    "Ručni pakiratelji/ručne pakirateljice",
    "Ručni perači/ručne peračice automobila",
    "Ručni perači/ručne peračice prozora",
    "Rukovatelji/rukovateljice strojevima za pranje",
    "Zaštitari/zaštitarke",
])
