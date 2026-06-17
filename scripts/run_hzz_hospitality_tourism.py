import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("hospitality_tourism", [
    "Barmeni/barmenice",
    "Domaćinska zanimanja u uredima, hotelima i ostalim objektima",
    "Dostavljači, nosači/dostavljačice, nosačice i srodna zanimanja",
    "Glavni kuhari/glavne kuharice",
    "Hotelski recepcionari/hotelske recepcionarke",
    "Konobari/konobarice",
    "Kuhari/kuharice",
    "Kuhinjski pomoćnik/kuhinjske pomoćnice",
    "Kušači i ocjenjivači/kušateljice i ocjenjivačice namirnica",
    "Priprematelji/pripremateljice brze hrane",
    "Recepcionari/recepcionarke (općenito)",
    "Službenici/službenice u kladionici, kasinu i srodna zanimanja",
    "Uslužna zanimanja, d. n.",
])
