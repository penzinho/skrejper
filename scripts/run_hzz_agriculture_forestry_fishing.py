import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("agriculture_forestry_fishing", [
    "Poljoprivredni tehničari/poljoprivredne tehničarke",
    "Radnici/radnice na poljoprivrednom gospodarstvu mješovite proizvodnje (biljne i stočarske)",
    "Radnici/radnice na ratarskoj farmi",
    "Radnici/radnice na stočarskoj farmi",
    "Radnici/radnice u ribarstvu i akvakulturi",
    "Radnici/radnice za jednostavne šumarske radove",
    "Radnici/radnice za jednostavne vrtlarske i hortikulturne radove",
    "Ribari/ribarke na otvorenom moru",
    "Savjetnici/savjetnice u poljoprivredi, šumarstvu i ribarstvu",
    "Stočari/stočarice",
    "Šumari/šumarke i srodna zanimanja",
    "Voćari/voćarke, vinogradari/vinogradarke i srodna zanimanja",
    "Vrtlari/vrtlarke, hortikulturni djelatnici/hortikulturne djelatnice i srodna zanimanja",
])
