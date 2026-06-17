import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("mechanics_shipbuilding", [
    "Alatničari/alatničarke i srodna zanimanja",
    "Instalateri/instalaterke i monteri/monterke cjevovoda",
    "Instalateri/instalaterke i serviseri/serviserke klimatizacijskih uređaja",
    "Kovinoglodači/kovinoglodačice i srodna zanimanja",
    "Limari/limarice i srodna zanimanja",
    "Mehaničari/mehaničarke i monteri/monterke industrijskih i ostalih strojeva i srodna zanimanja",
    "Mehaničari/mehaničarke i monteri/monterke motornih vozila",
    "Monteri/monterke metalnih konstrukcija i srodna zanimanja",
    "Polirači/poliračice, brusači/brusačice, oštrači/oštračice",
    "Precizni mehaničari/precizne mehaničarke",
    "Rukovatelji/rukovateljice dizalicama i sličnim uređajima",
    "Rukovatelji/rukovateljice strojevima i uređajima, d. n.",
    "Rukovoditelji/rukovoditeljice energetskim i srodnim postrojenjima",
    "Strojari/strojarke kotlovnica i srodna zanimanja",
    "Tehničari/tehničarke strojarstva, brodogradnje i srodna zanimanja",
    "Tehnički crtači/tehničke crtačice",
    "Zavarivači/zavarivačice i srodna zanimanja",
    "Zlatari/zlatarice, draguljari/draguljarke i srodna zanimanja",
])
