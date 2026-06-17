import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("textiles_leather", [
    "Krojači/krojačice, krznari/krznarice i klobučari/klobučarke",
    "Obućari/obućarke, kožni galanteristi/kožne galanteristice i srodna zanimanja",
    "Rukovatelji/rukovateljice šivaćim strojevima",
    "Sastavljači/sastavljačice strojeva, uređaja i opreme, d. n.",
    "Stručnjaci/stručnjakinje tehničko-tehnoloških znanosti (osim elektrotehnike) d. n.",
    "Šivač/šivačice, vezilje i srodna zanimanja",
    "Tapetar/tapetarke i srodna zanimanja",
    "Tehničko-tehnološki tehničari/tehničko-tehnološke tehničarke d. n.",
])
