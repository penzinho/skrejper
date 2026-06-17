import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("transport", [
    "Časnici stroja/časnice stroja i srodna zanimanja",
    "Inženjeri/inženjerke urbanizma i prometa",
    "Kapetani/kapetanice plovila i srodna zanimanja",
    "Kormilari/kormilarice, brodske strojovođe/brodske strojovotkinje i mornari/mornarke",
    "Poštari/poštarice",
    "Rukovatelji/rukovateljice pokretnim poljoprivrednim i šumarskim strojevima",
    "Rukovatelji/rukovateljice teretom",
    "Rukovatelji/rukovateljice teškim kamionima s dizalicom",
    "Skladišni službenici/skladišne službenice",
    "Vozači/vozačice autobusa i tramvaja",
    "Vozači/vozačice osobnih vozila, taksija i lakih dostavnih vozila",
    "Vozači/vozačice teretnih vozila i kamiona",
])
