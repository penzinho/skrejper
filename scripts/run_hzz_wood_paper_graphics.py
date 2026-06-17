import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("wood_paper_graphics", [
    "Izrađivači/izrađivačice tradicijskih proizvoda od drva i srodnih materijala, pletači/pletačice košara",
    "Grafički dizajneri/grafičke dizajnerice i dizajneri/dizajnerice multimedijskih sadržaja",
    "Rukovatelji/rukovateljice postrojenjima i strojevima za preradu drva",
    "Rukovatelji/rukovateljice tiskarskim, knjigoveškim i sličnim strojevima",
    "Slagari/slagarke",
    "Stolari/stolarice i srodna zanimanja",
])
