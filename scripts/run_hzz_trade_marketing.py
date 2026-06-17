import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("trade_marketing", [
    "Blagajnici/blagajnice, prodavači/prodavačice ulaznica i srodna zanimanja",
    "Prodavači/prodavačice u trgovinama",
    "Prodavači/prodavačice, d. n.",
    "Punjači/punjačice polica",
])
