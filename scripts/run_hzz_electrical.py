import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("electrical", [
    "Elektroinstalateri/elektroinstalaterke i srodna zanimanja",
    "Elektromehaničari/elektromehaničarke",
    "Monteri/monterke, serviseri/serviserke informacijsko-komunikacijskih uređaja",
    "Procesni tehničari/procesne tehničarke d. n.",
    "Sastavljači/sastavljačice električne i elektroničke opreme",
    "Tehničari/tehničarke za elektroniku",
    "Tehničari/tehničarke za elektrotehniku i srodna zanimanja",
    "Tehničari/tehničarke za telekomunikacije",
])
