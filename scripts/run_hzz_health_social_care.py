import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("health_social_care", [
    "Asistenti/asistentice zdravstvene njege",
    "Djelatnici/djelatnice za zdravstvenu i socijalnu skrb u kući",
    "Medicinske sestre/medicinski tehničari",
    "Medicinske sestre-primalje",
    "Njegovatelji/njegovateljice djece",
])
