import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_category import run
run("oeffentlicher_dienst_sicherheit")
