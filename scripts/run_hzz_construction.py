import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scrape_hzz_category import run

run("construction", [
    "Rukovatelji/rukovateljice strojevima za proizvodnju cementnih, mineralnih proizvoda i ostalih proizvoda od kamena",
    "Armirači i betonirci/armiračice i betonirke",
    "Fasaderi i gipsari/fasaderke i gipsarice",
    "Graditelji/graditeljice kuća",
    "Izolateri/izolaterke",
    "Kamenoresci/kamenoreskinje, klesari/klesarice i srodna zanimanja",
    "Krovopokrivači/krovopokrivačice",
    "Podopolagači/podopolagačice i srodna zanimanja",
    "Radnici/radnice u niskogradnji",
    "Radnici/radnice u visokogradnji",
    "Rukovatelji/rukovateljice građevinskim i sličnim strojevima",
    "Soboslikari/soboslikarice, ličioci/ličiteljice i srodna zanimanja",
    "Tesari i građevinski stolari/tesarice i građevinske stolarice",
    "Zidari/zidarice i srodna građevinska zanimanja d.n.",
    "Zidari/zidarke i srodna zanimanja",
])
