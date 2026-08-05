"""Flag public-sector employers (schools, hospitals, municipal bodies, ...).

NSZ and ZZZ RS listings lean heavily on the public sector, which is never a
staffing lead. Matching is on the *normalized* employer name; hits are flagged
(``public_sector_term``), never deleted, because the term list will always have
false positives (a private "škola stranih jezika" is a perfectly good lead).
"""

import re

from app.leadgen.normalize import norm_text

# Substring terms: long enough that a hit is meaningful anywhere in the name.
_SUBSTRING_TERMS = (
    "osnovna skola", "srednja skola", "muzicka skola", "tehnicka skola",
    "medicinska skola", "poljoprivredna skola", "elektrotehnicka skola",
    "gimnazija", "fakultet", "univerzitet", "sveuciliste", "visoka skola",
    "dom zdravlja", "zdravstveni centar", "klinicki centar", "opsta bolnica",
    "opca bolnica", "bolnica", "institut za javno zdravlje",
    "predskolska ustanova", "djeciji vrtic", "decji vrtic", "djecji vrtic",
    "centar za socijalni rad", "gerontoloski centar", "narodna biblioteka",
    "gradska biblioteka", "narodno pozoriste", "narodni muzej", "zavicajni muzej",
    "javna ustanova", "javno preduzece", "javno komunalno", "komunalno preduzece",
    "ministarstvo", "opstinska uprava", "opcinska uprava", "gradska uprava",
    "poreska uprava", "porezna uprava", "republicki zavod", "nacionalna sluzba",
    "zavod za zaposljavanje", "zavod za javno zdravstvo", "fond zdravstvenog",
    "penzijsko", "mirovinsko", "sud u", "osnovni sud", "okruzni sud",
    "opstinski sud", "opcinski sud", "tuzilastvo", "tuziteljstvo",
    "kazneno-popravni", "vojska", "policijska uprava", "dom ucenika",
    "studentski centar", "studentski dom", "crveni krst", "crveni kriz",
    "turisticka organizacija", "mjesna zajednica", "mesna zajednica",
)

# Whole-word terms: short abbreviations that would otherwise match inside
# ordinary words ("jump" contains "ju"). "os" is the Serbian abbreviation for
# osnovna škola (ОШ), extremely common on NSZ; the longer school forms
# ("srednja skola", "gimnazija", ...) are covered by the substring list.
_WORD_TERMS = ("ju", "jzu", "jp", "jkp", "jspu", "mup", "skola", "os",
               "opstina", "opcina", "grad", "predskolska")

_WORD_RE = re.compile(r"(?<![a-z0-9])(?:" + "|".join(_WORD_TERMS) + r")(?![a-z0-9])")


def public_sector_term(employer_name: str) -> str:
    """The matched term when the name looks public-sector, else ``""``."""
    key = norm_text(employer_name)
    if not key:
        return ""
    for term in _SUBSTRING_TERMS:
        if term in key:
            return term
    match = _WORD_RE.search(key)
    return match.group(0) if match else ""
