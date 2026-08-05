"""Text normalization shared by every source adapter.

Serbian sources (NSZ especially) mix Cyrillic and Latin freely, sometimes in
the same record, so *everything* that gets compared — company names, cities —
goes through transliteration first. Company-name normalization additionally
strips the legal-form suffixes (d.o.o., a.d., s.p., ...) that vary between a
job board's display name and a registry's legal name.
"""

import re
import unicodedata
from datetime import datetime

# Serbian Cyrillic -> Latin. Digraph letters must map before the general pass
# would ever see them (they are single codepoints, so a plain dict is enough).
_CYR_TO_LAT = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Ђ": "Đ", "Е": "E",
    "Ж": "Ž", "З": "Z", "И": "I", "Ј": "J", "К": "K", "Л": "L", "Љ": "Lj",
    "М": "M", "Н": "N", "Њ": "Nj", "О": "O", "П": "P", "Р": "R", "С": "S",
    "Т": "T", "Ћ": "Ć", "У": "U", "Ф": "F", "Х": "H", "Ц": "C", "Ч": "Č",
    "Џ": "Dž", "Ш": "Š",
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "ђ": "đ", "е": "e",
    "ж": "ž", "з": "z", "и": "i", "ј": "j", "к": "k", "л": "l", "љ": "lj",
    "м": "m", "н": "n", "њ": "nj", "о": "o", "п": "p", "р": "r", "с": "s",
    "т": "t", "ћ": "ć", "у": "u", "ф": "f", "х": "h", "ц": "c", "ч": "č",
    "џ": "dž", "ш": "š",
}

# Legal-form suffixes that job boards and registries disagree on. Matched as
# whole words at any position (they also appear mid-name: "Foo d.o.o. Sarajevo").
_LEGAL_FORMS = (
    "d.o.o", "doo", "d.d", "dd", "a.d", "ad", "j.d.o.o", "jdoo", "d.n.o",
    "dno", "o.d", "k.d", "s.p", "sp", "s.z.r", "szr", "s.t.r", "str",
    "s.u.r", "sur", "p.r", "pr", "z.r", "obrt", "gmbh", "llc", "ltd",
    "d.o.o.e.l", "export-import", "export import",
)
_LEGAL_FORM_RE = re.compile(
    r"(?<![a-z0-9])(?:" + "|".join(re.escape(form).replace(r"\.", r"\.?") for form in _LEGAL_FORMS) + r")\.?(?![a-z0-9])"
)

_WS_RE = re.compile(r"\s+")
_DATE_FORMATS = ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%d/%m/%Y", "%d. %m. %Y")

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:(?:\+|00)\s*38[1-7][\s/.-]*)?(?:0\d[\d\s/.-]{5,}\d|\d{2,3}[\s/.-]*\d{3}[\s/.-]*\d{3,4})"
)


# Digraph letters whose Latin form has two characters; in an all-caps word the
# second character must be upper too ("ЗАПОШЉАВАЊЕ" -> "ZAPOŠLJAVANJE").
_DIGRAPHS_UPPER = {"Љ": "LJ", "Њ": "NJ", "Џ": "DŽ"}


def translit(text: str) -> str:
    """Serbian Cyrillic -> Latin; Latin text passes through unchanged."""
    if not text:
        return ""
    out = []
    for index, ch in enumerate(text):
        if ch in _DIGRAPHS_UPPER:
            neighbours = text[max(0, index - 1):index] + text[index + 1:index + 2]
            all_caps = any(c.isupper() for c in neighbours) and not any(c.islower() for c in neighbours)
            out.append(_DIGRAPHS_UPPER[ch] if all_caps else _CYR_TO_LAT[ch])
        else:
            out.append(_CYR_TO_LAT.get(ch, ch))
    return "".join(out)


def clean_text(text: str) -> str:
    return _WS_RE.sub(" ", (text or "")).strip()


def norm_text(text: str) -> str:
    """Transliterate, strip diacritics, casefold, collapse whitespace.

    Same folding as ``app.desktop.pipeline.normalize_key`` plus the Cyrillic
    pass, so "Дрво-Стил д.о.о." and "Drvo-Stil doo" meet in the middle.
    """
    text = translit(text or "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join(text.casefold().split())


def norm_company(name: str) -> str:
    """Normalized company name for matching: no legal forms, no punctuation."""
    text = norm_text(name)
    text = _LEGAL_FORM_RE.sub(" ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def norm_city(city: str) -> str:
    return norm_text(re.sub(r"\d{4,5}", " ", city or ""))  # drop postal codes


def norm_tax_id(value: str) -> str:
    """Digits only; JIB/PIB/MB shorter than 8 digits is noise, not an id."""
    digits = re.sub(r"\D", "", value or "")
    return digits if len(digits) >= 8 else ""


def parse_date(value: str) -> str:
    """Best-effort ``-> YYYY-MM-DD``; empty string when unparseable."""
    text = clean_text(translit(value)).rstrip(".") if value else ""
    if not text:
        return ""
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    match = re.search(r"\d{1,2}\s*\.\s*\d{1,2}\s*\.\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\.\d{1,2}\.\d{2}\b", text)
    if match:
        text = re.sub(r"\s*\.\s*", ".", match.group(0))
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def extract_email(*sources: str) -> str:
    for source in sources:
        for match in EMAIL_RE.findall(source or ""):
            candidate = match.strip(" <>\"'(),;:").casefold()
            local, _, domain = candidate.partition("@")
            if local and "." in domain and not domain.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
                return candidate
    return ""


def extract_phone(*sources: str) -> str:
    for source in sources:
        match = PHONE_RE.search(source or "")
        if match:
            return clean_text(match.group(0))
    return ""
