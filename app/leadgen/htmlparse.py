"""Defensive HTML extraction helpers shared by the source adapters.

Parsers here key on what survives redesigns: URL patterns and *visible label
text* ("JIB:", "Adresa:", "Vrijedi do:"), not CSS class names. Every adapter's
parse functions take an HTML string — never a URL — so tests run on saved
fixtures without touching the network.
"""

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.leadgen.normalize import clean_text, norm_text

try:
    import lxml  # noqa: F401
    _PARSER = "lxml"
except ImportError:
    _PARSER = "html.parser"

# Elements that count as a "row" container around an anchor.
_CONTAINER_TAGS = ("li", "tr", "article")

DATE_RE = re.compile(r"\b\d{1,2}\s*\.\s*\d{1,2}\s*\.\s*\d{4}\b\.?|\b\d{4}-\d{2}-\d{2}\b")


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", _PARSER)


def links_matching(root, pattern: re.Pattern, base_url: str = "") -> list[tuple[re.Match, str, object]]:
    """All anchors whose href matches ``pattern``.

    Returns (match, absolute_url, anchor_tag) with in-page duplicates removed
    (same href listed once, first occurrence wins).
    """
    seen: set[str] = set()
    out = []
    for anchor in root.find_all("a", href=True):
        href = anchor["href"]
        match = pattern.search(href)
        if not match:
            continue
        absolute = urljoin(base_url or "", href)
        if absolute in seen:
            continue
        seen.add(absolute)
        out.append((match, absolute, anchor))
    return out


def container_of(anchor, max_hops: int = 10):
    """The nearest "card" ancestor around a link: a list item, a table row, or
    a div whose class says it is a card (Tailwind sites have no li/tr)."""
    node = anchor
    fallback = anchor
    for hop in range(max_hops):
        node = node.parent
        if node is None:
            break
        if node.name in _CONTAINER_TAGS:
            return node
        classes = " ".join(node.get("class") or ())
        if "card" in classes:
            return node
        if hop < 6:
            fallback = node
    return fallback


def labeled_value(root, labels: tuple[str, ...]) -> str:
    """The text value belonging to the first matching label on the page.

    Tries, in order: dt/dd and th/td pairs; any small element whose own text
    reads "Label: value"; an element that *is* the label followed by a sibling
    holding the value. Label comparison is diacritic/case-insensitive.
    """
    wanted = tuple(norm_text(label) for label in labels)

    def is_label(text: str) -> str | None:
        key = norm_text(text).rstrip(":")
        for target in wanted:
            if key == target or key.startswith(target + " "):
                return target
        return None

    for dt in root.find_all(["dt", "th", "h3", "h4"]):
        if is_label(dt.get_text()) is not None:
            value_el = dt.find_next_sibling(["dd", "td"])
            if value_el is not None:
                value = clean_text(value_el.get_text(" "))
                if value:
                    return value

    # Two subtleties: a label's container ancestors match too ("Puni naziv: X
    # JIB: Y ..." as one blob), and a short label is a prefix of a longer one
    # ("PDV" vs "PDV broj"). So every candidate is collected with a priority —
    # explicit "Label: value" colon forms first — and within a priority the
    # shortest value wins (the element closest to the label itself).
    candidates: list[tuple[int, str]] = []
    for element in root.find_all(["li", "p", "div", "span", "td", "strong", "b", "label"]):
        own = clean_text(element.get_text(" "))
        if not own or len(own) > 400:
            continue
        key_norm = norm_text(own)
        for label in labels:
            label_norm = norm_text(label)
            if key_norm.startswith(label_norm + ":"):
                value = clean_text(own.split(":", 1)[1])
                if value:
                    candidates.append((0, value))
            elif key_norm == label_norm or key_norm == label_norm + ":":
                sibling = element.find_next_sibling()
                if sibling is not None:
                    value = clean_text(sibling.get_text(" "))
                    if value:
                        candidates.append((0, value))
            elif key_norm.startswith(label_norm + " "):
                # Colon-less form ("JIB 4200..."): a weaker signal, used only
                # when no colon form exists anywhere on the page.
                prefix_match = re.match(rf"^\s*{re.escape(label)}\s+(.+)$", own, re.IGNORECASE)
                if prefix_match:
                    value = clean_text(prefix_match.group(1))
                    if value:
                        candidates.append((1, value))
    if not candidates:
        return ""
    return min(candidates, key=lambda entry: (entry[0], len(entry[1])))[1]


def labeled_link(root, labels: tuple[str, ...], base_url: str = "") -> str:
    """Like ``labeled_value`` but resolves to an href (for "Web:" fields).

    Matches only "Label:" or the exact label — a bare prefix would let the
    label "web" swallow an ad titled "Web novinar".
    """
    wanted = tuple(norm_text(label) for label in labels)
    for element in root.find_all(["li", "p", "div", "dd", "td", "span"]):
        own = norm_text(element.get_text(" "))
        if not own or len(own) > 200:
            continue
        if any(own == target or own.startswith(target + ":") for target in wanted):
            anchor = element.find("a", href=True)
            if anchor and anchor["href"].startswith(("http://", "https://")):
                return urljoin(base_url or "", anchor["href"])
    return ""


def page_title(root) -> str:
    h1 = root.find("h1")
    if h1 is not None:
        text = clean_text(h1.get_text(" "))
        if text:
            return text
    title = root.find("title")
    return clean_text(title.get_text()) if title is not None else ""


def dates_in(text: str) -> list[str]:
    """Every dd.mm.yyyy / yyyy-mm-dd date in the text, normalized to ISO."""
    from app.leadgen.normalize import parse_date

    return [d for d in (parse_date(m) for m in DATE_RE.findall(text or "")) if d]


def next_page_url(root, current_url: str) -> str:
    """The pagination "next" link, if the page declares one."""
    link = root.find("link", rel="next", href=True) or root.find("a", rel="next", href=True)
    if link is not None:
        return urljoin(current_url, link["href"])
    for anchor in root.find_all("a", href=True):
        text = norm_text(anchor.get_text())
        if text in ("sljedeca", "sledeca", "sljedeca stranica", "next", "dalje", ">", ">>", "»", "›"):
            return urljoin(current_url, anchor["href"])
    return ""
