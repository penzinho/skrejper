"""Cross-source employer matching.

The same firm shows up on Klix and MojPosao (and, once Croatia is imported,
on HZZ). Matching is two-pass and non-destructive — every employer row keeps
its own record; the group is expressed through ``canonical_key`` pointing at
the group's primary row ("source:source_id"):

1. exact match on the normalized tax id (JIB/PIB/OIB), when both sides have one;
2. fuzzy match on the normalized company name (legal forms stripped), gated by
   city agreement: same normalized city, or one side missing its city.

The canonical row is the one that knows the most (tax id > website > longer
legal name), so exports naturally show the richest record for each group.
"""

from collections import defaultdict

from app.leadgen.db import LeadDb
from app.leadgen.normalize import norm_city, norm_company, norm_tax_id
from app.leadgen.schema import employer_key

try:
    from rapidfuzz import fuzz

    def _similarity(a: str, b: str) -> float:
        return fuzz.token_sort_ratio(a, b)
except ImportError:  # keep the pipeline importable without the optional dep
    from difflib import SequenceMatcher

    def _similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, " ".join(sorted(a.split())), " ".join(sorted(b.split()))).ratio() * 100

FUZZY_THRESHOLD = 92.0


def _richness(employer: dict) -> tuple:
    return (
        bool(norm_tax_id(employer.get("tax_id", ""))),
        bool(employer.get("website")),
        len(employer.get("legal_name", "")),
        len(employer.get("name", "")),
    )


class _Groups:
    """Union-find over employer keys."""

    def __init__(self) -> None:
        self._parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(self, key: tuple[str, str]) -> tuple[str, str]:
        parent = self._parent.setdefault(key, key)
        if parent != key:
            parent = self.find(parent)
            self._parent[key] = parent
        return parent

    def union(self, a: tuple[str, str], b: tuple[str, str]) -> None:
        root_a, root_b = self.find(a), self.find(b)
        if root_a != root_b:
            self._parent[root_b] = root_a

    def groups(self) -> dict[tuple[str, str], list[tuple[str, str]]]:
        out: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for key in list(self._parent):
            out[self.find(key)].append(key)
        return out


def dedupe_employers(db: LeadDb) -> int:
    """Assign ``canonical_key`` groups; returns how many employers got grouped
    with at least one other record."""
    employers = db.employers()
    by_key = {(e["source"], e["source_id"]): e for e in employers}
    groups = _Groups()
    for key in by_key:
        groups.find(key)  # seed singletons

    # Pass 1: tax id.
    by_tax: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key, employer in by_key.items():
        tax = norm_tax_id(employer.get("tax_id", ""))
        if tax:
            by_tax[tax].append(key)
    for keys in by_tax.values():
        for other in keys[1:]:
            groups.union(keys[0], other)

    # Pass 2: fuzzy name + city, blocked by the name's first letter so the
    # comparison stays far from O(n²) across the whole table. The employer's
    # own city is stripped out of its name first — boards routinely display
    # "Eurotrans d.o.o. Banja Luka" where a registry says just "Eurotrans".
    blocks: dict[str, list[tuple[tuple[str, str], str, str]]] = defaultdict(list)
    for key, employer in by_key.items():
        name = norm_company(employer.get("name") or employer.get("legal_name") or "")
        city = norm_city(employer.get("city", ""))
        if city:
            city_tokens = set(city.split())
            name = " ".join(t for t in name.split() if t not in city_tokens) or name
        if len(name) < 4:
            continue
        blocks[name[0]].append((key, name, city))
    for entries in blocks.values():
        for i, (key_a, name_a, city_a) in enumerate(entries):
            for key_b, name_b, city_b in entries[i + 1:]:
                if groups.find(key_a) == groups.find(key_b):
                    continue
                if city_a and city_b and city_a != city_b:
                    continue
                if name_a == name_b or _similarity(name_a, name_b) >= FUZZY_THRESHOLD:
                    groups.union(key_a, key_b)

    mapping: dict[tuple[str, str], str] = {}
    grouped = 0
    for members in groups.groups().values():
        canonical = max(members, key=lambda key: _richness(by_key[key]))
        canonical_value = employer_key(*canonical)
        for member in members:
            mapping[member] = canonical_value
        if len(members) > 1:
            grouped += len(members)

    db.set_canonical_keys(mapping)
    return grouped
