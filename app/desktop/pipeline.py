"""Turn raw scraper jobs into export rows.

This is the logic that was copy-pasted across ``scripts/scrape_category.py``,
``scripts/scrape_hzz_category.py``, ``run_arbeitsagentur*.py`` and
``agent/main.py``: drop postings without an e-mail, dedupe, and decide which
posting ids are safe to remember in ``app.seen_store``.

The one rule worth restating, because it is easy to get backwards: **a posting
id is only remembered when the posting actually yielded an e-mail.** A posting
with no contact address may get one later, so burning its id would mean never
looking at it again.
"""

import unicodedata
from dataclasses import dataclass, field

# Croatian public-sector / childcare employers — never useful as outreach leads.
EXCLUDED_COMPANY_TERMS = ("djecji vrtic", "vrtic", "skola", "opcina")

HZZ_FIELDS = [
    "email", "phone", "company", "city", "employer_address",
    "title", "group", "employment_type", "working_hours",
    "valid_from", "valid_to", "detail_url", "source",
]
ARBEITSAGENTUR_FIELDS = [
    "email", "company", "city", "title",
    "category", "published_at", "detail_url", "employer_website",
    "refnr", "source",
]

SOURCE_FIELDS = {
    "hzz": HZZ_FIELDS,
    "arbeitsagentur": ARBEITSAGENTUR_FIELDS,
}

# Which job key holds the posting id that goes into seen_store.
SOURCE_ID_KEY = {
    "hzz": "detail_url",
    "arbeitsagentur": "refnr",
}


def normalize_key(value: str) -> str:
    """Casefold and strip diacritics, so "Dječji vrtić" matches "djecji vrtic"."""
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(c for c in normalized if not unicodedata.combining(c))
    return " ".join(normalized.casefold().split())


def is_excluded_company(company: str, terms: tuple[str, ...] = EXCLUDED_COMPANY_TERMS) -> bool:
    if not terms:
        return False
    key = normalize_key(company)
    return any(term in key for term in terms)


@dataclass
class Stats:
    """Why postings did not make it into the export."""

    seen: int = 0
    without_email: int = 0
    excluded_company: int = 0
    duplicate_email: int = 0
    duplicate_company: int = 0
    kept: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "seen": self.seen,
            "without_email": self.without_email,
            "excluded_company": self.excluded_company,
            "duplicate_email": self.duplicate_email,
            "duplicate_company": self.duplicate_company,
            "kept": self.kept,
        }


def _row_from_hzz(job: dict, group_label: str) -> dict[str, str]:
    return {
        "email": (job.get("email") or "").strip(),
        "phone": (job.get("phone") or "").strip(),
        "company": (job.get("company") or "").strip(),
        "city": (job.get("location") or "").strip(),
        "employer_address": (job.get("employer_address") or "").strip(),
        "title": (job.get("title") or "").strip(),
        "group": (job.get("group") or group_label or "").strip(),
        "employment_type": (job.get("employment_type") or "").strip(),
        "working_hours": (job.get("working_hours") or "").strip(),
        "valid_from": (job.get("valid_from") or "").strip(),
        "valid_to": (job.get("valid_to") or "").strip(),
        "detail_url": (job.get("detail_url") or "").strip(),
        "source": "hzz",
    }


def _row_from_arbeitsagentur(job: dict, group_label: str) -> dict[str, str]:
    return {
        "email": (job.get("email") or "").strip(),
        "company": (job.get("company") or "").strip(),
        "city": (job.get("location") or "").strip(),
        "title": (job.get("title") or "").strip(),
        "category": (job.get("category") or group_label or "").strip(),
        "published_at": (job.get("published_at") or "").strip(),
        "detail_url": (job.get("detail_url") or "").strip(),
        "employer_website": (job.get("employer_website") or "").strip(),
        "refnr": (job.get("refnr") or "").strip(),
        "source": "arbeitsagentur",
    }


_ROW_BUILDERS = {
    "hzz": _row_from_hzz,
    "arbeitsagentur": _row_from_arbeitsagentur,
}


@dataclass
class LeadCollector:
    """Accumulates export rows from the jobs a scraper streams via ``on_job``.

    Dedup state spans the whole run, so a collector can be reused across several
    categories/subgroups and still emit one row per employer.
    """

    source: str
    seen_emails: set[str] = field(default_factory=set)
    dedupe_company: bool = True
    exclude_terms: tuple[str, ...] = ()

    rows: list[dict[str, str]] = field(default_factory=list, init=False)
    new_ids: list[str] = field(default_factory=list, init=False)
    new_emails: list[str] = field(default_factory=list, init=False)
    stats: Stats = field(default_factory=Stats, init=False)

    _run_emails: set[str] = field(default_factory=set, init=False, repr=False)
    _run_companies: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.source not in SOURCE_FIELDS:
            raise ValueError(f"Unknown source {self.source!r}; expected one of {sorted(SOURCE_FIELDS)}")

    @property
    def fields(self) -> list[str]:
        return SOURCE_FIELDS[self.source]

    def add(self, job: dict, group_label: str = "") -> dict[str, str] | None:
        """Fold one scraped posting in. Returns the emitted row, or None if dropped."""
        self.stats.seen += 1
        email = (job.get("email") or "").strip()
        if not email:
            # No e-mail -> don't burn the id; the employer may add one later.
            self.stats.without_email += 1
            return None

        posting_id = (job.get(SOURCE_ID_KEY[self.source]) or "").strip()
        if posting_id:
            self.new_ids.append(posting_id)

        row = _ROW_BUILDERS[self.source](job, group_label)

        if is_excluded_company(row["company"], self.exclude_terms):
            self.stats.excluded_company += 1
            return None

        # Dedup on the e-mail address, not the company name (names vary in spelling).
        email_key = email.casefold()
        if email_key in self.seen_emails or email_key in self._run_emails:
            self.stats.duplicate_email += 1
            return None

        company_key = normalize_key(row["company"])
        if self.dedupe_company and company_key and company_key in self._run_companies:
            self.stats.duplicate_company += 1
            return None

        self._run_emails.add(email_key)
        self.new_emails.append(email_key)
        if company_key:
            self._run_companies.add(company_key)
        self.rows.append(row)
        self.stats.kept += 1
        return row
