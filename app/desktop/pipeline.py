"""Turn raw scraper jobs into export rows.

This is the logic that was copy-pasted across ``scripts/scrape_category.py``,
``scripts/scrape_hzz_category.py``, ``run_arbeitsagentur*.py`` and
``agent/main.py``: drop postings without an e-mail, dedupe, and decide which
posting ids are safe to remember in ``app.seen_store``.

The one rule worth restating, because it is easy to get backwards: **a posting
id is only remembered when the posting actually yielded an e-mail.** A posting
with no contact address may get one later, so burning its id would mean never
looking at it again.

The GVP member directory is the exception that proves the rule: there the
entries *without* an address are wanted too — as a worklist for finding one —
so a collector can be asked to keep them (``keep_without_email``). They go to
a separate list, never into the export proper, and their ids are still not
remembered.
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

GVP_FIELDS = [
    "email", "company", "city", "plz", "street", "country",
    "phone", "website", "managing_director", "business_fields",
    "member", "email_source", "source",
]

SOURCE_FIELDS = {
    "hzz": HZZ_FIELDS,
    "arbeitsagentur": ARBEITSAGENTUR_FIELDS,
    "gvp": GVP_FIELDS,
}

# Which job key holds the posting id that goes into seen_store.
SOURCE_ID_KEY = {
    "hzz": "detail_url",
    "arbeitsagentur": "refnr",
    "gvp": "member",
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
    # Entries kept *without* an address (only when the collector is asked to).
    missing: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "seen": self.seen,
            "without_email": self.without_email,
            "excluded_company": self.excluded_company,
            "duplicate_email": self.duplicate_email,
            "duplicate_company": self.duplicate_company,
            "kept": self.kept,
            "missing": self.missing,
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


def _row_from_gvp(job: dict, group_label: str) -> dict[str, str]:
    return {
        "email": (job.get("email") or "").strip(),
        "company": (job.get("company") or "").strip(),
        "city": (job.get("city") or "").strip(),
        "plz": (job.get("plz") or "").strip(),
        "street": (job.get("street") or "").strip(),
        "country": (job.get("country") or "").strip(),
        "phone": (job.get("phone") or "").strip(),
        "website": (job.get("website") or "").strip(),
        "managing_director": (job.get("managing_director") or "").strip(),
        "business_fields": (job.get("business_fields") or "").strip(),
        "member": (job.get("member") or "").strip(),
        "email_source": (job.get("email_source") or "").strip(),
        "source": "gvp",
    }


_ROW_BUILDERS = {
    "hzz": _row_from_hzz,
    "arbeitsagentur": _row_from_arbeitsagentur,
    "gvp": _row_from_gvp,
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
    # Keep entries that have no e-mail as a separate worklist (``missing_rows``).
    keep_without_email: bool = False

    rows: list[dict[str, str]] = field(default_factory=list, init=False)
    missing_rows: list[dict[str, str]] = field(default_factory=list, init=False)
    new_ids: list[str] = field(default_factory=list, init=False)
    new_emails: list[str] = field(default_factory=list, init=False)
    stats: Stats = field(default_factory=Stats, init=False)

    _run_emails: set[str] = field(default_factory=set, init=False, repr=False)
    _run_companies: set[str] = field(default_factory=set, init=False, repr=False)
    # Companies that produced at least one row with an address this run.
    _companies_with_email: set[str] = field(default_factory=set, init=False, repr=False)
    _missing_companies: set[str] = field(default_factory=set, init=False, repr=False)
    # Address-less entries of the company currently streaming in, held back
    # until the next company starts (see ``_queue_missing``).
    _pending_key: str = field(default="", init=False, repr=False)
    _pending: list[dict[str, str]] = field(default_factory=list, init=False, repr=False)
    _missing_outbox: list[dict[str, str]] = field(default_factory=list, init=False, repr=False)

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
        row = _ROW_BUILDERS[self.source](job, group_label)
        company_key = normalize_key(row["company"])

        if self.keep_without_email and company_key != self._pending_key:
            self._flush_pending()

        if not email:
            # No e-mail -> don't burn the id; the employer may add one later.
            self.stats.without_email += 1
            if self.keep_without_email and not is_excluded_company(row["company"], self.exclude_terms):
                self._queue_missing(row, company_key)
            return None

        posting_id = (job.get(SOURCE_ID_KEY[self.source]) or "").strip()
        if posting_id:
            self.new_ids.append(posting_id)

        if is_excluded_company(row["company"], self.exclude_terms):
            self.stats.excluded_company += 1
            return None

        if company_key:
            self._companies_with_email.add(company_key)

        # Dedup on the e-mail address, not the company name (names vary in spelling).
        email_key = email.casefold()
        if email_key in self.seen_emails or email_key in self._run_emails:
            self.stats.duplicate_email += 1
            return None

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

    # ---- entries without an address -------------------------------------
    #
    # A directory lists a company's head office and its branches as separate
    # entries, usually with the address on only one of them. Keeping every
    # address-less branch of a company whose head office *did* give an e-mail
    # would pad the worklist with entries nobody needs to look up. Entries are
    # sorted by company, so the branches of one company arrive together: the
    # address-less ones are held until the next company begins, then dropped if
    # the company yielded an e-mail in the meantime, else released.

    def _queue_missing(self, row: dict[str, str], company_key: str) -> None:
        self._pending_key = company_key
        self._pending.append(row)

    def _flush_pending(self) -> None:
        pending, key = self._pending, self._pending_key
        self._pending, self._pending_key = [], ""
        if not pending or (key and key in self._companies_with_email):
            return
        if self.dedupe_company and key:
            if key in self._missing_companies:
                return
            pending = pending[:1]
        if key:
            self._missing_companies.add(key)
        self.missing_rows.extend(pending)
        self._missing_outbox.extend(pending)
        self.stats.missing += len(pending)

    def take_missing(self) -> list[dict[str, str]]:
        """Address-less rows finalised since the last call (for streaming them out)."""
        rows, self._missing_outbox = self._missing_outbox, []
        return rows

    def finish(self) -> None:
        """Release whatever is still held back; call once the scraper is done."""
        self._flush_pending()
