import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from app.db.supabase import SupabaseStorage
from app.queue import enqueue_task


EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v2"
DEFAULT_INITIAL_DELAY_HOURS = 3
DEFAULT_RETRY_DELAY_HOURS = 48
DEFAULT_PAGE_LIMIT = 3
DEFAULT_SCRAPE_TIMEOUT_MS = 15000
CONTACT_PATH_HINTS = (
    "contact",
    "kontakt",
    "contacts",
    "about",
    "o-nama",
    "o_nama",
    "about-us",
    "careers",
    "career",
    "jobs",
    "job",
    "karijere",
    "karijera",
    "posao",
    "zaposlenje",
    "zaposljavanje",
    "careers-contact",
)
GENERIC_LOCAL_PART_PREFERENCES = (
    "contact",
    "kontakt",
    "info",
    "hr",
    "kadrovska",
    "kadrovi",
    "company",
    "office",
    "general",
    "jobs",
    "careers",
    "career",
    "hiring",
    "recruitment",
    "employment",
    "posao",
    "karijere",
)
LOW_PRIORITY_LOCAL_PART_KEYWORDS = (
    "onlineshop",
    "webshop",
    "shop",
    "store",
    "orders",
    "press",
    "media",
    "pr",
    "news",
    "marketing",
)
EMAIL_CONTEXT_KEYWORDS = (
    "contact",
    "kontakt",
    "email",
    "e-mail",
    "mail",
    "reach",
    "support",
    "info",
    "career",
    "careers",
    "job",
    "jobs",
    "apply",
    "application",
    "applications",
    "prijava",
    "prijave",
    "posao",
    "karijera",
    "karijere",
    "hr",
    "human resources",
    "recruitment",
    "hiring",
)
EXTERNAL_EMAIL_PENALTY_KEYWORDS = (
    "partner",
    "vendor",
    "external",
    "agency",
    "supplier",
    "outsourced",
)
MIN_CONTEXTUAL_EMAIL_SCORE = 25


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_for_matching(value: Any) -> str:
    cleaned = _clean_text(value) or ""
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_only.casefold()).strip()


def _looks_like_email(value: Any) -> bool:
    cleaned = _clean_text(value)
    return bool(cleaned and EMAIL_PATTERN.match(cleaned))


def _normalize_website_url(value: Any) -> str | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    if not parsed.netloc:
        return None
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}{parsed.path or ''}"


def _company_group_key(job: dict[str, Any]) -> str | None:
    website = _normalize_website_url(job.get("employer_website"))
    if website:
        parsed = urlparse(website)
        return f"website:{parsed.netloc.casefold()}"

    company = _normalize_for_matching(job.get("company"))
    if company:
        return f"company:{company}"

    detail_url = _clean_text(job.get("detail_url"))
    if detail_url:
        return f"detail:{detail_url}"

    return None


def _company_name_key(value: Any) -> str | None:
    normalized = _normalize_for_matching(value)
    return normalized or None


def _delay_hours_env(name: str, default: int) -> int:
    try:
        return max(int(os.getenv(name, str(default))), 0)
    except (TypeError, ValueError):
        return default


class FirecrawlLeadEnricher:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = FIRECRAWL_BASE_URL,
        timeout_seconds: float | None = None,
        scrape_timeout_ms: int | None = None,
        page_limit: int | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("FIRECRAWL_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds or float(os.getenv("FIRECRAWL_TIMEOUT_SECONDS", "45"))
        self.scrape_timeout_ms = scrape_timeout_ms or int(os.getenv("FIRECRAWL_SCRAPE_TIMEOUT_MS", str(DEFAULT_SCRAPE_TIMEOUT_MS)))
        self.page_limit = page_limit or int(os.getenv("FIRECRAWL_MAX_PAGES_PER_LEAD", str(DEFAULT_PAGE_LIMIT)))

    @property
    def is_configured(self) -> bool:
        return bool(_clean_text(self.api_key))

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.is_configured:
            raise RuntimeError("FIRECRAWL_API_KEY must be set before lead enrichment can use Firecrawl.")

        request = Request(
            f"{self.base_url}{path}",
            data=None if payload is None else json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method=method,
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:  # pragma: no cover - network interaction is mocked in tests
            details = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Firecrawl request failed with status {exc.code}: {details}") from exc
        except URLError as exc:  # pragma: no cover - network interaction is mocked in tests
            raise RuntimeError(f"Firecrawl request failed: {exc}") from exc

    def _scrape_page(self, url: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            "/scrape",
            {
                "url": url,
                "formats": ["markdown", "links"],
                "onlyMainContent": False,
                "maxAge": 172800000,
                "timeout": self.scrape_timeout_ms,
                "proxy": "basic",
                "storeInCache": True,
            },
        )
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    def _extract_markdown(self, scrape_data: dict[str, Any]) -> str:
        markdown = scrape_data.get("markdown")
        return markdown if isinstance(markdown, str) else ""

    def _extract_links(self, scrape_data: dict[str, Any]) -> list[str]:
        links = scrape_data.get("links")
        if isinstance(links, list):
            extracted: list[str] = []
            for item in links:
                if isinstance(item, str):
                    extracted.append(item)
                elif isinstance(item, dict):
                    candidate = _clean_text(item.get("url"))
                    if candidate:
                        extracted.append(candidate)
            return extracted
        return []

    def _base_host(self, url: str) -> str:
        host = urlparse(url).netloc.casefold()
        return host[4:] if host.startswith("www.") else host

    def _host_labels(self, host: str) -> tuple[str, ...]:
        return tuple(label for label in host.split(".") if label)

    def _domains_match(self, email_domain: str, website_host: str) -> bool:
        left = self._host_labels(email_domain.casefold())
        right = self._host_labels(website_host.casefold())
        if not left or not right:
            return False
        return left[-2:] == right[-2:] or email_domain.casefold().endswith(f".{website_host.casefold()}") or website_host.casefold().endswith(f".{email_domain.casefold()}")

    def _page_path_score(self, page_url: str) -> int:
        path = urlparse(page_url).path.casefold()
        for index, hint in enumerate(CONTACT_PATH_HINTS):
            if hint in path:
                return 30 - index
        return 0

    def _email_context_score(self, text: str, email: str, company: str | None = None) -> int:
        score = 0
        normalized_text = (text or "").casefold()
        normalized_email = email.casefold()
        company_tokens = [token for token in re.split(r"[^a-z0-9]+", _normalize_for_matching(company)) if len(token) >= 4]

        start = 0
        while True:
            index = normalized_text.find(normalized_email, start)
            if index < 0:
                break
            window_start = max(index - 120, 0)
            window_end = min(index + len(normalized_email) + 120, len(normalized_text))
            window = normalized_text[window_start:window_end]

            for keyword in EMAIL_CONTEXT_KEYWORDS:
                if keyword in window:
                    score += 8

            for keyword in EXTERNAL_EMAIL_PENALTY_KEYWORDS:
                if keyword in window:
                    score -= 18

            if any(token in window for token in company_tokens):
                score += 12

            start = index + len(normalized_email)

        if any(token in normalized_text for token in company_tokens):
            score += 4

        return score

    def _score_email(self, email: str, website_host: str, *, text: str, page_url: str, company: str | None) -> tuple[int, int]:
        local_part, _, domain = email.partition("@")
        score = 0
        if self._domains_match(domain, website_host):
            score += 100
        else:
            score += 10
        normalized_local = local_part.casefold()
        for index, preferred in enumerate(GENERIC_LOCAL_PART_PREFERENCES):
            if preferred in normalized_local:
                score += 40 - index
                break
        for keyword in LOW_PRIORITY_LOCAL_PART_KEYWORDS:
            if keyword in normalized_local:
                score -= 25
                break
        score += self._page_path_score(page_url)
        score += self._email_context_score(text, email, company=company)
        score += max(10 - len(normalized_local), 0)
        return score, -len(email)

    def _extract_emails_from_text(self, text: str, website_host: str, *, page_url: str, company: str | None) -> list[str]:
        candidates = {
            match.group(0).strip(".,;:()[]<>\"'")
            for match in re.finditer(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", text or "", re.IGNORECASE)
        }
        valid = [email for email in candidates if _looks_like_email(email)]
        ranked = sorted(
            valid,
            key=lambda email: self._score_email(email, website_host, text=text, page_url=page_url, company=company),
            reverse=True,
        )
        accepted: list[str] = []
        for email in ranked:
            score, _ = self._score_email(email, website_host, text=text, page_url=page_url, company=company)
            if self._domains_match(email.partition("@")[2], website_host) or score >= MIN_CONTEXTUAL_EMAIL_SCORE:
                accepted.append(email)
        return accepted

    def _build_candidate_urls(self, website_url: str, links: list[str]) -> list[str]:
        normalized_website = _normalize_website_url(website_url)
        if not normalized_website:
            return []

        parsed = urlparse(normalized_website)
        root = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        seen: set[str] = set()
        candidates: list[str] = []

        def _add(url: str | None) -> None:
            normalized = _normalize_website_url(url)
            if not normalized:
                return
            normalized_host = self._base_host(normalized)
            if normalized_host != self._base_host(root):
                return
            normalized = normalized.rstrip("/")
            if normalized in seen:
                return
            seen.add(normalized)
            candidates.append(normalized)

        _add(root)
        for hint in CONTACT_PATH_HINTS:
            _add(urljoin(f"{root}/", hint))

        prioritized_links: list[tuple[int, str]] = []
        for link in links:
            normalized = _normalize_website_url(link)
            if not normalized:
                continue
            if self._base_host(normalized) != self._base_host(root):
                continue
            path = urlparse(normalized).path.casefold()
            score = 0
            for index, hint in enumerate(CONTACT_PATH_HINTS):
                if hint in path:
                    score = 100 - index
                    break
            if score > 0:
                prioritized_links.append((score, normalized))

        for _, link in sorted(prioritized_links, reverse=True):
            _add(link)

        return candidates[: max(self.page_limit, 1)]

    def find_company_email(self, job: dict[str, Any]) -> str | None:
        website = _normalize_website_url(job.get("employer_website"))
        if not website:
            return None

        initial_scrape = self._scrape_page(website)
        website_host = self._base_host(website)
        emails = self._extract_emails_from_text(
            self._extract_markdown(initial_scrape),
            website_host,
            page_url=website,
            company=_clean_text(job.get("company")),
        )
        if emails:
            return emails[0]

        candidate_urls = self._build_candidate_urls(website, self._extract_links(initial_scrape))
        for candidate_url in candidate_urls[1:]:
            page_scrape = self._scrape_page(candidate_url)
            emails = self._extract_emails_from_text(
                self._extract_markdown(page_scrape),
                website_host,
                page_url=candidate_url,
                company=_clean_text(job.get("company")),
            )
            if emails:
                return emails[0]

        return None


def _select_reused_company_email(rows: list[dict[str, Any]]) -> str | None:
    candidates: list[tuple[int, str, str]] = []
    for row in rows:
        email = _clean_text(row.get("employer_email"))
        if not _looks_like_email(email):
            continue

        local_part = email.partition("@")[0].casefold()
        score = 0
        for index, preferred in enumerate(GENERIC_LOCAL_PART_PREFERENCES):
            if preferred in local_part:
                score += 100 - index
                break
        for keyword in LOW_PRIORITY_LOCAL_PART_KEYWORDS:
            if keyword in local_part:
                score -= 50
                break
        score += max(20 - len(local_part), 0)
        candidates.append((score, _clean_text(row.get("updated_at")) or "", email))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    return candidates[0][2]


def _company_history_state(rows: list[dict[str, Any]]) -> str | None:
    reusable_email = _select_reused_company_email(rows)
    if reusable_email:
        return reusable_email

    if any(
        row.get("email_enrichment_unusable")
        or int(row.get("email_enrichment_attempt_count", 0) or 0) > 0
        or row.get("email_enrichment_last_attempt_at")
        for row in rows
    ):
        return "skip"

    return None


def enrich_jobs_missing_email(
    *,
    jobs: list[dict[str, Any]],
    storage: SupabaseStorage | None = None,
    enricher: FirecrawlLeadEnricher | None = None,
) -> dict[str, int]:
    if not jobs:
        return {"enriched_count": 0, "reused_count": 0, "skipped_count": 0}

    enricher = enricher or FirecrawlLeadEnricher()
    if not enricher.is_configured:
        return {"enriched_count": 0, "reused_count": 0, "skipped_count": 0}

    groups: dict[str, list[dict[str, Any]]] = {}
    for job in jobs:
        if _looks_like_email(job.get("employer_email")):
            continue
        company_key = _company_group_key(job)
        if not company_key:
            continue
        groups.setdefault(company_key, []).append(job)

    prior_rows_by_company: dict[str, list[dict[str, Any]]] = {}
    if storage is not None:
        company_names = [_clean_text(grouped_jobs[0].get("company")) for grouped_jobs in groups.values()]
        prior_rows = storage.list_jobs_for_company_names([name for name in company_names if name])
        current_job_ids = {
            str(job["id"])
            for grouped_jobs in groups.values()
            for job in grouped_jobs
            if job.get("id")
        }
        for row in prior_rows:
            if str(row.get("id")) in current_job_ids:
                continue
            company_key = _company_name_key(row.get("company"))
            if company_key:
                prior_rows_by_company.setdefault(company_key, []).append(row)

    enriched_count = 0
    reused_count = 0
    skipped_count = 0
    for grouped_jobs in groups.values():
        company_key = _company_name_key(grouped_jobs[0].get("company")) or ""
        company_history = prior_rows_by_company.get(company_key, [])
        if company_history:
            history_state = _company_history_state(company_history)
            if history_state == "skip":
                if storage is not None:
                    job_ids = [str(job["id"]) for job in grouped_jobs if job.get("id")]
                    storage.mark_jobs_email_enrichment_unusable(job_ids)
                for job in grouped_jobs:
                    job["email_enrichment_unusable"] = True
                    job["_skipped_known_company"] = True
                skipped_count += len(grouped_jobs)
                continue

            if history_state:
                if storage is not None:
                    job_ids = [str(job["id"]) for job in grouped_jobs if job.get("id")]
                    if job_ids:
                        storage.update_jobs_employer_email(job_ids, history_state)
                for job in grouped_jobs:
                    job["employer_email"] = history_state
                    job["_reused_company_email"] = True
                reused_count += len(grouped_jobs)
                continue

        try:
            email = enricher.find_company_email(grouped_jobs[0])
        except Exception as exc:
            company = _clean_text(grouped_jobs[0].get("company")) or grouped_jobs[0].get("detail_url") or "unknown company"
            print(f"[lead-enrichment] Firecrawl failed for {company}: {exc}")
            continue

        if not _looks_like_email(email):
            continue

        for job in grouped_jobs:
            job["employer_email"] = email

        if storage is not None:
            job_ids = [str(job["id"]) for job in grouped_jobs if job.get("id")]
            if job_ids:
                storage.update_jobs_employer_email(job_ids, email)

        enriched_count += len(grouped_jobs)

    return {
        "enriched_count": enriched_count,
        "reused_count": reused_count,
        "skipped_count": skipped_count,
    }


def schedule_scrape_run_email_enrichment(
    *,
    run_id: str,
    storage: SupabaseStorage,
    delay_hours: int | None = None,
) -> dict[str, Any]:
    jobs = storage.list_jobs_pending_email_enrichment(run_id=run_id)
    if not jobs:
        return {"scheduled": False, "job_count": 0, "scheduled_for": None}

    delay = _delay_hours_env("FIRECRAWL_INITIAL_ENRICHMENT_DELAY_HOURS", DEFAULT_INITIAL_DELAY_HOURS)
    if delay_hours is not None:
        delay = max(int(delay_hours), 0)

    scheduled_for = _utcnow() + timedelta(hours=delay)
    storage.schedule_jobs_email_enrichment(
        [str(job["id"]) for job in jobs if job.get("id")],
        scheduled_for=scheduled_for.isoformat(),
    )

    try:
        enqueue_task(
            "app.tasks.enrich_scrape_run_emails",
            countdown_seconds=delay * 3600,
            run_id=run_id,
        )
    except Exception as exc:
        print(f"[lead-enrichment] Failed to schedule enrichment task for run {run_id}: {exc}")
        return {"scheduled": False, "job_count": len(jobs), "scheduled_for": scheduled_for.isoformat()}

    return {"scheduled": True, "job_count": len(jobs), "scheduled_for": scheduled_for.isoformat()}


def enrich_scrape_run_emails(
    *,
    run_id: str,
    storage: SupabaseStorage,
    enricher: FirecrawlLeadEnricher | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or _utcnow()
    jobs = storage.list_jobs_pending_email_enrichment(run_id=run_id)
    due_jobs = [
        job
        for job in jobs
        if not job.get("email_enrichment_next_attempt_at")
        or datetime.fromisoformat(str(job["email_enrichment_next_attempt_at"]).replace("Z", "+00:00")) <= now
    ]

    if not due_jobs:
        return {
            "run_id": run_id,
            "attempted_count": 0,
            "enriched_count": 0,
            "reused_company_email_count": 0,
            "skipped_known_company_count": 0,
            "retry_scheduled_count": 0,
            "unusable_count": 0,
            "scheduled_retry_for": None,
        }

    enrichment_result = enrich_jobs_missing_email(jobs=due_jobs, storage=storage, enricher=enricher)
    enriched_jobs = enrichment_result["enriched_count"] + enrichment_result["reused_count"]

    retry_delay_hours = _delay_hours_env("FIRECRAWL_RETRY_DELAY_HOURS", DEFAULT_RETRY_DELAY_HOURS)
    retry_scheduled_for = now + timedelta(hours=retry_delay_hours)
    retry_scheduled_count = 0
    unusable_count = 0

    for job in due_jobs:
        if job.get("_skipped_known_company"):
            unusable_count += 1
            continue

        if job.get("_reused_company_email"):
            continue

        next_attempt_count = int(job.get("email_enrichment_attempt_count", 0)) + 1
        resolved = _looks_like_email(job.get("employer_email"))
        next_attempt_at = None
        unusable = False

        if not resolved:
            if next_attempt_count >= 2:
                unusable = True
                unusable_count += 1
            else:
                next_attempt_at = retry_scheduled_for.isoformat()
                retry_scheduled_count += 1

        storage.update_job_email_enrichment_state(
            str(job["id"]),
            attempt_count=next_attempt_count,
            last_attempt_at=now.isoformat(),
            next_attempt_at=next_attempt_at,
            unusable=unusable,
        )

    if retry_scheduled_count:
        try:
            enqueue_task(
                "app.tasks.enrich_scrape_run_emails",
                countdown_seconds=retry_delay_hours * 3600,
                run_id=run_id,
            )
        except Exception as exc:
            print(f"[lead-enrichment] Failed to schedule retry task for run {run_id}: {exc}")

    return {
        "run_id": run_id,
        "attempted_count": len(due_jobs),
        "enriched_count": enriched_jobs,
        "reused_company_email_count": enrichment_result["reused_count"],
        "skipped_known_company_count": enrichment_result["skipped_count"],
        "retry_scheduled_count": retry_scheduled_count,
        "unusable_count": unusable_count,
        "scheduled_retry_for": retry_scheduled_for.isoformat() if retry_scheduled_count else None,
    }
