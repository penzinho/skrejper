import copy
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Callable

from app.db.supabase import SupabaseStorage, get_supabase_storage
from app.services.email_outreach import process_post_scrape_automations


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _parse_published_at(value: Any) -> str | None:
    cleaned = _clean_text(value)
    if not cleaned:
        return None

    normalized = re.sub(r"\s+", "", cleaned)
    for fmt in ("%d.%m.%Y.", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _normalize_for_matching(value: Any) -> str:
    cleaned = _clean_text(value) or ""
    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_only.casefold()).strip()


def _is_excluded_employer(company: Any) -> bool:
    normalized_company = _normalize_for_matching(company)
    if not normalized_company:
        return False

    return bool(re.search(r"\b(skola|vrtic)\b", normalized_company))


def normalize_hzz_job(job: dict[str, Any], *, category: str | None, run_id: str) -> dict[str, Any]:
    return {
        "title": _clean_text(job.get("title")),
        "company": _clean_text(job.get("company")),
        "location": _clean_text(job.get("location")),
        "detail_url": _clean_text(job.get("detail_url")),
        "published_at": _parse_published_at(job.get("valid_from")),
        "category": _clean_text(category),
        "source": "hzz",
        "employer_website": None,
        "employer_email": _clean_text(job.get("email")),
        "employer_address": _clean_text(job.get("employer_address")),
        "employer_phone": _clean_text(job.get("phone")),
        "last_run_id": run_id,
        "updated_at": _utcnow_iso(),
    }


def normalize_mojposao_job(job: dict[str, Any], *, run_id: str) -> dict[str, Any]:
    return {
        "title": _clean_text(job.get("title")),
        "company": _clean_text(job.get("company")),
        "location": _clean_text(job.get("location")),
        "detail_url": _clean_text(job.get("detail_url")),
        "published_at": _parse_published_at(job.get("published_at")),
        "category": _clean_text(job.get("category")),
        "source": "mojposao",
        "employer_website": _clean_text(job.get("employer_website")),
        "employer_email": None,
        "employer_address": None,
        "employer_phone": None,
        "last_run_id": run_id,
        "updated_at": _utcnow_iso(),
    }


def build_job_snapshot(raw_job: dict[str, Any], normalized_job: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(raw_job)
    for key, value in normalized_job.items():
        if key in {"last_run_id", "updated_at"}:
            continue
        payload[key] = value
    return payload


def _filter_job_pairs(
    raw_jobs: list[dict[str, Any]],
    normalized_jobs: list[dict[str, Any]],
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], int]:
    valid_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    invalid_count = 0

    for raw_job, normalized_job in zip(raw_jobs, normalized_jobs):
        if not normalized_job.get("detail_url"):
            invalid_count += 1
            continue

        if _is_excluded_employer(normalized_job.get("company")):
            continue

        valid_pairs.append((raw_job, normalized_job))

    return valid_pairs, invalid_count


def _company_dedupe_key(job: dict[str, Any]) -> str | None:
    normalized_company = _normalize_for_matching(job.get("company"))
    if normalized_company:
        return normalized_company

    detail_url = _clean_text(job.get("detail_url"))
    if detail_url:
        return f"detail:{detail_url}"

    return None


def _apply_company_limit(
    raw_jobs: list[dict[str, Any]],
    normalized_jobs: list[dict[str, Any]],
    *,
    company_limit: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    available_company_keys = {
        company_key
        for job in normalized_jobs
        if (company_key := _company_dedupe_key(job)) is not None
    }

    if company_limit is None:
        return raw_jobs, normalized_jobs, len(available_company_keys), len(available_company_keys)

    selected_raw_jobs: list[dict[str, Any]] = []
    selected_normalized_jobs: list[dict[str, Any]] = []
    seen_company_keys: set[str] = set()

    for raw_job, normalized_job in zip(raw_jobs, normalized_jobs):
        company_key = _company_dedupe_key(normalized_job)
        if company_key is None:
            continue

        if company_key in seen_company_keys:
            continue

        seen_company_keys.add(company_key)
        selected_raw_jobs.append(raw_job)
        selected_normalized_jobs.append(normalized_job)

        if len(selected_normalized_jobs) >= company_limit:
            break

    return (
        selected_raw_jobs,
        selected_normalized_jobs,
        len(available_company_keys),
        len(selected_normalized_jobs),
    )


def _run_scrape_and_store(
    *,
    source: str,
    filters: dict[str, Any],
    scraper: Callable[..., list[dict[str, Any]]],
    scraper_kwargs: dict[str, Any],
    normalizer: Callable[[dict[str, Any], str], dict[str, Any]],
    company_limit: int | None = None,
    storage: SupabaseStorage | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    run_id = storage.create_scrape_run(source, filters)
    summary = {
        "run_id": run_id,
        "source": source,
        "status": "running",
        "scraped_count": 0,
        "upserted_count": 0,
        "snapshot_count": 0,
        "failed_count": 0,
        "error": None,
        "automation_campaign_ids": [],
        "automation_errors": [],
        "company_limit": company_limit,
        "available_company_count": 0,
        "selected_company_count": 0,
    }

    try:
        raw_jobs = scraper(**scraper_kwargs)
        summary["scraped_count"] = len(raw_jobs)

        normalized_jobs = [normalizer(job, run_id) for job in raw_jobs]
        valid_pairs, invalid_count = _filter_job_pairs(raw_jobs, normalized_jobs)
        summary["failed_count"] = invalid_count

        selected_raw_jobs = [raw_job for raw_job, _ in valid_pairs]
        selected_valid_jobs = [normalized_job for _, normalized_job in valid_pairs]
        (
            selected_raw_jobs,
            selected_valid_jobs,
            available_company_count,
            selected_company_count,
        ) = _apply_company_limit(
            selected_raw_jobs,
            selected_valid_jobs,
            company_limit=company_limit,
        )
        summary["available_company_count"] = available_company_count
        summary["selected_company_count"] = selected_company_count

        if selected_valid_jobs:
            summary["upserted_count"] = storage.upsert_jobs(selected_valid_jobs)
            selected_detail_urls = {job["detail_url"] for job in selected_valid_jobs}
            snapshots = [
                {
                    "run_id": run_id,
                    "source": source,
                    "detail_url": normalized_job["detail_url"],
                    "job_payload": build_job_snapshot(raw_job, normalized_job),
                    "scraped_at": _utcnow_iso(),
                }
                for raw_job, normalized_job in zip(selected_raw_jobs, selected_valid_jobs)
                if normalized_job.get("detail_url") in selected_detail_urls
            ]
            summary["snapshot_count"] = storage.insert_job_snapshots(snapshots)

        storage.complete_scrape_run(
            run_id,
            scraped_count=summary["scraped_count"],
            upserted_count=summary["upserted_count"],
            snapshot_count=summary["snapshot_count"],
            failed_count=summary["failed_count"],
        )
        automation_result = process_post_scrape_automations(
            run_id=run_id,
            source=source,
            storage=storage,
        )
        summary["automation_campaign_ids"] = automation_result["campaign_ids"]
        summary["automation_errors"] = automation_result["errors"]
        summary["status"] = "completed"
        return summary
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        storage.fail_scrape_run(
            run_id,
            scraped_count=summary["scraped_count"],
            upserted_count=summary["upserted_count"],
            snapshot_count=summary["snapshot_count"],
            failed_count=summary["failed_count"],
            error=str(exc),
        )
        return summary


def scrape_and_store_hzz(
    max_pages: int = 3,
    category: str | None = None,
    company_limit: int | None = None,
    *,
    storage: SupabaseStorage | None = None,
    scraper: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if scraper is None:
        from app.scrapers.hzz import scrape_hzz as default_hzz_scraper

        scraper = default_hzz_scraper

    def _normalizer(job: dict[str, Any], run_id: str) -> dict[str, Any]:
        return normalize_hzz_job(job, category=category, run_id=run_id)

    return _run_scrape_and_store(
        source="hzz",
        filters={"max_pages": max_pages, "category": category, "company_limit": company_limit},
        scraper=scraper,
        scraper_kwargs={"max_pages": max_pages, "category": category, "company_limit": company_limit},
        normalizer=_normalizer,
        company_limit=company_limit,
        storage=storage,
    )


def scrape_and_store_mojposao(
    keyword: str = "",
    max_clicks: int = 5,
    category: str | None = None,
    company_limit: int | None = None,
    *,
    storage: SupabaseStorage | None = None,
    scraper: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if scraper is None:
        from app.scrapers.mojposao import scrape_mojposao as default_mojposao_scraper

        scraper = default_mojposao_scraper

    def _normalizer(job: dict[str, Any], run_id: str) -> dict[str, Any]:
        return normalize_mojposao_job(job, run_id=run_id)

    return _run_scrape_and_store(
        source="mojposao",
        filters={"keyword": keyword, "max_clicks": max_clicks, "category": category, "company_limit": company_limit},
        scraper=scraper,
        scraper_kwargs={"keyword": keyword, "max_clicks": max_clicks, "category": category, "company_limit": company_limit},
        normalizer=_normalizer,
        company_limit=company_limit,
        storage=storage,
    )
