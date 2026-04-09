import os
from datetime import datetime, timezone
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - exercised only when dependency is missing
    load_dotenv = None

try:
    from supabase import create_client
except ImportError:  # pragma: no cover - exercised only when dependency is missing
    create_client = None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_rows(response: Any) -> list[dict[str, Any]]:
    data = getattr(response, "data", None)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


class SupabaseStorage:
    def __init__(self, client: Any):
        self.client = client

    def create_scrape_run(self, source: str, filters: dict[str, Any]) -> str:
        payload = {
            "source": source,
            "filters": filters,
            "status": "running",
            "scraped_count": 0,
            "upserted_count": 0,
            "snapshot_count": 0,
            "failed_count": 0,
            "updated_at": _utcnow_iso(),
        }
        response = self.client.table("scrape_runs").insert(payload).execute()
        rows = _extract_rows(response)
        if not rows or "id" not in rows[0]:
            raise RuntimeError("Supabase did not return a scrape_runs id")
        return str(rows[0]["id"])

    def upsert_jobs(self, jobs: list[dict[str, Any]]) -> int:
        if not jobs:
            return 0
        self.client.table("jobs").upsert(jobs, on_conflict="source,detail_url").execute()
        return len(jobs)

    def insert_job_snapshots(self, snapshots: list[dict[str, Any]]) -> int:
        if not snapshots:
            return 0
        self.client.table("job_snapshots").insert(snapshots).execute()
        return len(snapshots)

    def complete_scrape_run(
        self,
        run_id: str,
        *,
        scraped_count: int,
        upserted_count: int,
        snapshot_count: int,
        failed_count: int,
    ) -> None:
        payload = {
            "status": "completed",
            "scraped_count": scraped_count,
            "upserted_count": upserted_count,
            "snapshot_count": snapshot_count,
            "failed_count": failed_count,
            "finished_at": _utcnow_iso(),
            "updated_at": _utcnow_iso(),
        }
        self.client.table("scrape_runs").update(payload).eq("id", run_id).execute()

    def fail_scrape_run(
        self,
        run_id: str,
        *,
        scraped_count: int,
        upserted_count: int,
        snapshot_count: int,
        failed_count: int,
        error: str,
    ) -> None:
        payload = {
            "status": "failed",
            "scraped_count": scraped_count,
            "upserted_count": upserted_count,
            "snapshot_count": snapshot_count,
            "failed_count": failed_count,
            "error": error,
            "finished_at": _utcnow_iso(),
            "updated_at": _utcnow_iso(),
        }
        self.client.table("scrape_runs").update(payload).eq("id", run_id).execute()


def get_supabase_storage() -> SupabaseStorage:
    if load_dotenv is not None:
        load_dotenv()

    if create_client is None:
        raise RuntimeError("The 'supabase' package is not installed.")

    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and either SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY must be set.")

    return SupabaseStorage(create_client(supabase_url, service_role_key))
