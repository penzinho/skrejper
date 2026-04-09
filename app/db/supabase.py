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


def _extract_first_row(response: Any) -> dict[str, Any] | None:
    rows = _extract_rows(response)
    return rows[0] if rows else None


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
        row = _extract_first_row(response)
        if not row or "id" not in row:
            raise RuntimeError("Supabase did not return a scrape_runs id")
        return str(row["id"])

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

    def list_jobs_for_email(
        self,
        *,
        source: str | None = None,
        run_id: str | None = None,
        job_ids: list[str] | None = None,
        only_not_emailed: bool = False,
        require_email: bool = True,
    ) -> list[dict[str, Any]]:
        query = self.client.table("jobs").select("*")
        if source:
            query = query.eq("source", source)
        if run_id:
            query = query.eq("last_run_id", run_id)
        if job_ids:
            query = query.in_("id", job_ids)
        if only_not_emailed:
            query = query.neq("email_status", "sent")
        if require_email:
            query = query.not_.is_("employer_email", "null")
        response = query.order("updated_at", desc=True).execute()
        return _extract_rows(response)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        response = self.client.table("jobs").select("*").eq("id", job_id).limit(1).execute()
        return _extract_first_row(response)

    def mark_jobs_email_queued(self, job_ids: list[str]) -> None:
        if not job_ids:
            return
        payload = {
            "email_status": "queued",
            "email_last_error": None,
            "updated_at": _utcnow_iso(),
        }
        self.client.table("jobs").update(payload).in_("id", job_ids).execute()

    def mark_job_email_sent(self, job_id: str, *, sent_at: str) -> None:
        current = self.get_job(job_id) or {}
        payload = {
            "email_status": "sent",
            "email_last_sent_at": sent_at,
            "email_last_error": None,
            "email_send_count": int(current.get("email_send_count", 0)) + 1,
            "updated_at": _utcnow_iso(),
        }
        self.client.table("jobs").update(payload).eq("id", job_id).execute()

    def mark_job_email_failed(self, job_id: str, *, error: str) -> None:
        payload = {
            "email_status": "failed",
            "email_last_error": error,
            "updated_at": _utcnow_iso(),
        }
        self.client.table("jobs").update(payload).eq("id", job_id).execute()

    def upsert_email_template(self, template: dict[str, Any]) -> dict[str, Any]:
        payload = {**template, "updated_at": _utcnow_iso()}
        response = self.client.table("email_templates").upsert(payload, on_conflict="name").execute()
        row = _extract_first_row(response)
        if not row:
            raise RuntimeError("Supabase did not return an email_templates row")
        return row

    def list_email_templates(self) -> list[dict[str, Any]]:
        response = self.client.table("email_templates").select("*").order("updated_at", desc=True).execute()
        return _extract_rows(response)

    def get_email_template(self, template_id: str) -> dict[str, Any] | None:
        response = self.client.table("email_templates").select("*").eq("id", template_id).limit(1).execute()
        return _extract_first_row(response)

    def create_email_campaign(self, campaign: dict[str, Any]) -> dict[str, Any]:
        payload = {**campaign, "updated_at": _utcnow_iso()}
        response = self.client.table("email_campaigns").insert(payload).execute()
        row = _extract_first_row(response)
        if not row:
            raise RuntimeError("Supabase did not return an email_campaigns row")
        return row

    def get_email_campaign(self, campaign_id: str) -> dict[str, Any] | None:
        response = self.client.table("email_campaigns").select("*").eq("id", campaign_id).limit(1).execute()
        return _extract_first_row(response)

    def list_email_campaigns(self) -> list[dict[str, Any]]:
        response = self.client.table("email_campaigns").select("*").order("created_at", desc=True).execute()
        return _extract_rows(response)

    def list_queued_email_campaigns(self) -> list[dict[str, Any]]:
        response = self.client.table("email_campaigns").select("*").eq("status", "queued").order("created_at").execute()
        return _extract_rows(response)

    def update_email_campaign(self, campaign_id: str, payload: dict[str, Any]) -> None:
        next_payload = {**payload, "updated_at": _utcnow_iso()}
        self.client.table("email_campaigns").update(next_payload).eq("id", campaign_id).execute()

    def insert_email_deliveries(self, deliveries: list[dict[str, Any]]) -> int:
        if not deliveries:
            return 0
        self.client.table("email_deliveries").insert(deliveries).execute()
        return len(deliveries)

    def list_email_deliveries(self, campaign_id: str) -> list[dict[str, Any]]:
        response = (
            self.client.table("email_deliveries")
            .select("*")
            .eq("campaign_id", campaign_id)
            .order("created_at")
            .execute()
        )
        return _extract_rows(response)

    def update_email_delivery(self, delivery_id: str, payload: dict[str, Any]) -> None:
        next_payload = {**payload, "updated_at": _utcnow_iso()}
        self.client.table("email_deliveries").update(next_payload).eq("id", delivery_id).execute()

    def upsert_email_automation_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        payload = {**rule, "updated_at": _utcnow_iso()}
        response = self.client.table("email_automation_rules").upsert(payload, on_conflict="name").execute()
        row = _extract_first_row(response)
        if not row:
            raise RuntimeError("Supabase did not return an email_automation_rules row")
        return row

    def list_email_automation_rules(self, *, source: str | None = None, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = self.client.table("email_automation_rules").select("*")
        if source:
            query = query.eq("source", source)
        if enabled_only:
            query = query.eq("enabled", True)
        response = query.order("updated_at", desc=True).execute()
        return _extract_rows(response)

    def get_email_automation_rule(self, rule_id: str) -> dict[str, Any] | None:
        response = self.client.table("email_automation_rules").select("*").eq("id", rule_id).limit(1).execute()
        return _extract_first_row(response)

    def upsert_email_warmup_settings(self, settings: dict[str, Any]) -> dict[str, Any]:
        payload = {**settings, "updated_at": _utcnow_iso()}
        response = self.client.table("email_warmup_settings").upsert(payload, on_conflict="name").execute()
        row = _extract_first_row(response)
        if not row:
            raise RuntimeError("Supabase did not return an email_warmup_settings row")
        return row

    def get_email_warmup_settings(self) -> dict[str, Any] | None:
        response = (
            self.client.table("email_warmup_settings")
            .select("*")
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        return _extract_first_row(response)

    def count_sent_email_deliveries_between(self, start_iso: str, end_iso: str) -> int:
        response = (
            self.client.table("email_deliveries")
            .select("id")
            .eq("status", "sent")
            .gte("sent_at", start_iso)
            .lt("sent_at", end_iso)
            .execute()
        )
        return len(_extract_rows(response))


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
