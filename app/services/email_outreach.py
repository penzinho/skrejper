import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.db.supabase import SupabaseStorage, get_supabase_storage


PLACEHOLDER_DESCRIPTIONS = {
    "company": "Recipient company name.",
    "job_title": "Scraped job title tied to the selected company.",
    "location": "Job location.",
    "source": "Scraper source such as hzz or mojposao.",
    "detail_url": "Original scraped listing URL.",
    "published_at": "Published date for the listing.",
    "employer_email": "Recipient email address.",
}
PLACEHOLDER_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    normalized = str(value).strip()
    if not normalized:
        return None
    normalized = normalized.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _serialize_datetime(value: datetime | str | None) -> str | None:
    coerced = _coerce_datetime(value)
    return coerced.isoformat() if coerced else None


def _render_content(value: str | None, merge_data: dict[str, Any]) -> str | None:
    if value is None:
        return None

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        replacement = merge_data.get(key)
        return "" if replacement is None else str(replacement)

    return PLACEHOLDER_PATTERN.sub(_replace, value)


def _build_merge_data(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "company": job.get("company"),
        "job_title": job.get("title"),
        "location": job.get("location"),
        "source": job.get("source"),
        "detail_url": job.get("detail_url"),
        "published_at": job.get("published_at"),
        "employer_email": job.get("employer_email"),
    }


def _summarize_campaign(campaign: dict[str, Any], deliveries: list[dict[str, Any]]) -> dict[str, Any]:
    sent_count = sum(1 for delivery in deliveries if delivery.get("status") == "sent")
    failed_count = sum(1 for delivery in deliveries if delivery.get("status") == "failed")
    queued_count = sum(1 for delivery in deliveries if delivery.get("status") == "queued")
    return {
        "campaign_id": str(campaign["id"]),
        "name": campaign.get("name"),
        "status": campaign.get("status"),
        "scheduled_for": campaign.get("scheduled_for"),
        "total_recipients": len(deliveries),
        "sent_count": sent_count,
        "failed_count": failed_count,
        "queued_count": queued_count,
    }


def _resolve_campaign_content(
    *,
    storage: SupabaseStorage,
    template_id: str | None,
    subject: str | None,
    html_content: str | None,
    text_content: str | None,
) -> tuple[str | None, str, str, str | None]:
    if template_id:
        template = storage.get_email_template(template_id)
        if not template:
            raise ValueError(f"Unknown email template '{template_id}'")
        return (
            template_id,
            _clean_text(template.get("subject")) or "",
            _clean_text(template.get("html_content")) or "",
            _clean_text(template.get("text_content")),
        )

    resolved_subject = _clean_text(subject)
    resolved_html = _clean_text(html_content)
    resolved_text = _clean_text(text_content)
    if not resolved_subject or not resolved_html:
        raise ValueError("Either template_id or both subject and html_content must be provided.")
    return None, resolved_subject, resolved_html, resolved_text


def _build_deliveries(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    deliveries: list[dict[str, Any]] = []
    queued_job_ids: list[str] = []
    seen_emails: set[str] = set()

    for job in jobs:
        recipient_email = _clean_text(job.get("employer_email"))
        if not recipient_email:
            continue

        recipient_key = recipient_email.casefold()
        if recipient_key in seen_emails:
            continue
        seen_emails.add(recipient_key)

        deliveries.append(
            {
                "job_id": job.get("id"),
                "recipient_email": recipient_email,
                "recipient_company": job.get("company"),
                "merge_data": _build_merge_data(job),
                "status": "queued",
            }
        )
        if job.get("id"):
            queued_job_ids.append(str(job["id"]))

    return deliveries, queued_job_ids


def get_placeholder_catalog() -> list[dict[str, str]]:
    return [{"key": key, "description": description} for key, description in PLACEHOLDER_DESCRIPTIONS.items()]


class ResendEmailSender:
    def __init__(self, api_key: str | None = None, *, base_url: str = "https://api.resend.com/emails"):
        self.api_key = api_key or os.getenv("RESEND_API_KEY")
        self.base_url = base_url

    def send_email(
        self,
        *,
        to_email: str,
        subject: str,
        html_content: str,
        text_content: str | None,
        from_email: str,
        reply_to_email: str | None = None,
    ) -> str | None:
        if not self.api_key:
            raise RuntimeError("RESEND_API_KEY must be set before sending email campaigns.")

        payload: dict[str, Any] = {
            "from": from_email,
            "to": [to_email],
            "subject": subject,
            "html": html_content,
        }
        if text_content:
            payload["text"] = text_content
        if reply_to_email:
            payload["reply_to"] = reply_to_email

        request = Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:  # pragma: no cover - network interaction is mocked in tests
            details = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Resend request failed with status {exc.code}: {details}") from exc
        except URLError as exc:  # pragma: no cover - network interaction is mocked in tests
            raise RuntimeError(f"Resend request failed: {exc}") from exc

        return _clean_text(body.get("id"))


def upsert_email_template(
    *,
    name: str,
    subject: str,
    html_content: str,
    text_content: str | None = None,
    storage: SupabaseStorage | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    template = storage.upsert_email_template(
        {
            "name": name,
            "subject": subject,
            "html_content": html_content,
            "text_content": _clean_text(text_content),
        }
    )
    return template


def list_email_templates(*, storage: SupabaseStorage | None = None) -> list[dict[str, Any]]:
    storage = storage or get_supabase_storage()
    return storage.list_email_templates()


def list_jobs_for_email(
    *,
    source: str | None = None,
    run_id: str | None = None,
    job_ids: list[str] | None = None,
    only_not_emailed: bool = False,
    require_email: bool = True,
    storage: SupabaseStorage | None = None,
) -> list[dict[str, Any]]:
    storage = storage or get_supabase_storage()
    return storage.list_jobs_for_email(
        source=source,
        run_id=run_id,
        job_ids=job_ids,
        only_not_emailed=only_not_emailed,
        require_email=require_email,
    )


def create_email_campaign(
    *,
    name: str,
    source: str | None = None,
    run_id: str | None = None,
    job_ids: list[str] | None = None,
    only_not_emailed: bool = False,
    require_email: bool = True,
    template_id: str | None = None,
    subject: str | None = None,
    html_content: str | None = None,
    text_content: str | None = None,
    sender_email: str | None = None,
    reply_to_email: str | None = None,
    created_by: str | None = None,
    scheduled_for: datetime | str | None = None,
    send_now: bool = False,
    mode: str = "manual_selection",
    queue_immediately: bool = False,
    storage: SupabaseStorage | None = None,
    email_sender: ResendEmailSender | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()

    if not job_ids and not run_id and not source:
        raise ValueError("At least one target selector must be provided: job_ids, run_id, or source.")

    resolved_template_id, resolved_subject, resolved_html, resolved_text = _resolve_campaign_content(
        storage=storage,
        template_id=template_id,
        subject=subject,
        html_content=html_content,
        text_content=text_content,
    )

    jobs = storage.list_jobs_for_email(
        source=source,
        run_id=run_id,
        job_ids=job_ids,
        only_not_emailed=only_not_emailed,
        require_email=require_email,
    )
    deliveries, queued_job_ids = _build_deliveries(jobs)
    if not deliveries:
        raise ValueError("No email-eligible jobs matched the campaign target selection.")

    normalized_scheduled_for = _serialize_datetime(scheduled_for)
    initial_status = "draft"
    if send_now or queue_immediately or normalized_scheduled_for:
        initial_status = "queued"

    campaign = storage.create_email_campaign(
        {
            "name": name,
            "mode": mode,
            "status": initial_status,
            "template_id": resolved_template_id,
            "source": source,
            "last_scrape_run_id": run_id,
            "subject": resolved_subject,
            "html_content": resolved_html,
            "text_content": resolved_text,
            "sender_email": _clean_text(sender_email),
            "reply_to_email": _clean_text(reply_to_email),
            "created_by": _clean_text(created_by),
            "filters": {
                "job_ids": job_ids or [],
                "only_not_emailed": only_not_emailed,
                "require_email": require_email,
            },
            "scheduled_for": normalized_scheduled_for,
            "total_recipients": len(deliveries),
            "sent_count": 0,
            "failed_count": 0,
        }
    )

    for delivery in deliveries:
        delivery["campaign_id"] = campaign["id"]
    storage.insert_email_deliveries(deliveries)
    storage.mark_jobs_email_queued(queued_job_ids)

    if send_now:
        return send_email_campaign(
            campaign_id=str(campaign["id"]),
            storage=storage,
            email_sender=email_sender,
        )

    stored_deliveries = storage.list_email_deliveries(str(campaign["id"]))
    return _summarize_campaign(campaign, stored_deliveries)


def upsert_email_automation_rule(
    *,
    name: str,
    source: str | None = None,
    template_id: str | None = None,
    subject: str | None = None,
    html_content: str | None = None,
    text_content: str | None = None,
    sender_email: str | None = None,
    reply_to_email: str | None = None,
    created_by: str | None = None,
    enabled: bool = True,
    auto_send: bool = False,
    delay_minutes: int = 0,
    only_not_emailed: bool = True,
    require_email: bool = True,
    storage: SupabaseStorage | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    resolved_template_id, resolved_subject, resolved_html, resolved_text = _resolve_campaign_content(
        storage=storage,
        template_id=template_id,
        subject=subject,
        html_content=html_content,
        text_content=text_content,
    )

    rule = storage.upsert_email_automation_rule(
        {
            "name": name,
            "enabled": enabled,
            "source": _clean_text(source),
            "template_id": resolved_template_id,
            "subject": resolved_subject,
            "html_content": resolved_html,
            "text_content": resolved_text,
            "sender_email": _clean_text(sender_email),
            "reply_to_email": _clean_text(reply_to_email),
            "created_by": _clean_text(created_by),
            "auto_send": auto_send,
            "delay_minutes": max(int(delay_minutes), 0),
            "only_not_emailed": only_not_emailed,
            "require_email": require_email,
            "filters": {},
        }
    )
    return rule


def list_email_automation_rules(*, storage: SupabaseStorage | None = None) -> list[dict[str, Any]]:
    storage = storage or get_supabase_storage()
    return storage.list_email_automation_rules()


def upsert_email_warmup_settings(
    *,
    enabled: bool = True,
    initial_daily_limit: int,
    daily_increment: int = 0,
    increment_interval_days: int = 1,
    max_daily_limit: int | None = None,
    started_at: datetime | str | None = None,
    name: str = "default",
    storage: SupabaseStorage | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    initial_limit = max(int(initial_daily_limit), 0)
    increment = max(int(daily_increment), 0)
    interval_days = max(int(increment_interval_days), 1)
    max_limit = max(int(max_daily_limit if max_daily_limit is not None else initial_limit), initial_limit)

    settings = storage.upsert_email_warmup_settings(
        {
            "name": name,
            "enabled": enabled,
            "initial_daily_limit": initial_limit,
            "daily_increment": increment,
            "increment_interval_days": interval_days,
            "max_daily_limit": max_limit,
            "started_at": _serialize_datetime(started_at) or _utcnow_iso(),
        }
    )
    return settings


def get_email_warmup_status(
    *,
    storage: SupabaseStorage | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    now = now or _utcnow()
    settings = storage.get_email_warmup_settings()
    if not settings:
        return {
            "settings": None,
            "effective_daily_limit": None,
            "sent_today": 0,
            "remaining_today": None,
        }

    effective_daily_limit = None
    sent_today = 0
    remaining_today = None

    if settings.get("enabled", True):
        started_at = _coerce_datetime(settings.get("started_at")) or now
        elapsed_days = max((now.date() - started_at.date()).days, 0)
        interval_days = max(int(settings.get("increment_interval_days", 1)), 1)
        initial_limit = max(int(settings.get("initial_daily_limit", 0)), 0)
        increment = max(int(settings.get("daily_increment", 0)), 0)
        max_limit = max(int(settings.get("max_daily_limit", initial_limit)), initial_limit)
        effective_daily_limit = min(max_limit, initial_limit + (elapsed_days // interval_days) * increment)

        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        sent_today = storage.count_sent_email_deliveries_between(day_start.isoformat(), day_end.isoformat())
        remaining_today = max(effective_daily_limit - sent_today, 0)

    return {
        "settings": settings,
        "effective_daily_limit": effective_daily_limit,
        "sent_today": sent_today,
        "remaining_today": remaining_today,
    }


def send_email_campaign(
    *,
    campaign_id: str,
    storage: SupabaseStorage | None = None,
    email_sender: ResendEmailSender | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    email_sender = email_sender or ResendEmailSender()
    now = now or _utcnow()

    campaign = storage.get_email_campaign(campaign_id)
    if not campaign:
        raise ValueError(f"Unknown email campaign '{campaign_id}'")

    deliveries = storage.list_email_deliveries(campaign_id)
    queued_deliveries = [delivery for delivery in deliveries if delivery.get("status") == "queued"]
    if not queued_deliveries:
        return _summarize_campaign(campaign, deliveries)

    warmup_status = get_email_warmup_status(storage=storage, now=now)
    remaining_today = warmup_status["remaining_today"]
    if remaining_today is None:
        max_to_send = len(queued_deliveries)
    else:
        max_to_send = min(len(queued_deliveries), remaining_today)

    if max_to_send <= 0:
        campaign["status"] = "queued"
        storage.update_email_campaign(campaign_id, {"status": "queued"})
        summary = _summarize_campaign(campaign, deliveries)
        summary["warmup_remaining_today"] = 0
        return summary

    from_email = _clean_text(campaign.get("sender_email")) or _clean_text(os.getenv("RESEND_FROM_EMAIL"))
    if not from_email:
        raise RuntimeError("Campaign sender_email is missing and RESEND_FROM_EMAIL is not set.")

    storage.update_email_campaign(campaign_id, {"status": "sending"})
    campaign["status"] = "sending"

    for delivery in queued_deliveries[:max_to_send]:
        merge_data = delivery.get("merge_data") or {}
        sent_at = now.isoformat()
        try:
            resend_email_id = email_sender.send_email(
                to_email=delivery["recipient_email"],
                subject=_render_content(campaign["subject"], merge_data) or "",
                html_content=_render_content(campaign["html_content"], merge_data) or "",
                text_content=_render_content(campaign.get("text_content"), merge_data),
                from_email=from_email,
                reply_to_email=_clean_text(campaign.get("reply_to_email")),
            )
            storage.update_email_delivery(
                str(delivery["id"]),
                {
                    "status": "sent",
                    "resend_email_id": resend_email_id,
                    "sent_at": sent_at,
                    "error": None,
                },
            )
            delivery["status"] = "sent"
            delivery["sent_at"] = sent_at
            if delivery.get("job_id"):
                storage.mark_job_email_sent(str(delivery["job_id"]), sent_at=sent_at)
        except Exception as exc:
            error_message = str(exc)
            storage.update_email_delivery(
                str(delivery["id"]),
                {
                    "status": "failed",
                    "error": error_message,
                },
            )
            delivery["status"] = "failed"
            delivery["error"] = error_message
            if delivery.get("job_id"):
                storage.mark_job_email_failed(str(delivery["job_id"]), error=error_message)

    refreshed_deliveries = storage.list_email_deliveries(campaign_id)
    sent_count = sum(1 for delivery in refreshed_deliveries if delivery.get("status") == "sent")
    failed_count = sum(1 for delivery in refreshed_deliveries if delivery.get("status") == "failed")
    queued_count = sum(1 for delivery in refreshed_deliveries if delivery.get("status") == "queued")

    if queued_count > 0:
        campaign_status = "queued"
        finished_at = None
    elif failed_count > 0 and sent_count > 0:
        campaign_status = "partial"
        finished_at = now.isoformat()
    elif failed_count > 0:
        campaign_status = "failed"
        finished_at = now.isoformat()
    else:
        campaign_status = "sent"
        finished_at = now.isoformat()

    storage.update_email_campaign(
        campaign_id,
        {
            "status": campaign_status,
            "sent_count": sent_count,
            "failed_count": failed_count,
            "finished_at": finished_at,
        },
    )
    campaign["status"] = campaign_status
    campaign["sent_count"] = sent_count
    campaign["failed_count"] = failed_count
    campaign["finished_at"] = finished_at

    summary = _summarize_campaign(campaign, refreshed_deliveries)
    latest_warmup = get_email_warmup_status(storage=storage, now=now)
    summary["warmup_remaining_today"] = latest_warmup["remaining_today"]
    return summary


def dispatch_due_email_campaigns(
    *,
    storage: SupabaseStorage | None = None,
    email_sender: ResendEmailSender | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    storage = storage or get_supabase_storage()
    email_sender = email_sender or ResendEmailSender()
    now = now or _utcnow()
    results: list[dict[str, Any]] = []

    for campaign in storage.list_queued_email_campaigns():
        scheduled_for = _coerce_datetime(campaign.get("scheduled_for"))
        if scheduled_for and scheduled_for > now:
            continue
        result = send_email_campaign(
            campaign_id=str(campaign["id"]),
            storage=storage,
            email_sender=email_sender,
            now=now,
        )
        results.append(result)
        if result.get("warmup_remaining_today") == 0:
            break

    return {"results": results}


def process_post_scrape_automations(
    *,
    run_id: str,
    source: str,
    storage: SupabaseStorage | None = None,
    email_sender: ResendEmailSender | None = None,
    now: datetime | None = None,
) -> dict[str, list[str]]:
    storage = storage or get_supabase_storage()
    now = now or _utcnow()

    campaign_ids: list[str] = []
    errors: list[str] = []
    rules = storage.list_email_automation_rules(enabled_only=True)

    for rule in rules:
        rule_source = _clean_text(rule.get("source"))
        if rule_source and rule_source != source:
            continue

        try:
            scheduled_for = now + timedelta(minutes=max(int(rule.get("delay_minutes", 0)), 0))
            should_send_now = bool(rule.get("auto_send")) and int(rule.get("delay_minutes", 0)) == 0
            result = create_email_campaign(
                name=f"{rule['name']} ({source} {run_id[:8]})",
                source=source,
                run_id=run_id,
                only_not_emailed=bool(rule.get("only_not_emailed", True)),
                require_email=bool(rule.get("require_email", True)),
                template_id=str(rule["template_id"]) if rule.get("template_id") else None,
                subject=rule.get("subject"),
                html_content=rule.get("html_content"),
                text_content=rule.get("text_content"),
                sender_email=rule.get("sender_email"),
                reply_to_email=rule.get("reply_to_email"),
                created_by=rule.get("created_by"),
                scheduled_for=scheduled_for if int(rule.get("delay_minutes", 0)) > 0 else None,
                send_now=should_send_now,
                mode="automation_after_scrape",
                queue_immediately=not should_send_now,
                storage=storage,
                email_sender=email_sender,
            )
            campaign_ids.append(result["campaign_id"])
        except Exception as exc:
            errors.append(f"{rule.get('name', 'automation')}: {exc}")

    return {"campaign_ids": campaign_ids, "errors": errors}
