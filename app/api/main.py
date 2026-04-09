import os
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.db.supabase import get_supabase_storage
from app.scrapers.hzz import get_hzz_categories
from app.scrapers.mojposao import get_mojposao_categories
from app.services.email_outreach import (
    create_email_campaign,
    dispatch_due_email_campaigns,
    get_email_warmup_status,
    get_placeholder_catalog,
    list_email_automation_rules,
    list_email_templates,
    list_jobs_for_email,
    send_email_campaign,
    upsert_email_automation_rule,
    upsert_email_template,
    upsert_email_warmup_settings,
)
from app.services.scrape_store import scrape_and_store_hzz, scrape_and_store_mojposao


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _get_allowed_origins() -> list[str]:
    configured = os.getenv("CORS_ALLOW_ORIGINS", "*")
    origins = [origin.strip() for origin in configured.split(",") if origin.strip()]
    return origins or ["*"]


class ScrapeSummaryResponse(BaseModel):
    run_id: str
    source: str
    status: str
    scraped_count: int
    upserted_count: int
    snapshot_count: int
    failed_count: int
    error: str | None = None
    automation_campaign_ids: list[str] = Field(default_factory=list)
    automation_errors: list[str] = Field(default_factory=list)


class HZZScrapeRequest(BaseModel):
    max_pages: int = Field(default=3, ge=1)
    category: str | None = None


class MojPosaoScrapeRequest(BaseModel):
    keyword: str = ""
    max_clicks: int = Field(default=5, ge=1)
    category: str | None = None


class RunAllScrapersRequest(BaseModel):
    hzz: HZZScrapeRequest = Field(default_factory=HZZScrapeRequest)
    mojposao: MojPosaoScrapeRequest = Field(default_factory=MojPosaoScrapeRequest)


class RunAllScrapersResponse(BaseModel):
    results: list[ScrapeSummaryResponse]


class EmailJobTargetResponse(BaseModel):
    id: str
    title: str | None = None
    company: str | None = None
    location: str | None = None
    source: str
    detail_url: str
    employer_email: str | None = None
    category: str | None = None
    published_at: str | None = None
    last_run_id: str | None = None
    email_status: str
    email_last_sent_at: str | None = None
    email_last_error: str | None = None
    email_send_count: int = 0


class EmailTemplateRequest(BaseModel):
    name: str
    subject: str
    html_content: str
    text_content: str | None = None


class EmailTemplateResponse(EmailTemplateRequest):
    id: str
    created_at: str | None = None
    updated_at: str | None = None


class EmailCampaignTargetRequest(BaseModel):
    job_ids: list[str] = Field(default_factory=list)
    source: str | None = None
    run_id: str | None = None
    only_not_emailed: bool = False
    require_email: bool = True


class CreateEmailCampaignRequest(BaseModel):
    name: str
    target: EmailCampaignTargetRequest
    template_id: str | None = None
    subject: str | None = None
    html_content: str | None = None
    text_content: str | None = None
    sender_email: str | None = None
    reply_to_email: str | None = None
    created_by: str | None = None
    scheduled_for: datetime | None = None
    send_now: bool = False


class EmailCampaignResponse(BaseModel):
    campaign_id: str
    name: str | None = None
    status: str
    scheduled_for: str | None = None
    total_recipients: int
    sent_count: int
    failed_count: int
    queued_count: int
    warmup_remaining_today: int | None = None


class DispatchDueCampaignsResponse(BaseModel):
    results: list[EmailCampaignResponse]


class EmailAutomationRuleRequest(BaseModel):
    name: str
    source: str | None = None
    template_id: str | None = None
    subject: str | None = None
    html_content: str | None = None
    text_content: str | None = None
    sender_email: str | None = None
    reply_to_email: str | None = None
    created_by: str | None = None
    enabled: bool = True
    auto_send: bool = False
    delay_minutes: int = Field(default=0, ge=0)
    only_not_emailed: bool = True
    require_email: bool = True


class EmailAutomationRuleResponse(EmailAutomationRuleRequest):
    id: str
    created_at: str | None = None
    updated_at: str | None = None


class EmailWarmupSettingsRequest(BaseModel):
    enabled: bool = True
    initial_daily_limit: int = Field(ge=0)
    daily_increment: int = Field(default=0, ge=0)
    increment_interval_days: int = Field(default=1, ge=1)
    max_daily_limit: int | None = Field(default=None, ge=0)
    started_at: datetime | None = None


class EmailWarmupStatusResponse(BaseModel):
    settings: dict[str, Any] | None = None
    effective_daily_limit: int | None = None
    sent_today: int
    remaining_today: int | None = None


app = FastAPI(title="Lead Generation API", version="1.0")

allowed_origins = _get_allowed_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials="*" not in allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _raise_for_failed_summary(summary: dict) -> None:
    if summary.get("status") != "failed":
        return

    error = summary.get("error") or "Scrape failed"
    status_code = 400 if str(error).startswith("Unknown ") else 500
    raise HTTPException(status_code=status_code, detail=summary)


def _run_service(callable_obj, *args, **kwargs):
    try:
        return callable_obj(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health() -> dict[str, datetime | str]:
    return {"status": "ok", "time": _utcnow()}


@app.get("/scrapers/hzz/categories")
def list_hzz_categories() -> list[dict[str, str]]:
    return get_hzz_categories()


@app.get("/scrapers/mojposao/categories")
def list_mojposao_categories() -> list[dict[str, str]]:
    return get_mojposao_categories()


@app.post("/scrapers/hzz", response_model=ScrapeSummaryResponse)
def run_hzz_scraper(payload: HZZScrapeRequest) -> dict:
    summary = scrape_and_store_hzz(max_pages=payload.max_pages, category=payload.category)
    _raise_for_failed_summary(summary)
    return summary


@app.post("/scrapers/mojposao", response_model=ScrapeSummaryResponse)
def run_mojposao_scraper(payload: MojPosaoScrapeRequest) -> dict:
    summary = scrape_and_store_mojposao(
        keyword=payload.keyword,
        max_clicks=payload.max_clicks,
        category=payload.category,
    )
    _raise_for_failed_summary(summary)
    return summary


@app.post("/scrapers/run-all", response_model=RunAllScrapersResponse)
def run_all_scrapers(payload: RunAllScrapersRequest) -> dict[str, list[dict]]:
    results = [
        scrape_and_store_hzz(max_pages=payload.hzz.max_pages, category=payload.hzz.category),
        scrape_and_store_mojposao(
            keyword=payload.mojposao.keyword,
            max_clicks=payload.mojposao.max_clicks,
            category=payload.mojposao.category,
        ),
    ]

    for result in results:
        _raise_for_failed_summary(result)

    return {"results": results}


@app.get("/jobs/email-targets", response_model=list[EmailJobTargetResponse])
def get_email_targets(
    source: str | None = None,
    run_id: str | None = None,
    only_not_emailed: bool = False,
    require_email: bool = True,
) -> list[dict[str, Any]]:
    return _run_service(
        list_jobs_for_email,
        source=source,
        run_id=run_id,
        only_not_emailed=only_not_emailed,
        require_email=require_email,
    )


@app.get("/email/placeholders")
def get_email_placeholders() -> list[dict[str, str]]:
    return get_placeholder_catalog()


@app.get("/email/templates", response_model=list[EmailTemplateResponse])
def get_email_templates() -> list[dict[str, Any]]:
    return _run_service(list_email_templates)


@app.post("/email/templates", response_model=EmailTemplateResponse)
def save_email_template(payload: EmailTemplateRequest) -> dict[str, Any]:
    return _run_service(
        upsert_email_template,
        name=payload.name,
        subject=payload.subject,
        html_content=payload.html_content,
        text_content=payload.text_content,
    )


@app.get("/email/campaigns")
def get_email_campaigns() -> list[dict[str, Any]]:
    return get_supabase_storage().list_email_campaigns()


@app.post("/email/campaigns", response_model=EmailCampaignResponse)
def create_campaign(payload: CreateEmailCampaignRequest) -> dict[str, Any]:
    return _run_service(
        create_email_campaign,
        name=payload.name,
        source=payload.target.source,
        run_id=payload.target.run_id,
        job_ids=payload.target.job_ids,
        only_not_emailed=payload.target.only_not_emailed,
        require_email=payload.target.require_email,
        template_id=payload.template_id,
        subject=payload.subject,
        html_content=payload.html_content,
        text_content=payload.text_content,
        sender_email=payload.sender_email,
        reply_to_email=payload.reply_to_email,
        created_by=payload.created_by,
        scheduled_for=payload.scheduled_for,
        send_now=payload.send_now,
    )


@app.post("/email/campaigns/{campaign_id}/send", response_model=EmailCampaignResponse)
def send_campaign(campaign_id: str) -> dict[str, Any]:
    return _run_service(send_email_campaign, campaign_id=campaign_id)


@app.post("/email/campaigns/dispatch-due", response_model=DispatchDueCampaignsResponse)
def dispatch_due_campaigns() -> dict[str, list[dict[str, Any]]]:
    return _run_service(dispatch_due_email_campaigns)


@app.get("/email/automation-rules", response_model=list[EmailAutomationRuleResponse])
def get_automation_rules() -> list[dict[str, Any]]:
    return _run_service(list_email_automation_rules)


@app.post("/email/automation-rules", response_model=EmailAutomationRuleResponse)
def save_automation_rule(payload: EmailAutomationRuleRequest) -> dict[str, Any]:
    return _run_service(
        upsert_email_automation_rule,
        name=payload.name,
        source=payload.source,
        template_id=payload.template_id,
        subject=payload.subject,
        html_content=payload.html_content,
        text_content=payload.text_content,
        sender_email=payload.sender_email,
        reply_to_email=payload.reply_to_email,
        created_by=payload.created_by,
        enabled=payload.enabled,
        auto_send=payload.auto_send,
        delay_minutes=payload.delay_minutes,
        only_not_emailed=payload.only_not_emailed,
        require_email=payload.require_email,
    )


@app.get("/email/warmup", response_model=EmailWarmupStatusResponse)
def get_warmup_status() -> dict[str, Any]:
    return _run_service(get_email_warmup_status)


@app.put("/email/warmup", response_model=EmailWarmupStatusResponse)
def save_warmup_settings(payload: EmailWarmupSettingsRequest) -> dict[str, Any]:
    _run_service(
        upsert_email_warmup_settings,
        enabled=payload.enabled,
        initial_daily_limit=payload.initial_daily_limit,
        daily_increment=payload.daily_increment,
        increment_interval_days=payload.increment_interval_days,
        max_daily_limit=payload.max_daily_limit,
        started_at=payload.started_at,
    )
    return _run_service(get_email_warmup_status)
