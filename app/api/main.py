import os
import secrets
from datetime import datetime, timezone
from typing import Annotated, Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

_CET = ZoneInfo("Europe/Zagreb")

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.datastructures import Headers

from app.db.supabase import get_supabase_storage
from app.queue import enqueue_task, get_task_status
from app.rate_limit import rate_limiter
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

SCRAPER_API_KEY_ENV_VAR = "SCRAPER_API_KEY"
CORS_ALLOW_ORIGINS_ENV_VAR = "CORS_ALLOW_ORIGINS"
LANDING_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ProTalent</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f1e8;
      --panel: rgba(255, 255, 255, 0.82);
      --text: #182127;
      --muted: #5c6a73;
      --accent: #0f766e;
      --accent-strong: #115e59;
      --border: rgba(24, 33, 39, 0.08);
      --shadow: 0 24px 60px rgba(24, 33, 39, 0.14);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 34%),
        radial-gradient(circle at bottom right, rgba(245, 158, 11, 0.12), transparent 30%),
        linear-gradient(135deg, #f8f5ee 0%, var(--bg) 55%, #ebe3d1 100%);
      display: grid;
      place-items: center;
      padding: 24px;
    }

    main {
      width: min(760px, 100%);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 28px;
      padding: 40px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }

    .eyebrow {
      display: inline-block;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.1);
      color: var(--accent-strong);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }

    h1 {
      margin: 18px 0 12px;
      font-size: clamp(2.4rem, 6vw, 4.4rem);
      line-height: 0.95;
      letter-spacing: -0.05em;
    }

    p {
      margin: 0;
      max-width: 54ch;
      color: var(--muted);
      font-size: 1.05rem;
      line-height: 1.7;
    }

    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 28px;
    }

    a {
      text-decoration: none;
    }

    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 46px;
      padding: 0 18px;
      border-radius: 14px;
      font-weight: 600;
      transition: transform 140ms ease, background 140ms ease;
    }

    .button.primary {
      background: var(--accent);
      color: #fff;
    }

    .button.secondary {
      background: rgba(24, 33, 39, 0.05);
      color: var(--text);
    }

    .button:hover {
      transform: translateY(-1px);
    }

    .meta {
      margin-top: 32px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
      display: grid;
      gap: 10px;
      color: var(--muted);
      font-size: 0.95rem;
    }

    code {
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      color: var(--accent-strong);
      background: rgba(15, 118, 110, 0.08);
      padding: 2px 6px;
      border-radius: 8px;
    }

    @media (max-width: 640px) {
      main {
        padding: 28px 22px;
        border-radius: 22px;
      }
    }
  </style>
</head>
<body>
  <main>
    <span class="eyebrow">Production Service</span>
    <h1>ProTalent</h1>
    <p>
      This service is online, secured with TLS, and operating normally.
    </p>
    <div class="actions">
      <a class="button primary" href="/health">Health Check</a>
    </div>
    <div class="meta">
      <div>Base URL: <code>https://scrape.protalent.hr</code></div>
      <div>Primary health endpoint: <code>GET /health</code></div>
    </div>
  </main>
</body>
</html>
"""


def _utcnow() -> datetime:
    return datetime.now(_CET)


def _get_allowed_origins() -> list[str]:
    configured = os.getenv(CORS_ALLOW_ORIGINS_ENV_VAR, "flow.protalent.hr")
    origins = [origin.strip() for origin in configured.split(",") if origin.strip()]
    return origins or ["flow.protalent.hr"]


def _normalize_origin(origin: str) -> str:
    return origin.strip().lower().rstrip("/")


def _extract_origin_host(origin: str) -> str | None:
    normalized_origin = _normalize_origin(origin)
    if not normalized_origin:
        return None
    parsed = urlsplit(normalized_origin if "://" in normalized_origin else f"https://{normalized_origin}")
    return parsed.hostname.lower() if parsed.hostname else None


def _split_allowed_origins(origins: list[str]) -> tuple[set[str], set[str]]:
    exact_origins: set[str] = set()
    allowed_hosts: set[str] = set()

    for origin in origins:
        normalized_origin = _normalize_origin(origin)
        if not normalized_origin:
            continue
        if "://" in normalized_origin:
            exact_origins.add(normalized_origin)
            continue

        host = _extract_origin_host(normalized_origin)
        if host:
            allowed_hosts.add(host)

    return exact_origins, allowed_hosts


def _build_cors_allowed_origins(origins: list[str]) -> list[str]:
    cors_origins: list[str] = []
    seen: set[str] = set()

    for origin in origins:
        normalized_origin = _normalize_origin(origin)
        if not normalized_origin:
            continue

        candidates = [normalized_origin] if "://" in normalized_origin else [
            f"https://{normalized_origin}",
            f"http://{normalized_origin}",
        ]
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            cors_origins.append(candidate)

    return cors_origins


def _is_allowed_origin(origin: str, exact_origins: set[str], allowed_hosts: set[str]) -> bool:
    normalized_origin = _normalize_origin(origin)
    if normalized_origin in exact_origins:
        return True

    origin_host = _extract_origin_host(normalized_origin)
    return origin_host in allowed_hosts if origin_host else False


class OriginValidationMiddleware:
    def __init__(self, app, allowed_origins: list[str]) -> None:
        self.app = app
        self.exact_origins, self.allowed_hosts = _split_allowed_origins(allowed_origins)

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        origin = Headers(scope=scope).get("origin")
        if origin and not _is_allowed_origin(origin, self.exact_origins, self.allowed_hosts):
            response = JSONResponse(status_code=403, content={"detail": "Origin not allowed"})
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


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
    company_limit: int | None = None
    available_company_count: int = 0
    selected_company_count: int = 0


class QueuedTaskResponse(BaseModel):
    task_id: str
    task_name: str
    status: str
    queued_at: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    ready: bool
    successful: bool
    result: Any = None
    error: str | None = None


class HZZScrapeRequest(BaseModel):
    max_pages: int = Field(default=3, ge=1)
    category: str | None = None
    company_limit: int | None = Field(default=None, ge=1)
    async_job: bool = True


class MojPosaoScrapeRequest(BaseModel):
    keyword: str = ""
    max_clicks: int = Field(default=5, ge=1)
    category: str | None = None
    company_limit: int | None = Field(default=None, ge=1)
    async_job: bool = True


class RunAllScrapersRequest(BaseModel):
    hzz: HZZScrapeRequest = Field(default_factory=HZZScrapeRequest)
    mojposao: MojPosaoScrapeRequest = Field(default_factory=MojPosaoScrapeRequest)
    async_job: bool = True


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
    async_job: bool = True


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


app = FastAPI(
    title="Lead Generation API",
    version="1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

allowed_origins = _get_allowed_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_build_cors_allowed_origins(allowed_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(OriginValidationMiddleware, allowed_origins=allowed_origins)


def rate_limit(limit: int, window_seconds: int = 60, scope: str | None = None):
    return Depends(rate_limiter.dependency(limit=limit, window_seconds=window_seconds, scope=scope))


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


def _serialize_queue_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(_CET).isoformat() if value.tzinfo else value.replace(tzinfo=_CET).isoformat()


def require_scraper_api_key(
    x_api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> None:
    expected_api_key = os.getenv(SCRAPER_API_KEY_ENV_VAR)
    if not expected_api_key or not x_api_key or not secrets.compare_digest(x_api_key, expected_api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


ProtectedScraperRoute = Annotated[None, Depends(require_scraper_api_key)]

HZZ_CATEGORIES_RATE_LIMIT = [rate_limit(60, 60, scope="GET:/scrapers/hzz/categories")]
MOJPOSAO_CATEGORIES_RATE_LIMIT = [rate_limit(60, 60, scope="GET:/scrapers/mojposao/categories")]
HZZ_SCRAPER_RATE_LIMIT = [rate_limit(10, 60, scope="POST:/scrapers/hzz")]
MOJPOSAO_SCRAPER_RATE_LIMIT = [rate_limit(10, 60, scope="POST:/scrapers/mojposao")]
RUN_ALL_SCRAPERS_RATE_LIMIT = [rate_limit(5, 60, scope="POST:/scrapers/run-all")]


def _queue_response(task_name: str, **kwargs) -> JSONResponse:
    task = _run_service(enqueue_task, task_name, **kwargs)
    return JSONResponse(status_code=202, content=task)


def _should_enqueue(async_job: bool) -> bool:
    return async_job


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def landing_page() -> str:
    return LANDING_PAGE_HTML


@app.get("/health", dependencies=[rate_limit(120, 60)])
def health() -> dict[str, datetime | str]:
    return {"status": "ok", "time": _utcnow()}


@app.get("/api/scrapers/hzz/categories", dependencies=HZZ_CATEGORIES_RATE_LIMIT, include_in_schema=False)
@app.get("/scrapers/hzz/categories", dependencies=HZZ_CATEGORIES_RATE_LIMIT)
def list_hzz_categories() -> list[dict[str, str]]:
    return get_hzz_categories()


@app.get("/api/scrapers/mojposao/categories", dependencies=MOJPOSAO_CATEGORIES_RATE_LIMIT, include_in_schema=False)
@app.get("/scrapers/mojposao/categories", dependencies=MOJPOSAO_CATEGORIES_RATE_LIMIT)
def list_mojposao_categories() -> list[dict[str, str]]:
    return get_mojposao_categories()


@app.post(
    "/api/scrapers/hzz",
    response_model=ScrapeSummaryResponse | QueuedTaskResponse,
    dependencies=HZZ_SCRAPER_RATE_LIMIT,
    include_in_schema=False,
)
@app.post("/scrapers/hzz", response_model=ScrapeSummaryResponse | QueuedTaskResponse, dependencies=HZZ_SCRAPER_RATE_LIMIT)
def run_hzz_scraper(payload: HZZScrapeRequest, _: ProtectedScraperRoute) -> dict | JSONResponse:
    if _should_enqueue(payload.async_job):
        return _queue_response(
            "app.tasks.scrape_hzz",
            max_pages=payload.max_pages,
            category=payload.category,
            company_limit=payload.company_limit,
        )

    summary = scrape_and_store_hzz(
        max_pages=payload.max_pages,
        category=payload.category,
        company_limit=payload.company_limit,
    )
    _raise_for_failed_summary(summary)
    return summary


@app.post(
    "/api/scrapers/mojposao",
    response_model=ScrapeSummaryResponse | QueuedTaskResponse,
    dependencies=MOJPOSAO_SCRAPER_RATE_LIMIT,
    include_in_schema=False,
)
@app.post(
    "/scrapers/mojposao",
    response_model=ScrapeSummaryResponse | QueuedTaskResponse,
    dependencies=MOJPOSAO_SCRAPER_RATE_LIMIT,
)
def run_mojposao_scraper(payload: MojPosaoScrapeRequest, _: ProtectedScraperRoute) -> dict | JSONResponse:
    if _should_enqueue(payload.async_job):
        return _queue_response(
            "app.tasks.scrape_mojposao",
            keyword=payload.keyword,
            max_clicks=payload.max_clicks,
            category=payload.category,
            company_limit=payload.company_limit,
        )

    summary = scrape_and_store_mojposao(
        keyword=payload.keyword,
        max_clicks=payload.max_clicks,
        category=payload.category,
        company_limit=payload.company_limit,
    )
    _raise_for_failed_summary(summary)
    return summary


@app.post(
    "/api/scrapers/run-all",
    response_model=RunAllScrapersResponse | QueuedTaskResponse,
    dependencies=RUN_ALL_SCRAPERS_RATE_LIMIT,
    include_in_schema=False,
)
@app.post(
    "/scrapers/run-all",
    response_model=RunAllScrapersResponse | QueuedTaskResponse,
    dependencies=RUN_ALL_SCRAPERS_RATE_LIMIT,
)
def run_all_scrapers(payload: RunAllScrapersRequest, _: ProtectedScraperRoute) -> dict[str, list[dict]] | JSONResponse:
    if _should_enqueue(payload.async_job):
        return _queue_response(
            "app.tasks.run_all_scrapers",
            hzz={
                "max_pages": payload.hzz.max_pages,
                "category": payload.hzz.category,
                "company_limit": payload.hzz.company_limit,
            },
            mojposao={
                "keyword": payload.mojposao.keyword,
                "max_clicks": payload.mojposao.max_clicks,
                "category": payload.mojposao.category,
                "company_limit": payload.mojposao.company_limit,
            },
        )

    results = [
        scrape_and_store_hzz(
            max_pages=payload.hzz.max_pages,
            category=payload.hzz.category,
            company_limit=payload.hzz.company_limit,
        ),
        scrape_and_store_mojposao(
            keyword=payload.mojposao.keyword,
            max_clicks=payload.mojposao.max_clicks,
            category=payload.mojposao.category,
            company_limit=payload.mojposao.company_limit,
        ),
    ]

    for result in results:
        _raise_for_failed_summary(result)

    return {"results": results}


@app.get("/jobs/email-targets", response_model=list[EmailJobTargetResponse], dependencies=[rate_limit(30, 60)])
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


@app.get("/queue/tasks/{task_id}", response_model=TaskStatusResponse, dependencies=[rate_limit(90, 60)])
def get_queue_task(task_id: str) -> dict[str, Any]:
    return _run_service(get_task_status, task_id)


@app.get("/email/placeholders", dependencies=[rate_limit(60, 60)])
def get_email_placeholders() -> list[dict[str, str]]:
    return get_placeholder_catalog()


@app.get("/email/templates", response_model=list[EmailTemplateResponse], dependencies=[rate_limit(45, 60)])
def get_email_templates() -> list[dict[str, Any]]:
    return _run_service(list_email_templates)


@app.post("/email/templates", response_model=EmailTemplateResponse, dependencies=[rate_limit(20, 60)])
def save_email_template(payload: EmailTemplateRequest) -> dict[str, Any]:
    return _run_service(
        upsert_email_template,
        name=payload.name,
        subject=payload.subject,
        html_content=payload.html_content,
        text_content=payload.text_content,
    )


@app.get("/email/campaigns", dependencies=[rate_limit(30, 60)])
def get_email_campaigns() -> list[dict[str, Any]]:
    return get_supabase_storage().list_email_campaigns()


@app.post("/email/campaigns", response_model=EmailCampaignResponse | QueuedTaskResponse, dependencies=[rate_limit(12, 60)])
def create_campaign(payload: CreateEmailCampaignRequest) -> dict[str, Any] | JSONResponse:
    if _should_enqueue(payload.async_job):
        return _queue_response(
            "app.tasks.create_email_campaign",
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
            scheduled_for=_serialize_queue_datetime(payload.scheduled_for),
            send_now=payload.send_now,
        )

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


@app.post("/email/campaigns/{campaign_id}/send", response_model=EmailCampaignResponse | QueuedTaskResponse, dependencies=[rate_limit(15, 60)])
def send_campaign(campaign_id: str, async_job: bool = True) -> dict[str, Any] | JSONResponse:
    if _should_enqueue(async_job):
        return _queue_response("app.tasks.send_email_campaign", campaign_id=campaign_id)

    return _run_service(send_email_campaign, campaign_id=campaign_id)


@app.post("/email/campaigns/dispatch-due", response_model=DispatchDueCampaignsResponse | QueuedTaskResponse, dependencies=[rate_limit(6, 60)])
def dispatch_due_campaigns(async_job: bool = True) -> dict[str, list[dict[str, Any]]] | JSONResponse:
    if _should_enqueue(async_job):
        return _queue_response("app.tasks.dispatch_due_email_campaigns")

    return _run_service(dispatch_due_email_campaigns)


@app.get("/email/automation-rules", response_model=list[EmailAutomationRuleResponse], dependencies=[rate_limit(30, 60)])
def get_automation_rules() -> list[dict[str, Any]]:
    return _run_service(list_email_automation_rules)


@app.post("/email/automation-rules", response_model=EmailAutomationRuleResponse, dependencies=[rate_limit(20, 60)])
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


@app.get("/email/warmup", response_model=EmailWarmupStatusResponse, dependencies=[rate_limit(30, 60)])
def get_warmup_status() -> dict[str, Any]:
    return _run_service(get_email_warmup_status)


@app.put("/email/warmup", response_model=EmailWarmupStatusResponse, dependencies=[rate_limit(20, 60)])
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
