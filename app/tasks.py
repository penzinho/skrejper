from app.queue import get_celery_app
from app.services.email_outreach import (
    create_email_campaign,
    dispatch_due_email_campaigns,
    send_email_campaign,
)
from app.services.lead_enrichment import enrich_scrape_run_emails
from app.services.scrape_store import scrape_and_store_hzz, scrape_and_store_mojposao


celery_app = get_celery_app()


@celery_app.task(name="app.tasks.scrape_hzz")
def scrape_hzz_task(
    max_pages: int = 3,
    category: str | None = None,
    company_limit: int | None = None,
) -> dict:
    return scrape_and_store_hzz(
        max_pages=max_pages,
        category=category,
        company_limit=company_limit,
    )


@celery_app.task(name="app.tasks.scrape_mojposao")
def scrape_mojposao_task(
    keyword: str = "",
    max_clicks: int = 5,
    category: str | None = None,
    company_limit: int | None = None,
) -> dict:
    return scrape_and_store_mojposao(
        keyword=keyword,
        max_clicks=max_clicks,
        category=category,
        company_limit=company_limit,
    )


@celery_app.task(name="app.tasks.run_all_scrapers")
def run_all_scrapers_task(
    hzz: dict | None = None,
    mojposao: dict | None = None,
) -> dict:
    hzz = hzz or {}
    mojposao = mojposao or {}
    return {
        "results": [
            scrape_and_store_hzz(
                max_pages=int(hzz.get("max_pages", 3)),
                category=hzz.get("category"),
                company_limit=hzz.get("company_limit"),
            ),
            scrape_and_store_mojposao(
                keyword=mojposao.get("keyword", ""),
                max_clicks=int(mojposao.get("max_clicks", 5)),
                category=mojposao.get("category"),
                company_limit=mojposao.get("company_limit"),
            ),
        ]
    }


@celery_app.task(name="app.tasks.create_email_campaign")
def create_email_campaign_task(
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
    scheduled_for: str | None = None,
    send_now: bool = False,
) -> dict:
    return create_email_campaign(
        name=name,
        source=source,
        run_id=run_id,
        job_ids=job_ids,
        only_not_emailed=only_not_emailed,
        require_email=require_email,
        template_id=template_id,
        subject=subject,
        html_content=html_content,
        text_content=text_content,
        sender_email=sender_email,
        reply_to_email=reply_to_email,
        created_by=created_by,
        scheduled_for=scheduled_for,
        send_now=send_now,
    )


@celery_app.task(name="app.tasks.send_email_campaign")
def send_email_campaign_task(campaign_id: str) -> dict:
    return send_email_campaign(campaign_id=campaign_id)


@celery_app.task(name="app.tasks.dispatch_due_email_campaigns")
def dispatch_due_email_campaigns_task() -> dict:
    return dispatch_due_email_campaigns()


@celery_app.task(name="app.tasks.enrich_scrape_run_emails")
def enrich_scrape_run_emails_task(run_id: str) -> dict:
    from app.db.supabase import get_supabase_storage

    return enrich_scrape_run_emails(run_id=run_id, storage=get_supabase_storage())
