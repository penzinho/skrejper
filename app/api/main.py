import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.scrapers.hzz import get_hzz_categories
from app.scrapers.mojposao import get_mojposao_categories
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
