import csv
import io
import os
import secrets
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.scrapers.hzz import scrape_hzz
from app.scrapers.meinestadt import scrape_meinestadt

HZZ_CSV_FIELDS = ["email", "first_name", "last_name", "company", "city", "country"]
MEINESTADT_CSV_FIELDS = [
    "email", "first_name", "last_name", "company", "city", "country",
    "title", "source", "category", "published_at", "detail_url", "employer_website",
]
EXCLUDED_COMPANY_TERMS = ("djecji vrtic", "vrtic", "skola", "opcina")


def _normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(c for c in normalized if not unicodedata.combining(c))
    return " ".join(normalized.casefold().split())


def _is_excluded_company(company: str) -> bool:
    key = _normalize_key(company)
    return any(term in key for term in EXCLUDED_COMPANY_TERMS)


def _dedupe_by_company(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result = []
    for row in rows:
        key = _normalize_key(row.get("company") or "") or (row.get("email") or "").casefold()
        if key and key not in seen:
            seen.add(key)
            result.append(row)
    return result


def _rows_to_csv_bytes(rows: list[dict], fields: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


app = FastAPI(
    title="OpenClaw Scraper Agent",
    version="1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _require_api_key(
    x_api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> None:
    expected = os.getenv("AGENT_API_KEY")
    if not expected or not x_api_key or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


Protected = Annotated[None, Depends(_require_api_key)]


class HZZScrapeRequest(BaseModel):
    category: str = "hospitality_tourism"
    max_pages: int = Field(default=200, ge=1)
    results_per_page: int = Field(default=75, ge=1)
    country: str = "Hrvatska"


class MeinestadtScrapeRequest(BaseModel):
    category: str = "logistics"
    max_pages: int = Field(default=10, ge=1)
    country: str = "Germany"


def _csv_response(csv_bytes: bytes, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv; charset=utf-8-sig",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/scrape/hzz")
def run_hzz(payload: HZZScrapeRequest, _: Protected) -> StreamingResponse:
    jobs = scrape_hzz(
        category=payload.category,
        max_pages=payload.max_pages,
        results_per_page=payload.results_per_page,
    )

    rows: list[dict] = []
    seen_companies: set[str] = set()

    for job in jobs:
        email = (job.get("email") or "").strip()
        if not email:
            continue
        company = (job.get("company") or "").strip()
        if _is_excluded_company(company):
            continue
        key = _normalize_key(company)
        if key in seen_companies:
            continue
        seen_companies.add(key)
        rows.append({
            "email": email,
            "first_name": "",
            "last_name": "",
            "company": company,
            "city": (job.get("location") or "").strip(),
            "country": payload.country,
        })

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"hzz-{payload.category}-{date_str}.csv"
    return _csv_response(_rows_to_csv_bytes(rows, HZZ_CSV_FIELDS), filename)


@app.post("/scrape/meinestadt")
def run_meinestadt(payload: MeinestadtScrapeRequest, _: Protected) -> StreamingResponse:
    rows: list[dict] = []

    def on_job(job: dict) -> None:
        email = (job.get("employer_email") or job.get("email") or "").strip()
        if not email:
            return
        rows.append({
            "email": email,
            "first_name": "",
            "last_name": "",
            "company": (job.get("company") or "").strip(),
            "city": (job.get("location") or "").strip(),
            "country": payload.country,
            "title": (job.get("title") or "").strip(),
            "source": "meinestadt",
            "category": (job.get("category") or "").strip(),
            "published_at": (job.get("published_at") or "").strip(),
            "detail_url": (job.get("detail_url") or "").strip(),
            "employer_website": (job.get("employer_website") or "").strip(),
        })

    scrape_meinestadt(
        category=payload.category,
        max_pages=payload.max_pages,
        on_job=on_job,
    )

    deduped = _dedupe_by_company(rows)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"meinestadt-{payload.category}-{date_str}.csv"
    return _csv_response(_rows_to_csv_bytes(deduped, MEINESTADT_CSV_FIELDS), filename)
