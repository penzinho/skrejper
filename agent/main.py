import os
import secrets
from datetime import datetime, timezone
from typing import Annotated, Any

from apify_client import ApifyClientAsync
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

DATASET_DOWNLOAD_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?format=csv&clean=true"

# A single shared async client; the token is read from the environment so the
# Actor calls are authenticated. The Actor to start is identified by
# APIFY_ACTOR_ID.
apify_client = ApifyClientAsync(token=os.getenv("APIFY_API_TOKEN"))


app = FastAPI(
    title="OpenClaw Scraper Agent",
    version="2.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _require_api_key(
    x_api_key: Annotated[str | None, Header(alias="x-api-key")] = None,
) -> None:
    expected = os.getenv("AGENT_API_KEY")
    if not expected or not x_api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    if not secrets.compare_digest(x_api_key.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


Protected = Annotated[None, Depends(_require_api_key)]


class ScrapeDispatchRequest(BaseModel):
    """Input forwarded verbatim to the Apify Actor as its run input."""

    source: str
    # Legacy single-value fields (still accepted by the Actor).
    category: str | None = None
    group: str | None = None
    # Multi-select fields + output control (forwarded verbatim to the Actor).
    hzz_categories: list[str] | None = None
    hzz_groups: list[str] | None = None
    meinestadt_categories: list[str] | None = None
    arbeitsagentur_categories: list[str] | None = None
    arbeitsagentur_keyword: str | None = None
    arbeitsagentur_location: str | None = None
    arbeitsagentur_radius: int | None = Field(default=None, ge=0)
    output_mode: str | None = None
    email_to: str | None = None
    max_pages: int = Field(default=1, ge=1)
    results_per_page: int | None = Field(default=None, ge=1)
    company_limit: int | None = Field(default=None, ge=1)
    country: str | None = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/scrape/dispatch", status_code=status.HTTP_202_ACCEPTED)
async def dispatch_scrape(payload: ScrapeDispatchRequest, _: Protected) -> dict:
    actor_id = os.getenv("APIFY_ACTOR_ID")
    if not actor_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="APIFY_ACTOR_ID is not configured",
        )

    run_input = payload.model_dump(exclude_none=True)

    # `.start()` returns as soon as the run is queued — it does not wait for the
    # Actor to finish, so this route stays non-blocking.
    run = await apify_client.actor(actor_id).start(run_input=run_input)

    return {
        "run_id": run["id"],
        "status": run.get("status", "READY"),
        "message": "Scrape job queued on Apify.",
    }


@app.get("/scrape/status/{run_id}")
async def scrape_status(run_id: str, _: Protected) -> dict:
    run: dict[str, Any] | None = await apify_client.run(run_id).get()
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    run_status = run.get("status")
    response: dict[str, Any] = {"run_id": run_id, "status": run_status}

    if run_status == "SUCCEEDED":
        dataset_id = run.get("defaultDatasetId")
        if dataset_id:
            response["dataset_id"] = dataset_id
            response["download_url"] = DATASET_DOWNLOAD_URL.format(dataset_id=dataset_id)

    return response
