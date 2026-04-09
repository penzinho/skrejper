# Skrejper

FastAPI service for scraping job listings, storing them in Supabase, and running follow-up email outreach workflows.

Current sources:

- `HZZ`
- `MojPosao`

The API supports synchronous execution and queued background jobs through Celery/Redis.

## What It Does

- Scrapes job listings from supported sources
- Stores normalized jobs and scrape runs in Supabase
- Exposes API endpoints for scraping, queue status, and email outreach
- Queues long-running work through Celery
- Protects scraping execution endpoints with an API key

## Requirements

- Python with a working virtualenv
- Redis for queued jobs
- Supabase project and service role key
- Playwright browser binaries for scraping

Examples below use the existing project virtualenv at `.venv`.

## Install

```bash
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
```

If `.venv` does not exist yet:

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/playwright install chromium
```

## Environment Variables

Required for core API and scraping:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SECRET_KEY=your-supabase-service-role-key
SCRAPER_API_KEY=replace-with-a-random-secret
```

Queue configuration:

```env
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
CELERY_RESULT_EXPIRES_SECONDS=3600
```

Optional API and scraper settings:

```env
CORS_ALLOW_ORIGINS=http://localhost:3000,http://localhost:5173
HEADLESS=true
BROWSER_CHANNEL=chrome
```

Optional email delivery settings:

```env
RESEND_API_KEY=your-resend-api-key
RESEND_FROM_EMAIL=sales@example.com
```

Notes:

- `SUPABASE_SERVICE_ROLE_KEY` is also accepted instead of `SUPABASE_SECRET_KEY`.
- If `CELERY_BROKER_URL` is not set, the app falls back to `REDIS_URL`, then `redis://localhost:6379/0`.
- If `SCRAPER_API_KEY` is missing, protected scraping routes will reject requests with `401`.

## Generate An API Key

Use a long random secret. For example:

```bash
openssl rand -hex 32
```

or:

```bash
./.venv/bin/python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Set the result as `SCRAPER_API_KEY`.

## Run The API

```bash
./.venv/bin/uvicorn app.main:app --reload
```

The app will be available at `http://127.0.0.1:8000`.

Useful endpoints:

- `GET /health`
- `GET /scrapers/hzz/categories`
- `GET /scrapers/mojposao/categories`
- `POST /scrapers/hzz`
- `POST /scrapers/mojposao`
- `POST /scrapers/run-all`
- `GET /queue/tasks/{task_id}`
- `GET /jobs/email-targets`
- `GET /email/templates`
- `POST /email/campaigns`

## Run The Worker

Start Redis first, then run a Celery worker:

```bash
./.venv/bin/celery -A app.celery_app.celery_app worker --loglevel=info
```

The scraping routes default to queued execution because `async_job` defaults to `true`.

## API Key Authentication

Only scraping execution endpoints are protected:

- `POST /scrapers/hzz`
- `POST /scrapers/mojposao`
- `POST /scrapers/run-all`

Send the API key in the `x-api-key` header:

```bash
curl -X POST http://127.0.0.1:8000/scrapers/hzz \
  -H 'content-type: application/json' \
  -H 'x-api-key: your-api-key' \
  -d '{"max_pages": 2, "category": "it", "async_job": false}'
```

Queued example:

```bash
curl -X POST http://127.0.0.1:8000/scrapers/mojposao \
  -H 'content-type: application/json' \
  -H 'x-api-key: your-api-key' \
  -d '{"keyword": "python", "max_clicks": 3, "async_job": true}'
```

If the header is missing or invalid, the API returns `401 Invalid API key`.

## Queue Flow

For queued requests, scraping endpoints return:

- `task_id`
- `task_name`
- `status`
- `queued_at`

Check task progress with:

```bash
curl http://127.0.0.1:8000/queue/tasks/<task_id>
```

## Run Tests

```bash
./.venv/bin/python -m unittest tests.test_api_routes
./.venv/bin/python -m unittest tests.test_scrape_store
./.venv/bin/python -m unittest tests.test_email_outreach
```

## Project Layout

```text
app/
  api/        FastAPI routes
  db/         Supabase storage layer
  scrapers/   Source-specific Playwright scrapers
  services/   Scrape storage and email logic
  queue.py    Celery setup and task status helpers
  tasks.py    Celery task definitions
```
