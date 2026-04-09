# API Docs

Manual API reference for wiring a Next.js frontend to this backend.

This service is a FastAPI app with:

- scrape trigger endpoints
- queue polling for long-running work
- read endpoints for categories, job email targets, templates, campaigns, automation rules, and warmup status
- email campaign creation and sending

The built-in OpenAPI docs are disabled, so this file documents the actual routes exposed by the app.

## Base URL

Use one of these depending on environment:

- Production: `https://scrape.protalent.hr`
- Local: `http://127.0.0.1:8000`

## Cross-Origin / Frontend Access

The API enforces both CORS and explicit origin validation.

- Allowed origins come from `CORS_ALLOW_ORIGINS`
- Default allowed origin is `flow.protalent.hr`
- If the browser `Origin` header is not allowed, the API returns `403`

Frontend implication:

- Your Next.js app must run from an allowed origin
- If you use a local frontend, add it to `CORS_ALLOW_ORIGINS`, for example `http://localhost:3000`

## Authentication

Only the scrape execution endpoints require an API key.

Protected routes:

- `POST /scrapers/hzz`
- `POST /scrapers/mojposao`
- `POST /scrapers/run-all`

Header:

```http
x-api-key: YOUR_SCRAPER_API_KEY
```

If missing or invalid:

- `401 {"detail":"Invalid API key"}`

Important:

- The email endpoints are not protected by API key in the current implementation
- They are still subject to origin checks and rate limits

## Response Conventions

### Datetime format

Datetimes are returned as ISO 8601 strings, typically UTC.

Examples:

- `2026-04-09T10:00:00+00:00`
- `2026-04-09`

### Queue pattern

Long-running operations usually default to queued execution.

If queued, the API returns:

```json
{
  "task_id": "uuid",
  "task_name": "app.tasks.some_task",
  "status": "queued",
  "queued_at": "2026-04-09T10:00:00+00:00"
}
```

Then the frontend should poll:

- `GET /queue/tasks/{task_id}`

When complete, the final result appears under `result`.

### Common error responses

- `400` for invalid business input such as an unknown category or invalid email/template usage
- `401` for missing/invalid scraper API key
- `403` for disallowed browser origin
- `429` for rate limit hits, with `Retry-After` response header
- `422` for invalid request body shape or failed validation
- `500` for internal failures such as storage or delivery setup issues

## Data The Frontend Can Actually Read

The current API exposes these read surfaces:

- service health: `GET /health`
- category catalogs: `GET /scrapers/hzz/categories`, `GET /scrapers/mojposao/categories`
- queue state and async results: `GET /queue/tasks/{task_id}`
- job list usable for email workflows: `GET /jobs/email-targets`
- email placeholder catalog: `GET /email/placeholders`
- email templates: `GET /email/templates`
- email campaigns: `GET /email/campaigns`
- email automation rules: `GET /email/automation-rules`
- email warmup status: `GET /email/warmup`

Not currently exposed by API:

- generic job search/list endpoint outside `GET /jobs/email-targets`
- scrape run history listing
- scrape run details by `run_id`
- job snapshot history
- email delivery rows per campaign
- single template/campaign/rule detail endpoints
- delete endpoints

## How The Backend Works

### 1. Scraping flow

The frontend triggers one or more scrapers.

- HZZ scraper stores jobs with source `hzz`
- MojPosao scraper stores jobs with source `mojposao`

Each scrape creates a `scrape_runs` row and upserts normalized rows into `jobs`.

The scrape response returns a summary:

- `run_id`
- counts for scraped, upserted, snapshots, failures
- automation campaign IDs created after scrape

### 2. Async queue flow

If `async_job` is omitted or set to `true`, the API enqueues a Celery task instead of executing immediately.

Frontend pattern:

1. `POST` route
2. receive `task_id`
3. poll `GET /queue/tasks/{task_id}`
4. when `ready === true`, read `result`

### 3. Email flow

The email system works off the normalized `jobs` table.

- `GET /jobs/email-targets` returns emailable jobs
- `POST /email/templates` stores reusable content
- `POST /email/campaigns` creates campaign + queued delivery rows
- `POST /email/campaigns/{campaign_id}/send` sends queued recipients
- `POST /email/campaigns/dispatch-due` processes queued campaigns whose `scheduled_for` is due

There is also an automation layer:

- scrape completion can automatically create email campaigns
- automation rules are managed via `/email/automation-rules`
- warmup limits cap how many emails can be sent per day

## Endpoint Reference

### `GET /`

Landing page HTML. Not useful for frontend data.

Response:

- HTML page

### `GET /health`

Basic service availability check.

Rate limit:

- `120 requests / 60s / IP`

Response:

```json
{
  "status": "ok",
  "time": "2026-04-09T10:00:00+00:00"
}
```

Frontend use:

- health badge
- boot-time connectivity check

### `GET /scrapers/hzz/categories`

Returns the valid HZZ category keys the frontend can send to the HZZ scrape endpoint.

Rate limit:

- `60 requests / 60s / IP`

Response shape:

```json
[
  {
    "key": "it",
    "label": "Informatički, računalni i stručnjaci za Internet"
  }
]
```

Frontend use:

- category dropdown for HZZ scraping

### `GET /scrapers/mojposao/categories`

Returns the valid MojPosao category keys.

Rate limit:

- `60 requests / 60s / IP`

Response shape:

```json
[
  {
    "key": "it_telecommunications",
    "label": "IT, telekomunikacije",
    "id": "11"
  }
]
```

Frontend use:

- category dropdown for MojPosao scraping

### `POST /scrapers/hzz`

Run or queue the HZZ scraper.

Auth:

- requires `x-api-key`

Rate limit:

- `10 requests / 60s / IP`

Request body:

```json
{
  "max_pages": 3,
  "category": "it",
  "company_limit": 300,
  "async_job": true
}
```

Fields:

- `max_pages`: integer, minimum `1`, default `3`
- `category`: optional HZZ category key
- `company_limit`: optional integer, minimum `1`; keeps up to this many unique companies and returns all available if fewer exist
- `async_job`: boolean, default `true`

Queued response:

```json
{
  "task_id": "uuid",
  "task_name": "app.tasks.scrape_hzz",
  "status": "queued",
  "queued_at": "2026-04-09T10:00:00+00:00"
}
```

Synchronous success response:

```json
{
  "run_id": "uuid",
  "source": "hzz",
  "status": "completed",
  "scraped_count": 12,
  "upserted_count": 10,
  "snapshot_count": 10,
  "failed_count": 2,
  "error": null,
  "automation_campaign_ids": ["uuid"],
  "automation_errors": [],
  "company_limit": 300,
  "available_company_count": 187,
  "selected_company_count": 187
}
```

Notes:

- If `category` is invalid, sync mode returns `400`
- `automation_campaign_ids` are campaign IDs created automatically after scrape
- data stored by this flow ends up in `scrape_runs`, `jobs`, and `job_snapshots`

### `POST /scrapers/mojposao`

Run or queue the MojPosao scraper.

Auth:

- requires `x-api-key`

Rate limit:

- `10 requests / 60s / IP`

Request body:

```json
{
  "keyword": "python",
  "max_clicks": 5,
  "category": "it_telecommunications",
  "company_limit": 300,
  "async_job": true
}
```

Fields:

- `keyword`: string, default `""`
- `max_clicks`: integer, minimum `1`, default `5`
- `category`: optional MojPosao category key
- `company_limit`: optional integer, minimum `1`; keeps up to this many unique companies and returns all available if fewer exist
- `async_job`: boolean, default `true`

Response:

- same queued shape as HZZ, but `task_name` is `app.tasks.scrape_mojposao`
- same scrape summary shape, but `source` is `mojposao`

Notes:

- MojPosao jobs do not currently populate `employer_email` during scrape
- for frontend UX, this means not every scraped source produces direct email targets

### `POST /scrapers/run-all`

Runs or queues both scrapers in one request.

Auth:

- requires `x-api-key`

Rate limit:

- `5 requests / 60s / IP`

Request body:

```json
{
  "async_job": true,
  "hzz": {
    "max_pages": 2,
    "category": "it",
    "company_limit": 300
  },
  "mojposao": {
    "keyword": "backend",
    "max_clicks": 2,
    "category": "it_telecommunications",
    "company_limit": 300,
    "async_job": true
  }
}
```

Important:

- top-level `async_job` decides whether the whole endpoint is queued
- nested `hzz.async_job` and `mojposao.async_job` are accepted by schema but ignored by this route
- nested `company_limit` values are forwarded to each scraper independently

Queued response:

```json
{
  "task_id": "uuid",
  "task_name": "app.tasks.run_all_scrapers",
  "status": "queued",
  "queued_at": "2026-04-09T10:00:00+00:00"
}
```

Synchronous success response:

```json
{
  "results": [
    {
      "run_id": "uuid",
      "source": "hzz",
      "status": "completed",
      "scraped_count": 4,
      "upserted_count": 4,
      "snapshot_count": 4,
      "failed_count": 0,
      "error": null,
      "automation_campaign_ids": [],
      "automation_errors": []
    },
    {
      "run_id": "uuid",
      "source": "mojposao",
      "status": "completed",
      "scraped_count": 9,
      "upserted_count": 8,
      "snapshot_count": 8,
      "failed_count": 1,
      "error": null,
      "automation_campaign_ids": [],
      "automation_errors": []
    }
  ]
}
```

### `GET /queue/tasks/{task_id}`

Polls Celery task state.

Rate limit:

- `90 requests / 60s / IP`

Response:

```json
{
  "task_id": "uuid",
  "status": "success",
  "ready": true,
  "successful": true,
  "result": {
    "run_id": "uuid"
  },
  "error": null
}
```

Fields:

- `status`: lowercased Celery status such as `pending`, `started`, `success`, `failure`
- `ready`: `true` when task is finished
- `successful`: `true` only for successful completion
- `result`: present only on success
- `error`: present only on failure

Frontend use:

- poll until `ready === true`
- if `successful === true`, use `result`
- if `successful === false` and `error` exists, show failure state

### `GET /jobs/email-targets`

Returns rows from the `jobs` table intended for email workflows.

This is currently the main frontend-readable job data endpoint.

Rate limit:

- `30 requests / 60s / IP`

Query params:

- `source`: optional string, for example `hzz` or `mojposao`
- `run_id`: optional scrape run ID
- `only_not_emailed`: boolean, default `false`
- `require_email`: boolean, default `true`

Response shape:

```json
[
  {
    "id": "uuid",
    "title": "Backend Developer",
    "company": "Acme",
    "location": "Zagreb",
    "source": "hzz",
    "detail_url": "https://example.com/job",
    "employer_email": "jobs@example.com",
    "category": "it",
    "published_at": "2026-04-09",
    "last_run_id": "uuid",
    "email_status": "not_sent",
    "email_last_sent_at": null,
    "email_last_error": null,
    "email_send_count": 0
  }
]
```

Data meaning:

- `email_status`: current outbound email state for this job
- `email_last_sent_at`: timestamp of last successful send
- `email_last_error`: last send error, if any
- `email_send_count`: total successful sends for this job

Frontend use:

- lead table
- campaign target picker
- status badges for email state

Limitations:

- there is no pagination in the current API
- there is no free-text search or sort contract exposed at the API layer

### `GET /email/placeholders`

Returns the merge tags supported in email content.

Rate limit:

- `60 requests / 60s / IP`

Response:

```json
[
  {
    "key": "company",
    "description": "Recipient company name."
  }
]
```

Available placeholders in current implementation:

- `company`
- `job_title`
- `location`
- `source`
- `detail_url`
- `published_at`
- `employer_email`

Frontend use:

- placeholder cheat sheet in template editor
- insert-variable UI

### `GET /email/templates`

Returns all email templates, newest updated first.

Rate limit:

- `45 requests / 60s / IP`

Response shape:

```json
[
  {
    "id": "uuid",
    "name": "Default Outreach",
    "subject": "Hi {{company}}",
    "html_content": "<p>Hello {{company}}</p>",
    "text_content": "Hello {{company}}",
    "created_at": "2026-04-09T10:00:00+00:00",
    "updated_at": "2026-04-09T10:00:00+00:00"
  }
]
```

Frontend use:

- template list
- template picker
- template editor preload

### `POST /email/templates`

Creates or updates a template by `name`.

Rate limit:

- `20 requests / 60s / IP`

Request body:

```json
{
  "name": "Default Outreach",
  "subject": "Hi {{company}}",
  "html_content": "<p>Hello {{company}}</p>",
  "text_content": "Hello {{company}}"
}
```

Response:

- same shape as `GET /email/templates` item

Notes:

- storage uses upsert by unique `name`
- there is no separate update endpoint

### `GET /email/campaigns`

Returns raw rows from the `email_campaigns` table.

Rate limit:

- `30 requests / 60s / IP`

Response shape:

```json
[
  {
    "id": "uuid",
    "name": "April Outreach",
    "mode": "manual_selection",
    "status": "queued",
    "template_id": "uuid",
    "source": "hzz",
    "last_scrape_run_id": "uuid",
    "subject": "Hi {{company}}",
    "html_content": "<p>Hello {{company}}</p>",
    "text_content": "Hello {{company}}",
    "sender_email": "sales@example.com",
    "reply_to_email": "reply@example.com",
    "created_by": "admin@example.com",
    "filters": {
      "job_ids": ["uuid"],
      "only_not_emailed": true,
      "require_email": true
    },
    "scheduled_for": "2026-04-09T12:00:00+00:00",
    "total_recipients": 20,
    "sent_count": 5,
    "failed_count": 1,
    "created_at": "2026-04-09T10:00:00+00:00",
    "updated_at": "2026-04-09T10:30:00+00:00",
    "finished_at": null
  }
]
```

Known status values from current logic:

- `draft`
- `queued`
- `sending`
- `sent`
- `partial`
- `failed`

Frontend use:

- campaigns list
- campaign detail summary page

Limitation:

- there is no endpoint for individual campaign deliveries, so per-recipient progress is not available from API right now

### `POST /email/campaigns`

Creates an email campaign and delivery queue, optionally sends immediately.

Rate limit:

- `12 requests / 60s / IP`

Request body:

```json
{
  "name": "April Outreach",
  "target": {
    "job_ids": ["uuid-1", "uuid-2"],
    "source": "hzz",
    "run_id": "uuid",
    "only_not_emailed": true,
    "require_email": true
  },
  "template_id": "uuid",
  "subject": "Hi {{company}}",
  "html_content": "<p>Hello {{company}}</p>",
  "text_content": "Hello {{company}}",
  "sender_email": "sales@example.com",
  "reply_to_email": "reply@example.com",
  "created_by": "admin@example.com",
  "scheduled_for": "2026-04-09T12:00:00+00:00",
  "send_now": false,
  "async_job": true
}
```

Field rules:

- target selection must include at least one of:
  - `target.job_ids`
  - `target.run_id`
  - `target.source`
- content must be provided either by:
  - `template_id`
  - or both `subject` and `html_content`
- `async_job` defaults to `true`
- `require_email` defaults to `true`
- `only_not_emailed` defaults to `false` in campaign creation

Queued response:

- same queue shape as other async endpoints
- task name: `app.tasks.create_email_campaign`

Synchronous response:

```json
{
  "campaign_id": "uuid",
  "name": "April Outreach",
  "status": "draft",
  "scheduled_for": null,
  "total_recipients": 2,
  "sent_count": 0,
  "failed_count": 0,
  "queued_count": 2,
  "warmup_remaining_today": null
}
```

Status behavior:

- `draft` if campaign is created but not scheduled and not sent immediately
- `queued` if `send_now` is true, if `scheduled_for` is set, or if automation queues it

Frontend use:

- create campaign wizard
- save draft
- schedule send

Important behavior:

- deliveries are deduplicated by recipient email within a campaign
- if no email-eligible jobs match the target, sync mode returns `400`

### `POST /email/campaigns/{campaign_id}/send`

Sends queued deliveries for one campaign, immediately or through queue.

Rate limit:

- `15 requests / 60s / IP`

Query params:

- `async_job`: boolean, default `true`

Queued response:

- standard queue shape
- task name: `app.tasks.send_email_campaign`

Synchronous response:

```json
{
  "campaign_id": "uuid",
  "name": "April Outreach",
  "status": "sent",
  "scheduled_for": null,
  "total_recipients": 20,
  "sent_count": 20,
  "failed_count": 0,
  "queued_count": 0,
  "warmup_remaining_today": 5
}
```

Warmup-aware behavior:

- sending may stop early if the daily warmup limit is reached
- in that case the campaign can remain `queued`
- `warmup_remaining_today` tells the frontend how many sends remain today after execution

### `POST /email/campaigns/dispatch-due`

Processes queued campaigns whose `scheduled_for` is due.

Rate limit:

- `6 requests / 60s / IP`

Query params:

- `async_job`: boolean, default `true`

Queued response:

- standard queue shape
- task name: `app.tasks.dispatch_due_email_campaigns`

Synchronous response:

```json
{
  "results": [
    {
      "campaign_id": "uuid",
      "name": "Scheduled Outreach",
      "status": "partial",
      "scheduled_for": "2026-04-09T12:00:00+00:00",
      "total_recipients": 20,
      "sent_count": 10,
      "failed_count": 1,
      "queued_count": 9,
      "warmup_remaining_today": 0
    }
  ]
}
```

Frontend use:

- admin action to flush due campaigns
- queue monitor page

### `GET /email/automation-rules`

Returns email automation rules.

Rate limit:

- `30 requests / 60s / IP`

Response shape:

```json
[
  {
    "id": "uuid",
    "name": "HZZ Auto Outreach",
    "source": "hzz",
    "template_id": "uuid",
    "subject": "Hi {{company}}",
    "html_content": "<p>Hello {{company}}</p>",
    "text_content": "Hello {{company}}",
    "sender_email": "sales@example.com",
    "reply_to_email": "reply@example.com",
    "created_by": "admin@example.com",
    "enabled": true,
    "auto_send": false,
    "delay_minutes": 60,
    "only_not_emailed": true,
    "require_email": true,
    "created_at": "2026-04-09T10:00:00+00:00",
    "updated_at": "2026-04-09T10:00:00+00:00"
  }
]
```

Behavior:

- after a scrape completes, enabled rules are checked
- if `source` is set, only matching scrape sources trigger the rule
- matching rules can create follow-up campaigns automatically

Frontend use:

- automation settings UI

### `POST /email/automation-rules`

Creates or updates an automation rule by `name`.

Rate limit:

- `20 requests / 60s / IP`

Request body:

```json
{
  "name": "HZZ Auto Outreach",
  "source": "hzz",
  "template_id": "uuid",
  "subject": "Hi {{company}}",
  "html_content": "<p>Hello {{company}}</p>",
  "text_content": "Hello {{company}}",
  "sender_email": "sales@example.com",
  "reply_to_email": "reply@example.com",
  "created_by": "admin@example.com",
  "enabled": true,
  "auto_send": false,
  "delay_minutes": 60,
  "only_not_emailed": true,
  "require_email": true
}
```

Response:

- same shape as `GET /email/automation-rules` item

Rules:

- content must come from `template_id` or from `subject` + `html_content`
- `delay_minutes` must be `>= 0`
- `only_not_emailed` defaults to `true`
- `require_email` defaults to `true`

### `GET /email/warmup`

Returns current warmup configuration and calculated daily quota state.

Rate limit:

- `30 requests / 60s / IP`

Response:

```json
{
  "settings": {
    "id": "uuid",
    "name": "default",
    "enabled": true,
    "initial_daily_limit": 10,
    "daily_increment": 5,
    "increment_interval_days": 1,
    "max_daily_limit": 50,
    "started_at": "2026-04-01T00:00:00+00:00",
    "created_at": "2026-04-01T00:00:00+00:00",
    "updated_at": "2026-04-09T00:00:00+00:00"
  },
  "effective_daily_limit": 15,
  "sent_today": 3,
  "remaining_today": 12
}
```

If no settings exist yet:

```json
{
  "settings": null,
  "effective_daily_limit": null,
  "sent_today": 0,
  "remaining_today": null
}
```

Frontend use:

- delivery quota dashboard
- safe-send indicators before sending campaigns

### `PUT /email/warmup`

Creates or updates the warmup settings, then returns the computed current status.

Rate limit:

- `20 requests / 60s / IP`

Request body:

```json
{
  "enabled": true,
  "initial_daily_limit": 10,
  "daily_increment": 5,
  "increment_interval_days": 1,
  "max_daily_limit": 50,
  "started_at": "2026-04-01T00:00:00+00:00"
}
```

Rules:

- `initial_daily_limit` is required and must be `>= 0`
- `daily_increment` defaults to `0`
- `increment_interval_days` defaults to `1`
- `max_daily_limit` can be omitted; backend will normalize it to at least the initial limit

Response:

- same shape as `GET /email/warmup`

## Source Of Truth For Returned Data

This is where each frontend-facing route gets its data from.

### Scrape categories

- `GET /scrapers/hzz/categories`: hardcoded catalog in the HZZ scraper module
- `GET /scrapers/mojposao/categories`: hardcoded catalog in the MojPosao scraper module

### Job email targets

- `GET /jobs/email-targets`: `jobs` table in Supabase

Main fields exposed to frontend:

- job identity: `id`
- listing info: `title`, `company`, `location`, `detail_url`, `source`, `category`, `published_at`
- email info: `employer_email`, `email_status`, `email_last_sent_at`, `email_last_error`, `email_send_count`
- scrape linkage: `last_run_id`

### Templates

- `GET /email/templates`
- `POST /email/templates`

Backed by:

- `email_templates` table

### Campaigns

- `GET /email/campaigns`
- `POST /email/campaigns`
- `POST /email/campaigns/{campaign_id}/send`
- `POST /email/campaigns/dispatch-due`

Backed by:

- `email_campaigns` table
- `email_deliveries` table
- `jobs` table email status fields

### Automation rules

- `GET /email/automation-rules`
- `POST /email/automation-rules`

Backed by:

- `email_automation_rules` table

### Warmup

- `GET /email/warmup`
- `PUT /email/warmup`

Backed by:

- `email_warmup_settings` table
- sent delivery counts from `email_deliveries`

## Recommended Frontend Integration Pattern

### For scraping

1. Load category options from the catalog endpoints
2. Submit scrape request with `max_pages`, selected `category`, optional `company_limit`, and `async_job: true`
3. Poll `/queue/tasks/{task_id}`
4. When complete, read the returned scrape summary
5. If you need leads for outreach, call `/jobs/email-targets?run_id=...`

### For email campaign creation

1. Read candidate jobs from `/jobs/email-targets`
2. Read templates from `/email/templates`
3. Read placeholders from `/email/placeholders`
4. Create campaign with `/email/campaigns`
5. If created as queued draft/scheduled, show status from `/email/campaigns`
6. If manually sending, call `/email/campaigns/{campaign_id}/send`

### For automation and warmup admin UI

1. Read `/email/automation-rules`
2. Save via `POST /email/automation-rules`
3. Read `/email/warmup`
4. Save via `PUT /email/warmup`

## Practical Notes For Next.js

- Browser requests will fail unless your frontend origin is allowed by backend config
- Scrape routes need `x-api-key`, so these are usually better called from a Next.js server action or route handler rather than directly from the browser
- Public read routes can be called from the browser if origin is allowed
- Queue polling can be done with SWR, React Query, or simple interval polling
- There is no pagination contract on list endpoints yet, so large datasets may need backend changes before exposing them broadly in UI

## Gaps You May Want Added Later

If the frontend needs richer workflows, these are the most obvious missing endpoints:

- list scrape runs
- get scrape run by ID
- list jobs with pagination/search/sort
- get one campaign by ID
- list campaign deliveries by campaign ID
- delete/update template, campaign, or automation rule
- manual job snapshot/history access
