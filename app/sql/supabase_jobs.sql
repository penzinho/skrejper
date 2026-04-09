create extension if not exists pgcrypto;

create table if not exists scrape_runs (
    id uuid primary key default gen_random_uuid(),
    source text not null,
    filters jsonb not null default '{}'::jsonb,
    status text not null,
    scraped_count integer not null default 0,
    upserted_count integer not null default 0,
    snapshot_count integer not null default 0,
    failed_count integer not null default 0,
    error text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    finished_at timestamptz
);

create table if not exists jobs (
    id uuid primary key default gen_random_uuid(),
    title text,
    company text,
    location text,
    detail_url text not null,
    published_at date,
    category text,
    source text not null,
    employer_website text,
    employer_email text,
    employer_address text,
    employer_phone text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    last_run_id uuid references scrape_runs(id) on delete set null,
    constraint jobs_source_detail_url_key unique (source, detail_url)
);

create table if not exists job_snapshots (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references scrape_runs(id) on delete cascade,
    source text not null,
    detail_url text not null,
    job_payload jsonb not null,
    scraped_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_jobs_source on jobs (source);
create index if not exists idx_jobs_published_at on jobs (published_at);
create index if not exists idx_job_snapshots_run_id on job_snapshots (run_id);
