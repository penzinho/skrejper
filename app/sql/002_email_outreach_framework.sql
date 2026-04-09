alter table jobs add column if not exists email_status text not null default 'not_sent';
alter table jobs add column if not exists email_last_sent_at timestamptz;
alter table jobs add column if not exists email_last_error text;
alter table jobs add column if not exists email_send_count integer not null default 0;

create table if not exists email_templates (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    subject text not null,
    html_content text not null,
    text_content text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists email_campaigns (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    mode text not null default 'manual_selection',
    status text not null default 'draft',
    template_id uuid references email_templates(id) on delete set null,
    source text,
    last_scrape_run_id uuid references scrape_runs(id) on delete set null,
    subject text not null,
    html_content text not null,
    text_content text,
    sender_email text,
    reply_to_email text,
    created_by text,
    filters jsonb not null default '{}'::jsonb,
    scheduled_for timestamptz,
    total_recipients integer not null default 0,
    sent_count integer not null default 0,
    failed_count integer not null default 0,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    finished_at timestamptz
);

create table if not exists email_deliveries (
    id uuid primary key default gen_random_uuid(),
    campaign_id uuid not null references email_campaigns(id) on delete cascade,
    job_id uuid references jobs(id) on delete set null,
    recipient_email text not null,
    recipient_company text,
    merge_data jsonb not null default '{}'::jsonb,
    status text not null default 'queued',
    resend_email_id text,
    error text,
    sent_at timestamptz,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    constraint email_deliveries_campaign_recipient_key unique (campaign_id, recipient_email)
);

create table if not exists email_automation_rules (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    enabled boolean not null default true,
    source text,
    template_id uuid references email_templates(id) on delete set null,
    subject text not null,
    html_content text not null,
    text_content text,
    sender_email text,
    reply_to_email text,
    created_by text,
    auto_send boolean not null default false,
    delay_minutes integer not null default 0,
    only_not_emailed boolean not null default true,
    require_email boolean not null default true,
    filters jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists email_warmup_settings (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    enabled boolean not null default true,
    initial_daily_limit integer not null default 20,
    daily_increment integer not null default 0,
    increment_interval_days integer not null default 1,
    max_daily_limit integer not null default 20,
    started_at timestamptz not null default timezone('utc', now()),
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_jobs_email_status on jobs (email_status);
create index if not exists idx_email_campaigns_status_scheduled_for on email_campaigns (status, scheduled_for);
create index if not exists idx_email_deliveries_campaign_id on email_deliveries (campaign_id);
create index if not exists idx_email_automation_rules_enabled on email_automation_rules (enabled);
create index if not exists idx_email_warmup_settings_name on email_warmup_settings (name);
