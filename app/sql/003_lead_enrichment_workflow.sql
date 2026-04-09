alter table jobs add column if not exists email_enrichment_attempt_count integer not null default 0;
alter table jobs add column if not exists email_enrichment_last_attempt_at timestamptz;
alter table jobs add column if not exists email_enrichment_next_attempt_at timestamptz;
alter table jobs add column if not exists email_enrichment_unusable boolean not null default false;

create index if not exists idx_jobs_email_enrichment_next_attempt_at
    on jobs (email_enrichment_next_attempt_at)
    where employer_email is null and email_enrichment_unusable = false;
