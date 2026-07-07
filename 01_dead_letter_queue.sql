-- Dead-letter queue for edge function pipelines.
-- Failed items land here with full context instead of vanishing.

create schema if not exists reliability;

create table if not exists reliability.dead_letters (
  id            uuid primary key default gen_random_uuid(),
  pipeline      text not null,              -- e.g. 'order-sync'
  payload       jsonb not null,             -- the original item, verbatim
  error_message text,
  error_detail  jsonb,                      -- stack, response body, status code...
  attempts      int not null default 0,     -- how many times it was tried before landing here
  first_failed_at timestamptz not null default now(),
  dead_at       timestamptz not null default now(),
  resolved_at   timestamptz,                -- set when manually recovered/replayed
  resolved_by   text
);

create index if not exists dead_letters_pipeline_idx
  on reliability.dead_letters (pipeline) where resolved_at is null;

comment on table reliability.dead_letters is
  'Terminal failures from edge function pipelines. Query where resolved_at is null for open incidents.';

-- Convenience: unresolved DLQ counts per pipeline (wire this to a dashboard or alert)
create or replace function reliability.dlq_summary()
returns table (pipeline text, open_count bigint, oldest timestamptz)
language sql stable as $$
  select pipeline, count(*), min(first_failed_at)
  from reliability.dead_letters
  where resolved_at is null
  group by pipeline
  order by count(*) desc;
$$;
