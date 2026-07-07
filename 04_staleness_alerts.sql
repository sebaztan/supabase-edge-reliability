-- Heartbeats + staleness detection.
-- Know within minutes when a pipeline silently stops running.

create schema if not exists reliability;

create table if not exists reliability.heartbeats (
  pipeline      text primary key,
  last_beat_at  timestamptz not null default now(),
  expected_interval interval not null default interval '15 minutes',
  meta          jsonb
);

-- Call this at the top of every cron invocation.
create or replace function reliability.beat(
  p_pipeline text,
  p_expected interval default null,
  p_meta jsonb default null
)
returns void language plpgsql as $$
begin
  insert into reliability.heartbeats (pipeline, last_beat_at, expected_interval, meta)
  values (p_pipeline, now(), coalesce(p_expected, interval '15 minutes'), p_meta)
  on conflict (pipeline) do update
    set last_beat_at = now(),
        expected_interval = coalesce(p_expected, reliability.heartbeats.expected_interval),
        meta = coalesce(p_meta, reliability.heartbeats.meta);
end;
$$;

-- Run this on its own cron (a different schedule than the pipelines it watches).
-- Wire the results to email / Slack / a webhook.
create or replace function reliability.check_stale_pipelines(
  p_grace interval default interval '5 minutes'
)
returns table (pipeline text, last_beat_at timestamptz, overdue_by interval)
language sql stable as $$
  select pipeline,
         last_beat_at,
         now() - (last_beat_at + expected_interval) as overdue_by
  from reliability.heartbeats
  where last_beat_at + expected_interval + p_grace < now()
  order by overdue_by desc;
$$;
