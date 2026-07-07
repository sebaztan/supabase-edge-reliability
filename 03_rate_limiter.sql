-- Per-minute sliding-window rate limiter backed by Postgres.
-- Works across concurrent edge function invocations (in-memory counters don't).

create schema if not exists reliability;

create table if not exists reliability.rate_limit_events (
  key        text not null,
  occurred_at timestamptz not null default now()
);

create index if not exists rate_limit_events_idx
  on reliability.rate_limit_events (key, occurred_at);

-- Try to acquire a slot. Returns true if allowed (and records the event),
-- false if the window is saturated. Atomic under concurrency.
create or replace function reliability.rate_limit_acquire(
  p_key text,
  p_max_per_minute int
)
returns boolean
language plpgsql as $$
declare
  current_count int;
begin
  -- serialize per key to make check+insert atomic
  perform pg_advisory_xact_lock(hashtext('rl:' || p_key));

  select count(*) into current_count
  from reliability.rate_limit_events
  where key = p_key
    and occurred_at > now() - interval '1 minute';

  if current_count >= p_max_per_minute then
    return false;
  end if;

  insert into reliability.rate_limit_events (key) values (p_key);
  return true;
end;
$$;

-- Housekeeping: call from a cron every few minutes.
create or replace function reliability.rate_limit_cleanup()
returns void language sql as $$
  delete from reliability.rate_limit_events
  where occurred_at < now() - interval '5 minutes';
$$;
