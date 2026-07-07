-- Retry queue with exponential backoff scheduling.
-- Transient failures get rescheduled; terminal failures are promoted to the DLQ.

create schema if not exists reliability;

create table if not exists reliability.retry_queue (
  id              uuid primary key default gen_random_uuid(),
  pipeline        text not null,
  payload         jsonb not null,
  attempts        int not null default 0,
  max_attempts    int not null default 5,
  next_attempt_at timestamptz not null default now(),
  last_error      text,
  claimed_at      timestamptz,             -- lease: prevents double-processing
  created_at      timestamptz not null default now()
);

create index if not exists retry_queue_due_idx
  on reliability.retry_queue (pipeline, next_attempt_at)
  where claimed_at is null;

-- Claim due items safely under concurrent cron runs.
-- FOR UPDATE SKIP LOCKED means two overlapping invocations never grab the same row.
create or replace function reliability.claim_retries(
  p_pipeline text,
  p_limit int default 20,
  p_lease interval default interval '5 minutes'
)
returns setof reliability.retry_queue
language plpgsql as $$
begin
  return query
  update reliability.retry_queue rq
  set claimed_at = now()
  where rq.id in (
    select id from reliability.retry_queue
    where pipeline = p_pipeline
      and next_attempt_at <= now()
      and (claimed_at is null or claimed_at < now() - p_lease)
    order by next_attempt_at
    limit p_limit
    for update skip locked
  )
  returning rq.*;
end;
$$;

-- Mark success: just delete the row.
create or replace function reliability.complete_retry(p_id uuid)
returns void language sql as $$
  delete from reliability.retry_queue where id = p_id;
$$;

-- Mark failure: reschedule with exponential backoff, or promote to DLQ at the cap.
create or replace function reliability.fail_retry(
  p_id uuid,
  p_error text,
  p_base_delay_seconds int default 30
)
returns void language plpgsql as $$
declare
  r reliability.retry_queue;
begin
  select * into r from reliability.retry_queue where id = p_id for update;
  if not found then return; end if;

  if r.attempts + 1 >= r.max_attempts then
    insert into reliability.dead_letters
      (pipeline, payload, error_message, attempts, first_failed_at)
    values
      (r.pipeline, r.payload, p_error, r.attempts + 1, r.created_at);
    delete from reliability.retry_queue where id = p_id;
  else
    update reliability.retry_queue
    set attempts        = attempts + 1,
        last_error      = p_error,
        claimed_at      = null,
        -- 30s, 60s, 120s, 240s, ... capped at 1h
        next_attempt_at = now() + least(
          make_interval(secs => p_base_delay_seconds * power(2, attempts)),
          interval '1 hour'
        )
    where id = p_id;
  end if;
end;
$$;
