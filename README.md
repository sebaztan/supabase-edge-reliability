# supabase-edge-reliability

**Production-grade reliability patterns for cron-driven Supabase Edge Functions.**

Dead-letter queues, exponential backoff retries, per-minute rate limiting, and staleness alerting — extracted from production e-commerce infrastructure processing real order volume across multiple storefronts.

## The problem

Supabase Edge Functions triggered by `pg_cron` fail **silently** in ways that are invisible until data is lost:

- **Rate-limit saturation.** Your cron function fans out to PostgREST or an external API, hits a rate limit mid-batch, and the remaining items are simply... dropped. No error surfaces to you. The cron "succeeded."
- **Transient failures with no retry.** A single 502 from an upstream API means that record never syncs. Ever.
- **Stale pipelines.** The cron stops firing (paused project, quota, misconfiguration) and nobody notices for days.

We discovered this the hard way: **paid e-commerce orders were being silently dropped** by a sync cron under rate-limit saturation. This toolkit is the structural fix we built, generalized so you don't have to build it yourself.

## What's included

| Module | What it does |
|---|---|
| `sql/01_dead_letter_queue.sql` | DLQ table + helper functions. Failed items land here instead of vanishing. |
| `sql/02_retry_queue.sql` | Retry table with exponential backoff scheduling (`next_attempt_at`), max-attempt caps, and terminal-failure promotion to the DLQ. |
| `sql/03_rate_limiter.sql` | Per-minute sliding-window rate limiter backed by Postgres — works across function invocations, unlike in-memory counters. |
| `sql/04_staleness_alerts.sql` | Heartbeat table + check function. Know within minutes when a pipeline stops running. |
| `src/dlq.ts` | TypeScript client for capturing failed payloads with full context (error, attempt count, original payload). |
| `src/retry.ts` | Backoff calculator + claim/complete/fail lifecycle for processing the retry queue safely (with `FOR UPDATE SKIP LOCKED`). |
| `src/rateLimiter.ts` | `acquire()` guard you call before each outbound request. Returns wait-or-proceed. |
| `src/heartbeat.ts` | One-line heartbeat recording for staleness detection. |
| `examples/` | A complete worked example: a sync cron that survives rate limits, retries transient failures, and alerts when stale. |

## Quick start

### 1. Apply the migrations

```bash
supabase db push --include-all
# or run the files in sql/ in order via the SQL editor
```

### 2. Wrap your outbound calls with the rate limiter

```ts
import { RateLimiter } from "./src/rateLimiter.ts";

const limiter = new RateLimiter(supabase, {
  key: "external-api",
  maxPerMinute: 50,
});

for (const item of batch) {
  await limiter.acquire(); // waits if the window is saturated — never drops
  await syncItem(item);
}
```

### 3. Capture failures instead of losing them

```ts
import { DeadLetterQueue } from "./src/dlq.ts";
import { RetryQueue } from "./src/retry.ts";

const retries = new RetryQueue(supabase, { maxAttempts: 5, baseDelaySeconds: 30 });

try {
  await syncItem(item);
} catch (err) {
  // schedules attempt 1 at +30s, attempt 2 at +60s, +120s, +240s...
  // after maxAttempts, the item is promoted to the DLQ automatically
  await retries.schedule("order-sync", item, err);
}
```

### 4. Record heartbeats and alert on staleness

```ts
import { recordHeartbeat } from "./src/heartbeat.ts";
await recordHeartbeat(supabase, "order-sync-cron");
```

```sql
-- run this on its own 5-minute cron, wire the result to email/Slack/whatever
select * from reliability.check_stale_pipelines(interval '15 minutes');
```

## Design principles

1. **Postgres is the source of truth.** No in-memory state — edge function instances are ephemeral and concurrent. Rate limits, retry schedules, and heartbeats all live in tables.
2. **Never drop, always defer.** Saturation means *wait* or *reschedule*, never skip.
3. **Failures are data.** Every failed item is preserved with its full payload and error context, so recovery is a query, not an archaeology project.
4. **Safe under concurrency.** Retry claiming uses `FOR UPDATE SKIP LOCKED` so overlapping cron runs never double-process.

## Requirements

- Supabase project (Postgres 15+)
- Edge Functions (Deno) — the TS modules are dependency-free and use `supabase-js` you already have

## Status

Early public release. The patterns are battle-tested in production (they recovered silently-dropped paid orders before extraction); the packaging is new. Issues and PRs very welcome — especially adapters for common patterns (webhook ingestion, payment provider sync, inventory sync).

## License

MIT
