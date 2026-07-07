// Complete worked example: a sync cron that
//   1. records a heartbeat (staleness detection),
//   2. rate-limits its outbound calls (never drops under saturation),
//   3. schedules transient failures for retry with backoff,
//   4. drains previously-failed items,
//   5. lands terminal failures in the DLQ (via the retry queue's attempt cap).
//
// Deploy as a Supabase Edge Function and trigger with pg_cron.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { RateLimiter, RateLimitTimeoutError } from "../src/rateLimiter.ts";
import { RetryQueue } from "../src/retry.ts";
import { recordHeartbeat } from "../src/heartbeat.ts";

interface OrderPayload {
  order_id: string;
  // ...your fields
}

Deno.serve(async () => {
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
  );

  // 1) Heartbeat first — if this function stops running, you'll know.
  await recordHeartbeat(supabase, "order-sync", "10 minutes");

  const limiter = new RateLimiter(supabase, {
    key: "upstream-api",
    maxPerMinute: 50,
    maxWaitMs: 45_000,
  });
  const retries = new RetryQueue(supabase, { maxAttempts: 5, baseDelaySeconds: 30 });

  // 2) Drain previously-failed items before taking on new work.
  const drained = await retries.drain<OrderPayload>("order-sync", (payload) =>
    syncOrder(payload, limiter),
  );

  // 3) Process the new batch.
  const { data: pending } = await supabase
    .from("orders")
    .select("*")
    .eq("synced", false)
    .limit(100);

  let ok = 0;
  let deferred = 0;

  for (const order of pending ?? []) {
    try {
      await syncOrder(order, limiter);
      ok++;
    } catch (err) {
      // Transient failure OR sustained saturation → schedule, never drop.
      await retries.schedule("order-sync", order, err);
      deferred++;
      if (err instanceof RateLimitTimeoutError) {
        // Window is saturated for the long haul — stop hammering, let the
        // retry queue pick the rest up next run.
        break;
      }
    }
  }

  return new Response(
    JSON.stringify({ ok, deferred, retried: drained }),
    { headers: { "Content-Type": "application/json" } },
  );
});

async function syncOrder(order: OrderPayload, limiter: RateLimiter): Promise<void> {
  await limiter.acquire(); // waits for a slot — the fix for silent drops
  const res = await fetch("https://upstream.example.com/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(order),
  });
  if (!res.ok) {
    throw new Error(`upstream responded ${res.status}: ${await res.text()}`);
  }
}
