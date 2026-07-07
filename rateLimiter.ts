// Per-minute rate limiter guard for outbound calls from edge functions.
// Backed by Postgres so it holds across concurrent invocations.

import type { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

export interface RateLimiterOptions {
  key: string;           // e.g. "postgrest", "mercadopago-api"
  maxPerMinute: number;
  maxWaitMs?: number;    // give up waiting after this (default 60s)
  pollIntervalMs?: number; // default 1000
}

export class RateLimiter {
  constructor(private supabase: SupabaseClient, private opts: RateLimiterOptions) {}

  /**
   * Wait until a slot is available, then take it.
   * Throws RateLimitTimeoutError if maxWaitMs elapses — callers should
   * schedule the item for retry rather than dropping it.
   */
  async acquire(): Promise<void> {
    const maxWait = this.opts.maxWaitMs ?? 60_000;
    const poll = this.opts.pollIntervalMs ?? 1_000;
    const deadline = Date.now() + maxWait;

    while (true) {
      const { data, error } = await this.supabase
        .schema("reliability")
        .rpc("rate_limit_acquire", {
          p_key: this.opts.key,
          p_max_per_minute: this.opts.maxPerMinute,
        });

      if (error) {
        // Fail open with a loud log: a broken limiter shouldn't halt the pipeline,
        // but you want to know about it.
        console.error(`[rate-limit] acquire errored for key=${this.opts.key}:`, error);
        return;
      }
      if (data === true) return;

      if (Date.now() + poll > deadline) {
        throw new RateLimitTimeoutError(this.opts.key, maxWait);
      }
      await new Promise((r) => setTimeout(r, poll));
    }
  }
}

export class RateLimitTimeoutError extends Error {
  constructor(key: string, waitedMs: number) {
    super(`Rate limit window for "${key}" stayed saturated for ${waitedMs}ms`);
    this.name = "RateLimitTimeoutError";
  }
}
