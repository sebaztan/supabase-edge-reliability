// Retry queue client: schedule transient failures, claim due items safely,
// and let Postgres handle backoff math + DLQ promotion.

import type { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

export interface RetryQueueOptions {
  maxAttempts?: number;      // default 5
  baseDelaySeconds?: number; // default 30 → 30s, 60s, 120s, 240s...
}

export interface RetryItem<T = unknown> {
  id: string;
  pipeline: string;
  payload: T;
  attempts: number;
}

export class RetryQueue {
  private maxAttempts: number;
  private baseDelaySeconds: number;

  constructor(private supabase: SupabaseClient, opts: RetryQueueOptions = {}) {
    this.maxAttempts = opts.maxAttempts ?? 5;
    this.baseDelaySeconds = opts.baseDelaySeconds ?? 30;
  }

  /** Schedule a failed item for retry (attempt 1 fires after baseDelaySeconds). */
  async schedule(pipeline: string, payload: unknown, error: unknown): Promise<void> {
    const { error: dbError } = await this.supabase
      .schema("reliability")
      .from("retry_queue")
      .insert({
        pipeline,
        payload,
        max_attempts: this.maxAttempts,
        last_error: error instanceof Error ? error.message : String(error),
        next_attempt_at: new Date(Date.now() + this.baseDelaySeconds * 1000).toISOString(),
      });
    if (dbError) {
      console.error(`[retry] FAILED to schedule retry for ${pipeline}:`, dbError);
    }
  }

  /** Claim up to `limit` due items. Safe under concurrent cron invocations. */
  async claim<T = unknown>(pipeline: string, limit = 20): Promise<RetryItem<T>[]> {
    const { data, error } = await this.supabase
      .schema("reliability")
      .rpc("claim_retries", { p_pipeline: pipeline, p_limit: limit });
    if (error) {
      console.error(`[retry] claim failed for ${pipeline}:`, error);
      return [];
    }
    return (data ?? []) as RetryItem<T>[];
  }

  /** The item succeeded — remove it from the queue. */
  async complete(id: string): Promise<void> {
    await this.supabase.schema("reliability").rpc("complete_retry", { p_id: id });
  }

  /** The item failed again — Postgres reschedules with backoff or promotes to DLQ. */
  async fail(id: string, error: unknown): Promise<void> {
    await this.supabase.schema("reliability").rpc("fail_retry", {
      p_id: id,
      p_error: error instanceof Error ? error.message : String(error),
      p_base_delay_seconds: this.baseDelaySeconds,
    });
  }

  /** Convenience: claim + process + complete/fail in one loop. */
  async drain<T = unknown>(
    pipeline: string,
    handler: (payload: T) => Promise<void>,
    limit = 20,
  ): Promise<{ succeeded: number; failed: number }> {
    const items = await this.claim<T>(pipeline, limit);
    let succeeded = 0;
    let failed = 0;
    for (const item of items) {
      try {
        await handler(item.payload);
        await this.complete(item.id);
        succeeded++;
      } catch (err) {
        await this.fail(item.id, err);
        failed++;
      }
    }
    return { succeeded, failed };
  }
}
