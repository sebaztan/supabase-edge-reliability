// One-line heartbeat recording for staleness detection.

import type { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

/**
 * Call at the top of every cron invocation.
 * `expectedInterval` (Postgres interval string, e.g. "10 minutes") only needs
 * to be passed once — it's persisted per pipeline.
 */
export async function recordHeartbeat(
  supabase: SupabaseClient,
  pipeline: string,
  expectedInterval?: string,
  meta?: Record<string, unknown>,
): Promise<void> {
  const { error } = await supabase.schema("reliability").rpc("beat", {
    p_pipeline: pipeline,
    p_expected: expectedInterval ?? null,
    p_meta: meta ?? null,
  });
  if (error) {
    console.error(`[heartbeat] failed for pipeline=${pipeline}:`, error);
  }
}
