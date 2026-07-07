// Dead-letter queue client for Supabase Edge Functions (Deno).
// Dependency-free beyond the supabase-js client you already have.

import type { SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

export interface DeadLetterInput {
  pipeline: string;
  payload: unknown;
  error: unknown;
  attempts?: number;
}

export class DeadLetterQueue {
  constructor(private supabase: SupabaseClient) {}

  /** Capture a terminally-failed item with full context. Never throws. */
  async capture({ pipeline, payload, error, attempts = 0 }: DeadLetterInput): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    const detail =
      error instanceof Error
        ? { name: error.name, stack: error.stack }
        : { raw: error };

    const { error: dbError } = await this.supabase
      .schema("reliability")
      .from("dead_letters")
      .insert({
        pipeline,
        payload,
        error_message: message,
        error_detail: detail,
        attempts,
      });

    // A DLQ that can itself fail silently defeats the purpose — at minimum, log loudly.
    if (dbError) {
      console.error(
        `[dlq] FAILED to capture dead letter for pipeline=${pipeline}:`,
        dbError,
        "original payload:",
        JSON.stringify(payload),
      );
    }
  }
}
