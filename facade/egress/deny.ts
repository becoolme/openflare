// vrt-no-network system worker (T6 review): the explicit deny-all
// `globalOutbound` target for system workers that must never reach a
// network at all — facades talk only to BINDING_BACKEND over unix, so
// anything reaching this worker is by definition a misroute.
//
// workerd's own network-service `allow = []` semantics are unverified on
// our build, so the deny is a real worker entrypoint — fetch() is a
// typed 403, connect() throws and closes, no props/env is ever read.
// Provably fail closed, and cheap (one tiny module).
import { WorkerEntrypoint } from "cloudflare:workers";
// T25/L12: no non-handler named exports may live in an entry module —
// constants come from the shared helper.
import { DENIED_HEADER, DENY_CODE } from "./shared.js";

export class DenyAll extends WorkerEntrypoint {
  async fetch(): Promise<Response> {
    return new Response(JSON.stringify({ error: "EGRESS_DENIED", code: DENY_CODE }), {
      status: 403,
      headers: {
        "content-type": "application/json",
        "cache-control": "no-store",
        [DENIED_HEADER]: DENY_CODE,
      },
    });
  }

  async connect(socket: unknown): Promise<never> {
    try {
      await (socket as { close?: () => unknown } | null | undefined)?.close?.();
    } catch {
      // best-effort close; the denial below is authoritative
    }
    throw Object.assign(new Error("vrt-no-network denies connect()"), {
      stableCode: "EGRESS_DENIED",
      denyCode: "connect",
    });
  }
}
export default DenyAll;
