// Pure session-registry + protocol logic for `facade-browser` — no
// WebSocket/Request objects, so every transition is unit-testable under
// `node --test`. The wire surface is CF's Browser Rendering `/v1/*`
// protocol (what `@cloudflare/puppeteer` speaks to `env.BROWSER.fetch`).
//
// Session model vs Cloudflare: a CF session is a browser that may outlive
// its client WebSocket (`keep_alive` lets a later `connect()` resume it).
// The vendored kitesurf engine ties a session to ONE engine WebSocket —
// `ws close` is the session's end. facade-browser therefore models:
//   pending  — acquire reserved the sessionId; first attach claims it.
//              `keep_alive` bounds how long this reservation survives
//              before the attach lands (the part of CF's keep_alive a
//              ws-bound engine can honour).
//   active   — a client ws is piped to an engine ws 1:1.
//   closed   — terminal. Re-attaching returns 410; the browser state is
//              gone (engine-side the socket closed).
// Reconnecting to an ACTIVE session fails (409) — one ws per session.

export const ENGINE_KITESURF = "kitesurf";

export interface BrowserLimits {
  /** Max simultaneous pending+active sessions per tenant. */
  maxConcurrentSessions: number;
  /** Wall-clock cap on one attached session (ms). */
  maxSessionMs: number;
  /** Cap for the `keep_alive` acquire param (ms). */
  maxKeepAliveMs: number;
  /** Unattached-session TTL when `keep_alive` is not given (ms). */
  pendingTtlMs: number;
  /** Closed-session records kept per tenant for /v1/history. */
  historyCap: number;
}

export const DEFAULT_LIMITS: BrowserLimits = {
  maxConcurrentSessions: 4,
  maxSessionMs: 900_000,
  maxKeepAliveMs: 600_000,
  pendingTtlMs: 60_000,
  historyCap: 100,
};

/** Strictly narrow the platform-injected `BROWSER_LIMITS` json binding;
 * any deviation → defaults are too risky to guess around, so fail closed. */
export function browserLimits(raw: unknown): BrowserLimits {
  if (raw === undefined || raw === null) return { ...DEFAULT_LIMITS };
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("BROWSER_LIMITS: not an object");
  }
  const src = raw as Record<string, unknown>;
  const num = (key: keyof BrowserLimits, lo: number, hi: number): number => {
    const v = src[key];
    if (v === undefined) return DEFAULT_LIMITS[key];
    if (typeof v !== "number" || !Number.isSafeInteger(v) || v < lo || v > hi) {
      throw new Error(`BROWSER_LIMITS.${key}: out of range`);
    }
    return v;
  };
  return {
    maxConcurrentSessions: num("maxConcurrentSessions", 1, 64),
    maxSessionMs: num("maxSessionMs", 1_000, 3_600_000),
    maxKeepAliveMs: num("maxKeepAliveMs", 0, 3_600_000),
    pendingTtlMs: num("pendingTtlMs", 1_000, 3_600_000),
    historyCap: num("historyCap", 0, 1_000),
  };
}

export type SessionState = "pending" | "active" | "closed";

export interface SessionRec {
  sessionId: string;
  engine: string;
  state: SessionState;
  createdAt: number;
  attachedAt?: number;
  closedAt?: number;
  /** Pending-window deadline (ms epoch) — keep_alive, or pendingTtlMs. */
  pendingUntil: number;
  /** Wall-clock kill time once attached (ms epoch). */
  deadline?: number;
  closeReason?: string;
}

export type AcquireError =
  | "bad_engine"
  | "concurrency_limit"
  | "bad_param";

export type AcquireResult =
  | { ok: true; sessionId: string }
  | { ok: false; error: AcquireError; detail: string };

export type AttachResult =
  | { ok: true; rec: SessionRec }
  | { ok: false; status: number; error: string };

/** Hard bound on retained closed-session records per tenant — the
 * response cap (`limits.historyCap`) is a view concern; this bounds the
 * store itself so a churning tenant cannot grow the isolate heap. */
const HISTORY_HARD_CAP = 256;

/** Per-tenant session table. `scope` is the host-injected tenant id
 * (ctx.props.instanceId) — a tenant only ever sees its own records. */
export class SessionRegistry {
  readonly #now: () => number;
  readonly #sessions = new Map<string, SessionRec>();
  readonly #history: SessionRec[] = [];

  constructor(now: () => number = Date.now) {
    this.#now = now;
  }

  /** Lazily expire pending reservations and bound live state. */
  #sweep(): void {
    const now = this.#now();
    for (const rec of this.#sessions.values()) {
      if (rec.state === "pending" && now > rec.pendingUntil) {
        rec.state = "closed";
        rec.closedAt = rec.pendingUntil;
        rec.closeReason = "acquire_ttl_expired";
        this.#pushHistory(rec);
        this.#sessions.delete(rec.sessionId);
      }
    }
  }

  #pushHistory(rec: SessionRec): void {
    this.#history.unshift(rec);
    if (this.#history.length > HISTORY_HARD_CAP) {
      this.#history.length = HISTORY_HARD_CAP;
    }
  }

  /** Engine selection — kitesurf is the only engine; every other name
   * fails closed (CF's `?browser=` semantics: unknown engine = error). */
  static checkEngine(name: string | null): "kitesurf" | { error: string } {
    if (name === null || name === "" || name === ENGINE_KITESURF) {
      return ENGINE_KITESURF;
    }
    return { error: `browser engine "${name}" is not available` };
  }

  /** `keep_alive` (ms) → the pending-attach window; absent → pendingTtlMs. */
  static keepAliveMs(
    raw: string | null,
    limits: BrowserLimits,
  ): { ok: true; ms: number } | { ok: false; detail: string } {
    if (raw === null) return { ok: true, ms: limits.pendingTtlMs };
    if (!/^\d+$/.test(raw)) {
      return { ok: false, detail: "keep_alive must be a non-negative integer" };
    }
    const ms = Math.min(Number(raw), limits.maxKeepAliveMs);
    return { ok: true, ms };
  }

  acquire(
    engine: string,
    keepAliveMs: number,
    limits: BrowserLimits,
    newId: () => string,
  ): AcquireResult {
    this.#sweep();
    let live = 0;
    for (const rec of this.#sessions.values()) {
      if (rec.state !== "closed") live++;
    }
    if (live >= limits.maxConcurrentSessions) {
      return {
        ok: false,
        error: "concurrency_limit",
        detail: `active sessions limit (${limits.maxConcurrentSessions}) reached`,
      };
    }
    const now = this.#now();
    const rec: SessionRec = {
      sessionId: newId(),
      engine,
      state: "pending",
      createdAt: now,
      pendingUntil: now + keepAliveMs,
    };
    this.#sessions.set(rec.sessionId, rec);
    return { ok: true, sessionId: rec.sessionId };
  }

  /** Claim a pending session for attach, or register a direct-connect
   * session (sessionId absent → mint one already-active). */
  attach(
    sessionId: string | undefined,
    engine: string,
    limits: BrowserLimits,
    newId: () => string,
  ): AttachResult {
    this.#sweep();
    const now = this.#now();
    if (sessionId === undefined) {
      const rec: SessionRec = {
        sessionId: newId(),
        engine,
        state: "active",
        createdAt: now,
        attachedAt: now,
        pendingUntil: now,
        deadline: now + limits.maxSessionMs,
      };
      this.#sessions.set(rec.sessionId, rec);
      return { ok: true, rec };
    }
    const rec = this.#sessions.get(sessionId);
    if (rec === undefined) {
      // A session we closed is known but gone — 410, not 404 (the id may
      // still sit in a client's reconnect loop).
      const closed = this.#history.some((h) => h.sessionId === sessionId);
      return closed
        ? { ok: false, status: 410, error: "session is closed" }
        : { ok: false, status: 404, error: "session not found" };
    }
    if (rec.state === "closed") {
      return { ok: false, status: 410, error: "session is closed" };
    }
    if (rec.state === "active") {
      return { ok: false, status: 409, error: "session is already attached" };
    }
    rec.state = "active";
    rec.attachedAt = now;
    rec.deadline = now + limits.maxSessionMs;
    return { ok: true, rec };
  }

  /** The ws path dropped (either end) → terminal close + history. */
  close(sessionId: string, reason: string): void {
    const rec = this.#sessions.get(sessionId);
    if (rec === undefined || rec.state === "closed") return;
    rec.state = "closed";
    rec.closedAt = this.#now();
    rec.closeReason = reason;
    this.#sessions.delete(sessionId);
    this.#pushHistory(rec);
  }

  /** A live session's wall-clock deadline (0 while pending). */
  deadline(sessionId: string): number {
    return this.#sessions.get(sessionId)?.deadline ?? 0;
  }

  sessions(): SessionRec[] {
    this.#sweep();
    return [...this.#sessions.values()];
  }

  history(historyCap: number): SessionRec[] {
    this.#sweep();
    // Recent = live sessions + closed ring, newest-activity first.
    const out = [...this.#history];
    for (const rec of this.#sessions.values()) out.push(rec);
    out.sort(
      (a, b) =>
        (b.closedAt ?? b.attachedAt ?? b.createdAt) -
        (a.closedAt ?? a.attachedAt ?? a.createdAt),
    );
    return out.slice(0, Math.max(historyCap, this.#sessions.size));
  }

  /** /v1/limits counters for this tenant. */
  counters(
    limits: BrowserLimits,
  ): {
    activeSessions: number;
    maxConcurrentSessions: number;
    allowedBrowserAcquisitions: number;
    timeUntilNextAllowedBrowserAcquisition: number;
  } {
    this.#sweep();
    let live = 0;
    let soonestFree = 0;
    const now = this.#now();
    for (const rec of this.#sessions.values()) {
      if (rec.state === "closed") continue;
      live++;
      const freesAt =
        rec.state === "pending" ? rec.pendingUntil : (rec.deadline ?? now);
      if (soonestFree === 0 || freesAt < soonestFree) soonestFree = freesAt;
    }
    const allowed = Math.max(0, limits.maxConcurrentSessions - live);
    return {
      activeSessions: live,
      maxConcurrentSessions: limits.maxConcurrentSessions,
      allowedBrowserAcquisitions: allowed,
      timeUntilNextAllowedBrowserAcquisition:
        allowed > 0 ? 0 : Math.max(0, soonestFree - now),
    };
  }
}

/** sessionId: 128-bit random hex — same 32-char shape CF returns; must be
 * unguessable because sessionId alone authorises attach. */
export function newSessionId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** CF-shaped session entry for /v1/sessions + /v1/history. A present
 * `connectionId` signals "a worker connection is established" (puppeteer
 * docs) — we set it exactly while a ws is piped. */
export function sessionView(rec: SessionRec): Record<string, unknown> {
  const view: Record<string, unknown> = {
    sessionId: rec.sessionId,
    startTime: rec.createdAt,
    status:
      rec.state === "pending"
        ? "pending"
        : rec.state === "active"
          ? "active"
          : "closed",
  };
  if (rec.state === "active") view.connectionId = rec.sessionId;
  if (rec.closedAt !== undefined) view.endTime = rec.closedAt;
  if (rec.closeReason !== undefined) view.reason = rec.closeReason;
  return view;
}
