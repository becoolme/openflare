export const ENGINE_KITESURF = "kitesurf";
export const DEFAULT_LIMITS = {
    maxConcurrentSessions: 4,
    maxSessionMs: 900_000,
    maxKeepAliveMs: 600_000,
    pendingTtlMs: 60_000,
    historyCap: 100
};
export function browserLimits(raw) {
    if (raw === undefined || raw === null) return {
        ...DEFAULT_LIMITS
    };
    if (typeof raw !== "object" || Array.isArray(raw)) {
        throw new Error("BROWSER_LIMITS: not an object");
    }
    const src = raw;
    const num = (key, lo, hi)=>{
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
        historyCap: num("historyCap", 0, 1_000)
    };
}
const HISTORY_HARD_CAP = 256;
export class SessionRegistry {
    #now;
    #sessions = new Map();
    #history = [];
    constructor(now = Date.now){
        this.#now = now;
    }
    #sweep() {
        const now = this.#now();
        for (const rec of this.#sessions.values()){
            if (rec.state === "pending" && now > rec.pendingUntil) {
                rec.state = "closed";
                rec.closedAt = rec.pendingUntil;
                rec.closeReason = "acquire_ttl_expired";
                this.#pushHistory(rec);
                this.#sessions.delete(rec.sessionId);
            }
        }
    }
    #pushHistory(rec) {
        this.#history.unshift(rec);
        if (this.#history.length > HISTORY_HARD_CAP) {
            this.#history.length = HISTORY_HARD_CAP;
        }
    }
    static checkEngine(name) {
        if (name === null || name === "" || name === ENGINE_KITESURF) {
            return ENGINE_KITESURF;
        }
        return {
            error: `browser engine "${name}" is not available`
        };
    }
    static keepAliveMs(raw, limits) {
        if (raw === null) return {
            ok: true,
            ms: limits.pendingTtlMs
        };
        if (!/^\d+$/.test(raw)) {
            return {
                ok: false,
                detail: "keep_alive must be a non-negative integer"
            };
        }
        const ms = Math.min(Number(raw), limits.maxKeepAliveMs);
        return {
            ok: true,
            ms
        };
    }
    acquire(engine, keepAliveMs, limits, newId) {
        this.#sweep();
        let live = 0;
        for (const rec of this.#sessions.values()){
            if (rec.state !== "closed") live++;
        }
        if (live >= limits.maxConcurrentSessions) {
            return {
                ok: false,
                error: "concurrency_limit",
                detail: `active sessions limit (${limits.maxConcurrentSessions}) reached`
            };
        }
        const now = this.#now();
        const rec = {
            sessionId: newId(),
            engine,
            state: "pending",
            createdAt: now,
            pendingUntil: now + keepAliveMs
        };
        this.#sessions.set(rec.sessionId, rec);
        return {
            ok: true,
            sessionId: rec.sessionId
        };
    }
    attach(sessionId, engine, limits, newId) {
        this.#sweep();
        const now = this.#now();
        if (sessionId === undefined) {
            const rec = {
                sessionId: newId(),
                engine,
                state: "active",
                createdAt: now,
                attachedAt: now,
                pendingUntil: now,
                deadline: now + limits.maxSessionMs
            };
            this.#sessions.set(rec.sessionId, rec);
            return {
                ok: true,
                rec
            };
        }
        const rec = this.#sessions.get(sessionId);
        if (rec === undefined) {
            const closed = this.#history.some((h)=>h.sessionId === sessionId);
            return closed ? {
                ok: false,
                status: 410,
                error: "session is closed"
            } : {
                ok: false,
                status: 404,
                error: "session not found"
            };
        }
        if (rec.state === "closed") {
            return {
                ok: false,
                status: 410,
                error: "session is closed"
            };
        }
        if (rec.state === "active") {
            return {
                ok: false,
                status: 409,
                error: "session is already attached"
            };
        }
        rec.state = "active";
        rec.attachedAt = now;
        rec.deadline = now + limits.maxSessionMs;
        return {
            ok: true,
            rec
        };
    }
    close(sessionId, reason) {
        const rec = this.#sessions.get(sessionId);
        if (rec === undefined || rec.state === "closed") return;
        rec.state = "closed";
        rec.closedAt = this.#now();
        rec.closeReason = reason;
        this.#sessions.delete(sessionId);
        this.#pushHistory(rec);
    }
    deadline(sessionId) {
        return this.#sessions.get(sessionId)?.deadline ?? 0;
    }
    sessions() {
        this.#sweep();
        return [
            ...this.#sessions.values()
        ];
    }
    history(historyCap) {
        this.#sweep();
        const out = [
            ...this.#history
        ];
        for (const rec of this.#sessions.values())out.push(rec);
        out.sort((a, b)=>(b.closedAt ?? b.attachedAt ?? b.createdAt) - (a.closedAt ?? a.attachedAt ?? a.createdAt));
        return out.slice(0, Math.max(historyCap, this.#sessions.size));
    }
    counters(limits) {
        this.#sweep();
        let live = 0;
        let soonestFree = 0;
        const now = this.#now();
        for (const rec of this.#sessions.values()){
            if (rec.state === "closed") continue;
            live++;
            const freesAt = rec.state === "pending" ? rec.pendingUntil : rec.deadline ?? now;
            if (soonestFree === 0 || freesAt < soonestFree) soonestFree = freesAt;
        }
        const allowed = Math.max(0, limits.maxConcurrentSessions - live);
        return {
            activeSessions: live,
            maxConcurrentSessions: limits.maxConcurrentSessions,
            allowedBrowserAcquisitions: allowed,
            timeUntilNextAllowedBrowserAcquisition: allowed > 0 ? 0 : Math.max(0, soonestFree - now)
        };
    }
}
export function newSessionId() {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return [
        ...bytes
    ].map((b)=>b.toString(16).padStart(2, "0")).join("");
}
export function sessionView(rec) {
    const view = {
        sessionId: rec.sessionId,
        startTime: rec.createdAt,
        status: rec.state === "pending" ? "pending" : rec.state === "active" ? "active" : "closed"
    };
    if (rec.state === "active") view.connectionId = rec.sessionId;
    if (rec.closedAt !== undefined) view.endTime = rec.closedAt;
    if (rec.closeReason !== undefined) view.reason = rec.closeReason;
    return view;
}
