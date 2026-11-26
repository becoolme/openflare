// Browser binding conformance probe — drives env.BROWSER through the
// vendored @cloudflare/puppeteer client (acquire → ws attach → CDP)
// and exercises the facade's /v1/* control surface directly.
import puppeteer from "./puppeteer.vendor.mjs";

const MARKUP =
  '<!doctype html><html><body>' +
  '<h1 style="color:teal;font-family:sans-serif">openflare browser conformance</h1>' +
  "<p>golden</p></body></html>";

const sha256 = async (bytes) => {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
};

const acquire = async (env, query = "") => {
  const res = await env.BROWSER.fetch(
    `https://b/v1/devtools/browser${query}`,
    { method: "POST" },
  );
  return res;
};

// The sessionId behind a live puppeteer connection: query /v1/sessions
// for the one entry marked active (pending acquires carry no
// connectionId).
const activeSessionId = async (env) => {
  const res = await env.BROWSER.fetch("https://b/v1/sessions");
  const { sessions } = await res.json();
  return sessions.find((s) => s.status === "active" && s.connectionId)
    ?.sessionId;
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const p = url.pathname;
    const j = (v) => Response.json(v);
    try {
      // launch() → acquire + ws attach → setContent → screenshot.
      // Returns the PNG sha256 so the golden lives in case.json.
      if (p === "/launch-screenshot") {
        const browser = await puppeteer.launch(env.BROWSER);
        try {
          const page = await browser.newPage();
          await page.setContent(MARKUP, { waitUntil: "load" });
          const png = await page.screenshot({ type: "png" });
          return j({
            bytes: png.byteLength,
            sha256: await sha256(png),
          });
        } finally {
          await browser.close();
        }
      }

      // launch() then navigate to an absolute URL — exercises the
      // engine's outbound fetch path instead of setContent.
      if (p === "/launch-navigate") {
        const target = url.searchParams.get("url");
        const browser = await puppeteer.launch(env.BROWSER);
        try {
          const page = await browser.newPage();
          await page.goto(target, { waitUntil: "load" });
          const title = await page.title();
          const png = await page.screenshot({ type: "png" });
          return j({ title, bytes: png.byteLength, sha256: await sha256(png) });
        } finally {
          await browser.close();
        }
      }

      // POST /v1/devtools/browser — the reservation the client makes.
      // Shape-derived: sessionId is random hex, so report its shape.
      if (p === "/acquire") {
        const res = await acquire(env);
        const body = await res.json().catch(() => null);
        return j({
          status: res.status,
          sessionIdLen: body?.sessionId?.length ?? 0,
          sessionIdHex:
            typeof body?.sessionId === "string" &&
            /^[0-9a-f]{32}$/.test(body.sessionId),
        });
      }

      // connect(endpoint, sessionId) — attach to a reserved session.
      if (p === "/acquire-attach") {
        const acq = await acquire(env);
        const { sessionId } = await acq.json();
        const browser = await puppeteer.connect(env.BROWSER, sessionId);
        const page = await browser.newPage();
        await page.setContent(MARKUP, { waitUntil: "load" });
        const png = await page.screenshot({ type: "png" });
        await browser.close();
        return j({ sha256: await sha256(png) });
      }

      // Engine selection fail-closed: ?browser=<unknown> must be 400.
      if (p === "/engine-rejected") {
        const res = await acquire(env, "?browser=chromium");
        return j({ status: res.status });
      }

      // Control surface, reduced to assertion-friendly derivations.
      if (p === "/limits") {
        const res = await env.BROWSER.fetch("https://b/v1/limits");
        const body = await res.json();
        return j({
          maxConcurrentSessions: body.maxConcurrentSessions,
          allowedBrowserAcquisitions: body.allowedBrowserAcquisitions,
          activeSessions: body.activeSessions,
        });
      }
      // Close a fresh session in-request, then find that exact id's
      // history record — self-contained, so cumulative history across
      // deploys/runs cannot skew the count.
      if (p === "/history") {
        const browser = await puppeteer.launch(env.BROWSER);
        const sessionId = await activeSessionId(env);
        await browser.close();
        let entry = null;
        for (let i = 0; i < 50 && entry === null; i++) {
          const res = await env.BROWSER.fetch("https://b/v1/history");
          const { history } = await res.json();
          entry = history.find((h) => h.sessionId === sessionId);
          if (entry === null) await new Promise((r) => setTimeout(r, 40));
        }
        return j({
          found: entry !== null,
          status: entry?.status,
          reason: entry?.reason,
        });
      }
      if (p === "/sessions") {
        const res = await env.BROWSER.fetch("https://b/v1/sessions");
        return j(await res.json());
      }

      // ── Attach edge cases ──────────────────────────────────────

      // Unknown-but-well-formed sessionId + ws upgrade → registry
      // miss → 404.
      if (p === "/bad-session-attach") {
        const res = await env.BROWSER.fetch(
          `https://b/v1/devtools/browser/${"0".repeat(32)}`,
          { headers: { upgrade: "websocket" } },
        );
        return j({ status: res.status });
      }

      // A pending sessionId without the ws upgrade → 400 (the upgrade
      // check runs before the session lookup). keep_alive=1 sweeps the
      // orphaned reservation on the next registry op.
      if (p === "/attach-no-upgrade") {
        const acq = await acquire(env, "?keep_alive=1");
        const { sessionId } = await acq.json();
        const res = await env.BROWSER.fetch(
          `https://b/v1/devtools/browser/${sessionId}`,
        );
        return j({ status: res.status });
      }

      // An acquired reservation never attached expires at keep_alive;
      // attach then finds it only in history → 410 gone.
      if (p === "/pending-expired") {
        const acq = await acquire(env, "?keep_alive=1");
        const { sessionId } = await acq.json();
        await new Promise((r) => setTimeout(r, 20));
        const res = await env.BROWSER.fetch(
          `https://b/v1/devtools/browser/${sessionId}`,
          { headers: { upgrade: "websocket" } },
        );
        return j({ status: res.status });
      }

      // Re-attaching a session that already closed → 410 (the registry
      // keeps closed ids in history so reconnect loops get gone, not
      // not-found).
      if (p === "/closed-reattach") {
        const browser = await puppeteer.launch(env.BROWSER);
        const sessionId = await activeSessionId(env);
        await browser.close();
        let status = 0;
        for (let i = 0; i < 50 && status !== 410; i++) {
          const res = await env.BROWSER.fetch(
            `https://b/v1/devtools/browser/${sessionId}`,
            { headers: { upgrade: "websocket" } },
          );
          status = res.status;
          if (status !== 410) await new Promise((r) => setTimeout(r, 40));
        }
        return j({ status });
      }

      // Attaching a session that already has a live socket → 409.
      if (p === "/double-attach") {
        const browser = await puppeteer.launch(env.BROWSER);
        try {
          const sessionId = await activeSessionId(env);
          const res = await env.BROWSER.fetch(
            `https://b/v1/devtools/browser/${sessionId}`,
            { headers: { upgrade: "websocket" } },
          );
          return j({ status: res.status });
        } finally {
          await browser.close();
        }
      }

      // ── Acquire parameter edges ────────────────────────────────

      // Any unknown engine name fails closed, not just chromium.
      if (p === "/engine-bogus") {
        const res = await acquire(env, "?browser=bogus");
        return j({ status: res.status });
      }

      // keep_alive must be a non-negative integer (ms).
      if (p === "/keep-alive-junk") {
        const res = await acquire(env, "?keep_alive=junk");
        return j({ status: res.status });
      }

      // Engine select also gates the direct ws-connect path
      // (?browser= on the upgrade URL, not just acquire).
      if (p === "/ws-engine-rejected") {
        const res = await env.BROWSER.fetch(
          "https://b/v1/devtools/browser?browser=chromium",
          { headers: { upgrade: "websocket" } },
        );
        return j({ status: res.status });
      }

      // ── Multi-session + limits under the real engine ───────────

      // Two coexisting sessions on one binding — distinct engine
      // sessions render the identical golden.
      if (p === "/dual-sessions") {
        const ba = await puppeteer.launch(env.BROWSER);
        const bb = await puppeteer.launch(env.BROWSER);
        try {
          const res = await env.BROWSER.fetch("https://b/v1/sessions");
          const { sessions } = await res.json();
          const actives = sessions.filter((s) => s.status === "active");
          const shot = async (b) => {
            const page = await b.newPage();
            await page.setContent(MARKUP, { waitUntil: "load" });
            return sha256(await page.screenshot({ type: "png" }));
          };
          const [a, b] = await Promise.all([shot(ba), shot(bb)]);
          return j({
            a,
            b,
            twoActive: actives.length === 2,
            distinctIds:
              actives.length === 2 &&
              actives[0].sessionId !== actives[1].sessionId,
          });
        } finally {
          await ba.close();
          await bb.close();
        }
      }

      // Pending reservations count toward the limit: keep acquiring
      // (5s keep_alive so slots free quickly after the probe) until the
      // registry refuses — the overflow must be 429, not 400.
      if (p === "/concurrency-limit") {
        let status = 0;
        let acquired = 0;
        for (let i = 0; i < 8; i++) {
          const res = await acquire(env, "?keep_alive=5000");
          status = res.status;
          if (status === 429) break;
          if (!res.ok) return j({ unexpectedStatus: status });
          await res.json();
          acquired++;
        }
        return j({
          overflow429: status === 429,
          acquiredWithinCap: acquired <= 4,
        });
      }

      // Engine discovery passthrough.
      if (p === "/json-version") {
        const res = await env.BROWSER.fetch("https://b/json/version");
        return j(await res.json());
      }

      return j({ error: "unknown path", path: p });
    } catch (e) {
      return j({ threw: true, name: e.name, message: String(e?.message ?? e) });
    }
  },
};
