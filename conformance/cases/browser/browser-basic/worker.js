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

// sessionIds this isolate opened — lets /history correlate its own
// closes (facade history is cumulative across runs; the deploy hook
// restarts the worker each run so the set resets per case).
const opened = new Set();

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
          opened.add(await activeSessionId(env));
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
        opened.add(sessionId);
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
      if (p === "/history") {
        const res = await env.BROWSER.fetch("https://b/v1/history");
        const body = await res.json();
        return j({
          closedWithClientClosed: body.history.filter(
            (s) =>
              opened.has(s.sessionId) &&
              s.status === "closed" &&
              s.reason === "client_closed",
          ).length,
        });
      }
      if (p === "/sessions") {
        const res = await env.BROWSER.fetch("https://b/v1/sessions");
        return j(await res.json());
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
