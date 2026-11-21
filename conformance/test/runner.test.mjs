import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  buildReport,
  conformanceExitCode,
  renderMarkdown,
  runCase,
  stageCaseDir,
  summarize,
  writeReport,
} from "../run.mjs";

function fakeStack(handler) {
  return new Promise((resolve) => {
    const srv = http.createServer(handler);
    srv.listen(0, "127.0.0.1", () =>
      resolve({ srv, url: `http://127.0.0.1:${srv.address().port}` }),
    );
  });
}

function entry(dir, id, caseJson) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "case.json"), JSON.stringify(caseJson));
  fs.writeFileSync(path.join(dir, "vrt.toml"), `name = "${id}"\nhostnames = ["${id}.vrt.local"]\n`);
  // Mirror loadCases: the discovered manifest is injected into the
  // case record so resolveHostname/deploy staging can find it.
  return { id, binding: "kv", dir, ok: true, case: { ...caseJson, manifest: "vrt.toml" }, errors: [] };
}

function steps(expectOverrides = {}) {
  return [
    {
      name: "ping",
      request: { path: "/ping" },
      expect: { status: 200, json: { pong: true }, ...expectOverrides },
    },
  ];
}

const baseCase = (extra = {}) => ({
  schemaVersion: 1,
  id: "t-case",
  binding: "kv",
  title: "t",
  evidence: ["x"],
  steps: steps(),
  ...extra,
});

test("runCase passes on matching responses, sends Host header", async () => {
  let seenHost;
  const { srv, url } = await fakeStack((req, res) => {
    seenHost = req.headers.host;
    res.setHeader("content-type", "application/json");
    res.end('{"pong":true}');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", baseCase());
    const r = await runCase(e, { url, noDeploy: true, readyTimeoutMs: 5000, timeoutMs: 3000, keepGoing: true, log: () => {} });
    assert.equal(r.status, "passed");
    assert.equal(seenHost, "t-case.vrt.local");
    assert.equal(r.steps[0].status, "passed");
  } finally {
    srv.close();
  }
});

test("runCase fails with diffs on mismatch", async () => {
  const { srv, url } = await fakeStack((_req, res) => res.end('{"pong":false}'));
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", baseCase({ readyTimeoutMs: 1000 }));
    const r = await runCase(e, { url, noDeploy: true, readyTimeoutMs: 1000, timeoutMs: 2000, keepGoing: true, log: () => {} });
    assert.equal(r.status, "failed");
    assert.ok(r.steps[0].failures[0].includes("pong"));
  } finally {
    srv.close();
  }
});

test("step retry polls until the expect passes", async () => {
  let hits = 0;
  const { srv, url } = await fakeStack((_req, res) => {
    hits++;
    res.end(hits < 3 ? '["retry","pending"]' : '["retry","done"]');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const c = baseCase();
    c.steps = [
      { name: "poll", request: { path: "/p" }, expect: { status: 200, bodyContains: ["done"] }, retry: { timeoutMs: 4000, intervalMs: 50 } },
    ];
    const e = entry(path.join(tmp, "t-case"), "t-case", c);
    const r = await runCase(e, { url, noDeploy: true, readyTimeoutMs: 1000, timeoutMs: 2000, keepGoing: true, log: () => {} });
    assert.equal(r.status, "passed");
    assert.ok(hits >= 3);
  } finally {
    srv.close();
  }
});

test("--stub skips requiresWorkerd cases", async () => {
  const { srv, url } = await fakeStack((_req, res) => res.end("x"));
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", baseCase());
    const r = await runCase(e, { url, stub: true, noDeploy: true, readyTimeoutMs: 100, timeoutMs: 500, keepGoing: true, log: () => {} });
    assert.equal(r.status, "skipped");
    assert.match(r.reason, /stub/);
    // non-requiresWorkerd cases still run on a stub stack
    const c2 = baseCase({ requiresWorkerd: false });
    const e2 = entry(path.join(tmp, "t-case2"), "t-case2", c2);
    const r2 = await runCase(e2, { url, stub: true, noDeploy: true, readyTimeoutMs: 1000, timeoutMs: 2000, keepGoing: true, log: () => {} });
    assert.equal(r2.status, "failed"); // fake stack body is "x", not matching expect
  } finally {
    srv.close();
  }
});

test("--host overrides the manifest hostname", async () => {
  let seenHost;
  const { srv, url } = await fakeStack((req, res) => {
    seenHost = req.headers.host;
    res.end('{"pong":true}');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", baseCase());
    const r = await runCase(e, { url, host: "override.example", noDeploy: true, readyTimeoutMs: 1000, timeoutMs: 2000, keepGoing: true, log: () => {} });
    assert.equal(r.status, "passed");
    assert.equal(seenHost, "override.example");
  } finally {
    srv.close();
  }
});

test("report builder + writer produce json and md", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-report-"));
  const report = buildReport({
    url: "http://x",
    stack: "stub",
    results: [
      { id: "a", binding: "kv", title: "A", status: "passed", steps: [{ name: "s", status: "passed" }] },
      { id: "b", binding: "d1", title: "B", status: "failed", steps: [{ name: "s", status: "failed", failures: ["boom"] }] },
      { id: "c", binding: "kv", title: "C", status: "skipped", reason: "needs x" },
    ],
  });
  assert.deepEqual(report.totals, { passed: 1, failed: 1, skipped: 1, stepsPassed: 1, stepsFailed: 1 });
  const { jsonPath, mdPath } = writeReport(report, tmp);
  const back = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
  assert.equal(back.totals.failed, 1);
  const md = fs.readFileSync(mdPath, "utf8");
  assert.match(md, /\| a \| kv \| passed \|/);
  assert.match(md, /boom/);
  assert.equal(summarize(report.cases).skipped, 1);
});

test("stageCaseDir copies asset subdirectories and drops case metadata", () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-stage-"));
  const dir = path.join(tmp, "assets-basic");
  fs.mkdirSync(path.join(dir, "public", "nested"), { recursive: true });
  fs.writeFileSync(path.join(dir, "case.json"), "{}");
  fs.writeFileSync(path.join(dir, "README.md"), "# docs");
  fs.writeFileSync(path.join(dir, "wrangler.jsonc"), "{}");
  fs.writeFileSync(path.join(dir, "worker.js"), "export default {};");
  fs.writeFileSync(path.join(dir, "public", "asset.txt"), "asset-conformance-ok");
  fs.writeFileSync(path.join(dir, "public", "nested", "deep.txt"), "deep");
  // A markdown file inside the asset tree is deployable content, not case
  // documentation.
  fs.writeFileSync(path.join(dir, "public", "notes.md"), "# asset");

  const staged = stageCaseDir({ id: "assets-basic", dir });
  assert.deepEqual(fs.readdirSync(staged).sort(), ["public", "worker.js", "wrangler.jsonc"]);
  assert.equal(
    fs.readFileSync(path.join(staged, "public", "asset.txt"), "utf8"),
    "asset-conformance-ok",
  );
  assert.equal(fs.readFileSync(path.join(staged, "public", "nested", "deep.txt"), "utf8"), "deep");
  assert.equal(fs.readFileSync(path.join(staged, "public", "notes.md"), "utf8"), "# asset");
});

test("conformanceExitCode: skips are fatal only under fail-on-skip", () => {  assert.equal(conformanceExitCode({ passed: 11, failed: 0, skipped: 0 }), 0);
  assert.equal(conformanceExitCode({ passed: 10, failed: 0, skipped: 1 }), 0);
  assert.equal(
    conformanceExitCode({ passed: 10, failed: 0, skipped: 1 }, { failOnSkip: true }),
    1,
  );
  assert.equal(
    conformanceExitCode({ passed: 0, failed: 1, skipped: 0 }, { failOnSkip: true }),
    1,
  );
  assert.equal(conformanceExitCode({ passed: 0, failed: 1, skipped: 2 }), 1);
});

// ── restart steps ───────────────────────────────────────────────────

function restartCase(extra = {}) {
  const c = baseCase({
    steps: [
      { name: "ping", request: { path: "/ping" }, expect: { status: 200, json: { ok: true } } },
      {
        name: "restart runtime",
        restart: {
          services: ["vrt-host", "vrt"],
          waitReady: {
            request: { path: "/state" },
            expect: { status: 200, json: { ready: true } },
            timeoutMs: 5000,
            intervalMs: 25,
          },
        },
      },
      { name: "after", request: { path: "/after" }, expect: { status: 200, json: { ok: true } } },
    ],
    ...extra,
  });
  return c;
}

const caseOpts = (url, overrides = {}) => ({
  url,
  noDeploy: true,
  restartCmd: "true",
  restartTimeoutMs: 5000,
  readyTimeoutMs: 2000,
  timeoutMs: 2000,
  keepGoing: true,
  log: () => {},
  ...overrides,
});

test("restart step runs the hook, then gates on the readiness probe", async () => {
  const logPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "conf-restart-")), "ran");
  const old = process.env.CONF_RESTART_LOG;
  process.env.CONF_RESTART_LOG = logPath;
  const seen = [];
  let stateHits = 0;
  const { srv, url } = await fakeStack((req, res) => {
    seen.push(req.url);
    if (req.url === "/state") {
      // The runtime needs a moment to come back after the hook returns:
      // the probe must poll, not assume readiness.
      if (++stateHits < 3) {
        res.statusCode = 503;
        return res.end("not ready");
      }
    }
    res.setHeader("content-type", "application/json");
    res.end(req.url === "/state" ? '{"ready":true}' : '{"ok":true}');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", restartCase());
    const r = await runCase(
      e,
      caseOpts(url, { restartCmd: 'printf "%s" "$RESTART_SERVICES" > "$CONF_RESTART_LOG"' }),
    );
    assert.equal(r.status, "passed", JSON.stringify(r));
    // The hook saw the ordered service list from case.json.
    assert.equal(fs.readFileSync(logPath, "utf8"), "vrt-host vrt");
    assert.deepEqual(r.steps[1].restart, ["vrt-host", "vrt"]);
    // Readiness retried while the fake stack reported 503, and the
    // post-restart step only ran afterwards.
    assert.ok(seen.filter((p) => p === "/state").length >= 2, seen.join(","));
    assert.ok(seen.indexOf("/ping") < seen.indexOf("/state"));
    assert.ok(seen.lastIndexOf("/state") < seen.indexOf("/after"));
  } finally {
    process.env.CONF_RESTART_LOG = old;
    srv.close();
  }
});

test("restart hook failure fails the case (fail closed)", async () => {
  const { srv, url } = await fakeStack((_req, res) => {
    res.setHeader("content-type", "application/json");
    res.end('{"ok":true,"ready":true}');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", restartCase());
    const r = await runCase(
      e,
      caseOpts(url, { restartCmd: 'echo "compose exploded" >&2; exit 3', keepGoing: false }),
    );
    assert.equal(r.status, "failed");
    assert.equal(r.steps[1].status, "failed");
    assert.match(r.steps[1].failures.join("\n"), /restart-cmd failed/);
    assert.match(r.steps[1].failures.join("\n"), /compose exploded/);
    // --fail-fast: the post-restart step must not run after the failure.
    assert.equal(r.steps.length, 2);
  } finally {
    srv.close();
  }
});

test("restart readiness timeout fails the case (fail closed)", async () => {
  const { srv, url } = await fakeStack((req, res) => {
    if (req.url === "/state") {
      res.statusCode = 503;
      return res.end("still booting");
    }
    res.setHeader("content-type", "application/json");
    res.end('{"ok":true}');
  });
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const c = restartCase();
    c.steps[1].restart.waitReady.timeoutMs = 150;
    const e = entry(path.join(tmp, "t-case"), "t-case", c);
    const r = await runCase(e, caseOpts(url, { keepGoing: false }));
    assert.equal(r.status, "failed");
    assert.match(r.steps[1].failures.join("\n"), /restart readiness/);
    assert.match(r.steps[1].failures.join("\n"), /status: expected 200, got 503/);
  } finally {
    srv.close();
  }
});

test("a restart case without a restart hook is skipped before deploying", async () => {
  const marker = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "conf-restart-")), "deployed");
  const old = process.env.CONF_DEPLOY_MARK;
  process.env.CONF_DEPLOY_MARK = marker;
  const { srv, url } = await fakeStack((_req, res) => res.end('{"ok":true}'));
  try {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "conf-test-"));
    const e = entry(path.join(tmp, "t-case"), "t-case", restartCase());
    const r = await runCase(
      e,
      caseOpts(url, {
        restartCmd: undefined,
        deployCmd: 'touch "$CONF_DEPLOY_MARK"',
      }),
    );
    assert.equal(r.status, "skipped");
    assert.match(r.reason, /restart-cmd/);
    assert.equal(fs.existsSync(marker), false, "skipped case must not deploy");
  } finally {
    process.env.CONF_DEPLOY_MARK = old;
    srv.close();
  }
});
