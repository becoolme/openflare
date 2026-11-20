import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  KNOWN_BINDINGS,
  loadCases,
  loadProjects,
  listProjects,
  validateCase,
  validateProjects,
  resolveHostname,
} from "../run.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CONF = path.join(HERE, "..");

function goodCase() {
  return {
    schemaVersion: 1,
    id: "x-case",
    binding: "kv",
    title: "t",
    evidence: ["https://developers.cloudflare.com/kv/"],
    steps: [
      {
        name: "s",
        request: { path: "/" },
        expect: { status: 200, json: { ok: true } },
      },
    ],
  };
}

test("validateCase accepts a minimal valid case", () => {
  const v = validateCase(goodCase(), "x-case");
  assert.equal(v.ok, true, v.errors.join("\n"));
});

test("validateCase rejects: bad id, missing evidence, empty steps, unknown fields", () => {
  const c = goodCase();
  c.id = "Bad_Id";
  c.evidence = [];
  c.steps = [];
  c.bogus = 1;
  const v = validateCase(c, "x-case");
  assert.equal(v.ok, false);
  const joined = v.errors.join("\n");
  assert.match(joined, /id:/);
  assert.match(joined, /evidence/);
  assert.match(joined, /steps/);
  assert.match(joined, /bogus/);
});

test("validateCase: id must equal dir name", () => {
  const v = validateCase(goodCase(), "other-name");
  assert.equal(v.ok, false);
  assert.match(v.errors.join("\n"), /case directory name/);
});

test("validateCase: expect field checks", () => {
  const c = goodCase();
  c.steps[0].expect = { status: 99 };
  const v = validateCase(c, "x-case");
  assert.equal(v.ok, false);
  assert.match(v.errors.join("\n"), /status/);
  const c2 = goodCase();
  c2.steps[0].expect = { wat: 1 };
  assert.equal(validateCase(c2, "x-case").ok, false);
});

test("validateCase: request bodies are exclusive", () => {
  const c = goodCase();
  c.steps[0].request = { path: "/", body: "a", json: {} };
  const v = validateCase(c, "x-case");
  assert.equal(v.ok, false);
  assert.match(v.errors.join("\n"), /only one of body\/json\/bodyBase64/);
});

test("every shipped case validates and declares evidence", () => {
  const entries = loadCases(path.join(CONF, "cases"));
  assert.ok(entries.length >= 10, `expected >=10 cases, got ${entries.length}`);
  const bad = entries.filter((e) => !e.ok);
  assert.deepEqual(bad.map((b) => b.errors), []);
  for (const e of entries) {
    assert.ok(e.case.evidence.length >= 1, `${e.id} needs evidence`);
    assert.ok(KNOWN_BINDINGS.includes(e.binding), `${e.binding} known`);
    assert.ok(resolveHostname(e), `${e.id} resolves a hostname`);
  }
});

test("bindings coverage: all seven binding areas present", () => {
  const entries = loadCases(path.join(CONF, "cases"));
  const bindings = new Set(entries.map((e) => e.binding));
  for (const b of ["kv", "r2", "d1", "queues", "cache", "cron", "durable-objects"])
    assert.ok(bindings.has(b), `missing binding coverage: ${b}`);
});

test("real-projects.json validates", () => {
  const v = loadProjects(CONF);
  assert.equal(v.ok, true, v.errors.join("\n"));
  assert.ok(v.projects.length >= 10, "need >=10 projects");
});

test("validateProjects rejects bad shapes", () => {
  assert.equal(validateProjects({ schemaVersion: 1, projects: "x" }).ok, false);
  const v = validateProjects({
    schemaVersion: 1,
    projects: [
      { name: "a", repo: "ftp://nope", commit: "zzz", bindings: ["kv"], expect: "great" },
      { name: "a", repo: "https://github.com/a/b", commit: null, bindings: [], expect: "works" },
    ],
  });
  assert.equal(v.ok, false);
  const j = v.errors.join("\n");
  assert.match(j, /repo/);
  assert.match(j, /commit/);
  assert.match(j, /expect/);
  assert.match(j, /duplicate/);
});

test("listProjects prints one line per project", () => {
  const v = loadProjects(CONF);
  const lines = listProjects(v).split("\n");
  assert.equal(lines.length, v.projects.length);
  assert.match(lines[0], /github\.com/);
});

test("resolveHostname prefers explicit case.hostname", () => {
  const e = { dir: "/nonexistent", case: { hostname: "h.example" } };
  assert.equal(resolveHostname(e), "h.example");
});
