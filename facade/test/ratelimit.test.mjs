import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const core = await import(facadeModule("ratelimit-core.js"));
const { RateLimiter } = await import(facadeModule("facade-ratelimit.js"));
const { RateLimitStore, validateRateLimitKey, ratelimitParams } = core;

const P = { scope: "t1\u0001w1\u00011001", limit: 3, periodSec: 60 };

test("within limit succeeds, over limit fails, window roll restores", () => {
  let now = 1_000_000_000_000 - (1_000_000_000_000 % 60000);
  const s = new RateLimitStore(() => now);
  assert.deepEqual([1, 2, 3, 4, 5].map(() => s.hit(P, "ip")), [true, true, true, false, false]);
  now += 59_999;
  assert.equal(s.hit(P, "ip"), false);
  now += 1;
  assert.equal(s.hit(P, "ip"), true);
});

test("keys and scopes are isolated", () => {
  const s = new RateLimitStore(() => 0);
  for (let i = 0; i < 3; i++) s.hit(P, "a");
  assert.equal(s.hit(P, "a"), false);
  assert.equal(s.hit(P, "b"), true);
  assert.equal(s.hit({ ...P, scope: "t2\u0001w1\u00011001" }, "a"), true);
  assert.equal(s.hit({ ...P, scope: "t1\u0001w1\u00011002" }, "a"), true);
});

test("table is bounded: expired swept first, then oldest evicted", () => {
  let now = 0;
  const s = new RateLimitStore(() => now, 4);
  const p = { scope: "s", limit: 1, periodSec: 10 };
  for (const k of ["a", "b", "c", "d"]) s.hit(p, k);
  assert.equal(s.size, 4);
  s.hit(p, "e"); // full, nothing expired -> evict oldest ("a")
  assert.equal(s.size, 4);
  assert.equal(s.hit(p, "d"), false); // d survived and is still limited
  assert.equal(s.hit(p, "a"), true); // a was evicted: counter reset (documented fail-open)
  now += 10_000; // everything expired
  s.hit(p, "z");
  assert.ok(s.size <= 4);
  assert.equal(s.hit(p, "z"), false);
});

test("scope A flooding never evicts scope B counters", () => {
  const s = new RateLimitStore(() => 0, 100, 10);
  const a = { scope: "A", limit: 1, periodSec: 60 };
  const b = { scope: "B", limit: 1, periodSec: 60 };
  for (let i = 0; i < 5; i++) s.hit(b, `b${i}`);
  for (let i = 0; i < 1000; i++) s.hit(a, `a${i}`);
  for (let i = 0; i < 5; i++) assert.equal(s.hit(b, `b${i}`), false, `b${i} kept`);
  assert.equal(s.size, 15);
});

test("per-scope counts stay consistent across window roll and sweep", () => {
  let now = 0;
  const s = new RateLimitStore(() => now, 50, 5);
  const p = { scope: "A", limit: 1, periodSec: 10 };
  for (let i = 0; i < 5; i++) s.hit(p, `k${i}`);
  now += 10_000;
  for (let i = 0; i < 5; i++) assert.equal(s.hit(p, `k${i}`), true);
  assert.equal(s.size, 5);
  now += 10_000;
  s.hit(p, "new");
  assert.ok(s.size <= 5);
});

test("makeRoom examines a bounded number of entries (live head, expired tail)", () => {
  let now = 0;
  const s = new RateLimitStore(() => now, 300, 300);
  const long = { scope: "S", limit: 1, periodSec: 60 };
  const short = { scope: "S", limit: 1, periodSec: 10 };
  for (let i = 0; i < 150; i++) s.hit(long, `l${i}`);
  for (let i = 0; i < 150; i++) s.hit(short, `s${i}`);
  assert.equal(s.size, 300);
  now += 10_000;
  s.hit(long, "fresh");
  assert.ok(s.lastMakeRoomChecks <= 64, `checks=${s.lastMakeRoomChecks}`);
  assert.ok(s.size <= 300);
});

test("key validation", () => {
  assert.equal(validateRateLimitKey({ key: "ok" }), "ok");
  for (const bad of [undefined, null, 1, "x", {}, { key: 1 }, { key: null }, { key: "a".repeat(1025) }, { key: "é".repeat(600) }]) {
    assert.throws(() => validateRateLimitKey(bad), TypeError);
  }
  assert.equal(validateRateLimitKey({ key: "a".repeat(1024) }).length, 1024);
});

const props = (o = {}) => ({ instanceId: "tenant-1", workerId: "w-1", namespaceId: "1001", limit: 2, period: 60, ...o });

test("entrypoint returns {success} and shares counters per scope only", async () => {
  const a = new RateLimiter({ props: props({ namespaceId: "7771" }) }, {});
  const b = new RateLimiter({ props: props({ namespaceId: "7771" }) }, {});
  const other = new RateLimiter({ props: props({ namespaceId: "7771", instanceId: "tenant-2" }) }, {});
  assert.deepEqual(await a.limit({ key: "k" }), { success: true });
  assert.deepEqual(await b.limit({ key: "k" }), { success: true });
  assert.deepEqual(await a.limit({ key: "k" }), { success: false });
  assert.deepEqual(await other.limit({ key: "k" }), { success: true });
  await assert.rejects(a.limit({ key: 5 }), TypeError);
});

test("props fail closed", () => {
  for (const bad of [undefined, null, [], props({ limit: 0 }), props({ limit: 1_000_001 }), props({ limit: 1.5 }),
    props({ period: 30 }), props({ namespaceId: "abc" }), props({ namespaceId: "" }), props({ instanceId: "" }),
    props({ workerId: "a b" }), props({ limit: "3" })]) {
    assert.throws(() => ratelimitParams(bad));
  }
  assert.equal(ratelimitParams(props()).periodSec, 60);
});
