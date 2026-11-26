import test from "node:test";
import assert from "node:assert/strict";
import { deepDiffs, diffReports, renderDiffMarkdown } from "../diff.mjs";

test("deepDiffs localizes object and array differences", () => {
  const diffs = deepDiffs(
    { a: 1, b: { c: 2, d: 3 }, list: [1, 2] },
    { a: 1, b: { c: 9, e: 4 }, list: [1, 2, 3] },
  );
  assert.deepEqual(diffs.sort(), [
    "b.c: 2 → 9",
    "b.d: 3 → <missing in b>",
    "b.e: <missing in a> → 4",
    "list[2]: <missing in a> → 3",
  ]);
});

test("deepDiffs handles scalar and type mismatches", () => {
  assert.deepEqual(deepDiffs("x", "y"), ["(value): \"x\" → \"y\""]);
  assert.deepEqual(deepDiffs({ a: 1 }, [1]), ["(value): {\"a\":1} → [1]"]);
  assert.deepEqual(deepDiffs(null, null), []);
});

function report(stack, cases) {
  return { url: `https://${stack}`, stack, generatedAt: "t", cases };
}

const step = (name, status, observed) => ({
  name,
  status,
  ...(observed === undefined ? {} : { observed }),
});

test("diffReports aligns cases and classifies verdicts", () => {
  const a = report("workerd", [
    { id: "same", status: "passed", steps: [step("s", "passed", { status: 200, body: { ok: true } })] },
    { id: "drift", status: "passed", steps: [step("s", "passed", { status: 200, body: { v: 1 } })] },
    { id: "only-a", status: "passed", steps: [] },
    { id: "skip-a", status: "skipped", reason: "restart", steps: [] },
    { id: "norec", status: "passed", steps: [step("s", "passed")] },
  ]);
  const b = report("cloudflare", [
    { id: "same", status: "passed", steps: [step("s", "passed", { status: 200, body: { ok: true } })] },
    { id: "drift", status: "passed", steps: [step("s", "passed", { status: 404, body: { v: 2 } })] },
    { id: "only-b", status: "passed", steps: [] },
    { id: "skip-a", status: "passed", steps: [step("s", "passed", { status: 200 })] },
    { id: "norec", status: "passed", steps: [step("s", "passed", { status: 200, body: "x" })] },
  ]);
  const d = diffReports(a, b);
  const verdicts = Object.fromEntries(d.cases.map((c) => [c.id, c.verdict]));
  assert.equal(verdicts.same, "identical");
  assert.equal(verdicts.drift, "diverged");
  assert.equal(verdicts["only-a"], "missing-b");
  assert.equal(verdicts["only-b"], "missing-a");
  assert.equal(verdicts["skip-a"], "skipped");
  assert.equal(verdicts.norec, "not-recorded");
  const drift = d.cases.find((c) => c.id === "drift");
  assert.deepEqual(drift.steps[0].diffs[0], "status: 200 → 404");
  assert.match(drift.steps[0].diffs[1], /body\.v: 1 → 2/);
});

test("renderDiffMarkdown lists only non-identical cases", () => {
  const a = report("workerd", [
    { id: "same", status: "passed", steps: [step("s", "passed", { status: 200, body: {} })] },
    { id: "drift", status: "passed", steps: [step("s", "passed", { status: 200, body: 1 })] },
  ]);
  const b = report("cloudflare", [
    { id: "same", status: "passed", steps: [step("s", "passed", { status: 200, body: {} })] },
    { id: "drift", status: "passed", steps: [step("s", "passed", { status: 200, body: 2 })] },
  ]);
  const md = renderDiffMarkdown(diffReports(a, b));
  assert.match(md, /drift — diverged/);
  assert.doesNotMatch(md, /same — /);
});
