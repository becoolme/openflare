#!/usr/bin/env node
/**
 * diff.mjs — compare two recorded conformance reports field-by-field.
 *
 *   node conformance/diff.mjs --a results-openflare/latest.json \
 *                             --b results-cloudflare/latest.json \
 *                             --out conformance/results/diff [--no-fail]
 *
 * run.mjs --record stores each step's observed {status, body}; this tool
 * aligns cases by id and steps by name, then deep-diffs the OBSERVED
 * responses — not each side against its own expectations, which is what
 * the per-run report already does. A divergence here is a real
 * behavioral difference between the two stacks: the place to look when
 * triaging compat gaps or expectation errors in the case fixtures.
 *
 * Step verdicts: identical | diverged | missing-a | missing-b |
 * not-recorded (either side ran without --record for that step).
 * Case verdict: the worst of its step verdicts
 * (diverged > missing > not-recorded > identical).
 *
 * Exit code: 1 when any case diverges or is missing on a side, unless
 * --no-fail. Skipped cases compare as "skipped" — they are never
 * divergences.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// ── deep diff ───────────────────────────────────────────────────────

const isObj = (v) => v !== null && typeof v === "object" && !Array.isArray(v);

const fmt = (v) => {
  const s = typeof v === "string" ? JSON.stringify(v) : JSON.stringify(v);
  return s && s.length > 80 ? s.slice(0, 77) + "..." : s;
};

/**
 * Symmetric JSON diff: returns human-readable "path: a→b" strings.
 * Compares parsed bodies field-by-field; arrays order-sensitively.
 */
export function deepDiffs(a, b, at = "", diffs = []) {
  if (Object.is(a, b)) return diffs;
  if (isObj(a) && isObj(b)) {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      const p = at ? `${at}.${k}` : k;
      if (!(k in a)) diffs.push(`${p}: <missing in a> → ${fmt(b[k])}`);
      else if (!(k in b)) diffs.push(`${p}: ${fmt(a[k])} → <missing in b>`);
      else deepDiffs(a[k], b[k], p, diffs);
    }
    return diffs;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    for (let i = 0; i < Math.max(a.length, b.length); i++) {
      const p = `${at}[${i}]`;
      if (i >= a.length) diffs.push(`${p}: <missing in a> → ${fmt(b[i])}`);
      else if (i >= b.length) diffs.push(`${p}: ${fmt(a[i])} → <missing in b>`);
      else deepDiffs(a[i], b[i], p, diffs);
    }
    return diffs;
  }
  diffs.push(`${at || "(value)"}: ${fmt(a)} → ${fmt(b)}`);
  return diffs;
}

// ── report alignment ────────────────────────────────────────────────

const WORST = { diverged: 4, "missing-a": 3, "missing-b": 3, "not-recorded": 2, skipped: 1, identical: 0 };

function worstOf(steps) {
  return steps.reduce(
    (w, s) => (WORST[s.verdict] > WORST[w] ? s.verdict : w),
    "identical",
  );
}

function diffStep(aStep, bStep) {
  if (!aStep || !bStep)
    return { verdict: aStep ? "missing-b" : "missing-a" };
  if (aStep.status === "skipped" || bStep.status === "skipped")
    return { verdict: "skipped" };
  if (!aStep.observed || !bStep.observed)
    return { verdict: "not-recorded" };
  const diffs = [];
  if (aStep.observed.status !== bStep.observed.status)
    diffs.push(`status: ${aStep.observed.status} → ${bStep.observed.status}`);
  diffs.push(...deepDiffs(aStep.observed.body ?? null, bStep.observed.body ?? null, "body"));
  return diffs.length ? { verdict: "diverged", diffs } : { verdict: "identical" };
}

function diffCase(aCase, bCase) {
  if (!aCase || !bCase)
    return { verdict: aCase ? "missing-b" : "missing-a", steps: [] };
  if (aCase.status === "skipped" || bCase.status === "skipped")
    return {
      verdict: "skipped",
      steps: [],
      reason: (aCase.status === "skipped" ? aCase : bCase).reason,
    };
  const names = new Set([
    ...(aCase.steps ?? []).map((s) => s.name),
    ...(bCase.steps ?? []).map((s) => s.name),
  ]);
  const steps = [...names].map((name) => ({
    name,
    ...diffStep(
      (aCase.steps ?? []).find((s) => s.name === name),
      (bCase.steps ?? []).find((s) => s.name === name),
    ),
  }));
  return { verdict: worstOf(steps), steps };
}

/** Compare two run.mjs report objects. */
export function diffReports(a, b) {
  const ids = new Set([
    ...(a.cases ?? []).map((c) => c.id),
    ...(b.cases ?? []).map((c) => c.id),
  ]);
  const cases = [...ids].sort().map((id) => ({
    id,
    ...diffCase(
      (a.cases ?? []).find((c) => c.id === id),
      (b.cases ?? []).find((c) => c.id === id),
    ),
  }));
  const summary = {};
  for (const c of cases) summary[c.verdict] = (summary[c.verdict] ?? 0) + 1;
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    a: { url: a.url, stack: a.stack, generatedAt: a.generatedAt },
    b: { url: b.url, stack: b.stack, generatedAt: b.generatedAt },
    cases,
    summary,
  };
}

export function renderDiffMarkdown(report) {
  const s = report.summary;
  const lines = [
    `# Conformance diff — ${report.generatedAt}`,
    "",
    `a: \`${report.a.url}\` (${report.a.stack})`,
    `b: \`${report.b.url}\` (${report.b.stack})`,
    "",
    `**${s.identical ?? 0} identical, ${s.diverged ?? 0} diverged, ` +
      `${(s["missing-a"] ?? 0) + (s["missing-b"] ?? 0)} missing, ` +
      `${s["not-recorded"] ?? 0} not recorded, ${s.skipped ?? 0} skipped**`,
    "",
  ];
  const interesting = report.cases.filter((c) => !["identical", "skipped"].includes(c.verdict));
  if (!interesting.length) {
    lines.push("All compared cases identical.");
  } else {
    for (const c of interesting) {
      lines.push(`## ${c.id} — ${c.verdict}${c.reason ? ` (${c.reason})` : ""}`, "");
      for (const st of (c.steps ?? []).filter((st) => st.verdict !== "identical")) {
        lines.push(`- step \`${st.name}\`: ${st.verdict}`);
        for (const d of st.diffs ?? []) lines.push(`  - ${d}`);
      }
      lines.push("");
    }
  }
  return lines.join("\n") + "\n";
}

// ── CLI ─────────────────────────────────────────────────────────────

const USAGE = `usage: node conformance/diff.mjs --a <report.json> --b <report.json> [options]

Compare two conformance reports recorded with run.mjs --record.

  --a <file>      first report (openflare run)
  --b <file>      second report (cloudflare run)
  --out <dir>     write diff.json + diff.md here
  --no-fail       always exit 0 (default: exit 1 on any divergence/missing)
`;

export function main(argv, { log = console.log } = {}) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--a") args.a = argv[++i];
    else if (argv[i] === "--b") args.b = argv[++i];
    else if (argv[i] === "--out") args.out = argv[++i];
    else if (argv[i] === "--no-fail") args.noFail = true;
    else if (argv[i] === "-h" || argv[i] === "--help") {
      log(USAGE);
      return 0;
    } else throw new Error(`unknown flag: ${argv[i]}\n${USAGE}`);
  }
  if (!args.a || !args.b) throw new Error(`--a and --b are required\n${USAGE}`);
  const a = JSON.parse(fs.readFileSync(args.a, "utf8"));
  const b = JSON.parse(fs.readFileSync(args.b, "utf8"));
  const report = diffReports(a, b);
  const md = renderDiffMarkdown(report);
  if (args.out) {
    fs.mkdirSync(args.out, { recursive: true });
    fs.writeFileSync(path.join(args.out, "diff.json"), JSON.stringify(report, null, 2) + "\n");
    fs.writeFileSync(path.join(args.out, "diff.md"), md);
    log(`wrote ${path.join(args.out, "diff.json")} + diff.md`);
  } else {
    log(md);
  }
  const bad = (report.summary.diverged ?? 0) +
    (report.summary["missing-a"] ?? 0) + (report.summary["missing-b"] ?? 0);
  return args.noFail || bad === 0 ? 0 : 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    process.exit(main(process.argv.slice(2)));
  } catch (e) {
    console.error(e.message);
    process.exit(2);
  }
}
