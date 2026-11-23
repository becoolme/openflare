import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync, statSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const script = fileURLToPath(new URL("../e2e-versioning.sh", import.meta.url));
const source = readFileSync(script, "utf8");

test("e2e-versioning.sh parses and is executable", () => {
  execFileSync("bash", ["-n", script], { stdio: "pipe" });
  const mode = statSync(script).mode & 0o777;
  assert.equal(mode & 0o100, 0o100, "owner execute bit must be set");
});

// Guard the acceptance contract: deleting a lifecycle step must not
// silently shrink what the real-machine run proves.
test("e2e-versioning.sh covers the whole version lifecycle", () => {
  const steps = [
    ["deploy v1", /of deploy --root \/data "\$WORKER_DIR\/worker-v1"/],
    ["preview deploy", /of deploy --root \/data --preview "\$WORKER_DIR\/worker-v2"/],
    ["promote", /of promote --root \/data --worker "\$WORKER" "\$V2"/],
    ["rollback", /of rollback --root \/data "\$WORKER" "\$V1"/],
    ["dry-run prune", /of versions prune --root \/data "\$WORKER" --dry-run --max-age-days 1/],
    ["real prune", /of versions prune --root \/data "\$WORKER" --max-age-days 1/],
    ["count prune", /of versions prune --root \/data "\$WORKER" --max-count 1/],
    ["share", /of versions share --root \/data --worker "\$WORKER" --ttl-seconds 60/],
  ];
  for (const [name, pattern] of steps) {
    assert.match(source, pattern, `missing lifecycle step: ${name}`);
  }
  // The observable rules the run asserts.
  assert.match(source, /\[ "\$code" = "403" \]/, "missing 403 assertion");
  assert.match(source, /preview_token_expired/, "missing expired-token assertion");
  assert.match(source, /wait_preview_code "\$P2_URL" 404/, "missing archived-preview 404 assertion");
  assert.match(source, /x-vrt-preview-version/, "missing preview marker assertion");
  assert.match(source, /sha_state/, "missing dry-run zero-write assertion");
});

test("e2e-versioning.sh never leaks a preview token", () => {
  // The bearer token is redacted in every diagnostic path, and the
  // script tears the stack down even on failure.
  assert.match(source, /redact\(\)/, "missing token redaction helper");
  assert.match(source, /down -v/, "missing stack teardown");
  assert.match(source, /__vrt_preview_token=\)\[\^&"\[:space:\]\]\+/, "redaction must cover the token");
  assert.doesNotMatch(source, /echo "\$S_URL"/, "the share URL must never be printed");
  assert.doesNotMatch(source, /echo "\$P2_URL"/, "the preview URL must never be printed");
});

test("e2e-versioning.sh refuses the stub runtime", () => {
  const result = spawnSync("bash", [script], {
    env: { ...process.env, WORKERD_SOURCE: "stub", WORKERD_SHA256: "", WORKERD_IMAGE: "" },
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.equal(result.status, 2, result.stderr);
  assert.match(result.stderr, /REAL workerd/);
});

// Opt-in real-machine run (needs docker + a pinned workerd):
//   VRT_E2E_VERSIONING=1 WORKERD_SOURCE=file WORKERD_SHA256=<hex> \
//     node --test scripts/test/e2e-versioning.test.mjs
test(
  "e2e-versioning.sh passes on the standalone compose stack",
  { skip: process.env.VRT_E2E_VERSIONING !== "1" ? "set VRT_E2E_VERSIONING=1 to run" : false },
  () => {
    const result = spawnSync("bash", [script], { env: process.env, stdio: "inherit" });
    assert.equal(result.status, 0);
  },
);
