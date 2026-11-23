/**
 * Image-contract guard for the conformance gate.
 *
 * The gate builds the standalone stack from source and deploys every
 * case with `of deploy`, which bundles the worker through the
 * `of-bundler` helper subprocess. A source-built `of` does NOT embed
 * the helper (only release.yml sets OF_EMBED_BUNDLER), so the image
 * must co-install it next to `of` — resolution step 4 in
 * vrt-local/src/bundle.rs. When that COPY was missing the stack booted
 * fine and every bundle-backed case failed with
 * "of-bundler helper not found".
 *
 * This test is static on purpose: it runs in the per-PR `ci` job,
 * which never builds or boots the image, so it can fail the PR that
 * drops the helper without waiting for the weekly gate.
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "../..");
const DOCKERFILE = path.join(ROOT, "deploy/docker/Dockerfile.vrt-local");

const dockerfile = fs.readFileSync(DOCKERFILE, "utf8");

function stage(name) {
  const lines = dockerfile.split("\n");
  const start = lines.findIndex((l) => new RegExp(`^FROM\\s+.*\\sAS\\s+${name}$`).test(l));
  assert.notEqual(start, -1, `Dockerfile.vrt-local has no stage ${name}`);
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((l) => l.startsWith("FROM "));
  return (end === -1 ? rest : rest.slice(0, end)).join("\n");
}

test("builder stage compiles the of-bundler helper", () => {
  const body = stage("builder");
  // of-bundler's lock is the higher toolchain floor (rolldown/oxc need
  // 1.97); one toolchain builds both trees, so the pin must satisfy it.
  assert.match(dockerfile, /^ARG RUST_IMAGE=rust:1\.97\.0-bookworm$/m);
  assert.match(body, /--manifest-path of-bundler\/Cargo\.toml/);
  assert.match(body, /of-bundler\/target\/release\/of-bundler \/out\/of-bundler/);
  assert.match(
    body,
    /rm -rf of-bundler\/target/,
    "the helper target tree must not stay in the image layer",
  );
  // Sequential in one stage: the two target trees must never coexist
  // (the runner's free disk cannot hold both at once).
  assert.ok(
    body.indexOf("--manifest-path of-bundler") > body.indexOf("-p vrt-local --bin of"),
    "helper build runs after the service builds",
  );
});

test("runtime co-installs of-bundler next to of and fails closed", () => {
  const body = stage("runtime");
  assert.match(
    body,
    /COPY --from=builder \/out\/of-bundler \/usr\/local\/bin\/of-bundler/,
    "runtime must co-install the helper (bundle.rs resolution step 4)",
  );
  assert.match(
    body,
    /test -x \/usr\/local\/bin\/of-bundler/,
    "the build must fail when the helper is missing, not the first deploy",
  );
  // `of` is what the compose services exec; resolution step 4 looks in
  // the running binary's directory, so the helper must land beside it.
  assert.match(body, /\/out\/of .*\/usr\/local\/bin\//);
});
