import assert from "node:assert/strict";
import { test } from "node:test";

import { buildLock, serializeLock } from "../update-workerd-lock.mjs";

const SHA = "a".repeat(64);
const SHA_B = "b".repeat(64);

function manifest(overrides = {}) {
  return {
    tag: "vrt-20261121-abc1234",
    upstream: { commit: "7545be644952d71d76b904b37ae71b10c0ba8cad", tag: "v1.20261120.1" },
    binaries: { "linux-x64": SHA, "linux-arm64": SHA_B },
    assets: {
      "workerd-linux-x64.gz": SHA_B,
      "workerd-linux-arm64.gz": SHA,
      "workerd.capnp": SHA,
    },
    ...overrides,
  };
}

test("builds a github-release lock from a fork manifest", () => {
  const lock = buildLock(manifest());
  assert.deepEqual(lock, {
    source: "github-release",
    version: "vrt-20261121-abc1234",
    upstream: {
      version: "v1.20261120.1",
      commit: "7545be644952d71d76b904b37ae71b10c0ba8cad",
    },
    artifacts: {
      "linux-x64": {
        url: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd-linux-x64.gz",
        archiveSha256: SHA_B,
        binarySha256: SHA,
        capnpUrl:
          "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd.capnp",
        capnpSha256: SHA,
      },
      "linux-arm64": {
        url: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd-linux-arm64.gz",
        archiveSha256: SHA,
        binarySha256: SHA_B,
        capnpUrl:
          "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd.capnp",
        capnpSha256: SHA,
      },
    },
  });
});

test("uses the upstream commit prefix when the release has no upstream tag", () => {
  const lock = buildLock(
    manifest({ upstream: { commit: "7545be644952d71d76b904b37ae71b10c0ba8cad" } }),
  );
  assert.equal(lock.upstream.version, "7545be644952");
});

test("rejects a tag that is not a VRT release", () => {
  assert.throws(() => buildLock(manifest({ tag: "v1.20261120.1" })), /VRT release tag/);
});

test("rejects a manifest without the schema asset", () => {
  const bad = manifest();
  delete bad.assets["workerd.capnp"];
  assert.throws(() => buildLock(bad), /workerd\.capnp/);
});

test("rejects a missing binary digest", () => {
  const bad = manifest();
  delete bad.binaries["linux-arm64"];
  assert.throws(() => buildLock(bad), /linux-arm64/);
});

test("rejects a missing asset digest", () => {
  const bad = manifest();
  bad.assets["workerd-linux-x64.gz"] = "not-a-digest";
  assert.throws(() => buildLock(bad), /workerd-linux-x64\.gz/);
});

test("serializes with two-space indentation and a trailing newline", () => {
  const text = serializeLock(buildLock(manifest()));
  assert.ok(text.endsWith("}\n"));
  assert.match(text, /\n {2}"source": "github-release",/);
});
