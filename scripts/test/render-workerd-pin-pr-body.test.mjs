import assert from "node:assert/strict";
import { test } from "node:test";

import { renderBody } from "../render-workerd-pin-pr-body.mjs";

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);
const CAPNP = "c".repeat(64);

function lock(overrides = {}) {
  return {
    source: "github-release",
    version: "vrt-20261121-abc1234",
    upstream: { version: "1.20261120.1", commit: "7545be644952d71d76b904b37ae71b10c0ba8cad" },
    artifacts: {
      "linux-x64": {
        url: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd-linux-x64.gz",
        archiveSha256: SHA_A,
        binarySha256: SHA_B,
        capnpUrl: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd.capnp",
        capnpSha256: CAPNP,
      },
      "linux-arm64": {
        url: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd-linux-arm64.gz",
        archiveSha256: SHA_B,
        binarySha256: SHA_A,
        capnpUrl: "https://github.com/becoolme/workerd/releases/download/vrt-20261121-abc1234/workerd.capnp",
        capnpSha256: CAPNP,
      },
    },
    ...overrides,
  };
}

const BASE = { tag: "vrt-20261121-abc1234", previousVersion: "vrt-20261120-2d52b58" };

test("renders the bump, the upstream workerd and every digest", () => {
  const body = renderBody({ ...BASE, lock: lock() });
  assert.match(body, /`vrt-20261120-2d52b58` -> `vrt-20261121-abc1234`/);
  assert.match(
    body,
    /\[`vrt-20261121-abc1234`\]\(https:\/\/github\.com\/becoolme\/workerd\/releases\/tag\/vrt-20261121-abc1234\)/,
  );
  assert.match(body, /`1\.20261120\.1` at `7545be644952`/);
  assert.match(body, new RegExp(`\\| \`linux-x64\` \\| \`${SHA_A}\` \\| \`${SHA_B}\` \\|`));
  assert.match(body, new RegExp(`\\| \`linux-arm64\` \\| \`${SHA_B}\` \\| \`${SHA_A}\` \\|`));
  assert.match(body, new RegExp(`\`workerd.capnp\` sha256: \`${CAPNP}\``));
  // The reproduce command has to name the version the lock records.
  assert.match(body, /--upstream-version 1\.20261120\.1 --check/);
  assert.match(body, /gh workflow run ci\.yml --ref chore\/pin-workerd-vrt-20261121-abc1234/);
});

test("takes the upstream version from the lock when no override is given", () => {
  const body = renderBody({ ...BASE, lock: lock() });
  assert.match(body, /`1\.20261120\.1` at/);
});

test("rejects a lock that is not the release being pinned", () => {
  assert.throws(
    () => renderBody({ ...BASE, tag: "vrt-20261121-fffffff", lock: lock() }),
    /does not match/,
  );
});

test("rejects a lock without a linux-x64 artifact", () => {
  const bad = lock();
  delete bad.artifacts["linux-x64"];
  assert.throws(() => renderBody({ ...BASE, lock: bad }), /linux-x64/);
});

test("rejects a lock without an upstream pin", () => {
  const bad = lock();
  delete bad.upstream;
  assert.throws(() => renderBody({ ...BASE, lock: bad }), /upstream commit\/version/);
});
