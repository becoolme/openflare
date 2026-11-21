import assert from "node:assert/strict";
import { test } from "node:test";

import {
  candidateTags,
  resolveUpstreamVersion,
} from "../resolve-workerd-upstream-version.mjs";

const COMMIT = "7545be644952d71d76b904b37ae71b10c0ba8cad";

// A fake GitHub: `tags` is what the tags endpoint returns, `ancestors` lists the tags the
// compare endpoint reports as reachable from COMMIT ("ahead"); everything else is "behind".
function fakeGitHub({ tags, ancestors, compareStatus = {} }) {
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(url);
    if (url.includes("/tags?")) {
      const page = Number(new URL(url).searchParams.get("page") ?? "1");
      const body = page === 1 ? tags.map((name) => ({ name })) : [];
      return { ok: true, status: 200, json: async () => body, text: async () => "" };
    }
    if (url.includes("/compare/")) {
      const base = decodeURIComponent(url.split("/compare/")[1].split("...")[0]);
      const status = compareStatus[base] ?? (ancestors.includes(base) ? "ahead" : "behind");
      if (status === "missing") {
        return { ok: false, status: 404, json: async () => ({}), text: async () => "Not Found" };
      }
      return { ok: true, status: 200, json: async () => ({ status }), text: async () => "" };
    }
    throw new Error(`unexpected url ${url}`);
  };
  return { fetchImpl, calls };
}

const TAGS = [
  "v1.20261121.1",
  "v1.20261120.1",
  "v1.20261119.1",
  "workerd-with-only-v8-13.0-roll",
  "v1.20261118.2",
];

test("sorts release tags newest first and drops non-release tags", () => {
  assert.deepEqual(
    candidateTags(TAGS).map((entry) => entry.version),
    ["1.20261121.1", "1.20261120.1", "1.20261119.1", "1.20261118.2"],
  );
  assert.deepEqual(
    candidateTags(["1.20261120.1", "v1.20261120.2"]).map((entry) => entry.version),
    ["1.20261120.2", "1.20261120.1"],
  );
});

test("prefers the manifest's upstream tag and needs no network", async () => {
  const { fetchImpl, calls } = fakeGitHub({ tags: TAGS, ancestors: [] });
  const version = await resolveUpstreamVersion(
    { upstream: { commit: COMMIT, tag: "v1.20261120.1" } },
    { fetchImpl, token: "t" },
  );
  assert.equal(version, "1.20261120.1");
  assert.deepEqual(calls, []);
});

test("walks tags newest-first and returns the nearest ancestor", async () => {
  const { fetchImpl, calls } = fakeGitHub({ tags: TAGS, ancestors: ["v1.20261120.1"] });
  const version = await resolveUpstreamVersion(
    { upstream: { commit: COMMIT, tag: "" } },
    { fetchImpl, token: "t" },
  );
  assert.equal(version, "1.20261120.1");
  // v1.20261121.1 was tested first (behind), then the ancestor: stop there.
  assert.equal(calls.filter((url) => url.includes("/compare/")).length, 2);
  assert.match(calls[1], /compare\/v1\.20261121\.1\.\.\.7545be64/);
  assert.match(calls.at(-1), /compare\/v1\.20261120\.1\.\.\.7545be64/);
  assert.equal(calls.filter((url) => url.includes("/tags?")).length, 1);
});

test("treats an identical compare as an ancestor", async () => {
  const { fetchImpl } = fakeGitHub({
    tags: TAGS,
    ancestors: [],
    compareStatus: { "v1.20261121.1": "identical" },
  });
  const version = await resolveUpstreamVersion(
    { upstream: { commit: COMMIT, tag: "" } },
    { fetchImpl },
  );
  assert.equal(version, "1.20261121.1");
});

test("skips tags the API no longer resolves", async () => {
  const { fetchImpl } = fakeGitHub({
    tags: TAGS,
    ancestors: ["v1.20261119.1"],
    compareStatus: { "v1.20261121.1": "missing" },
  });
  const version = await resolveUpstreamVersion(
    { upstream: { commit: COMMIT, tag: "" } },
    { fetchImpl },
  );
  assert.equal(version, "1.20261119.1");
});

test("fails closed when no tag in range is an ancestor", async () => {
  const { fetchImpl } = fakeGitHub({ tags: TAGS, ancestors: [] });
  await assert.rejects(
    resolveUpstreamVersion({ upstream: { commit: COMMIT, tag: "" } }, { fetchImpl, limit: 2 }),
    /no release tag among the newest 2/,
  );
});

test("fails closed when the manifest has no upstream commit", async () => {
  await assert.rejects(resolveUpstreamVersion({ upstream: { tag: "" } }), /upstream\.commit/);
});

test("fails closed when the manifest's tag is not a workerd release", async () => {
  await assert.rejects(
    resolveUpstreamVersion({ upstream: { commit: COMMIT, tag: "vrt-20261120-2d52b58" } }),
    /not a workerd release tag/,
  );
});

test("fails closed when the upstream repo has no release tags", async () => {
  const { fetchImpl } = fakeGitHub({ tags: ["nightly"], ancestors: [] });
  await assert.rejects(
    resolveUpstreamVersion({ upstream: { commit: COMMIT, tag: "" } }, { fetchImpl }),
    /no v1\.YYYYMMDD\.N release tags/,
  );
});

test("surfaces API errors instead of guessing", async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 403,
    json: async () => ({}),
    text: async () => "rate limit exceeded",
  });
  await assert.rejects(
    resolveUpstreamVersion({ upstream: { commit: COMMIT, tag: "" } }, { fetchImpl }),
    /HTTP 403 rate limit exceeded/,
  );
});
