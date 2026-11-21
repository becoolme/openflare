// resolve-workerd-upstream-version.mjs -- name the workerd release a fork pin is based on.
//
// A fork release manifest records the synced upstream commit and, when that commit already
// carried a cloudflare/workerd release tag, the tag itself:
//
//   { "upstream": { "commit": "7545be64...", "tag": "v1.20261120.1" } }
//
// `tag` is empty when the sync was cut from a commit that has no release tag yet. The lock still
// has to name a workerd release (`upstream.version`, `1.YYYYMMDD.N`), because consumers parse it
// as a version (see `vrt-config/compat/gen_compat_flags.py`), so this resolves the nearest
// ancestor release tag of the synced commit:
//
//   node scripts/resolve-workerd-upstream-version.mjs --manifest manifest.json
//   -> 1.20261120.1
//
// Ancestry comes from the GitHub compare API (`status: ahead` means the tag is reachable from
// the commit). Release tags are scanned newest first and the first ancestor wins, which is the
// closest release the pin can be described as. Everything is bounded and fail-closed: an
// unnameable pin is an error, never a guess.

import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const DEFAULT_REPO = "cloudflare/workerd";
const DEFAULT_API = "https://api.github.com";
const DEFAULT_LIMIT = 30;
const TAGS_PER_PAGE = 100;
const MAX_PAGES = 2;
const WORKERD_VERSION_RE = /^1\.\d{8}\.\d+$/;
const RELEASE_TAG_RE = /^v?1\.(\d{8})\.(\d+)$/;

function fail(message) {
  throw new Error(message);
}

/**
 * Upstream release tags, newest first. Non-release tags (nightly rolls, "workerd-with-only-...")
 * and anything that is not `v1.YYYYMMDD.N` are dropped; the order of the API response is not
 * trusted, the tags are sorted here.
 */
export function candidateTags(names) {
  const tags = [];
  for (const name of names) {
    if (typeof name !== "string") continue;
    const match = RELEASE_TAG_RE.exec(name);
    if (!match) continue;
    tags.push({ tag: name, version: name.replace(/^v/, ""), day: match[1], seq: Number(match[2]) });
  }
  return tags.sort((a, b) => {
    if (a.day !== b.day) return a.day < b.day ? 1 : -1;
    return b.seq - a.seq;
  });
}

async function requestJson(url, { token, fetchImpl, allow404 = false }) {
  const headers = {
    accept: "application/vnd.github+json",
    "user-agent": "openflare-resolve-workerd-upstream-version",
  };
  if (token) headers.authorization = `Bearer ${token}`;
  const response = await fetchImpl(url, { headers });
  if (allow404 && response.status === 404) return null;
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    fail(`${url} failed: HTTP ${response.status} ${body.slice(0, 200)}`.trim());
  }
  return response.json();
}

/**
 * Resolve the workerd release version for a fork release manifest.
 * Returns `1.YYYYMMDD.N`; throws when the pin cannot be named.
 */
export async function resolveUpstreamVersion(manifest, options = {}) {
  const repo = options.repo ?? DEFAULT_REPO;
  const apiBase = (options.apiBase ?? DEFAULT_API).replace(/\/$/, "");
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  const token = options.token ?? "";
  const limit = options.limit ?? DEFAULT_LIMIT;

  const commit = `${manifest?.upstream?.commit ?? ""}`.trim();
  if (commit.length < 7) fail("manifest.upstream.commit is missing; cannot name the upstream release");

  // A manifest that names its tag needs no network, and is authoritative when it does.
  const tagged = `${manifest?.upstream?.tag ?? ""}`.trim();
  if (tagged) {
    const version = tagged.replace(/^v/, "");
    if (!WORKERD_VERSION_RE.test(version)) {
      fail(`manifest.upstream.tag is not a workerd release tag: ${JSON.stringify(tagged)}`);
    }
    return version;
  }

  const names = [];
  for (let page = 1; page <= MAX_PAGES; page += 1) {
    const batch = await requestJson(
      `${apiBase}/repos/${repo}/tags?per_page=${TAGS_PER_PAGE}&page=${page}`,
      { token, fetchImpl },
    );
    if (!Array.isArray(batch) || batch.length === 0) break;
    for (const entry of batch) names.push(entry?.name);
    if (batch.length < TAGS_PER_PAGE) break;
  }

  const candidates = candidateTags(names);
  if (candidates.length === 0) {
    fail(`no v1.YYYYMMDD.N release tags in ${repo}; cannot name the upstream release`);
  }

  let tried = 0;
  for (const candidate of candidates) {
    if (tried >= limit) break;
    tried += 1;
    const url = `${apiBase}/repos/${repo}/compare/${encodeURIComponent(candidate.tag)}...${encodeURIComponent(commit)}`;
    // A tag deleted under us is not an ancestor, but it is not fatal either.
    const data = await requestJson(url, { token, fetchImpl, allow404: true });
    if (!data) continue;
    if (data.status === "ahead" || data.status === "identical") return candidate.version;
  }

  fail(
    `no release tag among the newest ${tried} is an ancestor of ${commit} in ${repo}; ` +
      "raise --limit or check that the fork synced a real upstream commit",
  );
}

function parseArgs(argv) {
  const args = { manifest: null, repo: DEFAULT_REPO, limit: DEFAULT_LIMIT };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--manifest") args.manifest = argv[++i];
    else if (arg.startsWith("--manifest=")) args.manifest = arg.slice("--manifest=".length);
    else if (arg === "--repo") args.repo = argv[++i];
    else if (arg.startsWith("--repo=")) args.repo = arg.slice("--repo=".length);
    else if (arg === "--limit") args.limit = Number(argv[++i]);
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length));
    else fail(`unknown argument: ${arg}`);
  }
  if (!args.manifest) fail("--manifest <path> is required");
  if (!Number.isInteger(args.limit) || args.limit < 1) fail("--limit must be a positive integer");
  return args;
}

async function main(argv) {
  const args = parseArgs(argv);
  const manifest = JSON.parse(readFileSync(args.manifest, "utf8"));
  const version = await resolveUpstreamVersion(manifest, {
    repo: args.repo,
    limit: args.limit,
    token: process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN ?? "",
  });
  process.stdout.write(`${version}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(`resolve-workerd-upstream-version: ${error.message}`);
    process.exit(1);
  });
}
