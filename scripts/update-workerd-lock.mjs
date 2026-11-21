// update-workerd-lock.mjs -- write deploy/docker/workerd.lock from a fork release manifest.
//
// The workerd fork publishes a manifest.json with every release:
//
//   { "tag": "vrt-20261121-abc1234",
//     "upstream": { "commit": "...", "tag": "v1.20261120.1" },
//     "binaries": { "linux-x64": "<ELF sha256>", ... },
//     "assets":   { "workerd-linux-x64.gz": "<sha256>", "workerd.capnp": "<sha256>", ... } }
//
// This turns it into the lock the CI and `scripts/fetch-workerd.sh` read, so a pin bump is
// mechanical and every digest is copied from the release instead of typed by hand.
//
//   gh release download vrt-20261121-abc1234 --pattern manifest.json --repo becoolme/workerd
//   node scripts/update-workerd-lock.mjs --manifest manifest.json
//   node scripts/update-workerd-lock.mjs --manifest manifest.json --check   # CI/PR check
//
// `upstream.version` must be the workerd release the fork is based on (`1.YYYYMMDD.N`), because
// consumers parse it as a workerd version (see `vrt-config/compat/gen_compat_flags.py`). When a
// release was cut from an upstream commit that carries no release tag yet, the manifest's
// `upstream.tag` is empty and the version has to be named explicitly:
//
//   node scripts/update-workerd-lock.mjs --manifest manifest.json --upstream-version 1.20261120.1
//
// Anything missing or inconsistent fails: a lock that cannot be verified is not a pin.

import { readFileSync, writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const DEFAULT_REPO = "becoolme/workerd";
const DEFAULT_OUT = "deploy/docker/workerd.lock";
const PLATFORMS = ["linux-x64", "linux-arm64"];
const WORKERD_VERSION_RE = /^1\.\d{8}\.\d+$/;

function fail(message) {
  throw new Error(message);
}

/**
 * The workerd release the fork is based on. The manifest's tag wins; a release cut from an
 * untagged upstream commit must name it with `--upstream-version`.
 */
function upstreamVersionOf(manifest, options) {
  const tagged = (manifest?.upstream?.tag ?? "").replace(/^v/, "");
  const explicit = (options.upstreamVersion ?? "").replace(/^v/, "");
  if (tagged && explicit && tagged !== explicit) {
    fail(`--upstream-version ${explicit} contradicts the manifest's upstream.tag ${tagged}`);
  }
  const version = tagged || explicit;
  if (!version) {
    fail(
      "manifest.upstream.tag is empty; pass --upstream-version 1.YYYYMMDD.N " +
        "(the lock records the workerd release consumers parse)",
    );
  }
  if (!WORKERD_VERSION_RE.test(version)) {
    fail(`upstream workerd version is not 1.YYYYMMDD.N: ${JSON.stringify(version)}`);
  }
  return version;
}

/**
 * Build the lock object for a fork release manifest.
 * Throws when the manifest cannot produce a fully verified pin.
 */
export function buildLock(manifest, options = {}) {
  const repo = options.repo ?? DEFAULT_REPO;
  const tag = manifest?.tag;
  if (typeof tag !== "string" || !/^vrt-\d{8}-[0-9a-f]{7,40}$/.test(tag)) {
    fail(`manifest.tag is not a VRT release tag: ${JSON.stringify(tag)}`);
  }
  const upstreamCommit = manifest?.upstream?.commit;
  if (typeof upstreamCommit !== "string" || upstreamCommit.length < 7) {
    fail("manifest.upstream.commit is missing; releases must record the synced workerd commit");
  }
  const upstreamVersion = upstreamVersionOf(manifest, options);
  const binaries = manifest?.binaries ?? {};
  const assets = manifest?.assets ?? {};
  const capnpSha = assets["workerd.capnp"];
  if (typeof capnpSha !== "string" || !/^[0-9a-f]{64}$/.test(capnpSha)) {
    fail("manifest.assets['workerd.capnp'] is missing; the release must ship the schema");
  }

  const base = `https://github.com/${repo}/releases/download/${tag}`;
  const artifacts = {};
  for (const platform of PLATFORMS) {
    const binary = binaries[platform];
    if (typeof binary !== "string" || !/^[0-9a-f]{64}$/.test(binary)) {
      fail(`manifest.binaries['${platform}'] is missing or not a sha256`);
    }
    const assetName = `workerd-${platform}.gz`;
    const archive = assets[assetName];
    if (typeof archive !== "string" || !/^[0-9a-f]{64}$/.test(archive)) {
      fail(`manifest.assets['${assetName}'] is missing or not a sha256`);
    }
    artifacts[platform] = {
      url: `${base}/${assetName}`,
      archiveSha256: archive,
      binarySha256: binary,
      capnpUrl: `${base}/workerd.capnp`,
      capnpSha256: capnpSha,
    };
  }

  return {
    source: "github-release",
    version: tag,
    upstream: {
      // Consumers (vrt-config compat checks) parse this as a workerd semver.
      version: upstreamVersion,
      commit: upstreamCommit,
    },
    artifacts,
  };
}

export function serializeLock(lock) {
  return `${JSON.stringify(lock, null, 2)}\n`;
}

function parseArgs(argv) {
  const args = { out: DEFAULT_OUT, check: false, manifest: null, repo: DEFAULT_REPO };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--check") args.check = true;
    else if (arg === "--manifest") args.manifest = argv[++i];
    else if (arg.startsWith("--manifest=")) args.manifest = arg.slice("--manifest=".length);
    else if (arg === "--out") args.out = argv[++i];
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
    else if (arg === "--repo") args.repo = argv[++i];
    else if (arg.startsWith("--repo=")) args.repo = arg.slice("--repo=".length);
    else if (arg === "--upstream-version") args.upstreamVersion = argv[++i];
    else if (arg.startsWith("--upstream-version=")) {
      args.upstreamVersion = arg.slice("--upstream-version=".length);
    } else fail(`unknown argument: ${arg}`);
  }
  if (!args.manifest) fail("--manifest <path> is required");
  return args;
}

function main(argv) {
  const args = parseArgs(argv);
  const manifest = JSON.parse(readFileSync(args.manifest, "utf8"));
  const rendered = serializeLock(
    buildLock(manifest, { repo: args.repo, upstreamVersion: args.upstreamVersion }),
  );

  if (args.check) {
    let current;
    try {
      current = readFileSync(args.out, "utf8");
    } catch {
      fail(`${args.out} does not exist; run without --check to write it`);
    }
    if (current !== rendered) {
      fail(`${args.out} does not match the release manifest; run the script to update it`);
    }
    console.log(`${args.out} matches ${manifest.tag}`);
    return;
  }

  writeFileSync(args.out, rendered);
  console.log(`wrote ${args.out} for ${manifest.tag} (upstream ${manifest.upstream.commit.slice(0, 12)})`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(`update-workerd-lock: ${error.message}`);
    process.exit(1);
  }
}
