// render-workerd-pin-pr-body.mjs -- the pull request body for an automated workerd pin bump.
//
// `.github/workflows/pin-workerd.yml` regenerates `deploy/docker/workerd.lock` from a fork
// release manifest and opens the bump as a PR. The body has to carry the evidence a reviewer
// needs (which release, which upstream workerd, every digest), and it has to be derived from the
// lock itself so the table can never disagree with the committed pin.
//
//   node scripts/render-workerd-pin-pr-body.mjs --lock deploy/docker/workerd.lock \
//     --tag vrt-20261121-abc1234 --previous vrt-20261120-2d52b58 --upstream-version 1.20261120.1

import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const FORK_REPO = "becoolme/workerd";

function fail(message) {
  throw new Error(message);
}

/**
 * Render the PR body for `lock`. Pure: every digest comes from the lock it will ship.
 */
export function renderBody({ lock, tag, previousVersion, upstreamVersion, upstreamCommit, repo }) {
  if (!lock?.artifacts?.["linux-x64"]) fail("lock has no linux-x64 artifact");
  if (lock.version !== tag) fail(`lock.version ${lock.version} does not match ${tag}`);
  const fork = repo ?? FORK_REPO;
  const releaseUrl = `https://github.com/${fork}/releases/tag/${tag}`;
  const commit = upstreamCommit ?? lock.upstream?.commit ?? "";
  const version = upstreamVersion ?? lock.upstream?.version ?? "";
  if (!commit || !version) fail("lock has no upstream commit/version");

  const rows = Object.entries(lock.artifacts).map(
    ([platform, artifact]) =>
      `| \`${platform}\` | \`${artifact.archiveSha256}\` | \`${artifact.binarySha256}\` |`,
  );
  const capnp = Object.values(lock.artifacts)[0]?.capnpSha256 ?? "";

  return [
    `Automated pin bump: \`${previousVersion}\` -> \`${tag}\`.`,
    "",
    "Generated from the fork release's `manifest.json` by",
    "`.github/workflows/pin-workerd.yml`; every digest below is copied from the release, none is",
    "typed by hand.",
    "",
    "| | |",
    "| --- | --- |",
    `| Fork release | [\`${tag}\`](${releaseUrl}) |`,
    `| Upstream workerd | \`${version}\` at \`${commit.slice(0, 12)}\` |`,
    `| Previous pin | \`${previousVersion}\` |`,
    "| Lock | `deploy/docker/workerd.lock` |",
    "",
    "| platform | release asset sha256 | binary sha256 |",
    "| --- | --- | --- |",
    ...rows,
    "",
    `\`workerd.capnp\` sha256: \`${capnp}\``,
    "",
    "### Verification",
    "",
    "- `real-workerd` re-downloads both release assets, re-checks the archive and binary digests,",
    "  then runs the full `VRT_WORKERD_BIN` suite against the new binary.",
    "- `Rust tests` fetches the new `workerd.capnp` and checks the generated configs against it.",
    "",
    "Reproduce the lock locally:",
    "",
    "```sh",
    `gh release download ${tag} --pattern manifest.json --repo ${fork} --dir /tmp/pin --clobber`,
    `node scripts/update-workerd-lock.mjs --manifest /tmp/pin/manifest.json \\`,
    `  --upstream-version ${version} --check`,
    "```",
    "",
    "Merge only after this PR's CI is green. If this PR has no checks (the workflow opens it with",
    "`GITHUB_TOKEN`, which does not always trigger `pull_request` workflows), the pin workflow",
    "dispatches `ci.yml` on the branch instead:",
    "",
    "```sh",
    `gh workflow run ci.yml --ref chore/pin-workerd-${tag}`,
    "```",
    "",
  ].join("\n");
}

function parseArgs(argv) {
  const args = { lock: "deploy/docker/workerd.lock" };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--lock") args.lock = argv[++i];
    else if (arg.startsWith("--lock=")) args.lock = arg.slice("--lock=".length);
    else if (arg === "--tag") args.tag = argv[++i];
    else if (arg.startsWith("--tag=")) args.tag = arg.slice("--tag=".length);
    else if (arg === "--previous") args.previous = argv[++i];
    else if (arg.startsWith("--previous=")) args.previous = arg.slice("--previous=".length);
    else if (arg === "--upstream-version") args.upstreamVersion = argv[++i];
    else if (arg.startsWith("--upstream-version=")) {
      args.upstreamVersion = arg.slice("--upstream-version=".length);
    } else fail(`unknown argument: ${arg}`);
  }
  if (!args.tag) fail("--tag <vrt-tag> is required");
  if (!args.previous) fail("--previous <vrt-tag> is required");
  return args;
}

function main(argv) {
  const args = parseArgs(argv);
  const lock = JSON.parse(readFileSync(args.lock, "utf8"));
  process.stdout.write(
    renderBody({
      lock,
      tag: args.tag,
      previousVersion: args.previous,
      upstreamVersion: args.upstreamVersion,
    }),
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(`render-workerd-pin-pr-body: ${error.message}`);
    process.exit(1);
  }
}
