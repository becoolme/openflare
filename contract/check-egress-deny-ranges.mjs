// Drift gate for `egress-deny-ranges.json`: rebuilds the contract
// document from the PUBLISHED worker tables (facade/dist/.../ip.js)
// and compares it byte-for-byte with the committed fixture. The Rust
// side renders the same document from `egress::{IPV4,IPV6}_DENY_RANGES`
// (tests/egress.rs), so any divergence between the two tables fails one
// of the two gates. `--check` is the default mode; `--write` refreshes.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_PATH = path.join(HERE, "egress-deny-ranges.json");
const IP_MODULE = path.join(
  HERE,
  "../facade/dist/facade/egress/ip.js",
);

const { IPV4_DENY, IPV6_DENY, IPV6_EMBEDDED, parseIpv4, parseIpv6 } =
  await import(IP_MODULE);

function v4Text(addr) {
  return [addr >>> 24, (addr >>> 16) & 0xff, (addr >>> 8) & 0xff, addr & 0xff].join(".");
}

// Render a table row back to canonical CIDR text so the fixture's
// `cidr` strings are re-derived from the parsed table — not hand-copied.
function v4Cidr(base, bits) {
  return `${v4Text(base)}/${bits}`;
}
function v6Cidr(base, bits) {
  // Re-expand the base as eight uncompressed groups.
  const groups = [];
  for (let i = 0; i < 8; i++) {
    groups.push(((base >> BigInt((7 - i) * 16)) & 0xffffn).toString(16));
  }
  return `[${groups.join(":")}]/${bits}`; // bracket-tagged; normalized below
}
// Canonical RFC5952 text for a base address: longest zero run (>=2)
// collapses to "::"; the mapped form renders its low 32 bits as dotted
// IPv4 (`::ffff:a.b.c.d`) to match Rust's IpAddr::fmt exactly.
function v6CidrCanonical(base, bits) {
  const groups = [];
  for (let i = 0; i < 8; i++) {
    groups.push(((base >> BigInt((7 - i) * 16)) & 0xffffn).toString(16));
  }
  let text;
  if (groups.slice(0, 5).every((g) => g === "0") && groups[5] === "ffff") {
    const v4 = Number(base & 0xffffffffn);
    text = `::ffff:${v4Text(v4)}`;
  } else {
    let best = -1, bestLen = 0;
    for (let i = 0; i < 8; i++) {
      if (groups[i] !== "0") continue;
      let j = i;
      while (j < 8 && groups[j] === "0") j++;
      if (j - i > bestLen) { best = i; bestLen = j - i; }
      i = j;
    }
    if (bestLen < 2) {
      text = groups.join(":");
    } else {
      const left = groups.slice(0, best).join(":");
      const right = groups.slice(best + bestLen).join(":");
      if (left === "" && right === "") text = "::";
      else if (left === "") text = `::${right}`;
      else if (right === "") text = `${left}::`;
      else text = `${left}::${right}`;
    }
  }
  return `${text}/${bits}`;
}

function rows(table, v6) {
  return table.map(([base, bits, code]) => ({
    cidr: v6 ? v6CidrCanonical(base, bits) : v4Cidr(base, bits),
    code,
  }));
}

const doc = {
  embeddedPrefixes: IPV6_EMBEDDED.map(([base, bits]) => {
    const t = v6CidrCanonical(base, bits);
    return t;
  }),
  generatedBy: "vrt-config egress.rs deny-range tables (single source of truth)",
  ipv4: rows(IPV4_DENY, false),
  ipv6: rows(IPV6_DENY, true),
  schemaVersion: 1,
};

const rendered = JSON.stringify(doc, null, 2) + "\n";

if (process.argv.includes("--write")) {
  writeFileSync(FIXTURE_PATH, rendered);
  console.log(`wrote ${FIXTURE_PATH}`);
  process.exit(0);
}
const golden = readFileSync(FIXTURE_PATH, "utf8");
if (golden !== rendered) {
  console.error("contract fixture drifted: egress-deny-ranges.json != dist ip.js tables");
  console.error("regenerate via UPDATE_GOLDEN=1 cargo test -p vrt-config (Rust is source of truth)");
  process.exit(1);
}
console.log("contract fixture matches dist ip.js tables");

