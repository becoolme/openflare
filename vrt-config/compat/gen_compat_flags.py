#!/usr/bin/env python3
"""Regenerate the compat-flag lists from the vendored
`compatibility-date.capnp` (workerd repo, src/workerd/io/). The vendored
snapshot corresponds to upstream `main` pulled 2026-11-09 (workerd
binary 2026-07-22); refresh both together when bumping workerd.

Outputs:
  - ../src/compat_flags.rs                  (vrt-config)
  - ../../../../../server/src/services/workerProjectConfig/compatFlags.ts

Rule: a tenant `compatibilityFlags[]` entry must be a KNOWN STABLE flag.
Anything else — experimental (`$experimental`), or simply unknown to the
snapshot — is rejected (fail closed). System/facade workers are exempt:
the platform emits `service_binding_extra_handlers` itself, and assemble
derives `--experimental` argv when required.
"""
import hashlib, json, re, pathlib, sys

SRC = pathlib.Path(__file__).parent / "compatibility-date.capnp"
LOCK = pathlib.Path(__file__).parent.parent.parent / "deploy" / "docker" / "workerd.lock"

# RV-R83 provenance. The vendored snapshot was taken from upstream `main`
# on 2026-11-09 and matches the workerd binary built 2026-07-22 — this is
# NOT the pinned runtime in deploy/docker/workerd.lock (1.20260916.1 =
# 2026-09-16). Flags added upstream after the pinned release may therefore
# be accepted by the control plane yet unknown to the pinned workerd.
# Re-vendor compatibility-date.capnp at the pinned release tag (a human
# step; this generator never downloads), set these two constants to the
# tag's date, regenerate, and the known-difference test goes green.
SNAPSHOT_PULLED = "2026-11-09"
SOURCE_WORKERD_DATE = "2026-07-22"

_lock_version = json.loads(LOCK.read_text())["version"]
_m = re.fullmatch(r"1\.(\d{4})(\d{2})(\d{2})\.\d+", _lock_version)
assert _m, f"unparseable workerd.lock version {_lock_version!r}"
PINNED_WORKERD_VERSION = _lock_version
# Upper bound for tenant compat dates: the pinned release's own date —
# a later date names behaviour the pinned workerd does not implement.
MAX_COMPAT_DATE = f"{_m.group(1)}-{_m.group(2)}-{_m.group(3)}"
CAPNP_SHA256 = hashlib.sha256(SRC.read_bytes()).hexdigest()
RUST_OUT = pathlib.Path(__file__).parent.parent / "src" / "compat_flags.rs"
TS_OUT = (
    pathlib.Path(__file__).parent.parent.parent.parent.parent
    / "src/services/workerProjectConfig/compatFlags.ts"
)

lines = SRC.read_text().splitlines()
stable, experimental = [], []
pairs = []  # (enable, disable) of ONE capnp field — mutually exclusive
enable_dates = {}  # enable flag -> date the behaviour becomes the default
i = 0
while i < len(lines):
    if re.match(r"\s*\w+\s+@\d+\s*:", lines[i]):
        name = dname = None
        exp = False
        edate = None
        for j in range(i + 1, min(i + 8, len(lines))):
            l = lines[j]
            if re.match(r"\s*\w+\s+@\d+\s*:", l):
                break
            e = re.search(r'\$compatEnableFlag\("([^"]+)"\)', l)
            d = re.search(r'\$compatDisableFlag\("([^"]+)"\)', l)
            ed = re.search(r'\$compatEnableDate\("([^"]+)"\)', l)
            if e: name = e.group(1)
            if d: dname = d.group(1)
            if ed: edate = ed.group(1)
            if "$experimental" in l: exp = True
        if name and dname:
            pairs.append((name, dname))
        for n in (name, dname):
            if n:
                (experimental if exp else stable).append(n)
        # MIG-1: only the ENABLE flag carries the default-date — past it,
        # declaring the flag is a redundant no-op workerd warns about
        # (the emit layer strips such declarations; see
        # `flags_for_compat_date`). A disable flag stays meaningful.
        if name and edate:
            enable_dates[name] = edate
    i += 1

assert "service_binding_extra_handlers" in experimental, "parse drift"
assert "nodejs_compat" in stable, "parse drift"
assert len(stable) > 250 and len(experimental) > 30, "unexpected drift"

stable.sort(); experimental.sort(); pairs.sort()
assert ('nodejs_compat', 'no_nodejs_compat') in pairs, 'parse drift: pairs'
assert enable_dates.get('nodejs_compat') == '2026-08-04', 'parse drift: enable dates'
enable_date_rows = sorted(enable_dates.items())
TS_ONLY = '--ts-only' in sys.argv  # Rust regen is queued separately

def rs_list(name, items):
    rows = ",\n".join(f'    "{x}"' for x in items)
    return f"pub const {name}: &[&str] = &[\n{rows},\n];\n"

provenance_rs = (
    "//! Provenance (RV-R83): vendored capnp sha256 " + CAPNP_SHA256 + ";\n"
    + "//! snapshot pulled from upstream main on " + SNAPSHOT_PULLED + ", workerd binary dated\n"
    + "//! " + SOURCE_WORKERD_DATE + "; pinned runtime is workerd " + PINNED_WORKERD_VERSION
    + " (deploy/docker/workerd.lock).\n"
    + "//! `gen_compat_flags.py --check` runs in CI and fails on any drift.\n"
)

rust_consts = (
    "\n/// workerd release pinned by `deploy/docker/workerd.lock`.\n"
    + f'pub const PINNED_WORKERD_VERSION: &str = "{PINNED_WORKERD_VERSION}";\n'
    + "/// Date of the workerd build the vendored capnp snapshot was taken from.\n"
    + "/// Differs from the pinned release until the snapshot is re-vendored.\n"
    + f'pub const SNAPSHOT_WORKERD_DATE: &str = "{SOURCE_WORKERD_DATE}";\n'
    + "/// Latest `compatibility_date` the pinned workerd implements (the date in\n"
    + "/// its release version). Later dates must be rejected.\n"
    + f'pub const WORKERD_MAX_COMPAT_DATE: &str = "{MAX_COMPAT_DATE}";\n'
)

rust_text = (
    """//! GENERATED by `compat/gen_compat_flags.py` from the vendored
//! `compat/compatibility-date.capnp` — do not edit by hand.
"""
    + provenance_rs
    + """//!
//! Tenant `compatibilityFlags[]` are validated against
//! [`STABLE_COMPAT_FLAGS`] (emit layer, defense in depth behind the
//! control-plane zod rule). [`EXPERIMENTAL_COMPAT_FLAGS`] additionally
//! drives `--experimental` argv derivation in vrt-host assemble.

"""
    + rs_list("STABLE_COMPAT_FLAGS", stable)
    + "\n"
    + rs_list("EXPERIMENTAL_COMPAT_FLAGS", experimental)
    + "\n/// MIG-1: `(enable flag, date it becomes the default)` — past the\n"
    + "/// date a declaration is a redundant no-op that makes workerd print a\n"
    + "/// warning line (a non-benign verdict under `check`). The emit layer\n"
    + "/// strips such declarations via [`flags_for_compat_date`]; DISABLE flags\n"
    + "/// and opt-in flags are untouched. Generated from `$compatEnableDate`.\n"
    + "pub const COMPAT_FLAG_ENABLE_DATES: &[(&str, &str)] = &[\n"
    + "".join(f'    ("{a}", "{b}"),\n' for a, b in enable_date_rows)
    + "];\n"
    + rust_consts
    + """
/// Tenant-declarable flag: known to the vendored snapshot AND not
/// marked `$experimental`. Unknown names are rejected (fail closed —
/// a stale snapshot never silently admits a new experimental flag).
pub fn is_tenant_allowed_compat_flag(name: &str) -> bool {
    STABLE_COMPAT_FLAGS.binary_search(&name).is_ok()
}

/// `$experimental`-annotated flag — system/facade workers may carry
/// these; they force `--experimental` in the workerd argv.
pub fn is_experimental_compat_flag(name: &str) -> bool {
    EXPERIMENTAL_COMPAT_FLAGS.binary_search(&name).is_ok()
}

/// MIG-1: drop flags the given `compatibility_date` already defaults ON.
/// workerd answers an explicit re-declaration with a "flag is redundant"
/// warning that `workerd test` surfaces and `check` (correctly) treats
/// as non-benign — refusing the deployment over a warning that carries
/// zero signal. Stripping at render time keeps the declared semantics
/// identical (the flag is on either way) while producing no warning.
/// Boundary is INCLUSIVE of the enable date (ISO dates compare
/// lexicographically); unknown and disable flags pass through.
pub fn flags_for_compat_date<'a, I>(flags: I, compat_date: &str) -> Vec<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    flags
        .into_iter()
        .filter(|flag| {
            match COMPAT_FLAG_ENABLE_DATES
                .binary_search_by_key(flag, |(name, _)| *name)
            {
                Ok(idx) => compat_date < COMPAT_FLAG_ENABLE_DATES[idx].1,
                Err(_) => true,
            }
        })
        .collect()
}
""")

def ts_pair(a, b):
    line = f"  ['{a}', '{b}'],"
    # prettier (printWidth 100) wraps a pair that does not fit on one line.
    return line if len(line) <= 100 else f"  [\n    '{a}',\n    '{b}',\n  ],"

ts_text = (
    """// GENERATED by vrt-config/compat/gen_compat_flags.py
// from the vendored compatibility-date.capnp — do not edit by hand.
// CI runs `gen_compat_flags.py --check`; any drift fails the build.
//
// Provenance (RV-R83): vendored capnp sha256 """ + CAPNP_SHA256 + """;
// snapshot pulled from upstream main on """ + SNAPSHOT_PULLED + """, workerd binary dated
// """ + SOURCE_WORKERD_DATE + """; pinned runtime is workerd """ + PINNED_WORKERD_VERSION + """
// (deploy/docker/workerd.lock).
//
// Tenant `compatibilityFlags[]` must be a KNOWN STABLE flag. Experimental
// (`$experimental`) and unknown names are both rejected (fail closed): with
// `--experimental` on argv (consumer generations) an experimental flag a
// tenant slipped in would actually apply — the control plane must stop it.
const stable: ReadonlySet<string> = new Set([
"""
    + "\n".join(f"  '{x}'," for x in stable)
    + """
])

const experimental: ReadonlySet<string> = new Set([
"""
    + "\n".join(f"  '{x}'," for x in experimental)
    + """
])

// RV-R137: [enable, disable] flag pairs of ONE capnp field. Declaring both
// members of a pair is a contradiction (workerd refuses to start the worker,
// which would fail the whole cell preflight) — rejected at the control plane.
export const COMPAT_FLAG_CONFLICT_PAIRS: readonly (readonly [string, string])[] = [
"""
    + "\n".join(ts_pair(a, b) for a, b in pairs)
    + """
]

/** workerd release pinned by deploy/docker/workerd.lock. */
export const PINNED_WORKERD_VERSION = '""" + PINNED_WORKERD_VERSION + """'

/** Date of the workerd build the vendored capnp snapshot was taken from
 * (differs from the pinned release until the snapshot is re-vendored). */
export const SNAPSHOT_WORKERD_DATE = '""" + SOURCE_WORKERD_DATE + """'

/** Latest compatibility_date the pinned workerd implements; later dates are
 * rejected at the control plane (the Rust twin is WORKERD_MAX_COMPAT_DATE). */
export const WORKERD_MAX_COMPAT_DATE = '""" + MAX_COMPAT_DATE + """'

export function isTenantAllowedCompatFlag(name: string): boolean {
  return stable.has(name)
}

export function isExperimentalCompatFlag(name: string): boolean {
  return experimental.has(name)
}
""")

CHECK = "--check" in sys.argv
outputs = [(TS_OUT, ts_text)] if TS_ONLY else [(RUST_OUT, rust_text), (TS_OUT, ts_text)]
if CHECK:
    # RV-R83: drift gate for CI — regenerate in memory, byte-compare, write
    # nothing. Exit 1 names every stale output.
    stale = [str(path) for path, text in outputs if not path.exists() or path.read_text() != text]
    if stale:
        for path in stale:
            print(f"DRIFT: {path} differs from generator output; rerun gen_compat_flags.py", file=sys.stderr)
        sys.exit(1)
    print(f"OK: compat flag outputs match generator (stable={len(stable)} experimental={len(experimental)})")
    sys.exit(0)
for path, text in outputs:
    path.write_text(text)
print(f"stable={len(stable)} experimental={len(experimental)} — wrote " + " and ".join(str(p) for p, _ in outputs))
