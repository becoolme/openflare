#!/usr/bin/env python3
"""Regenerate the compat-flag lists from the vendored
`compatibility-date.capnp` (workerd repo, src/workerd/io/). The vendored
snapshot is taken from the same release tag as `deploy/docker/workerd.lock`;
refresh it and the generated Rust list together when bumping workerd.

Outputs:
  - ../src/compat_flags.rs                  (vrt-config, always)
  - a TypeScript copy for a control plane, only with `--ts-out PATH`

Flags: `--check` compares in memory and writes nothing (exit 1 on drift);
`--ts-only` limits the run to the `--ts-out` file.
`--check-workerd PATH` additionally asks that pinned runtime to load a
config containing each generated stable and experimental flag.

Rule: a tenant `compatibilityFlags[]` entry must be a KNOWN STABLE flag.
Anything else — experimental (`$experimental` or a pinned-runtime
override), or simply unknown to the snapshot — is rejected (fail closed).
System/facade workers are exempt: the platform emits
`service_binding_extra_handlers` itself, and assemble derives
`--experimental` argv when required.
"""
import hashlib, json, pathlib, re, subprocess, sys, tempfile, time

SRC = pathlib.Path(__file__).parent / "compatibility-date.capnp"
LOCK = pathlib.Path(__file__).parent.parent.parent / "deploy" / "docker" / "workerd.lock"

# RV-R83 provenance. Keep the vendored source release equal to
# workerd.lock's `upstream.version`. This generator never downloads; a
# workerd bump requires a human to re-vendor
# src/workerd/io/compatibility-date.capnp from the pinned upstream commit
# (the lock records it as `upstream.commit`):
# https://raw.githubusercontent.com/cloudflare/workerd/<upstream.commit>/src/workerd/io/compatibility-date.capnp
# The fork release's own `workerd.capnp` asset is the config schema, NOT
# this list — vendoring it would drop every `$compatEnableFlag`.
SOURCE_WORKERD_VERSION = "1.20261123.1"
SOURCE_CAPNP_SHA256 = "d57805b521720436eb28797e55968e7a9a3dfda8558bc3247494b0e5ae91a785"

_lock = json.loads(LOCK.read_text())
# A fork pin names its own release (vrt-<date>-<sha>); the workerd version it is based on lives in
# `upstream.version`. Prefer it, and fall back to `version` for the npm pin.
_lock_version = (_lock.get("upstream") or {}).get("version") or _lock["version"]
_m = re.fullmatch(r"1\.(\d{4})(\d{2})(\d{2})\.\d+", _lock_version)
assert _m, (
    f"workerd.lock needs an upstream workerd version (1.YYYYMMDD.N), got {_lock_version!r}; "
    "fork pins must record upstream.version"
)
PINNED_WORKERD_VERSION = _lock_version
assert SOURCE_WORKERD_VERSION == PINNED_WORKERD_VERSION, (
    "re-vendor compatibility-date.capnp from the workerd.lock release tag and update "
    "SOURCE_WORKERD_VERSION"
)
_source_m = re.fullmatch(r"1\.(\d{4})(\d{2})(\d{2})\.\d+", SOURCE_WORKERD_VERSION)
assert _source_m, f"unparseable compat snapshot version {SOURCE_WORKERD_VERSION!r}"
SOURCE_WORKERD_DATE = f"{_source_m.group(1)}-{_source_m.group(2)}-{_source_m.group(3)}"
# Upper bound for tenant compat dates: the pinned release's own date —
# a later date names behaviour the pinned workerd does not implement.
MAX_COMPAT_DATE = f"{_m.group(1)}-{_m.group(2)}-{_m.group(3)}"
# These flags are recognized by workerd but cannot be loaded in the minimal
# no-code test fixture. Keep the allowlist narrow so other config failures
# (including an unsupported flag) fail the CI gate.
WORKERD_FLAG_CONFIG_PREREQUISITES = {
    "typescript_implemented_streams":
        "A worker cannot use typescript implemented streams without per-isolate JavaScript bootstrap enabled",
}
CAPNP_SHA256 = hashlib.sha256(SRC.read_bytes()).hexdigest()
assert CAPNP_SHA256 == SOURCE_CAPNP_SHA256, (
    "vendored compatibility-date.capnp differs from the pinned workerd source; "
    "re-vendor it from the matching release tag and update SOURCE_CAPNP_SHA256"
)
RUST_OUT = pathlib.Path(__file__).parent.parent / "src" / "compat_flags.rs"

def _ts_out():
    if "--ts-out" not in sys.argv:
        return None
    i = sys.argv.index("--ts-out")
    if i + 1 >= len(sys.argv) or sys.argv[i + 1].startswith("--"):
        sys.exit("--ts-out needs a path")
    return pathlib.Path(sys.argv[i + 1])


TS_OUT = _ts_out()

def _workerd_bin():
    if "--check-workerd" not in sys.argv:
        return None
    i = sys.argv.index("--check-workerd")
    if i + 1 >= len(sys.argv) or sys.argv[i + 1].startswith("--"):
        sys.exit("--check-workerd needs a workerd binary path")
    if "--check" not in sys.argv:
        sys.exit("--check-workerd requires --check")
    path = pathlib.Path(sys.argv[i + 1])
    if not path.is_file():
        sys.exit(f"workerd binary not found: {path}")
    return path.resolve()


WORKERD_BIN = _workerd_bin()

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
assert len(stable) == len(set(stable)), "duplicate stable compatibility flag"
assert len(experimental) == len(set(experimental)), "duplicate experimental compatibility flag"

stable.sort(); experimental.sort(); pairs.sort()
assert ('nodejs_compat', 'no_nodejs_compat') in pairs, 'parse drift: pairs'
assert enable_dates.get('nodejs_compat') == '2026-08-04', 'parse drift: enable dates'
enable_date_rows = sorted(enable_dates.items())
TS_ONLY = '--ts-only' in sys.argv  # Rust regen is queued separately
if TS_ONLY and TS_OUT is None:
    sys.exit("--ts-only needs --ts-out PATH")

def rs_list(name, items):
    rows = ",\n".join(f'    "{x}"' for x in items)
    return f"pub const {name}: &[&str] = &[\n{rows},\n];\n"


def rs_pair(a, b):
    line = f'    ("{a}", "{b}"),'
    return line if len(line) <= 68 else f'    (\n        "{a}",\n        "{b}",\n    ),'


def check_workerd_flags(binary):
    """Verify generated flag names with the exact workerd binary used by CI.

    Each flag gets its own Worker so mutually exclusive enable/disable pairs
    cannot mask valid names. A no-match test filter makes `workerd test` load
    the config without executing Worker code. Some valid flags require extra
    Worker config to instantiate; when workerd reports such a per-worker
    precondition, remove that worker and re-run so the remaining flags are
    still checked. Unknown compatibility flags always fail the check.
    """
    deadline = time.monotonic() + 900
    with tempfile.TemporaryDirectory(prefix="vrt-compat-flags-") as tmp:
        root = pathlib.Path(tmp)
        (root / "main.js").write_text(
            'export default { async fetch() { return new Response("ok"); } };\n'
        )

        def check_group(label, flags, experimental):
            if not flags:
                return
            pending = list(enumerate(flags))
            blocked = []
            while pending:
                workers = []
                for index, flag in pending:
                    workers.append(
                        "    (name = \"compat-{}-{:03d}\", worker = (\n"
                        "      compatibilityDate = \"2020-01-01\",\n"
                        "      compatibilityFlags = [{}],\n"
                        "      modules = [(name = \"main.js\", esModule = embed \"main.js\")],\n"
                        "    )),\n".format(label, index, json.dumps(flag))
                    )
                config = root / f"{label}.capnp"
                config.write_text(
                    'using Workerd = import "/workerd/workerd.capnp";\n'
                    "const config :Workerd.Config = (\n"
                    "  services = [\n"
                    + "".join(workers)
                    + "  ],\n);\n"
                )
                cmd = [str(binary), "test"]
                if experimental:
                    cmd.append("--experimental")
                cmd.extend([config.name, "__vrt_compat_flag_validation_no_tests__"])
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    sys.exit(f"timed out validating compatibility flags with {binary}")
                try:
                    result = subprocess.run(
                        cmd,
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=min(180, remaining),
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    sys.exit(f"timed out validating {label} compatibility flags with {binary}")
                output = result.stdout + result.stderr
                if "no such compatibility flag" in output.lower() or "No such flag" in output:
                    detail = "\n".join(output.splitlines()[:30])
                    sys.exit(
                        f"pinned workerd rejected generated {label} compatibility flags "
                        f"({binary}):\n{detail}"
                    )
                if "No tests found!" in output:
                    break
                blocker = re.search(rf"service compat-{label}-(\d+):", output)
                if blocker is None:
                    detail = "\n".join(output.splitlines()[:30]) or "(no workerd output)"
                    sys.exit(
                        f"pinned workerd failed to load generated {label} compatibility flags "
                        f"({binary}, exit {result.returncode}):\n{detail}"
                    )
                blocked_index = int(blocker.group(1))
                blocked_flag = next(
                    (flag for index, flag in pending if index == blocked_index), None
                )
                if blocked_flag is None:
                    sys.exit(f"workerd named unexpected {label} compatibility worker {blocked_index}")
                prerequisite = WORKERD_FLAG_CONFIG_PREREQUISITES.get(blocked_flag)
                if prerequisite is None or prerequisite not in output:
                    detail = "\n".join(output.splitlines()[:30]) or "(no workerd output)"
                    sys.exit(
                        f"pinned workerd failed to load generated {label} flag {blocked_flag!r} "
                        f"without a recognized fixture prerequisite ({binary}):\n{detail}"
                    )
                blocked.append(blocked_flag)
                pending = [(index, flag) for index, flag in pending if index != blocked_index]
            else:
                sys.exit(f"no {label} compatibility flags could be loaded by pinned workerd")
            if blocked:
                print(
                    f"NOTE: {label} flags recognized but need additional Worker config: "
                    + ", ".join(blocked)
                )

        check_group("stable", stable, experimental=False)
        check_group("experimental", experimental, experimental=True)
        print(
            f"OK: {binary} accepts generated compatibility flags "
            f"(stable={len(stable)} experimental={len(experimental)})"
        )

provenance_rs = (
    "//! Provenance (RV-R83): vendored capnp sha256 " + CAPNP_SHA256 + ";\n"
    + "//! snapshot from workerd v" + SOURCE_WORKERD_VERSION + "; pinned runtime is workerd "
    + PINNED_WORKERD_VERSION + " (deploy/docker/workerd.lock).\n"
    + "//! `gen_compat_flags.py --check` runs in CI and fails on any drift.\n"
)

rust_consts = (
    "\n/// workerd release pinned by `deploy/docker/workerd.lock`.\n"
    + f'pub const PINNED_WORKERD_VERSION: &str = "{PINNED_WORKERD_VERSION}";\n'
    + "/// Date of the workerd release tag the vendored capnp snapshot was taken from.\n"
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
    + "\n".join(rs_pair(a, b) for a, b in enable_date_rows)
    + "\n"
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
            match COMPAT_FLAG_ENABLE_DATES.binary_search_by_key(flag, |(name, _)| *name) {
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
// snapshot from workerd v""" + SOURCE_WORKERD_VERSION + """; pinned runtime is workerd """ + PINNED_WORKERD_VERSION + """
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

/** Date of the workerd release tag the vendored capnp snapshot was taken from. */
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
outputs = [] if TS_ONLY else [(RUST_OUT, rust_text)]
if TS_OUT is not None:
    outputs.append((TS_OUT, ts_text))
if CHECK:
    # RV-R83: drift gate for CI — regenerate in memory, byte-compare, write
    # nothing. Exit 1 names every stale output.
    stale = [str(path) for path, text in outputs if not path.exists() or path.read_text() != text]
    if stale:
        for path in stale:
            print(f"DRIFT: {path} differs from generator output; rerun gen_compat_flags.py", file=sys.stderr)
        sys.exit(1)
    print(f"OK: compat flag outputs match generator (stable={len(stable)} experimental={len(experimental)})")
    if WORKERD_BIN is not None:
        check_workerd_flags(WORKERD_BIN)
    sys.exit(0)
for path, text in outputs:
    path.write_text(text)
print(f"stable={len(stable)} experimental={len(experimental)} — wrote " + " and ".join(str(p) for p, _ in outputs))
