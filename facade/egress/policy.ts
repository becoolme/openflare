// Egress policy shape + decision engine for the vrt-egress worker (T6).
//
// The policy document is a typed JSON structure injected by the platform
// (vrt-config renders it into the `VRT_EGRESS_POLICY` text binding; the
// shape is mirrored by `vrt_config::egress::EgressPolicy`). Anything
// missing, malformed, or carrying unexpected fields fails closed — the
// result is a deny-all policy, never a permit.
//
// URL handling is deliberately delegated to WHATWG `new URL()`: every
// exotic IPv4 spelling (integer, octal, hex, short forms, trailing dot)
// and IDN/punycode form is canonicalized by the URL parser before this
// policy sees it, so the checks below only ever run on normalized
// hostnames. Reimplementing lenient parsing here would be a bug source.
import {
  IPV6_DENY,
  IPV6_EMBEDDED,
  ipv4DenyCode,
  ipv6DenyCode,
  parseIpv4,
  parseIpv6,
  type IpDenyCode,
} from "./ip.js";

export interface EgressPolicy {
  /** Effective ports (after scheme defaulting) allowed without a whitelist entry. */
  allowedPorts: ReadonlySet<number>;
  /** Whitelisted (host, port) pairs — the only path to internal endpoints. */
  allow: ReadonlyArray<{ key: string; port: number }>;
  /** Operator-added name denies; `*.suffix` = label-boundary suffix. */
  denyNames: ReadonlyArray<{ exact: string } | { suffix: string }>;
  requestTimeoutMs: number;
  /**
   * MIG-14: raw-TCP `connect()` ports allowed to public space. An EMPTY
   * set preserves the pre-MIG-14 posture — every connect() is denied.
   * `TCP_NEVER_PORTS` (mail) can never appear: parse rejects the whole
   * policy rather than silently dropping them.
   */
  tcpAllowedPorts: ReadonlySet<number>;
  /**
   * MIG-14/MIG-15 reserved: platform-injected exact `{host,port}`
   * internal exceptions — bypass the TCP port list AND all name/IP
   * checks, including private/CGNAT/ULA literals (reaching in-cluster
   * space is their purpose). Never `TCP_NEVER_PORTS`, never the
   * builtin-denied names, never trivially-local literals (this-net,
   * loopback, link-local, multicast, unspecified — either family or
   * embedded unwrap). Only platform Rust code can produce a non-empty
   * list: the field is unrepresentable in the operator params file and
   * in every tenant surface.
   */
  internalTcpAllow: ReadonlyArray<{ key: string; port: number }>;
  /**
   * MIG-15: typed "internal TCP target provider" routes. An exact
   * `{host,port}` match is relayed through the INTERNAL_NET service
   * binding (loopback-only network) to `upstream{Host,Port}` — the
   * provider-side proxy. The first host label is the platform-minted
   * binding id; the egress worker injects it as a `VRT-PG1 <id>`
   * preamble so tenants can neither select another binding's identity
   * nor reach the proxy with an unvouched id.
   */
  internalTcpTargets: ReadonlyArray<{
    key: string;
    port: number;
    upstreamHost: string;
    upstreamPort: number;
    /** MIG-15 r2: preamble-protocol selector — the egress maps `kind`
     * to a preamble encoder; unknown values fail closed at parse. */
    kind: "pg-v1";
  }>;
  /** MIG-14: upstream TCP `connect()` establishment bound (ms). */
  tcpConnectTimeoutMs: number;
  /** MIG-14: bidirectional inactivity bound before teardown (ms). */
  tcpIdleTimeoutMs: number;
  /**
   * MIG-14: concurrent TCP sessions per egress isolate — the CONNECT
   * event carries no tenant identity, so the isolate is the enforceable
   * granularity.
   */
  tcpMaxConcurrent: number;
}

export type DenyCode =
  | "config"
  | "url"
  | "scheme"
  | "userinfo"
  | "port"
  | "unqualified"
  | "denied_name"
  | "concurrent_limit"
  | "connect_timeout"
  | "idle_timeout"
  | IpDenyCode;

export type Decision =
  | { allow: true }
  | { allow: false; code: DenyCode; detail?: string };

/** MIG-14: parsed `host:port` CONNECT authority. */
export interface TcpTarget {
  /** WHATWG hostname exactly as parsed — v6 stays bracketed. */
  host: string;
  /** Dial form: identical to `host` except v6 brackets are stripped. */
  dialHost: string;
  port: number;
  /** hostKey() of the canonical host, for exception matching. */
  key: string | null;
}

export type TcpDecision =
  | {
      allow: true;
      target: TcpTarget;
      internal?: { upstreamHost: string; upstreamPort: number };
    }
  | { allow: false; code: DenyCode; detail?: string; port: number | null };

// Built-in denied names — cannot be removed by configuration; operators
// can only append through `denyNames`.
const DENY_EXACT: ReadonlyArray<string> = [
  "localhost",
  "metadata.google.internal",
];
const DENY_SUFFIX: ReadonlyArray<string> = [
  ".localhost",
  ".local",
  ".internal",
  ".svc",
  ".cluster.local",
];

/**
 * Whitelist entries may never point at the builtin-denied core: the two
 * exact names, or an IP literal already denied by the range tables.
 * Rationale: a private/loopback literal is unreachable under layers 1–2
 * anyway, so such an entry only makes sense under a broken layer 1 —
 * where it would punch a loopback hole (loopback escapes egress
 * NetworkPolicy entirely). Rejecting it at parse time keeps the whole
 * policy invalid → deny all (fail closed). Suffix-denied NAMES like
 * `api.vibeploy.internal` stay whiteltable — that explicit escape is
 * the documented purpose of `allow`.
 */
function builtinDeniedKey(key: string): boolean {
  if (key.startsWith("ip4:")) {
    return ipv4DenyCode(Number(key.slice(4))) !== null;
  }
  if (key.startsWith("ip6:")) {
    return ipv6DenyCode(BigInt(`0x${key.slice(4)}`)) !== null;
  }
  const name = key.slice(5);
  return DENY_EXACT.includes(name);
}

// MIG-14: internal-exception entries may point at private/CGNAT/ULA/
// reserved literals — that is their purpose — but never at the
// trivially-local subset (this-net, loopback, link-local/metadata,
// multicast, unspecified). Embedded-IPv4 unwraps classify the inner
// v4 by the same subset, so `[::ffff:127.0.0.1]` is no loophole.
const INTERNAL_NEVER_V4: ReadonlySet<IpDenyCode> = new Set([
  "ip_this_net",
  "ip_loopback",
  "ip_link_local",
  "ip_multicast",
]);
const INTERNAL_NEVER_V6: ReadonlySet<IpDenyCode> = new Set([
  "ip_unspecified",
  "ip_loopback",
  "ip_link_local",
  "ip_multicast",
]);

function inV6(addr: bigint, base: bigint, bits: number): boolean {
  const shift = 128n - BigInt(bits);
  return addr >> shift === base >> shift;
}

function internalDeniedKey(key: string): boolean {
  if (key.startsWith("ip4:")) {
    const code = ipv4DenyCode(Number(key.slice(4)));
    return code !== null && INTERNAL_NEVER_V4.has(code);
  }
  if (key.startsWith("ip6:")) {
    const addr = BigInt(`0x${key.slice(4)}`);
    if (addr === 0n || addr === 1n) return true; // ::, ::1
    for (const [base, bits, code] of IPV6_DENY) {
      if (inV6(addr, base, bits)) return INTERNAL_NEVER_V6.has(code);
    }
    for (const [base, bits] of IPV6_EMBEDDED) {
      if (inV6(addr, base, bits)) {
        const code = ipv4DenyCode(Number(addr & 0xffffffffn));
        return code !== null && INTERNAL_NEVER_V4.has(code);
      }
    }
    return false;
  }
  return DENY_EXACT.includes(key.slice(5));
}

const MAX_ALLOWED_PORTS = 32;
const MAX_ALLOW_ENTRIES = 64;
const MAX_DENY_NAMES = 64;
const MIN_TIMEOUT_MS = 1_000;
const MAX_TIMEOUT_MS = 120_000;
const MAX_HOST_LEN = 253;
// MIG-14 bounds — byte-mirrors of the vrt-config egress constants.
export const TCP_NEVER_PORTS: ReadonlySet<number> = new Set([25, 465, 587]);
const MAX_TCP_ALLOWED_PORTS = MAX_ALLOWED_PORTS;
const MAX_INTERNAL_TCP_ALLOW = MAX_ALLOW_ENTRIES;
const MAX_TCP_IDLE_TIMEOUT_MS = 600_000;
const MAX_TCP_MAX_CONCURRENT = 1024;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function stripTrailingDots(host: string): string {
  let end = host.length;
  while (end > 0 && host.charCodeAt(end - 1) === 0x2e) end -= 1;
  return host.slice(0, end);
}

// Loose-but-bounded shape for operator-configured names; the authoritative
// canonicalization is always the URL parser on the request side.
const NAME_RE = /^[a-z0-9.-]+$/i;
function nameShape(name: string): boolean {
  return (
    name.length > 0 &&
    name.length <= MAX_HOST_LEN &&
    NAME_RE.test(name) &&
    name.split(".").every((label) => label.length > 0)
  );
}

/** Canonical match key: `ip4:<u32>` | `ip6:<hex>` | `name:<stripped>`. */
export function hostKey(hostname: string): string | null {
  if (hostname.startsWith("[")) {
    const v6 = parseIpv6(hostname);
    return v6 === null ? null : `ip6:${v6.toString(16)}`;
  }
  const v4 = parseIpv4(hostname);
  if (v4 !== null) return `ip4:${v4}`;
  const name = stripTrailingDots(hostname);
  if (!nameShape(name)) return null;
  return `name:${name.toLowerCase()}`;
}

/** Canonicalize an operator-supplied host through the same URL parser. */
function policyHostKey(raw: unknown): string | null {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > MAX_HOST_LEN) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(`http://${raw}/`);
  } catch {
    return null;
  }
  if (
    url.username !== "" ||
    url.password !== "" ||
    url.port !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    return null;
  }
  return hostKey(url.hostname);
}

function parsePorts(raw: unknown): ReadonlySet<number> | null {
  if (!Array.isArray(raw) || raw.length === 0 || raw.length > MAX_ALLOWED_PORTS) {
    return null;
  }
  for (const port of raw) {
    if (!Number.isInteger(port) || (port as number) < 1 || (port as number) > 65535) {
      return null;
    }
  }
  return new Set(raw as number[]);
}

function parseAllow(raw: unknown): EgressPolicy["allow"] | null {
  if (!Array.isArray(raw) || raw.length > MAX_ALLOW_ENTRIES) return null;
  const entries: Array<{ key: string; port: number }> = [];
  for (const item of raw) {
    if (!isRecord(item)) return null;
    const keys = Object.keys(item).sort();
    if (keys.length !== 2 || keys[0] !== "host" || keys[1] !== "port") return null;
    const port = item.port;
    if (!Number.isInteger(port) || (port as number) < 1 || (port as number) > 65535) {
      return null;
    }
    const key = policyHostKey(item.host);
    if (key === null || builtinDeniedKey(key)) return null;
    entries.push({ key, port: port as number });
  }
  return entries;
}

/** MIG-14: like parseAllow, but mail ports are rejected outright and
 *  the never-internal literal subset replaces the full deny tables. */
/** MIG-15: internal TCP target routes — platform-injected entries.
 * `kind` selects the preamble protocol (`pg-v1` — identity line
 * `VRT-PG1 <id>`); unknown kinds fail closed. `upstreamHost` is
 * restricted to a loopback literal: the internal
 * network is loopback-only and any other value would be an operator
 * mistake (fail closed at parse, never silently routed). */
function parseInternalTcpTargets(
  raw: unknown,
): EgressPolicy["internalTcpTargets"] | null {
  // Absent key = no internal targets (older renders predating MIG-15
  // must still parse — rolling-upgrade compat); present-but-malformed
  // fails closed.
  if (raw === undefined) return [];
  if (!Array.isArray(raw) || raw.length > MAX_INTERNAL_TCP_ALLOW) return null;
  const entries: EgressPolicy["internalTcpTargets"][number][] = [];
  for (const item of raw) {
    if (!isRecord(item)) return null;
    const keys = Object.keys(item).sort();
    if (
      keys.length !== 5 ||
      keys[0] !== "host" ||
      keys[1] !== "kind" ||
      keys[2] !== "port" ||
      keys[3] !== "upstreamHost" ||
      keys[4] !== "upstreamPort"
    ) {
      return null;
    }
    // Typed preamble-protocol selector — unknown kinds fail closed so
    // the egress worker never relays to an undecodable target.
    if (item.kind !== "pg-v1") return null;
    const port = item.port;
    const upstreamPort = item.upstreamPort;
    if (
      !Number.isInteger(port) ||
      (port as number) < 1 ||
      (port as number) > 65535 ||
      !Number.isInteger(upstreamPort) ||
      (upstreamPort as number) < 1 ||
      (upstreamPort as number) > 65535
    ) {
      return null;
    }
    const key = policyHostKey(item.host);
    if (key === null || builtinDeniedKey(key)) return null;
    const upKey = policyHostKey(item.upstreamHost);
    if (
      item.upstreamHost !== "127.0.0.1" &&
      item.upstreamHost !== "[::1]" &&
      upKey === null
    ) {
      return null;
    }
    entries.push({
      key,
      port: port as number,
      upstreamHost: item.upstreamHost as string,
      upstreamPort: upstreamPort as number,
      kind: item.kind as "pg-v1",
    });
  }
  return entries;
}

function parseInternalTcpAllow(raw: unknown): EgressPolicy["internalTcpAllow"] | null {
  if (!Array.isArray(raw) || raw.length > MAX_INTERNAL_TCP_ALLOW) return null;
  const entries: Array<{ key: string; port: number }> = [];
  for (const item of raw) {
    if (!isRecord(item)) return null;
    const keys = Object.keys(item).sort();
    if (keys.length !== 2 || keys[0] !== "host" || keys[1] !== "port") return null;
    const port = item.port;
    if (
      !Number.isInteger(port) ||
      (port as number) < 1 ||
      (port as number) > 65535 ||
      TCP_NEVER_PORTS.has(port as number)
    ) {
      return null;
    }
    const key = policyHostKey(item.host);
    if (key === null || internalDeniedKey(key)) return null;
    entries.push({ key, port: port as number });
  }
  return entries;
}

/** MIG-14: tcpAllowedPorts — like parsePorts but EMPTY is legal
 *  (deny all) and mail ports invalidate the whole policy. */
function parseTcpPorts(raw: unknown): ReadonlySet<number> | null {
  if (!Array.isArray(raw) || raw.length > MAX_TCP_ALLOWED_PORTS) return null;
  for (const port of raw) {
    if (
      !Number.isInteger(port) ||
      (port as number) < 1 ||
      (port as number) > 65535 ||
      TCP_NEVER_PORTS.has(port as number)
    ) {
      return null;
    }
  }
  return new Set(raw as number[]);
}

function parseDenyNames(raw: unknown): EgressPolicy["denyNames"] | null {
  if (!Array.isArray(raw) || raw.length > MAX_DENY_NAMES) return null;
  const rules: Array<{ exact: string } | { suffix: string }> = [];
  for (const item of raw) {
    if (typeof item !== "string") return null;
    if (item.startsWith("*.")) {
      const suffix = stripTrailingDots(item.slice(2)).toLowerCase();
      if (!nameShape(suffix)) return null;
      rules.push({ suffix });
    } else {
      const exact = stripTrailingDots(item).toLowerCase();
      if (!nameShape(exact)) return null;
      rules.push({ exact });
    }
  }
  return rules;
}

/**
 * Strictly parse the injected policy document (JSON string or already
 * decoded object). Any deviation — missing/extra keys, wrong types,
 * out-of-range values — returns null (deny everything).
 */
export function parseEgressPolicy(raw: unknown): EgressPolicy | null {
  let doc = raw;
  if (typeof doc === "string") {
    try {
      doc = JSON.parse(doc);
    } catch {
      return null;
    }
  }
  if (!isRecord(doc)) return null;
  const expected = new Set([
    "allowedPorts",
    "allow",
    "denyNames",
    "internalTcpAllow",
    "internalTcpTargets",
    "requestTimeoutMs",
    "tcpAllowedPorts",
    "tcpConnectTimeoutMs",
    "tcpIdleTimeoutMs",
    "tcpMaxConcurrent",
  ]);
  const keys = Object.keys(doc);
  // Exact-key-set check, EXCEPT `internalTcpTargets` is optional —
  // renders predating MIG-15 lack it and must still parse (a tenant's
  // last rendered config keeps serving until its next rollout).
  if (
    keys.some((k) => !expected.has(k)) ||
    [...expected].some((k) => k !== "internalTcpTargets" && !keys.includes(k))
  ) {
    return null;
  }
  const allowedPorts = parsePorts(doc.allowedPorts);
  if (allowedPorts === null) return null;
  const allow = parseAllow(doc.allow);
  if (allow === null) return null;
  const denyNames = parseDenyNames(doc.denyNames);
  if (denyNames === null) return null;
  const internalTcpAllow = parseInternalTcpAllow(doc.internalTcpAllow);
  if (internalTcpAllow === null) return null;
  const internalTcpTargets = parseInternalTcpTargets(doc.internalTcpTargets);
  if (internalTcpTargets === null) return null;
  const tcpAllowedPorts = parseTcpPorts(doc.tcpAllowedPorts);
  if (tcpAllowedPorts === null) return null;
  const timeout = doc.requestTimeoutMs;
  if (
    !Number.isInteger(timeout) ||
    (timeout as number) < MIN_TIMEOUT_MS ||
    (timeout as number) > MAX_TIMEOUT_MS
  ) {
    return null;
  }
  const tcpConnectTimeoutMs = doc.tcpConnectTimeoutMs;
  if (
    !Number.isInteger(tcpConnectTimeoutMs) ||
    (tcpConnectTimeoutMs as number) < MIN_TIMEOUT_MS ||
    (tcpConnectTimeoutMs as number) > MAX_TIMEOUT_MS
  ) {
    return null;
  }
  const tcpIdleTimeoutMs = doc.tcpIdleTimeoutMs;
  if (
    !Number.isInteger(tcpIdleTimeoutMs) ||
    (tcpIdleTimeoutMs as number) < MIN_TIMEOUT_MS ||
    (tcpIdleTimeoutMs as number) > MAX_TCP_IDLE_TIMEOUT_MS
  ) {
    return null;
  }
  const tcpMaxConcurrent = doc.tcpMaxConcurrent;
  if (
    !Number.isInteger(tcpMaxConcurrent) ||
    (tcpMaxConcurrent as number) < 1 ||
    (tcpMaxConcurrent as number) > MAX_TCP_MAX_CONCURRENT
  ) {
    return null;
  }
  return {
    allowedPorts,
    allow,
    denyNames,
    requestTimeoutMs: timeout as number,
    tcpAllowedPorts,
    internalTcpAllow,
    internalTcpTargets,
    tcpConnectTimeoutMs: tcpConnectTimeoutMs as number,
    tcpIdleTimeoutMs: tcpIdleTimeoutMs as number,
    tcpMaxConcurrent: tcpMaxConcurrent as number,
  };
}

/**
 * Decide whether an outbound target URL may be fetched. Pure function —
 * the caller owns the deny response. Whitelist `(host, port)` entries
 * bypass name/IP denies but never the scheme/userinfo checks.
 */
export function evaluateTarget(rawUrl: string, policy: EgressPolicy | null): Decision {
  if (policy === null) return { allow: false, code: "config" };
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return { allow: false, code: "url" };
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return { allow: false, code: "scheme" };
  }
  if (url.username !== "" || url.password !== "") {
    return { allow: false, code: "userinfo" };
  }
  const host = url.hostname;
  if (host === "") return { allow: false, code: "url" };
  const port =
    url.port === "" ? (url.protocol === "http:" ? 80 : 443) : Number(url.port);
  const key = hostKey(host);
  if (key !== null && policy.allow.some((e) => e.key === key && e.port === port)) {
    return { allow: true };
  }
  if (!policy.allowedPorts.has(port)) {
    return { allow: false, code: "port", detail: String(port) };
  }
  const denied = deniedHostCode(host, policy);
  if (denied !== null) return denied;
  return { allow: true };
}

/**
 * Host-level deny shared by the fetch and raw-TCP paths: IP literal
 * ranges (incl. embedded unwraps), the builtin denied names, operator
 * denyNames, unqualified names. `host` is the WHATWG hostname (v6
 * stays bracketed). Returns the deny decision or null.
 */
function deniedHostCode(host: string, policy: EgressPolicy): Decision | null {
  if (host.startsWith("[")) {
    const v6 = parseIpv6(host);
    if (v6 === null) return { allow: false, code: "url" };
    const denied = ipv6DenyCode(v6);
    return denied === null ? null : { allow: false, code: denied };
  }
  const v4 = parseIpv4(host);
  if (v4 !== null) {
    const denied = ipv4DenyCode(v4);
    return denied === null ? null : { allow: false, code: denied };
  }
  const name = stripTrailingDots(host);
  for (const exact of DENY_EXACT) {
    if (name === exact) return { allow: false, code: "denied_name", detail: exact };
  }
  for (const suffix of DENY_SUFFIX) {
    if (name.endsWith(suffix)) {
      return { allow: false, code: "denied_name", detail: `*${suffix}` };
    }
  }
  for (const rule of policy.denyNames) {
    if ("exact" in rule) {
      if (name === rule.exact) {
        return { allow: false, code: "denied_name", detail: rule.exact };
      }
    } else if (name.endsWith(`.${rule.suffix}`)) {
      return { allow: false, code: "denied_name", detail: `*.${rule.suffix}` };
    }
  }
  // Single-label names can only resolve via the pod's DNS search domains
  // (svc.cluster.local etc.) — never a public hostname.
  if (!name.includes(".")) return { allow: false, code: "unqualified" };
  return null;
}

/**
 * Parse the `host:port` CONNECT authority workerd hands the handler
 * as `(await socket.opened).localAddress`. Strict: the WHATWG parse must yield
 * exactly an authority — any userinfo/path/query/hash, missing port,
 * or empty host means the address was not what it claimed (fail
 * closed via "url"). Alternate IPv4 spellings normalize through the
 * same URL parser as fetch targets, so `0x7f.1:43` lands on
 * `ip_loopback`, never on the dial path.
 */
function parseConnectAuthority(localAddress: unknown): TcpTarget | null {
  if (
    typeof localAddress !== "string" ||
    localAddress.length === 0 ||
    localAddress.length > MAX_HOST_LEN + 6
  ) {
    return null;
  }
  let url: URL;
  try {
    url = new URL(`http://${localAddress}/`);
  } catch {
    return null;
  }
  if (
    url.username !== "" ||
    url.password !== "" ||
    url.port === "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== "" ||
    url.hostname === ""
  ) {
    return null;
  }
  const host = url.hostname;
  return {
    host,
    dialHost: host.startsWith("[") ? host.slice(1, -1) : host,
    port: Number(url.port),
    key: hostKey(host),
  };
}

/**
 * MIG-14: decide whether a raw-TCP `connect()` may proceed.
 * `localAddress` is the CONNECT authority workerd reports in the
 * SocketInfo `socket.opened` resolves with (verified on a real
 * workerd; the socket has no address properties — see
 * workers-runtime-egress.md §6).
 *
 * Order matters: mail ports die before even the internal-exception
 * list (no exception can punch an SMTP hole); exceptions then bypass
 * the port list and every host check — the platform vouches for them.
 * Everything else needs `tcpAllowedPorts` membership plus the same
 * name/IP checks fetch applies. The resolved-IP guarantee itself is
 * enforced by workerd: our own `connect()` dials through
 * `outbound-network`, whose restrictPeers() validates the resolved
 * address at connect time — the same DNS-rebinding story as fetch.
 */
export function evaluateTcpTarget(
  localAddress: unknown,
  policy: EgressPolicy | null,
): TcpDecision {
  const target = parseConnectAuthority(localAddress);
  const port = target === null ? null : target.port;
  if (policy === null) return { allow: false, code: "config", port };
  if (target === null) return { allow: false, code: "url", port };
  if (TCP_NEVER_PORTS.has(target.port)) {
    return { allow: false, code: "port", detail: String(target.port), port };
  }
  if (
    target.key !== null &&
    policy.internalTcpAllow.some((e) => e.key === target.key && e.port === target.port)
  ) {
    return { allow: true, target };
  }
  // MIG-15: typed internal TCP target providers (hyperdrive is the
  // first implementation) — matched BEFORE the public port/host
  // checks; they deliberately bypass them because the synthetic name
  // is unresolvable and the upstream is platform-pinned.
  const internal = policy.internalTcpTargets.find(
    (e) => e.key === target.key && e.port === target.port,
  );
  if (internal !== undefined) {
    return { allow: true, target, internal };
  }
  if (!policy.tcpAllowedPorts.has(target.port)) {
    return { allow: false, code: "port", detail: String(target.port), port };
  }
  const denied = deniedHostCode(target.host, policy);
  if (denied !== null) return { ...denied, port };
  return { allow: true, target };
}
