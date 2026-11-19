// IPv4/IPv6 literal classification for the vrt egress policy (T6).
//
// IPv4 inputs reach this module already WHATWG-normalized to dotted
// decimal — the URL parser canonicalizes every alternate spelling
// (0x7f.1, 0177.1, 2130706433, 127.1, trailing dots), so only strict
// dotted-quad is accepted here; no lenient parsing is re-implemented.
//
// IPv6 inputs arrive as canonical "[hex]" hostnames. Embedded IPv4
// forms — mapped ::ffff:0.0.0.0/96, deprecated compatible ::/96, and
// NAT64 64:ff9b::/96 — are unwrapped and the inner IPv4 is classified
// by the same table.

export type IpDenyCode =
  | "ip_this_net"
  | "ip_private"
  | "ip_loopback"
  | "ip_cgnat"
  | "ip_link_local"
  | "ip_multicast"
  | "ip_reserved"
  | "ip_ula"
  | "ip_unspecified"
  | "ip_embedded";

const IPV4_QUAD_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

/** Strict dotted-quad → u32 (unsigned number); null otherwise. */
export function parseIpv4(host: string): number | null {
  const m = IPV4_QUAD_RE.exec(host);
  if (m === null) return null;
  let addr = 0;
  for (let i = 1; i <= 4; i += 1) {
    const octet = Number(m[i]);
    if (octet > 255) return null;
    addr = addr * 256 + octet;
  }
  return addr;
}

// [base address, prefix bits, deny code]
// Single source of truth lives in vrt-config `egress::IPV4_DENY_RANGES`
// and `contract/egress-deny-ranges.json` — this table is its byte-exact
// mirror, drift-checked by the fixture probes in test/egress.test.mjs
// and contract/check-egress-deny-ranges.mjs.
export const IPV4_DENY: ReadonlyArray<readonly [number, number, IpDenyCode]> = [
  [0x00000000, 8, "ip_this_net"], // 0.0.0.0/8 "this network"
  [0x0a000000, 8, "ip_private"], // 10.0.0.0/8 RFC1918
  [0x64400000, 10, "ip_cgnat"], // 100.64.0.0/10 CGNAT RFC6598
  [0x7f000000, 8, "ip_loopback"], // 127.0.0.0/8
  [0xa9fe0000, 16, "ip_link_local"], // 169.254.0.0/16 (incl. cloud metadata)
  [0xac100000, 12, "ip_private"], // 172.16.0.0/12 RFC1918
  [0xc0000000, 24, "ip_reserved"], // 192.0.0.0/24 IETF protocol assignments
  [0xc0000200, 24, "ip_reserved"], // 192.0.2.0/24 TEST-NET-1
  [0xc0a80000, 16, "ip_private"], // 192.168.0.0/16 RFC1918
  [0xc6120000, 15, "ip_reserved"], // 198.18.0.0/15 benchmarking RFC2544
  [0xc6336400, 24, "ip_reserved"], // 198.51.100.0/24 TEST-NET-2
  [0xcb007100, 24, "ip_reserved"], // 203.0.113.0/24 TEST-NET-3
  [0xe0000000, 4, "ip_multicast"], // 224.0.0.0/4
  // 240.0.0.0/4 also covers 255.255.255.255/32 (limited broadcast).
  [0xf0000000, 4, "ip_reserved"], // 240.0.0.0/4 reserved
];

function inV4(addr: number, base: number, bits: number): boolean {
  const hostBits = 32 - bits;
  return Math.floor(addr / 2 ** hostBits) === Math.floor(base / 2 ** hostBits);
}

/** Deny code when `addr` falls inside a denied range; null = public. */
export function ipv4DenyCode(addr: number): IpDenyCode | null {
  for (const [base, bits, code] of IPV4_DENY) {
    if (inV4(addr, base, bits)) return code;
  }
  return null;
}

const HEX_GROUP_RE = /^[0-9a-f]{1,4}$/;

/** Parse a bracketed WHATWG IPv6 hostname to a 128-bit BigInt. */
export function parseIpv6(host: string): bigint | null {
  if (!host.startsWith("[") || !host.endsWith("]")) return null;
  let inner = host.slice(1, -1).toLowerCase();
  // An embedded dotted-quad tail (canonical hostnames never carry one,
  // but other inputs may) is converted to its two hex groups first.
  if (inner.includes(".")) {
    const colon = inner.lastIndexOf(":");
    const quad = parseIpv4(inner.slice(colon + 1));
    if (quad === null) return null;
    inner = `${inner.slice(0, colon + 1)}${(quad >>> 16).toString(16)}:${(
      quad & 0xffff
    ).toString(16)}`;
  }
  if (inner === "") return null;
  const halves = inner.split("::");
  if (halves.length > 2) return null;
  const groupsOf = (s: string) => (s === "" ? [] : s.split(":"));
  const head = groupsOf(halves[0]);
  const tail = halves.length === 2 ? groupsOf(halves[1]) : [];
  for (const g of [...head, ...tail]) {
    if (!HEX_GROUP_RE.test(g)) return null;
  }
  const missing = 8 - head.length - tail.length;
  if (halves.length === 2) {
    if (missing < 1) return null; // "::" must compress at least one group
  } else if (missing !== 0) {
    return null;
  }
  let addr = 0n;
  for (const g of [...head, ...new Array<string>(missing).fill("0"), ...tail]) {
    addr = (addr << 16n) | BigInt(`0x${g}`);
  }
  return addr;
}

// [base, prefix bits, deny code] — mirrors `egress::IPV6_DENY_RANGES`.
// ::/128 and ::1/128 stay special-cased in ipv6DenyCode (they precede
// the embedded-IPv4 unwrapping below); they are rows in the table too
// so the contract fixture describes the complete set.
export const IPV6_DENY: ReadonlyArray<readonly [bigint, number, IpDenyCode]> = [
  [0n, 128, "ip_unspecified"], // ::/128 unspecified
  [1n, 128, "ip_loopback"], // ::1/128 loopback
  [0x0100n << 112n, 64, "ip_reserved"], // 100::/64 discard RFC6666
  [0x20010000n << 96n, 32, "ip_reserved"], // 2001::/32 Teredo
  [0x20010db8n << 96n, 32, "ip_reserved"], // 2001:db8::/32 documentation
  [0x2002n << 112n, 16, "ip_reserved"], // 2002::/16 6to4 — whole range denied:
  // the embedded IPv4 sits in bits 16..48 and 6to4 relays are deprecated
  // anycast; denying the whole /16 is stricter and simpler than judging
  // each embedded address (mirrors contract fixture decision).
  [0x0064ff9b0001n << 80n, 48, "ip_reserved"], // 64:ff9b:1::/48 local-use NAT64 RFC8215
  [0xfc00n << 112n, 7, "ip_ula"], // fc00::/7 unique local
  [0xfe80n << 112n, 10, "ip_link_local"], // fe80::/10 link-local
  [0xff00n << 112n, 8, "ip_multicast"], // ff00::/8 multicast
];

// Prefixes whose low 32 bits embed an IPv4 classified by IPV4_DENY —
// mirrors `egress::EMBEDDED_IPV4_PREFIXES`.
export const IPV6_EMBEDDED: ReadonlyArray<readonly [bigint, number]> = [
  [0x0000ffffn << 32n, 96], // ::ffff:0.0.0.0/96 IPv4-mapped
  [0x0064ff9bn << 96n, 96], // 64:ff9b::/96 NAT64 RFC6052
  [0n, 96], // ::/96 deprecated IPv4-compatible
];

function inV6(addr: bigint, base: bigint, bits: number): boolean {
  const shift = 128n - BigInt(bits);
  return addr >> shift === base >> shift;
}

/** Deny code for a 128-bit IPv6 value; null = public. */
export function ipv6DenyCode(addr: bigint): IpDenyCode | null {
  if (addr === 0n) return "ip_unspecified";
  if (addr === 1n) return "ip_loopback";
  for (const [base, bits, code] of IPV6_DENY) {
    if (inV6(addr, base, bits)) return code;
  }
  for (const [base, bits] of IPV6_EMBEDDED) {
    if (inV6(addr, base, bits)) {
      const inner = ipv4DenyCode(Number(addr & 0xffffffffn));
      return inner === null ? null : "ip_embedded";
    }
  }
  return null;
}
