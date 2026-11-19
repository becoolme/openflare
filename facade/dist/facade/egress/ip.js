const IPV4_QUAD_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;
export function parseIpv4(host) {
    const m = IPV4_QUAD_RE.exec(host);
    if (m === null) return null;
    let addr = 0;
    for(let i = 1; i <= 4; i += 1){
        const octet = Number(m[i]);
        if (octet > 255) return null;
        addr = addr * 256 + octet;
    }
    return addr;
}
export const IPV4_DENY = [
    [
        0x00000000,
        8,
        "ip_this_net"
    ],
    [
        0x0a000000,
        8,
        "ip_private"
    ],
    [
        0x64400000,
        10,
        "ip_cgnat"
    ],
    [
        0x7f000000,
        8,
        "ip_loopback"
    ],
    [
        0xa9fe0000,
        16,
        "ip_link_local"
    ],
    [
        0xac100000,
        12,
        "ip_private"
    ],
    [
        0xc0000000,
        24,
        "ip_reserved"
    ],
    [
        0xc0000200,
        24,
        "ip_reserved"
    ],
    [
        0xc0a80000,
        16,
        "ip_private"
    ],
    [
        0xc6120000,
        15,
        "ip_reserved"
    ],
    [
        0xc6336400,
        24,
        "ip_reserved"
    ],
    [
        0xcb007100,
        24,
        "ip_reserved"
    ],
    [
        0xe0000000,
        4,
        "ip_multicast"
    ],
    [
        0xf0000000,
        4,
        "ip_reserved"
    ]
];
function inV4(addr, base, bits) {
    const hostBits = 32 - bits;
    return Math.floor(addr / 2 ** hostBits) === Math.floor(base / 2 ** hostBits);
}
export function ipv4DenyCode(addr) {
    for (const [base, bits, code] of IPV4_DENY){
        if (inV4(addr, base, bits)) return code;
    }
    return null;
}
const HEX_GROUP_RE = /^[0-9a-f]{1,4}$/;
export function parseIpv6(host) {
    if (!host.startsWith("[") || !host.endsWith("]")) return null;
    let inner = host.slice(1, -1).toLowerCase();
    if (inner.includes(".")) {
        const colon = inner.lastIndexOf(":");
        const quad = parseIpv4(inner.slice(colon + 1));
        if (quad === null) return null;
        inner = `${inner.slice(0, colon + 1)}${(quad >>> 16).toString(16)}:${(quad & 0xffff).toString(16)}`;
    }
    if (inner === "") return null;
    const halves = inner.split("::");
    if (halves.length > 2) return null;
    const groupsOf = (s)=>s === "" ? [] : s.split(":");
    const head = groupsOf(halves[0]);
    const tail = halves.length === 2 ? groupsOf(halves[1]) : [];
    for (const g of [
        ...head,
        ...tail
    ]){
        if (!HEX_GROUP_RE.test(g)) return null;
    }
    const missing = 8 - head.length - tail.length;
    if (halves.length === 2) {
        if (missing < 1) return null;
    } else if (missing !== 0) {
        return null;
    }
    let addr = 0n;
    for (const g of [
        ...head,
        ...new Array(missing).fill("0"),
        ...tail
    ]){
        addr = addr << 16n | BigInt(`0x${g}`);
    }
    return addr;
}
export const IPV6_DENY = [
    [
        0n,
        128,
        "ip_unspecified"
    ],
    [
        1n,
        128,
        "ip_loopback"
    ],
    [
        0x0100n << 112n,
        64,
        "ip_reserved"
    ],
    [
        0x20010000n << 96n,
        32,
        "ip_reserved"
    ],
    [
        0x20010db8n << 96n,
        32,
        "ip_reserved"
    ],
    [
        0x2002n << 112n,
        16,
        "ip_reserved"
    ],
    [
        0x0064ff9b0001n << 80n,
        48,
        "ip_reserved"
    ],
    [
        0xfc00n << 112n,
        7,
        "ip_ula"
    ],
    [
        0xfe80n << 112n,
        10,
        "ip_link_local"
    ],
    [
        0xff00n << 112n,
        8,
        "ip_multicast"
    ]
];
export const IPV6_EMBEDDED = [
    [
        0x0000ffffn << 32n,
        96
    ],
    [
        0x0064ff9bn << 96n,
        96
    ],
    [
        0n,
        96
    ]
];
function inV6(addr, base, bits) {
    const shift = 128n - BigInt(bits);
    return addr >> shift === base >> shift;
}
export function ipv6DenyCode(addr) {
    if (addr === 0n) return "ip_unspecified";
    if (addr === 1n) return "ip_loopback";
    for (const [base, bits, code] of IPV6_DENY){
        if (inV6(addr, base, bits)) return code;
    }
    for (const [base, bits] of IPV6_EMBEDDED){
        if (inV6(addr, base, bits)) {
            const inner = ipv4DenyCode(Number(addr & 0xffffffffn));
            return inner === null ? null : "ip_embedded";
        }
    }
    return null;
}
