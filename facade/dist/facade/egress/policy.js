import { IPV6_DENY, IPV6_EMBEDDED, ipv4DenyCode, ipv6DenyCode, parseIpv4, parseIpv6 } from "./ip.js";
const DENY_EXACT = [
    "localhost",
    "metadata.google.internal"
];
const DENY_SUFFIX = [
    ".localhost",
    ".local",
    ".internal",
    ".svc",
    ".cluster.local"
];
function builtinDeniedKey(key) {
    if (key.startsWith("ip4:")) {
        return ipv4DenyCode(Number(key.slice(4))) !== null;
    }
    if (key.startsWith("ip6:")) {
        return ipv6DenyCode(BigInt(`0x${key.slice(4)}`)) !== null;
    }
    const name = key.slice(5);
    return DENY_EXACT.includes(name);
}
const INTERNAL_NEVER_V4 = new Set([
    "ip_this_net",
    "ip_loopback",
    "ip_link_local",
    "ip_multicast"
]);
const INTERNAL_NEVER_V6 = new Set([
    "ip_unspecified",
    "ip_loopback",
    "ip_link_local",
    "ip_multicast"
]);
function inV6(addr, base, bits) {
    const shift = 128n - BigInt(bits);
    return addr >> shift === base >> shift;
}
function internalDeniedKey(key) {
    if (key.startsWith("ip4:")) {
        const code = ipv4DenyCode(Number(key.slice(4)));
        return code !== null && INTERNAL_NEVER_V4.has(code);
    }
    if (key.startsWith("ip6:")) {
        const addr = BigInt(`0x${key.slice(4)}`);
        if (addr === 0n || addr === 1n) return true;
        for (const [base, bits, code] of IPV6_DENY){
            if (inV6(addr, base, bits)) return INTERNAL_NEVER_V6.has(code);
        }
        for (const [base, bits] of IPV6_EMBEDDED){
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
export const TCP_NEVER_PORTS = new Set([
    25,
    465,
    587
]);
const MAX_TCP_ALLOWED_PORTS = MAX_ALLOWED_PORTS;
const MAX_INTERNAL_TCP_ALLOW = MAX_ALLOW_ENTRIES;
const MAX_TCP_IDLE_TIMEOUT_MS = 600_000;
const MAX_TCP_MAX_CONCURRENT = 1024;
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function stripTrailingDots(host) {
    let end = host.length;
    while(end > 0 && host.charCodeAt(end - 1) === 0x2e)end -= 1;
    return host.slice(0, end);
}
const NAME_RE = /^[a-z0-9.-]+$/i;
function nameShape(name) {
    return name.length > 0 && name.length <= MAX_HOST_LEN && NAME_RE.test(name) && name.split(".").every((label)=>label.length > 0);
}
export function hostKey(hostname) {
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
function policyHostKey(raw) {
    if (typeof raw !== "string" || raw.length === 0 || raw.length > MAX_HOST_LEN) {
        return null;
    }
    let url;
    try {
        url = new URL(`http://${raw}/`);
    } catch  {
        return null;
    }
    if (url.username !== "" || url.password !== "" || url.port !== "" || url.pathname !== "/" || url.search !== "" || url.hash !== "") {
        return null;
    }
    return hostKey(url.hostname);
}
function parsePorts(raw) {
    if (!Array.isArray(raw) || raw.length === 0 || raw.length > MAX_ALLOWED_PORTS) {
        return null;
    }
    for (const port of raw){
        if (!Number.isInteger(port) || port < 1 || port > 65535) {
            return null;
        }
    }
    return new Set(raw);
}
function parseAllow(raw) {
    if (!Array.isArray(raw) || raw.length > MAX_ALLOW_ENTRIES) return null;
    const entries = [];
    for (const item of raw){
        if (!isRecord(item)) return null;
        const keys = Object.keys(item).sort();
        if (keys.length !== 2 || keys[0] !== "host" || keys[1] !== "port") return null;
        const port = item.port;
        if (!Number.isInteger(port) || port < 1 || port > 65535) {
            return null;
        }
        const key = policyHostKey(item.host);
        if (key === null || builtinDeniedKey(key)) return null;
        entries.push({
            key,
            port: port
        });
    }
    return entries;
}
function parseInternalTcpTargets(raw) {
    if (raw === undefined) return [];
    if (!Array.isArray(raw) || raw.length > MAX_INTERNAL_TCP_ALLOW) return null;
    const entries = [];
    for (const item of raw){
        if (!isRecord(item)) return null;
        const keys = Object.keys(item).sort();
        if (keys.length !== 5 || keys[0] !== "host" || keys[1] !== "kind" || keys[2] !== "port" || keys[3] !== "upstreamHost" || keys[4] !== "upstreamPort") {
            return null;
        }
        if (item.kind !== "pg-v1") return null;
        const port = item.port;
        const upstreamPort = item.upstreamPort;
        if (!Number.isInteger(port) || port < 1 || port > 65535 || !Number.isInteger(upstreamPort) || upstreamPort < 1 || upstreamPort > 65535) {
            return null;
        }
        const key = policyHostKey(item.host);
        if (key === null || builtinDeniedKey(key)) return null;
        const upKey = policyHostKey(item.upstreamHost);
        if (item.upstreamHost !== "127.0.0.1" && item.upstreamHost !== "[::1]" && upKey === null) {
            return null;
        }
        entries.push({
            key,
            port: port,
            upstreamHost: item.upstreamHost,
            upstreamPort: upstreamPort,
            kind: item.kind
        });
    }
    return entries;
}
function parseInternalTcpAllow(raw) {
    if (!Array.isArray(raw) || raw.length > MAX_INTERNAL_TCP_ALLOW) return null;
    const entries = [];
    for (const item of raw){
        if (!isRecord(item)) return null;
        const keys = Object.keys(item).sort();
        if (keys.length !== 2 || keys[0] !== "host" || keys[1] !== "port") return null;
        const port = item.port;
        if (!Number.isInteger(port) || port < 1 || port > 65535 || TCP_NEVER_PORTS.has(port)) {
            return null;
        }
        const key = policyHostKey(item.host);
        if (key === null || internalDeniedKey(key)) return null;
        entries.push({
            key,
            port: port
        });
    }
    return entries;
}
function parseTcpPorts(raw) {
    if (!Array.isArray(raw) || raw.length > MAX_TCP_ALLOWED_PORTS) return null;
    for (const port of raw){
        if (!Number.isInteger(port) || port < 1 || port > 65535 || TCP_NEVER_PORTS.has(port)) {
            return null;
        }
    }
    return new Set(raw);
}
function parseDenyNames(raw) {
    if (!Array.isArray(raw) || raw.length > MAX_DENY_NAMES) return null;
    const rules = [];
    for (const item of raw){
        if (typeof item !== "string") return null;
        if (item.startsWith("*.")) {
            const suffix = stripTrailingDots(item.slice(2)).toLowerCase();
            if (!nameShape(suffix)) return null;
            rules.push({
                suffix
            });
        } else {
            const exact = stripTrailingDots(item).toLowerCase();
            if (!nameShape(exact)) return null;
            rules.push({
                exact
            });
        }
    }
    return rules;
}
export function parseEgressPolicy(raw) {
    let doc = raw;
    if (typeof doc === "string") {
        try {
            doc = JSON.parse(doc);
        } catch  {
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
        "tcpMaxConcurrent"
    ]);
    const keys = Object.keys(doc);
    if (keys.some((k)=>!expected.has(k)) || [
        ...expected
    ].some((k)=>k !== "internalTcpTargets" && !keys.includes(k))) {
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
    if (!Number.isInteger(timeout) || timeout < MIN_TIMEOUT_MS || timeout > MAX_TIMEOUT_MS) {
        return null;
    }
    const tcpConnectTimeoutMs = doc.tcpConnectTimeoutMs;
    if (!Number.isInteger(tcpConnectTimeoutMs) || tcpConnectTimeoutMs < MIN_TIMEOUT_MS || tcpConnectTimeoutMs > MAX_TIMEOUT_MS) {
        return null;
    }
    const tcpIdleTimeoutMs = doc.tcpIdleTimeoutMs;
    if (!Number.isInteger(tcpIdleTimeoutMs) || tcpIdleTimeoutMs < MIN_TIMEOUT_MS || tcpIdleTimeoutMs > MAX_TCP_IDLE_TIMEOUT_MS) {
        return null;
    }
    const tcpMaxConcurrent = doc.tcpMaxConcurrent;
    if (!Number.isInteger(tcpMaxConcurrent) || tcpMaxConcurrent < 1 || tcpMaxConcurrent > MAX_TCP_MAX_CONCURRENT) {
        return null;
    }
    return {
        allowedPorts,
        allow,
        denyNames,
        requestTimeoutMs: timeout,
        tcpAllowedPorts,
        internalTcpAllow,
        internalTcpTargets,
        tcpConnectTimeoutMs: tcpConnectTimeoutMs,
        tcpIdleTimeoutMs: tcpIdleTimeoutMs,
        tcpMaxConcurrent: tcpMaxConcurrent
    };
}
export function evaluateTarget(rawUrl, policy) {
    if (policy === null) return {
        allow: false,
        code: "config"
    };
    let url;
    try {
        url = new URL(rawUrl);
    } catch  {
        return {
            allow: false,
            code: "url"
        };
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
        return {
            allow: false,
            code: "scheme"
        };
    }
    if (url.username !== "" || url.password !== "") {
        return {
            allow: false,
            code: "userinfo"
        };
    }
    const host = url.hostname;
    if (host === "") return {
        allow: false,
        code: "url"
    };
    const port = url.port === "" ? url.protocol === "http:" ? 80 : 443 : Number(url.port);
    const key = hostKey(host);
    if (key !== null && policy.allow.some((e)=>e.key === key && e.port === port)) {
        return {
            allow: true
        };
    }
    if (!policy.allowedPorts.has(port)) {
        return {
            allow: false,
            code: "port",
            detail: String(port)
        };
    }
    const denied = deniedHostCode(host, policy);
    if (denied !== null) return denied;
    return {
        allow: true
    };
}
function deniedHostCode(host, policy) {
    if (host.startsWith("[")) {
        const v6 = parseIpv6(host);
        if (v6 === null) return {
            allow: false,
            code: "url"
        };
        const denied = ipv6DenyCode(v6);
        return denied === null ? null : {
            allow: false,
            code: denied
        };
    }
    const v4 = parseIpv4(host);
    if (v4 !== null) {
        const denied = ipv4DenyCode(v4);
        return denied === null ? null : {
            allow: false,
            code: denied
        };
    }
    const name = stripTrailingDots(host);
    for (const exact of DENY_EXACT){
        if (name === exact) return {
            allow: false,
            code: "denied_name",
            detail: exact
        };
    }
    for (const suffix of DENY_SUFFIX){
        if (name.endsWith(suffix)) {
            return {
                allow: false,
                code: "denied_name",
                detail: `*${suffix}`
            };
        }
    }
    for (const rule of policy.denyNames){
        if ("exact" in rule) {
            if (name === rule.exact) {
                return {
                    allow: false,
                    code: "denied_name",
                    detail: rule.exact
                };
            }
        } else if (name.endsWith(`.${rule.suffix}`)) {
            return {
                allow: false,
                code: "denied_name",
                detail: `*.${rule.suffix}`
            };
        }
    }
    if (!name.includes(".")) return {
        allow: false,
        code: "unqualified"
    };
    return null;
}
function parseConnectAuthority(localAddress) {
    if (typeof localAddress !== "string" || localAddress.length === 0 || localAddress.length > MAX_HOST_LEN + 6) {
        return null;
    }
    let url;
    try {
        url = new URL(`http://${localAddress}/`);
    } catch  {
        return null;
    }
    if (url.username !== "" || url.password !== "" || url.port === "" || url.pathname !== "/" || url.search !== "" || url.hash !== "" || url.hostname === "") {
        return null;
    }
    const host = url.hostname;
    return {
        host,
        dialHost: host.startsWith("[") ? host.slice(1, -1) : host,
        port: Number(url.port),
        key: hostKey(host)
    };
}
export function evaluateTcpTarget(localAddress, policy) {
    const target = parseConnectAuthority(localAddress);
    const port = target === null ? null : target.port;
    if (policy === null) return {
        allow: false,
        code: "config",
        port
    };
    if (target === null) return {
        allow: false,
        code: "url",
        port
    };
    if (TCP_NEVER_PORTS.has(target.port)) {
        return {
            allow: false,
            code: "port",
            detail: String(target.port),
            port
        };
    }
    if (target.key !== null && policy.internalTcpAllow.some((e)=>e.key === target.key && e.port === target.port)) {
        return {
            allow: true,
            target
        };
    }
    const internal = policy.internalTcpTargets.find((e)=>e.key === target.key && e.port === target.port);
    if (internal !== undefined) {
        return {
            allow: true,
            target,
            internal
        };
    }
    if (!policy.tcpAllowedPorts.has(target.port)) {
        return {
            allow: false,
            code: "port",
            detail: String(target.port),
            port
        };
    }
    const denied = deniedHostCode(target.host, policy);
    if (denied !== null) return {
        ...denied,
        port
    };
    return {
        allow: true,
        target
    };
}
