export const POLICY_BINDING = "VRT_EGRESS_POLICY";
export const DENIED_HEADER = "x-vrt-egress-denied";
export const ERROR_HEADER = "x-vrt-egress-error";
export const DENY_CODE = "no_outbound";
const INTERNAL_PREFIXES = [
    "x-vrt-",
    "x-open-compute-"
];
export function stripInternalHeaders(headers) {
    const out = new Headers();
    headers.forEach((value, key)=>{
        const lower = key.toLowerCase();
        if (INTERNAL_PREFIXES.some((p)=>lower.startsWith(p))) return;
        if (lower === "host") return;
        out.append(key, value);
    });
    return out;
}
export function classifyUpstreamError(err) {
    const name = err instanceof Error ? err.name : "";
    const msg = (err instanceof Error ? err.message : String(err ?? "")).toLowerCase();
    if (name === "TimeoutError" || /timed?[\s_-]?out|timeout/.test(msg)) return "timeout";
    if (name === "AbortError") return "aborted";
    if (/certificate|cert\b|tls|ssl|handshake|issuer|self.?signed|x509/.test(msg)) return "tls";
    if (/dns|resolv|getaddrinfo|enotfound|name or service|nxdomain/.test(msg)) return "dns";
    if (/connect|econn|refus|unreach|reset|restrictpeers|socket|forbidden|denied/.test(msg)) return "connect";
    return "other";
}
export function upstreamEndpoint(url) {
    try {
        const u = new URL(url);
        const port = u.port === "" ? u.protocol === "https:" ? 443 : u.protocol === "http:" ? 80 : null : Number(u.port);
        return {
            host: u.hostname,
            port
        };
    } catch  {
        return {
            host: "",
            port: null
        };
    }
}
export function logUpstreamError(cls, url, elapsedMs, err) {
    const ep = upstreamEndpoint(url);
    console.error(JSON.stringify({
        event: "egress_upstream_error",
        class: cls,
        host: ep.host.slice(0, 253),
        port: ep.port,
        ms: Math.max(0, Math.round(elapsedMs)),
        errName: err instanceof Error ? err.name.slice(0, 64) : typeof err
    }));
}
export function upstreamFetchError(cls) {
    const msg = `vrt egress upstream: ${cls}`;
    const err = cls === "timeout" ? new DOMException(msg, "TimeoutError") : new Error(msg);
    return Object.assign(err, {
        stableCode: "EGRESS_UPSTREAM",
        egressClass: cls
    });
}
export function logTcpDeny(port, code) {
    console.error(JSON.stringify({
        event: "egress_tcp_deny",
        port,
        code
    }));
}
export function logTcpClose(port, upBytes, downBytes, elapsedMs, reason) {
    console.error(JSON.stringify({
        event: "egress_tcp_close",
        port,
        upBytes,
        downBytes,
        ms: Math.max(0, Math.round(elapsedMs)),
        reason
    }));
}
export function tcpDenied(code, detail) {
    return Object.assign(new Error(`vrt egress denies connect(): ${code}`), {
        stableCode: "EGRESS_DENIED",
        denyCode: code,
        ...detail === undefined ? {} : {
            detail
        }
    });
}
export async function closeQuietly(socket) {
    try {
        await socket?.close?.();
    } catch  {}
}
