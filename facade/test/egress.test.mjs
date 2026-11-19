// vrt-egress (T6): outbound URL policy, internal-header stripping,
// redirect pass-through, and fail-closed configuration.
// Tests import the PUBLISHED dist bytes — never the .ts sources.
import test from "node:test";
import assert from "node:assert/strict";
import "./shims/register.mjs";
import { facadeModule } from "./helpers.mjs";

// T25/L12: only handlers are exported by entry modules now — constants
// and the header stripper come from the shared helper module.
const { EgressProxy } = await import(facadeModule("egress/egress.js"));
const {
  POLICY_BINDING,
  DENIED_HEADER,
  ERROR_HEADER,
  classifyUpstreamError,
  logUpstreamError,
  upstreamEndpoint,
  upstreamFetchError,
} = await import(
  facadeModule("egress/shared.js")
);
const { evaluateTarget, evaluateTcpTarget, parseEgressPolicy, TCP_NEVER_PORTS } = await import(
  facadeModule("egress/policy.js")
);

const POLICY = {
  allowedPorts: [80, 443],
  allow: [
    { host: "api.vibeploy.internal", port: 8443 },
    { host: "93.184.216.34", port: 9001 }, // public IP literal, custom port
    { host: "[2606:4700:4700::1111]", port: 9443 }, // public v6 literal
  ],
  denyNames: ["*.tenant.example", "blocked.example"],
  requestTimeoutMs: 30_000,
  // MIG-14 TCP dimension — mirrors the vrt-config canonical JSON.
  tcpAllowedPorts: [43],
  // Platform-injected exact {host,port} internal exceptions (MIG-15
  // interface): private literals and .internal names are their purpose.
  internalTcpAllow: [
    { host: "192.168.20.9", port: 8432 },
    { host: "pg.vibeploy.internal", port: 5432 },
  ],
  tcpConnectTimeoutMs: 10_000,
  tcpIdleTimeoutMs: 60_000,
  tcpMaxConcurrent: 4,
};
const POLICY_JSON = JSON.stringify(POLICY);

function makeProxy(env = { [POLICY_BINDING]: POLICY_JSON }) {
  return new EgressProxy({ props: {} }, env);
}
function req(url, headers = {}) {
  // Plain object — the entrypoint only needs url + headers, and Request's
  // constructor would refuse some of the schemes we deliberately probe.
  return { url, headers: new Headers(headers) };
}

// [url, expected] — "allow" reaches the stubbed upstream (status 200),
// anything else is the x-vrt-egress-denied code with no upstream call.
const CASES = [
  // --- allowed: schemes, default ports, normalization ---
  ["https://example.com/", "allow"],
  ["http://example.com/", "allow"],
  ["http://example.com:80/", "allow"],
  ["https://example.com:443/", "allow"],
  ["http://example.com:443/", "allow"], // non-default for scheme but allowlisted port
  ["https://EXAMPLE.COM./", "allow"], // case + trailing dot
  ["https://a.b.c.example.com/path?q=1#frag", "allow"],
  ["https://xn--nxasmq6b.example/", "allow"], // punycode
  ["https://bücher.example/", "allow"], // unicode → punycode
  ["https://8.8.8.8/", "allow"],
  ["http://8.8.8.8:80/", "allow"],
  ["http://100.63.255.255/", "allow"], // just below CGNAT
  ["http://100.128.0.1/", "allow"], // just above CGNAT
  ["http://172.15.255.255/", "allow"], // just below 172.16/12
  ["http://172.32.0.1/", "allow"], // just above 172.31
  ["http://9.255.255.255/", "allow"], // just below 10/8
  ["http://11.0.0.0/", "allow"], // just above 10/8
  ["http://192.167.0.1/", "allow"], // just below 192.168/16
  ["http://192.169.0.1/", "allow"], // just above
  ["http://010.0.0.1/", "allow"], // octal-looking → 8.0.0.1, public
  ["http://0x10.0.0.1/", "allow"], // hex → 16.0.0.1, public
  ["http://134744073/", "allow"], // integer → 8.8.8.9, public
  // --- allowed: IPv6 ---
  ["https://[2001:4860:4860::8888]/", "allow"],
  ["https://[2001:4860:4860:0:0:0:0:8888]/", "allow"], // uncompressed
  ["https://[::ffff:8.8.8.8]/", "allow"], // v4-mapped public
  ["https://[::FFFF:0808:0808]/", "allow"], // mapped public, upper + hex groups
  ["https://[64:ff9b::808:808]/", "allow"], // NAT64 public embed
  ["https://[64:ff9b::8.8.8.8]/", "allow"], // NAT64 dotted embed
  ["https://[::808:808]/", "allow"], // compat embed of public 8.8.8.8
  // --- allowed: whitelist entries bypass name/IP denies at their port ---
  ["https://api.vibeploy.internal:8443/", "allow"],
  ["https://API.VIBEPLOY.INTERNAL.:8443/", "allow"], // case + trailing dot
  ["http://93.184.216.34:9001/", "allow"], // public IP, explicit whitelist port
  ["http://[2606:4700:4700::1111]:9443/", "allow"], // v6 whitelist
  // --- malformed URL ---
  ["not a url", "url"],
  ["http://", "url"],
  ["//example.com/", "url"], // protocol-relative, no base → parse error
  ["http://[zz::]/", "url"],
  ["http://[bad::]/", "allow"], // "bad" is valid hex → public v6
  ["http://%zz.example/", "url"],
  ["https://ex%20ample.com/", "url"], // percent-encoded space → parse failure
  ["http://[fe80::1%25eth0]/", "url"], // zone-id not supported → parse failure
  ["http://example.com:65536/", "url"], // port out of range → parse error
  ["http://example.com:abc/", "url"],
  // --- scheme ---
  ["ftp://example.com/", "scheme"],
  ["file:///etc/passwd", "scheme"],
  ["ws://example.com/", "scheme"],
  ["wss://example.com/", "scheme"],
  ["gopher://example.com/", "scheme"],
  ["javascript:alert(1)", "scheme"],
  ["data:text/plain,hello", "scheme"],
  ["ssh://example.com/", "scheme"],
  ["HTTPS://example.com/", "allow"], // scheme case-insensitive
  // --- userinfo ---
  ["https://user@example.com/", "userinfo"],
  ["https://user:pass@example.com/", "userinfo"],
  ["https://:pass@example.com/", "userinfo"],
  ["https://user%40a@example.com/", "userinfo"],
  ["http://user:pass@93.184.216.34:9001/", "userinfo"], // whitelist can't save userinfo
  // --- non-default ports without whitelist ---
  ["https://example.com:8080/", "port"],
  ["http://example.com:81/", "port"],
  ["http://example.com:0/", "port"],
  ["http://example.com:22/", "port"],
  ["https://example.com:3128/", "port"],
  ["https://example.com:65535/", "port"], // valid port but not allowlisted
  ["https://api.vibeploy.internal/", "denied_name"], // whitelist only covers :8443 → falls into .internal deny
  ["http://93.184.216.34/", "allow"], // same public IP, default port — fine anyway
  // --- IPv4 literals: every alternate spelling WHATWG normalizes ---
  ["http://127.0.0.1/", "ip_loopback"],
  ["http://127.0.0.2/", "ip_loopback"],
  ["http://127.255.255.255/", "ip_loopback"],
  ["http://127.1/", "ip_loopback"], // short form → 127.0.0.1
  ["http://127.0.1/", "ip_loopback"], // short form
  ["http://0x7f.0.0.1/", "ip_loopback"], // hex octet
  ["http://0x7f.1/", "ip_loopback"], // hex + short form → 127.0.0.1
  ["http://0x7f000001/", "ip_loopback"], // full-hex integer
  ["http://2130706433/", "ip_loopback"], // decimal integer
  ["http://0177.1/", "ip_loopback"], // leading-zero octal → 127.0.0.1
  ["http://127.0.0.1./", "ip_loopback"], // trailing dot
  ["http://0.0.0.0/", "ip_this_net"],
  ["http://0.0.0.1/", "ip_this_net"],
  ["http://0/", "ip_this_net"], // integer 0 → 0.0.0.0
  ["http://0.1/", "ip_this_net"], // → 0.0.0.1
  ["http://10.0.0.1/", "ip_private"],
  ["http://10.255.255.255/", "ip_private"],
  ["http://167772161/", "ip_private"], // integer → 10.0.0.1
  ["http://10.0.0.1./", "ip_private"], // trailing dot
  ["http://172.16.0.1/", "ip_private"],
  ["http://172.31.255.255/", "ip_private"],
  ["http://192.168.0.1/", "ip_private"],
  ["http://192.168.255.255/", "ip_private"],
  ["http://100.64.0.0/", "ip_cgnat"],
  ["http://100.127.255.255/", "ip_cgnat"],
  ["http://169.254.0.1/", "ip_link_local"],
  ["http://169.254.169.254/", "ip_link_local"], // cloud metadata
  ["http://2851995649/", "ip_link_local"], // integer → 169.254.0.1
  ["http://0xa9fe.0.1/", "url"], // >255 part must be final → parse failure
  ["http://0xa9.0xfe.0.1/", "ip_link_local"], // hex octets → 169.254.0.1
  ["http://224.0.0.1/", "ip_multicast"],
  ["http://239.255.255.255/", "ip_multicast"],
  ["http://240.0.0.1/", "ip_reserved"],
  ["http://255.255.255.255/", "ip_reserved"],
  // --- IPv6 literals ---
  ["http://[::1]/", "ip_loopback"],
  ["http://[::]/", "ip_unspecified"],
  ["http://[fc00::1]/", "ip_ula"],
  ["http://[fd00::1]/", "ip_ula"],
  ["http://[FC00::1]/", "ip_ula"], // case
  ["http://[fe80::1]/", "ip_link_local"],
  ["http://[febf::1]/", "ip_link_local"], // fe80::/10 upper edge
  ["http://[ff02::1]/", "ip_multicast"],
  ["http://[::ffff:127.0.0.1]/", "ip_embedded"], // mapped loopback
  ["http://[::ffff:10.0.0.1]/", "ip_embedded"], // mapped private
  ["http://[::ffff:169.254.169.254]/", "ip_embedded"], // mapped metadata
  ["http://[::ffff:7f00:1]/", "ip_embedded"], // mapped, hex-group form
  ["http://[::10.0.0.1]/", "ip_embedded"], // compat private
  ["http://[::127.0.0.1]/", "ip_embedded"], // compat loopback
  ["http://[::a00:1]/", "ip_embedded"], // compat, hex-group form of 10.0.0.1
  ["http://[64:ff9b::10.0.0.1]/", "ip_embedded"], // NAT64 private embed
  ["http://[64:ff9b::127.0.0.1]/", "ip_embedded"], // NAT64 loopback embed
  ["http://[64:ff9b::a00:1]/", "ip_embedded"], // NAT64, hex-group form
  // --- denied names: builtin, cannot be removed ---
  ["http://localhost/", "denied_name"],
  ["http://LOCALHOST/", "denied_name"],
  ["http://localhost./", "denied_name"], // trailing dot
  ["http://foo.localhost/", "denied_name"],
  ["http://a.b.localhost/", "denied_name"],
  ["http://foo.local/", "denied_name"],
  ["http://foo.internal/", "denied_name"],
  ["http://foo.internal./", "denied_name"], // trailing dot
  ["http://foo.svc/", "denied_name"],
  ["http://bar.svc.cluster.local/", "denied_name"],
  ["http://a.b.cluster.local/", "denied_name"],
  ["http://metadata.google.internal/", "denied_name"],
  ["http://METADATA.GOOGLE.INTERNAL/", "denied_name"],
  ["http://kubernetes.default.svc.cluster.local/", "denied_name"],
  ["http://%6cocalhost/", "denied_name"], // %-decoded by URL parser → localhost
  ["http://nolocalhost/", "unqualified"], // endsWith must be label-boundary
  ["http://mylocalhost/", "unqualified"],
  ["http://foo.example.internal.com/", "allow"], // suffix is not a suffix here
  // --- configured denyNames ---
  ["http://api.tenant.example/", "denied_name"], // *.tenant.example suffix
  ["http://a.b.tenant.example/", "denied_name"],
  ["http://tenant.example/", "allow"], // suffix rule does not catch bare name…
  ["https://example.com\\@evil.com/", "allow"], // \\ is a path separator for http(s): host stays example.com
  ["http://blocked.example/", "denied_name"], // exact
  ["http://BLOCKED.EXAMPLE./", "denied_name"], // exact + case + dot
  ["http://sub.blocked.example/", "allow"], // exact rule doesn't cover children
  // --- single-label names (would only resolve via DNS search domains) ---
  ["http://metadata/", "unqualified"],
  ["http://api/", "unqualified"],
  ["http://internal/", "unqualified"],
];

test(`egress policy table (${CASES.length} cases)`, async (t) => {
  for (const [url, expected] of CASES) {
    await t.test(url, async () => {
      const called = [];
      const prev = globalThis.fetch;
      globalThis.fetch = async () => {
        called.push(true);
        return new Response("upstream", { status: 200 });
      };
      try {
        const res = await makeProxy().fetch(req(url));
        if (expected === "allow") {
          assert.equal(res.status, 200);
          assert.equal(res.headers.get(DENIED_HEADER), null);
          assert.equal(called.length, 1);
        } else {
          assert.equal(res.status, 403);
          assert.equal(res.headers.get(DENIED_HEADER), expected);
          assert.equal(called.length, 0, "denied request must not be forwarded");
          const body = await res.json();
          assert.equal(body.error, "EGRESS_DENIED");
          assert.equal(body.code, expected);
        }
      } finally {
        globalThis.fetch = prev;
      }
    });
  }
});

test("evaluateTarget agrees with entrypoint on every case", () => {
  const policy = parseEgressPolicy(POLICY_JSON);
  for (const [url, expected] of CASES) {
    const d = evaluateTarget(url, policy);
    if (expected === "allow") assert.equal(d.allow, true, url);
    else {
      assert.equal(d.allow, false, url);
      assert.equal(d.code, expected, url);
    }
  }
});

test("strips x-vrt-* / x-open-compute-* / host from the forwarded request", async () => {
  const seen = [];
  const prev = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    seen.push(init.headers);
    return new Response("ok");
  };
  try {
    await makeProxy().fetch(
      req("https://example.com/", {
        "x-vrt-egress-denied": "spoof",
        "x-vrt-internal": "1",
        "X-Open-Compute-Binding": "x",
        host: "evil.internal",
        accept: "text/plain",
        "x-tenant-tag": "keep",
      }),
    );
  } finally {
    globalThis.fetch = prev;
  }
  const out = seen[0];
  assert.equal(out.get("x-vrt-internal"), null);
  assert.equal(out.get("x-vrt-egress-denied"), null);
  assert.equal(out.get("x-open-compute-binding"), null);
  assert.equal(out.get("host"), null);
  assert.equal(out.get("accept"), "text/plain");
  assert.equal(out.get("x-tenant-tag"), "keep");
});

test("multi-value headers survive stripping", async () => {
  const seen = [];
  const prev = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    seen.push(init.headers);
    return new Response("ok");
  };
  try {
    const headers = new Headers();
    headers.append("x-multi", "a");
    headers.append("x-multi", "b");
    await makeProxy().fetch(req("https://example.com/", headers));
  } finally {
    globalThis.fetch = prev;
  }
  assert.equal(seen[0].get("x-multi"), "a, b");
});

test("redirect is manual: 3xx passes through with location preserved", async () => {
  const prev = globalThis.fetch;
  const seen = [];
  globalThis.fetch = async (input, init) => {
    seen.push(init.redirect);
    return new Response(null, {
      status: 301,
      headers: { location: "http://169.254.169.254/latest" },
    });
  };
  try {
    const res = await makeProxy().fetch(req("https://example.com/"));
    assert.equal(seen[0], "manual");
    assert.equal(res.status, 301);
    // The tenant sees the redirect; re-issuing it would re-run policy
    // and hit ip_link_local (covered by the table above).
    assert.equal(res.headers.get("location"), "http://169.254.169.254/latest");
  } finally {
    globalThis.fetch = prev;
  }
});

test("upstream response x-vrt-* spoofing is stripped", async () => {
  const prev = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response("ok", {
      headers: { "x-vrt-egress-denied": "fake", "x-real": "1" },
    });
  try {
    const res = await makeProxy().fetch(req("https://example.com/"));
    assert.equal(res.headers.get("x-vrt-egress-denied"), null);
    assert.equal(res.headers.get("x-real"), "1");
  } finally {
    globalThis.fetch = prev;
  }
});

test("MIG-10: upstream throw → fetch() rejection with the stable class code, never a synthetic 502", async () => {
  const prev = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("TLS peer's certificate is not trusted; reason = unable to get local issuer certificate");
  };
  try {
    await assert.rejects(
      makeProxy().fetch(req("https://example.com/")),
      (err) => err instanceof Error && err.message === "vrt egress upstream: tls",
    );
  } finally {
    globalThis.fetch = prev;
  }
});

test("MIG-10: upstream timeout → TimeoutError rejection", async () => {
  const prev = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new DOMException("The operation timed out", "TimeoutError");
  };
  try {
    await assert.rejects(
      makeProxy().fetch(req("https://example.com/")),
      (err) => err.name === "TimeoutError" && /vrt egress upstream: timeout/.test(err.message),
    );
  } finally {
    globalThis.fetch = prev;
  }
});

test("MIG-10: upstream failure is logged — class/host/port/duration, never path/query/userinfo", async () => {
  const prev = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("connect ECONNREFUSED");
  };
  const errs = [];
  const prevErr = console.error;
  console.error = (...a) => errs.push(a.join(" "));
  try {
    await assert.rejects(
      makeProxy().fetch(req("https://example.com/private?token=abc123&sig=z9")),
      (err) => /vrt egress upstream: connect/.test(err.message),
    );
    assert.equal(errs.length, 1);
    const line = JSON.parse(errs[0]);
    assert.equal(line.event, "egress_upstream_error");
    assert.equal(line.class, "connect");
    assert.equal(line.host, "example.com");
    assert.equal(line.port, 443);
    assert.equal(typeof line.ms, "number");
    // The adversarial bit: credentials and path/query must never leak.
    assert.doesNotMatch(errs[0], /abc123|private|sig=z9|token/);
  } finally {
    console.error = prevErr;
    globalThis.fetch = prev;
  }
});

test("MIG-10: classifyUpstreamError covers every class", () => {
  const cases = [
    [new Error("TLS peer's certificate is not trusted"), "tls"],
    [new Error("ssl handshake failed"), "tls"],
    [new Error("connect ECONNREFUSED 10.0.0.1:443"), "connect"],
    [new Error("connect() blocked by restrictPeers()"), "connect"],
    [new Error("DNS resolution failed getaddrinfo ENOTFOUND"), "dns"],
    [new Error("The operation timed out"), "timeout"],
    [new DOMException("x", "TimeoutError"), "timeout"],
    [new DOMException("x", "AbortError"), "aborted"],
    [new Error("something undefined"), "other"],
    ["not-an-error", "other"],
    [undefined, "other"],
  ];
  for (const [err, want] of cases) {
    assert.equal(classifyUpstreamError(err), want, String(err));
  }
});

test("MIG-10: upstreamEndpoint yields host+port only", () => {
  assert.deepEqual(upstreamEndpoint("https://u:p@example.com/a?b#c"), {
    host: "example.com",
    port: 443,
  });
  assert.deepEqual(upstreamEndpoint("http://h.test:8080/x"), { host: "h.test", port: 8080 });
  assert.deepEqual(upstreamEndpoint("not a url"), { host: "", port: null });
});

test("MIG-10: upstreamFetchError — timeout is TimeoutError, others are Error with stable code", () => {
  const t = upstreamFetchError("timeout");
  assert.equal(t.name, "TimeoutError");
  const c = upstreamFetchError("connect");
  assert.equal(c.name, "Error");
  assert.equal(c.message, "vrt egress upstream: connect");
});

test("101 without a webSocket property → typed 502, never forwarded", async () => {
  const prev = globalThis.fetch;
  globalThis.fetch = async () => ({ status: 101 });
  try {
    const res = await makeProxy().fetch(req("https://example.com/"));
    assert.equal(res.status, 502);
    assert.equal(res.headers.get(ERROR_HEADER), "upstream");
    assert.equal(res.headers.get(DENIED_HEADER), null);
  } finally {
    globalThis.fetch = prev;
  }
});

test("101 rebuild re-strips spoofed response headers", async () => {
  // Under plain undici `new Response({status:101})` throws, so the
  // workerd upgrade path cannot execute here — the header-strip intent
  // is covered by asserting stripInternalHeaders directly, and the
  // fail-closed fallback is covered by the 502 below.
  const { stripInternalHeaders } = await import(facadeModule("egress/shared.js"));
  const spoofed = new Headers({
    "x-vrt-tenant": "spoof",
    "x-open-compute-internal": "spoof",
    "sec-websocket-protocol": "chat",
    connection: "upgrade",
  });
  const stripped = stripInternalHeaders(spoofed);
  assert.equal(stripped.get("x-vrt-tenant"), null);
  assert.equal(stripped.get("x-open-compute-internal"), null);
  assert.equal(stripped.get("sec-websocket-protocol"), "chat");
  assert.equal(stripped.get("connection"), "upgrade");

  const prev = globalThis.fetch;
  globalThis.fetch = async () => ({ status: 101, webSocket: { accept() {} } });
  try {
    const res = await makeProxy().fetch(req("https://example.com/"));
    // undici rejects 101 construction → fail closed typed 502; in
    // workerd the same code path returns the rebuilt upgrade response.
    assert.equal(res.status, 502);
    assert.equal(res.headers.get(ERROR_HEADER), "upstream");
  } finally {
    globalThis.fetch = prev;
  }
});

// --- MIG-14: raw-TCP connect() policy + forwarding ---

// [localAddress, expected] — "allow" reaches the injected upstream,
// anything else is a typed deny code with NO upstream dial.
const TCP_CASES = [
  // --- allowed: public names/literals on the policy port ---
  ["whois.verisign-grs.com:43", "allow"],
  ["WHOIS.RIPE.NET:43", "allow"], // case-normalized
  ["8.8.8.8:43", "allow"], // public v4 literal
  ["[2001:4860:4860::8888]:43", "allow"], // public v6 literal
  ["[::ffff:8.8.8.8]:43", "allow"], // mapped public
  // --- ports ---
  ["example.com:22", "port"],
  ["example.com:443", "port"], // HTTP port is not a TCP port
  ["example.com:0", "port"],
  ["example.com", "url"], // no port → malformed authority
  ["example.com:", "url"],
  ["example.com:25", "port"], // SMTP — never, even if listed
  ["example.com:465", "port"],
  ["example.com:587", "port"],
  // --- malformed authorities ---
  ["u:p@example.com:43", "url"],
  ["example.com:43/extra", "url"],
  ["example.com:43?q=1", "url"],
  ["[bad::]:43", "allow"], // "bad" is valid hex → public v6
  ["[fe80::1%25eth0]:43", "url"],
  ["", "url"],
  [42, "url"], // non-string localAddress
  [undefined, "url"],
  // --- IP literals: every WHATWG-normalized spelling ---
  ["127.0.0.1:43", "ip_loopback"],
  ["0x7f.1:43", "ip_loopback"], // hex+short → 127.0.0.1
  ["2130706433:43", "ip_loopback"], // integer
  ["0177.1:43", "ip_loopback"], // octal
  ["10.0.0.5:43", "ip_private"],
  ["167772161:43", "ip_private"], // integer → 10.0.0.1
  ["172.16.8.9:43", "ip_private"],
  ["192.168.1.1:43", "ip_private"],
  ["100.64.5.5:43", "ip_cgnat"],
  ["169.254.169.254:43", "ip_link_local"], // cloud metadata
  ["0.0.0.0:43", "ip_this_net"],
  ["224.0.0.1:43", "ip_multicast"],
  ["255.255.255.255:43", "ip_reserved"],
  // --- IPv6 literals + embedded unwraps ---
  ["[::1]:43", "ip_loopback"],
  ["[::]:43", "ip_unspecified"],
  ["[fd00::1]:43", "ip_ula"],
  ["[fe80::1]:43", "ip_link_local"],
  ["[ff02::1]:43", "ip_multicast"],
  ["[::ffff:127.0.0.1]:43", "ip_embedded"],
  ["[::ffff:10.0.0.1]:43", "ip_embedded"],
  ["[::ffff:169.254.169.254]:43", "ip_embedded"],
  ["[::127.0.0.1]:43", "ip_embedded"], // compat loopback
  ["[64:ff9b::10.0.0.1]:43", "ip_embedded"], // NAT64 private
  // --- names ---
  ["localhost:43", "denied_name"],
  ["foo.localhost:43", "denied_name"],
  ["foo.internal:43", "denied_name"],
  ["bar.svc:43", "denied_name"],
  ["metadata.google.internal:43", "denied_name"],
  ["api.tenant.example:43", "denied_name"], // configured denyNames apply to TCP too
  ["blocked.example:43", "denied_name"],
  ["metadata:43", "unqualified"],
  ["pg:43", "unqualified"],
  // --- internal exceptions bypass port + name/IP checks ---
  ["192.168.20.9:8432", "allow"], // private literal exception
  ["PG.VIBEPLOY.INTERNAL:5432", "allow"], // .internal name exception
  ["192.168.20.9:443", "port"], // exception is exact {host,port}; port check runs first
];

test(`tcp connect() policy table (${TCP_CASES.length} cases)`, async (t) => {
  for (const [localAddress, expected] of TCP_CASES) {
    await t.test(String(localAddress), async () => {
      const dialed = [];
      const prevHook = globalThis.__vrtTestSocketConnect;
      globalThis.__vrtTestSocketConnect = (target) => {
        dialed.push(target);
        return deadSocket();
      };
      try {
        if (expected === "allow") {
          const pair = socketPair(localAddress);
          const done = makeProxy().connect(pair.tenant).then(() => "closed", (e) => e);
          // Let the handler open the upstream + install pipes.
          await new Promise((r) => setTimeout(r, 10));
          assert.equal(dialed.length, 1);
          pair.abortAll();
          await done;
        } else {
          let closed = false;
          await assert.rejects(
            makeProxy().connect({ opened: Promise.resolve({ localAddress }), close: () => (closed = true) }),
            (err) =>
              err.denyCode === expected && err.stableCode === "EGRESS_DENIED",
          );
          assert.equal(closed, true);
          assert.equal(dialed.length, 0, "denied connect must never dial");
        }
      } finally {
        globalThis.__vrtTestSocketConnect = prevHook;
      }
    });
  }
});

test("connect fails closed unless opened carries the authority", async (t) => {
  // Real workerd shape: no address properties on the socket; the
  // authority is only in the `opened` SocketInfo. A legacy
  // `socket.localAddress` property must be ignored, and a missing /
  // rejecting `opened` fails closed.
  for (const [name, sock] of [
    ["no opened", { localAddress: "w.example:43" }],
    ["opened rejects", { opened: Promise.reject(new Error("x")) }],
    ["opened without localAddress", { opened: Promise.resolve({ remoteAddress: "w.example:43" }) }],
  ]) {
    await t.test(`fail closed: ${name}`, async () => {
      let closed = false;
      await assert.rejects(
        makeProxy().connect({ ...sock, close: () => (closed = true) }),
        (err) => err.denyCode === "url" && err.stableCode === "EGRESS_DENIED",
      );
      assert.equal(closed, true);
    });
  }
});

test("evaluateTcpTarget agrees with entrypoint on every case", () => {
  const policy = parseEgressPolicy(POLICY_JSON);
  for (const [localAddress, expected] of TCP_CASES) {
    const d = evaluateTcpTarget(localAddress, policy);
    if (expected === "allow") assert.equal(d.allow, true, localAddress);
    else {
      assert.equal(d.allow, false, localAddress);
      assert.equal(d.code, expected, localAddress);
    }
  }
});

test("empty tcpAllowedPorts denies everything (pre-MIG-14 posture)", () => {
  const policy = parseEgressPolicy(
    JSON.stringify({ ...POLICY, tcpAllowedPorts: [] }),
  );
  for (const addr of ["example.com:43", "8.8.8.8:43"]) {
    const d = evaluateTcpTarget(addr, policy);
    assert.equal(d.allow, false);
    assert.equal(d.code, "port");
  }
  // …but a platform internal exception still matches.
  const d = evaluateTcpTarget("192.168.20.9:8432", policy);
  assert.equal(d.allow, true);
});

test("mail ports are denied before internal exceptions can match", () => {
  // Constructed by hand — parse() would reject this policy outright.
  const policy = {
    ...parseEgressPolicy(POLICY_JSON),
    internalTcpAllow: [{ key: "name:smtp.evil.example", port: 25 }],
  };
  const d = evaluateTcpTarget("smtp.evil.example:25", policy);
  assert.equal(d.allow, false);
  assert.equal(d.code, "port");
});

// A socket's {readable, writable, close()} over two TransformStreams —
// close() severs BOTH directions the way real workerd socket.close()
// does: the source side is aborted (its readable errors) and the sink
// side is cancelled (writes to its writable reject). Without this the
// surviving pipe would never settle — mirroring the real contract is
// what makes the leak assertions meaningful.
function closableSide(sourceTs, sinkTs) {
  return {
    readable: sourceTs.readable,
    writable: sinkTs.writable,
    closed: false,
    async close() {
      try { await sourceTs.writable.abort("closed"); } catch {}
      try { await sinkTs.readable.cancel("closed"); } catch {}
      this.closed = true;
    },
  };
}

// Bidirectional stream plumbing for the forwarding tests. `tenant` is
// the inbound CONNECT socket workerd would hand the handler; `upstream`
// is what our injected cloudflare:sockets connect() returns.
function socketPair(localAddress) {
  const tUp = new TransformStream(); // tenant → handler
  const upIn = new TransformStream(); // handler → upstream
  const upOut = new TransformStream(); // upstream → handler
  const tIn = new TransformStream(); // handler → tenant
  const tenant = {
    opened: Promise.resolve({ localAddress }),
    ...closableSide(tUp, tIn),
  };
  const upstream = {
    ...closableSide(upOut, upIn),
    opened: Promise.resolve({ localAddress, remoteAddress: "caller" }),
  };
  return {
    tenant, upstream, tUp, upIn, upOut, tIn,
    abortAll: () => Promise.all([tenant.close(), upstream.close()]),
  };
}

function deadSocket({ opened } = {}) {
  return { ...closableSide(new TransformStream(), new TransformStream()),
    opened: opened ?? Promise.resolve({}) };
}

test("connect() forwards bytes bidirectionally and closes on upstream EOF", async () => {
  const pair = socketPair("whois.example:43");
  const dialed = [];
  globalThis.__vrtTestSocketConnect = (target, opts) => {
    dialed.push([target, opts]);
    return pair.upstream;
  };
  const errs = [];
  const prevErr = console.error;
  console.error = (...a) => errs.push(a.join(" "));
  try {
    const done = makeProxy().connect(pair.tenant);
    await new Promise((r) => setTimeout(r, 5));
    // Plaintext pass-through — the egress worker never requests TLS on
    // the upstream dial (tenant-side starttls stays tenant-side bytes).
    assert.deepEqual(dialed, [[{ hostname: "whois.example", port: 43 }, undefined]]);
    const enc = new TextEncoder();
    const w = pair.tUp.writable.getWriter();
    await w.write(enc.encode("M14PING\n"));
    const upReader = pair.upIn.readable.getReader();
    const got1 = await upReader.read();
    assert.deepEqual(new TextDecoder().decode(got1.value), "M14PING\n");
    const upWriter = pair.upOut.writable.getWriter();
    await upWriter.write(enc.encode("M14PONG\n"));
    const tReader = pair.tIn.readable.getReader();
    const got2 = await tReader.read();
    assert.deepEqual(new TextDecoder().decode(got2.value), "M14PONG\n");
    // Release locks so close() can sever the streams (real workerd
    // socket.close() severs regardless; the fake can only abort an
    // unlocked TransformStream).
    w.releaseLock();
    upReader.releaseLock();
    tReader.releaseLock();
    // Server EOF → handler tears down and closes the upstream socket.
    await upWriter.close();
    upWriter.releaseLock();
    await done;
    assert.equal(pair.upstream.closed, true);
    // Structured close line: counters + reason, no hostname.
    const close = errs.map((l) => { try { return JSON.parse(l); } catch { return null; } })
      .find((e) => e?.event === "egress_tcp_close");
    assert.ok(close, "egress_tcp_close must be logged");
    assert.equal(close.port, 43);
    assert.equal(close.upBytes, 8);
    assert.equal(close.downBytes, 8);
    assert.equal(close.reason, "eof");
    assert.doesNotMatch(errs.join("\n"), /whois\.example/);
  } finally {
    console.error = prevErr;
    await pair.abortAll();
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("DNS rebinding: upstream dial gets the hostname, never a resolved IP", async () => {
  // The rebinding defense is structural: if the handler resolved the
  // name itself and dialed the IP, the check and the dial could act on
  // different answers (attacker flips DNS between them). Passing the
  // hostname verbatim means workerd's outbound-network restrictPeers()
  // validates the SAME resolution it dials — one answer, no window.
  const pair = socketPair("whois.rebind.example:43");
  const dialed = [];
  globalThis.__vrtTestSocketConnect = (target, opts) => {
    dialed.push({ target, opts });
    return pair.upstream;
  };
  try {
    const done = makeProxy().connect(pair.tenant);
    await new Promise((r) => setTimeout(r, 5)); // authority resolves via opened first
    assert.equal(dialed.length, 1);
    assert.equal(dialed[0].target.hostname, "whois.rebind.example");
    assert.equal(dialed[0].target.port, 43);
    // Not an IP: the dial is a NAME, resolved+filtered by workerd.
    assert.equal(/\d+\.\d+\.\d+\.\d+/.test(dialed[0].target.hostname), false);
    await pair.abortAll();
    await done;
  } finally {
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("tenant secureTransport is never forwarded to the upstream dial", async () => {
  // The inbound socket may carry starttls semantics from the tenant's
  // own connect() call — the egress relay must never ask workerd to
  // TLS-wrap the UPSTREAM leg (that would double-wrap and break the
  // tenant's own upgrade exchange). The upstream connect() call must
  // carry no options at all.
  for (const st of [undefined, "starttls", "on", "off"]) {
    const pair = socketPair("whois.example:43");
    pair.tenant.secureTransport = st;
    const dialed = [];
    globalThis.__vrtTestSocketConnect = (target, opts) => {
      dialed.push(opts);
      return pair.upstream;
    };
    try {
      const done = makeProxy().connect(pair.tenant);
      await new Promise((r) => setTimeout(r, 5));
      assert.equal(dialed.length, 1);
      assert.equal(dialed[0], undefined, `secureTransport=${st}`);
      await pair.abortAll();
      await done;
    } finally {
      delete globalThis.__vrtTestSocketConnect;
    }
  }
});

test("denied connect() logs egress_tcp_deny with port+code only", async () => {
  const errs = [];
  const prevErr = console.error;
  console.error = (...a) => errs.push(a.join(" "));
  try {
    await assert.rejects(
      makeProxy().connect({ opened: Promise.resolve({ localAddress: "sneaky.internal-host.example:22" }), close() {} }),
      (err) => err.denyCode === "port",
    );
    const line = JSON.parse(errs.find((l) => l.includes("egress_tcp_deny")));
    assert.deepEqual(Object.keys(line).sort(), ["code", "event", "port"]);
    assert.equal(line.port, 22);
    assert.equal(line.code, "port");
    assert.doesNotMatch(errs.join("\n"), /sneaky|internal-host/);
  } finally {
    console.error = prevErr;
  }
});

test("concurrency limit: tcpMaxConcurrent=1 denies the second connect", async () => {
  const p1 = socketPair("whois.example:43");
  globalThis.__vrtTestSocketConnect = () => p1.upstream;
  const limitJson = JSON.stringify({ ...POLICY, tcpMaxConcurrent: 1 });
  const proxy = makeProxy({ [POLICY_BINDING]: limitJson });
  try {
    const first = proxy.connect(p1.tenant);
    await new Promise((r) => setTimeout(r, 10)); // first session occupies the slot
    const p2 = socketPair("whois2.example:43");
    await assert.rejects(
      proxy.connect(p2.tenant),
      (err) => err.denyCode === "concurrent_limit" && err.stableCode === "EGRESS_DENIED",
    );
    assert.equal(p2.tenant.closed, true);
    await p1.abortAll();
    await first; // teardown on pipe error — resolves, never hangs
  } finally {
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("connect timeout: opened that never resolves → connect_timeout deny", async () => {
  const upstream = deadSocket({ opened: new Promise(() => {}) });
  globalThis.__vrtTestSocketConnect = () => upstream;
  const json = JSON.stringify({ ...POLICY, tcpConnectTimeoutMs: 1_000 });
  try {
    await assert.rejects(
      makeProxy({ [POLICY_BINDING]: json }).connect({ opened: Promise.resolve({ localAddress: "w.example:43" }), readable: new ReadableStream(), writable: new WritableStream(), close() {} }),
      (err) => err.denyCode === "connect_timeout",
    );
    assert.equal(upstream.closed, true);
  } finally {
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("idle timeout: silent pipes → idle_timeout deny + upstream close", async () => {
  const pair = socketPair("w.example:43");
  globalThis.__vrtTestSocketConnect = () => pair.upstream;
  const json = JSON.stringify({ ...POLICY, tcpIdleTimeoutMs: 1_000 });
  try {
    await assert.rejects(
      makeProxy({ [POLICY_BINDING]: json }).connect(pair.tenant),
      (err) => err.denyCode === "idle_timeout",
    );
    assert.equal(pair.upstream.closed, true);
  } finally {
    await pair.abortAll();
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("upstream refusal → EGRESS_UPSTREAM class, socket closed", async () => {
  const upstream = deadSocket({
    opened: Promise.reject(new Error("connect ECONNREFUSED 93.184.216.34:43")),
  });
  globalThis.__vrtTestSocketConnect = () => upstream;
  let closed = false;
  try {
    await assert.rejects(
      makeProxy().connect({ opened: Promise.resolve({ localAddress: "w.example:43" }), close: () => (closed = true) }),
      (err) => err.stableCode === "EGRESS_UPSTREAM" && err.egressClass === "connect",
    );
    assert.equal(upstream.closed, true);
    assert.equal(closed, true);
  } finally {
    delete globalThis.__vrtTestSocketConnect;
  }
});

test("connect() with no/invalid policy binding fails closed (config)", async () => {
  for (const env of [{}, { [POLICY_BINDING]: "not json" }]) {
    let closed = false;
    await assert.rejects(
      new EgressProxy({ props: {} }, env).connect({ opened: Promise.resolve({ localAddress: "w.example:43" }), close: () => (closed = true) }),
      (err) => err.denyCode === "config",
    );
    assert.equal(closed, true);
  }
});

test("default export is the entrypoint class", async () => {
  const mod = await import(facadeModule("egress/egress.js"));
  assert.equal(mod.default, EgressProxy);
});

// --- fail-closed policy parsing ---

const BAD_CONFIGS = [
  ["missing binding", {}],
  ["undefined binding", { [POLICY_BINDING]: undefined }],
  ["non-json string", { [POLICY_BINDING]: "not json {" }],
  ["array doc", { [POLICY_BINDING]: "[]" }],
  ["missing key", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allowedPorts: undefined }) }],
  ["extra key", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, extra: 1 }) }],
  ["empty ports", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allowedPorts: [] }) }],
  ["port 0", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allowedPorts: [0] }) }],
  ["port 65536", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allowedPorts: [65536] }) }],
  ["non-int port", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allowedPorts: [80.5] }) }],
  ["allow entry missing port", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "x.y" }] }) }],
  ["allow entry extra key", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "x.y", port: 1, z: 2 }] }) }],
  ["allow entry bad host", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "u@h", port: 443 }] }) }],
  ["denyNames non-string", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, denyNames: [42] }) }],
  ["denyNames bad chars", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, denyNames: ["a b"] }) }],
  ["allow localhost", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "localhost", port: 80 }] }) }],
  ["allow metadata.google.internal", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "metadata.google.internal", port: 443 }] }) }],
  ["allow private IP literal", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "10.0.0.5", port: 9000 }] }) }],
  ["allow loopback v6", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "[::1]", port: 80 }] }) }],
  ["allow metadata IP", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, allow: [{ host: "169.254.169.254", port: 80 }] }) }],
  ["timeout too small", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, requestTimeoutMs: 999 }) }],
  ["timeout too large", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, requestTimeoutMs: 120_001 }) }],
  ["timeout non-int", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, requestTimeoutMs: "30" }) }],
  // --- MIG-14 TCP fields ---
  ["missing tcpAllowedPorts", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: undefined }) }],
  ["missing internalTcpAllow", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: undefined }) }],
  ["tcp mail port 25", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: [25] }) }],
  ["tcp mail port 587", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: [43, 587] }) }],
  ["tcp port 0", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: [0] }) }],
  ["tcp port 65536", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: [65536] }) }],
  ["tcp ports >32", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpAllowedPorts: Array.from({ length: 33 }, (_, i) => i + 1) }) }],
  ["internal exception missing port", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "x.y" }] }) }],
  ["internal exception extra key", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "x.y", port: 1, z: 2 }] }) }],
  ["internal exception mail port", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "192.168.1.5", port: 465 }] }) }],
  ["internal exception localhost", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "localhost", port: 5432 }] }) }],
  ["internal exception metadata name", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "metadata.google.internal", port: 80 }] }) }],
  ["internal exception loopback", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "127.0.0.1", port: 5432 }] }) }],
  ["internal exception this-net", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "0.0.0.7", port: 443 }] }) }],
  ["internal exception metadata IP", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "169.254.169.254", port: 80 }] }) }],
  ["internal exception multicast", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "224.0.0.1", port: 443 }] }) }],
  ["internal exception v6 loopback", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[::1]", port: 5432 }] }) }],
  ["internal exception v6 link-local", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[fe80::1]", port: 5432 }] }) }],
  ["internal exception mapped loopback", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[::ffff:127.0.0.1]", port: 5432 }] }) }],
  ["internal exception mapped metadata", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[::ffff:169.254.169.254]", port: 80 }] }) }],
  ["internal exception NAT64 loopback", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[64:ff9b::127.0.0.1]", port: 80 }] }) }],
  ["internal exception compat metadata", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, internalTcpAllow: [{ host: "[::a9fe:a9fe]", port: 80 }] }) }],
  ["tcpConnectTimeoutMs too small", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpConnectTimeoutMs: 999 }) }],
  ["tcpConnectTimeoutMs too large", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpConnectTimeoutMs: 120_001 }) }],
  ["tcpIdleTimeoutMs too small", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpIdleTimeoutMs: 999 }) }],
  ["tcpIdleTimeoutMs too large", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpIdleTimeoutMs: 600_001 }) }],
  ["tcpMaxConcurrent zero", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpMaxConcurrent: 0 }) }],
  ["tcpMaxConcurrent too large", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpMaxConcurrent: 1025 }) }],
  ["tcpMaxConcurrent non-int", { [POLICY_BINDING]: JSON.stringify({ ...POLICY, tcpMaxConcurrent: 4.5 }) }],
];

test("invalid/absent policy config fails closed on every request", async (t) => {
  for (const [name, env] of BAD_CONFIGS) {
    await t.test(name, async () => {
      let called = false;
      const prev = globalThis.fetch;
      globalThis.fetch = async () => ((called = true), new Response("x"));
      try {
        const res = await makeProxy(env).fetch(req("https://example.com/"));
        assert.equal(res.status, 403);
        assert.equal(res.headers.get(DENIED_HEADER), "config");
        assert.equal(called, false);
      } finally {
        globalThis.fetch = prev;
      }
    });
  }
});

test("policy binding accepts a decoded object too", async () => {
  const proxy = makeProxy({ [POLICY_BINDING]: POLICY });
  const prev = globalThis.fetch;
  globalThis.fetch = async () => new Response("ok");
  try {
    const res = await proxy.fetch(req("https://example.com/"));
    assert.equal(res.status, 200);
  } finally {
    globalThis.fetch = prev;
  }
});

test("builtin denied names cannot be removed by config", () => {
  // denyNames can only ADD rules; builtin entries still fire even when
  // the operator's list is empty or names unrelated hosts.
  const p = parseEgressPolicy(
    JSON.stringify({ ...POLICY, denyNames: ["unrelated.example"] }),
  );
  assert.notEqual(p, null);
  for (const url of [
    "http://localhost/",
    "http://foo.cluster.local/",
    "http://metadata.google.internal/",
  ]) {
    const d = evaluateTarget(url, p);
    assert.equal(d.allow, false, url);
    assert.equal(d.code, "denied_name", url);
  }
});

// ── Cross-language contract: contract/egress-deny-ranges.json ────────
// The fixture is rendered by vrt-config from its authoritative
// egress::{IPV4,IPV6}_DENY_RANGES / EMBEDDED_IPV4_PREFIXES tables. For
// every declared range we probe first, last, first-1, last+1 and assert
// evaluateTarget's code equals the fixture-computed expectation — so a
// table edited on only one side fails here AND in the byte gate
// (`contract/check-egress-deny-ranges.mjs --check`).

import { readFileSync } from "node:fs";
const FIXTURE = JSON.parse(
  readFileSync(new URL("../../contract/egress-deny-ranges.json", import.meta.url), "utf8"),
);
const { parseIpv6 } = await import(facadeModule("egress/ip.js"));

assert.equal(FIXTURE.schemaVersion, 1);
for (const row of [...FIXTURE.ipv4, ...FIXTURE.ipv6]) {
  assert.deepEqual(Object.keys(row).sort(), ["cidr", "code"]);
}
assert.ok(Array.isArray(FIXTURE.embeddedPrefixes));

function parseCidr(cidr) {
  const i = cidr.lastIndexOf("/");
  return [cidr.slice(0, i), Number(cidr.slice(i + 1))];
}
const v4ToInt = (text) =>
  text.split(".").reduce((acc, o) => ((acc << 8) | Number(o)) >>> 0, 0);
const intToV4 = (n) =>
  [(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255].join(".");
function bigToV6(addr) {
  const groups = [];
  for (let i = 0; i < 8; i++) {
    groups.push(((addr >> BigInt((7 - i) * 16)) & 0xffffn).toString(16));
  }
  return `[${groups.join(":")}]`; // uncompressed — always a valid literal
}
const inRange = (addr, base, bits, width) =>
  bits === 0 || addr >> BigInt(width - bits) === base >> BigInt(width - bits);

// Mirror of the worker's classification, driven by the FIXTURE tables
// (not the TS tables) — the contract assertion.
function expectedV4Code(addr) {
  for (const { cidr, code } of FIXTURE.ipv4) {
    const [baseText, bits] = parseCidr(cidr);
    if (inRange(BigInt(addr), BigInt(v4ToInt(baseText)), bits, 32)) {
      return code;
    }
  }
  return null;
}
function expectedV6Code(addr) {
  for (const { cidr, code } of FIXTURE.ipv6) {
    const [baseText, bits] = parseCidr(cidr);
    if (inRange(addr, parseIpv6(`[${baseText}]`), bits, 128)) return code;
  }
  for (const cidr of FIXTURE.embeddedPrefixes) {
    const [baseText, bits] = parseCidr(cidr);
    if (inRange(addr, parseIpv6(`[${baseText}]`), bits, 128)) {
      const inner = Number(addr & 0xffffffffn);
      return expectedV4Code(inner) === null ? null : "ip_embedded";
    }
  }
  return null;
}
const PROBE_POLICY = parseEgressPolicy(
  JSON.stringify({
    allowedPorts: [80, 443],
    allow: [],
    denyNames: [],
    requestTimeoutMs: 30_000,
    internalTcpAllow: [],
    tcpAllowedPorts: [43],
    tcpConnectTimeoutMs: 10_000,
    tcpIdleTimeoutMs: 60_000,
    tcpMaxConcurrent: 64,
  }),
);
const decisionCode = (url) => {
  const d = evaluateTarget(url, PROBE_POLICY);
  return d.allow ? null : d.code;
};

test("contract fixture: TS deny table ≡ fixture, 4 probes per range", () => {
  let probes = 0;
  for (const { cidr } of FIXTURE.ipv4) {
    const [baseText, bits] = parseCidr(cidr);
    const base = v4ToInt(baseText);
    const size = 2 ** (32 - bits);
    for (const probe of [base - 1, base, base + size - 1, base + size]) {
      if (probe < 0 || probe > 0xffffffff) continue;
      const ip = intToV4(probe >>> 0);
      assert.equal(
        decisionCode(`http://${ip}/`),
        expectedV4Code(probe >>> 0),
        `v4 probe ${ip} (boundary of ${cidr})`,
      );
      probes++;
    }
  }
  for (const { cidr } of FIXTURE.ipv6) {
    const [baseText, bits] = parseCidr(cidr);
    const base = parseIpv6(`[${baseText}]`);
    const size = 1n << BigInt(128 - bits);
    for (const probe of [base - 1n, base, base + size - 1n, base + size]) {
      if (probe < 0n || probe > (1n << 128n) - 1n) continue;
      const literal = bigToV6(probe);
      assert.equal(
        decisionCode(`http://${literal}/`),
        expectedV6Code(probe),
        `v6 probe ${literal} (boundary of ${cidr})`,
      );
      probes++;
    }
  }
  // Embedded prefixes: every fixture v4 range base embedded must yield
  // ip_embedded; a public v4 embedded must be allowed.
  for (const cidr of FIXTURE.embeddedPrefixes) {
    const [baseText] = parseCidr(cidr);
    const prefixBase = parseIpv6(`[${baseText}]`);
    for (const { cidr: v4cidr } of FIXTURE.ipv4) {
      const [v4baseText] = parseCidr(v4cidr);
      const addr = prefixBase | BigInt(v4ToInt(v4baseText));
      // Direct deny rows precede embedded unwrapping (e.g. ::/96 ⊕
      // 0.0.0.0 = "::" → ip_unspecified, not ip_embedded), so compare
      // against the fixture-evaluated code, not a literal.
      assert.equal(
        decisionCode(`http://${bigToV6(addr)}/`),
        expectedV6Code(addr),
        `embedded ${bigToV6(addr)} (${v4baseText} in ${cidr})`,
      );
      probes++;
    }
    const pub = prefixBase | BigInt(v4ToInt("8.8.8.8"));
    assert.equal(
      decisionCode(`http://${bigToV6(pub)}/`),
      null,
      `embedded public ${bigToV6(pub)}`,
    );
    probes++;
  }
  assert.ok(probes >= 100, `probe coverage too thin: ${probes}`);
});

// ── vrt-no-network / DenyAll ─────────────────────────────────────────
test("DenyAll: fetch is a typed 403, connect throws and closes", async () => {
  const { DenyAll } = await import(facadeModule("egress/deny.js"));
  const { DENIED_HEADER: DENY_HDR, DENY_CODE } = await import(
    facadeModule("egress/shared.js")
  );
  const worker = new DenyAll({ props: {} }, {});
  const res = await worker.fetch({ url: "https://anything.example/" });
  assert.equal(res.status, 403);
  assert.equal(res.headers.get(DENY_HDR), DENY_CODE);
  let closed = false;
  await assert.rejects(worker.connect({ close: () => (closed = true) }));
  assert.equal(closed, true);
});

// --- MIG-15: internal TCP target providers (hyperdrive pg-wire) ---

const INTERNAL_POLICY = {
  ...POLICY,
  internalTcpTargets: [
    {
      host: "hd0123456789abcdef01234567.hyperdrive.vrt.internal",
      port: 5432,
      kind: "pg-v1",
      upstreamHost: "127.0.0.1",
      upstreamPort: 19543,
    },
  ],
};

test("MIG-15: synthetic internal host routes internally with pinned upstream", async () => {
  const policy = parseEgressPolicy(JSON.stringify(INTERNAL_POLICY));
  assert.notEqual(policy, null);
  const d = evaluateTcpTarget("hd0123456789abcdef01234567.hyperdrive.vrt.internal:5432", policy);
  assert.equal(d.allow, true);
  assert.equal(d.internal.upstreamHost, "127.0.0.1");
  assert.equal(d.internal.upstreamPort, 19543);
  // A NON-declared sibling synthetic name still dies on denied_name.
  const other = evaluateTcpTarget("hd9999.hyperdrive.vrt.internal:5432", policy);
  assert.equal(other.allow, false);
  assert.equal(other.code, "port"); // port list runs before name checks
  // Wrong port on the declared host does not take the internal route.
  const wrongPort = evaluateTcpTarget("hd0123456789abcdef01234567.hyperdrive.vrt.internal:5433", policy);
  assert.equal(wrongPort.allow, false);
});

test("MIG-15: connect() dials INTERNAL_NET and writes the VRT-PG1 preamble first", async () => {
  const dialed = [];
  const pair = socketPair("hd0123456789abcdef01234567.hyperdrive.vrt.internal:5432");
  const env = {
    [POLICY_BINDING]: JSON.stringify(INTERNAL_POLICY),
    INTERNAL_NET: {
      connect: (addr) => {
        dialed.push(addr);
        return pair.upstream;
      },
    },
  };
  const written = [];
  const reader = pair.upIn.readable.getReader();
  const done = new EgressProxy({ props: {} }, env).connect(pair.tenant).then(
    () => "closed",
    (e) => e,
  );
  for (let i = 0; i < 50 && dialed.length === 0; i++) {
    await new Promise((r) => setTimeout(r, 10));
  }
  assert.equal(dialed.length, 1);
  assert.deepEqual(dialed[0], { hostname: "127.0.0.1", port: 19543 });
  const first = await reader.read();
  assert.equal(
    Buffer.from(first.value).toString(),
    "VRT-PG1 hd0123456789abcdef01234567\n",
    "preamble carries the first-label binding id before any tenant byte",
  );
  pair.abortAll();
  await done;
});

test("MIG-15: connect() without the INTERNAL_NET binding fails closed (config)", async () => {
  const env = { [POLICY_BINDING]: JSON.stringify(INTERNAL_POLICY) };
  const pair = socketPair("hd0123456789abcdef01234567.hyperdrive.vrt.internal:5432");
  let closed = false;
  await assert.rejects(
    new EgressProxy({ props: {} }, env)
      .connect({ ...pair.tenant, close: () => (closed = true) }),
    (err) => err.denyCode === "config" && err.stableCode === "EGRESS_DENIED",
  );
});

test("MIG-15: internalTcpTargets schema fails closed on malformed entries", () => {
  const bad = (mut) => {
    const doc = { ...POLICY, internalTcpTargets: [mut] };
    assert.equal(parseEgressPolicy(JSON.stringify(doc)), null);
  };
  bad({ host: "x.hyperdrive.vrt.internal", port: 5432, kind: "pg-v1", upstreamHost: 19543, upstreamPort: "127.0.0.1" }); // swapped types
  bad({ host: "x.hyperdrive.vrt.internal", port: 5432, kind: "pg-v1", upstreamHost: "127.0.0.1" }); // missing upstreamPort
  bad({ host: "localhost", port: 5432, kind: "pg-v1", upstreamHost: "127.0.0.1", upstreamPort: 19543 }); // builtin-denied host
  bad({ host: "x.hyperdrive.vrt.internal", port: 0, kind: "pg-v1", upstreamHost: "127.0.0.1", upstreamPort: 19543 });
  bad({ host: "x.hyperdrive.vrt.internal", port: 5432, kind: "pg-v1", upstreamHost: "127.0.0.1", upstreamPort: 19543, extra: 1 });
  bad({ host: "x.hyperdrive.vrt.internal", port: 5432, upstreamHost: "127.0.0.1", upstreamPort: 19543 }); // missing kind
  bad({ host: "x.hyperdrive.vrt.internal", port: 5432, kind: "pg-v2", upstreamHost: "127.0.0.1", upstreamPort: 19543 }); // unknown kind
});

test("MIG-15: policies predating internalTcpTargets still parse (rolling upgrade)", () => {
  assert.notEqual(parseEgressPolicy(POLICY_JSON), null);
});
