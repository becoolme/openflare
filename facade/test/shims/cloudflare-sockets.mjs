// Test-only shim for `cloudflare:sockets`. In real workerd, connect()
// routes through the importing worker's globalOutbound — here, unit
// tests inject the upstream side through the global hook below, so the
// fake decides what `opened` does and which streams the pipe sees.
export function connect(target, options) {
  const impl = globalThis.__vrtTestSocketConnect;
  if (typeof impl !== "function") {
    throw new Error("cloudflare:sockets connect() without test injection");
  }
  return impl(target, options);
}
