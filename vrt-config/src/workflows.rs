//! W1: vendored Cloudflare Workflows engine artifacts (miniflare
//! `5.20260916.0-alpha`, MIT — see
//! `system-workers/workflows/VENDOR.md`). The bytes are compiled into
//! the crate so `vrt-host` can materialize them next to each
//! generation's `config.capnp` with the same digest-verified embed
//! path every other module uses. `SHA256SUMS` is re-verified in
//! `vrt-config/tests/workflows_vendor.rs` — drift fails closed.

/// Raw bytes of `binding.worker.js` — the Engine DO + WorkflowBinding
/// service entrypoint. Emitted as the engine service's `workflows.mjs`.
pub const ENGINE_WORKER_JS: &[u8] = include_bytes!("../system-workers/workflows/binding.worker.js");

/// Raw bytes of `wrapped-binding.worker.js` — emits `makeBinding` for
/// the user-facing `env.<binding>` API. Inlined into
/// `config.extensions` (workerd requires wrapped-binding modules to be
/// extension-internal).
pub const WRAPPED_BINDING_JS: &[u8] =
    include_bytes!("../system-workers/workflows/wrapped-binding.worker.js");

/// W3-r4: VRT-forked wrapped binding — create()/createBatch() confirm
/// engine durability (bounded get()+status() poll: any non-"queued"
/// status is a committed ENGINE_STATUS_KEY ⇒ INSTANCE_METADATA
/// committed) before resolving, and throw a typed
/// `instance.create_unconfirmed` error on timeout instead of handing
/// out phantom ids. Emitted INSTEAD of `WRAPPED_BINDING_JS`; the
/// vendored file stays byte-identical for the SHA256SUMS drift check.
pub const WRAPPED_BINDING_VRT_JS: &[u8] =
    include_bytes!("../system-workers/workflows/wrapped-binding.vrt.worker.js");

/// `Extension.Module.name` for the wrapped binding. Must be a
/// fully-qualified non-file URL per workerd.capnp Extension.Module
/// (`new_module_registry` rejects path-like names); miniflare uses
/// `workflows:local-wrapped-binding` — we keep the same scheme shape.
pub const WRAPPED_BINDING_MODULE: &str = "workflows:local-wrapped-binding";

/// Engine module name inside the engine service worker's `modules`.
pub const ENGINE_MODULE_NAME: &str = "workflows.mjs";

/// Raw bytes of `zod.worker.js` — miniflare `dist/src/workers/shared/`
/// bundle (zod 4.4.3, MIT). The engine imports it as `miniflare:zod`
/// at module scope; W1-r1's omission crashed every engine service at
/// config load and took the whole tenant down (P0). Registered as an
/// `internal = true` module in `config.extensions` — privileged
/// extension modules may `import "node-internal:*"` without
/// `nodejs_compat` (same as miniflare's own registration).
pub const ZOD_WORKER_JS: &[u8] = include_bytes!("../system-workers/workflows/zod.worker.js");

/// `Extension.Module.name` the engine resolves `miniflare:zod` to.
pub const ZOD_MODULE_NAME: &str = "miniflare:zod";

/// Engine service compatibility date (miniflare WORKFLOWS_PLUGIN).
pub const ENGINE_COMPAT_DATE: &str = "2024-10-22";

/// Engine service compatibility flags (miniflare WORKFLOWS_PLUGIN).
/// `experimental` is `$experimental`-annotated in
/// compatibility-date.capnp → the tenant workerd needs
/// `--experimental` on argv; assemble derives that from the presence
/// of workflows (same mechanism as qconsumer). The flag list applies
/// ONLY to the engine service — tenant workers keep their own flags.
pub const ENGINE_COMPAT_FLAGS: &[&str] = &["experimental"];

/// Engine service loopback binding name — the vendored engine calls
/// `env.MINIFLARE_LOOPBACK.fetch(...)` for two things:
///   - `GET  /core/workflow-storage/<wf>/<id>?waitForPendingDelete=1`
///     polled by `create()` — "has the deferred on-disk SQLite delete
///     finished?".
///   - `DELETE /core/workflow-storage/<wf>/<id>?defer=1`
///     asks the host to unlink the DO's persisted SQLite file after
///     `storage.deleteAll()` + `abort()`.
/// W6: both endpoints are real — answered by vrt-host's control
/// listener (`wf_delete::router`), reached through the per-worker
/// loopback shim below.
pub const LOOPBACK_BINDING: &str = "MINIFLARE_LOOPBACK";

/// capnp service name PREFIX of the per-worker loopback shim —
/// `vrt-wf-loopback-<workerId>`. Internal service — never bound to a
/// socket, so it is unreachable from ingress by construction.
pub const LOOPBACK_SERVICE_PREFIX: &str = "vrt-wf-loopback";

/// W6: `vrt-wf-loopback-<workerId>` service name for one worker.
pub fn loopback_service_name(worker_id: &str) -> String {
    format!("{LOOPBACK_SERVICE_PREFIX}-{worker_id}")
}

/// capnp service name of the shared `external` designator the shim
/// calls — resolved by `--external-addr` to vrt-host's loopback
/// control listener (`/core/workflow-storage` routes on
/// `wf_delete::router`). Emitted only when the host configured a
/// control-listen address; absent → the shim answers 501 explicitly
/// (same fail-loud contract the W1 stub had).
pub const WFDEL_SERVICE: &str = "vrt-wfdel";

/// Per-worker loopback shim (inline — tiny, no vendor file needed).
///
/// The vendored engine addresses storage deletes by WORKFLOW NAME;
/// vrt-host's handler validates by storage identity `uniqueKey`
/// (unforgeable — a tenant-controlled name can never mint a path).
/// The shim's `WF_UK` env (json `{wfName: uniqueKey}`, emitted per
/// declaring worker from desired) performs that lookup with an
/// own-property check, then forwards the request to `VRT_WFDEL`
/// (vrt-host's control listener) preserving method + query. Any
/// undeclared name or malformed path fails closed BEFORE the host
/// call; an unreachable host surfaces as the upstream's own error.
pub const LOOPBACK_WORKER_JS: &str = r#"export default {
  async fetch(req, env) {
    const bad = (code, status) =>
      new Response(JSON.stringify({ error: { code } }), {
        status,
        headers: { "content-type": "application/json" },
      });
    const url = new URL(req.url);
    // /core/workflow-storage/<wfName>[/<hexId>][?<query>]
    const m = url.pathname.match(
      /^\/core\/workflow-storage\/([^/]+)(\/[^/]+)?$/,
    );
    if (!m) return bad("loopback.path", 400);
    const name = decodeURIComponent(m[1]);
    const table =
      typeof env.WF_UK === "string" ? JSON.parse(env.WF_UK) : env.WF_UK || {};
    // W3-r2 pattern: own-property only — "__proto__"/"constructor"
    // must never resolve through the chain.
    const uk = Object.hasOwn(table, name) ? table[name] : null;
    if (!uk) return bad("workflow.undeclared", 400);
    if (!env.VRT_WFDEL) {
      // No control listener on this host (degraded standalone /
      // vrt-local). DELETE must stay an explicit failure — never a
      // fake success — but the wait endpoint answers 200 truthfully:
      // nothing can ever be queued here, and create() polls this on
      // every call (a non-ok reply would break create outright, a
      // regression vs. the W1 stub this shim replaced).
      if (req.method === "GET") {
        return new Response(JSON.stringify({ pending: false }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return bad("wfdel.unconfigured", 501);
    }
    const fwd = `http://wfdel/core/workflow-storage/${uk}${m[2] || ""}${url.search}`;
    return await env.VRT_WFDEL.fetch(fwd, { method: req.method });
  },
};
"#;

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes))
}

/// RFC 2104 HMAC-SHA256 (manual — no `hmac` crate in the workspace).
/// `key` ≤ 64 bytes is zero-padded into the block; longer keys are
/// pre-hashed per spec.
pub(crate) fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut k = [0u8; 64];
    if key.len() <= 64 {
        k[..key.len()].copy_from_slice(key);
    } else {
        let d = sha2::Sha256::digest(key);
        k[..32].copy_from_slice(&d);
    }
    let mut ipad = k;
    let mut opad = k;
    for i in 0..64 {
        ipad[i] ^= 0x36;
        opad[i] ^= 0x5c;
    }
    let mut inner = sha2::Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let ih = inner.finalize();
    let mut outer = sha2::Sha256::new();
    outer.update(opad);
    outer.update(&ih);
    outer.finalize().into()
}

/// W3-r2: facade op token = HMAC-SHA256(host pepper, "vrt-wfop:" +
/// uniqueKey). The pepper is per-vrt-host-process random material —
/// never persisted, never on the op channel — so tokens rotate on
/// host restart and are unforgeable from a DB row (unlike the former
/// bare `sha256("vrt-wfop:" + uniqueKey)`).
pub fn wfop_token(pepper: &str, unique_key: &str) -> String {
    hex::encode(hmac_sha256(
        pepper.as_bytes(),
        format!("vrt-wfop:{unique_key}").as_bytes(),
    ))
}

/// sha256 of the vendored engine bundle (lowercase hex) — computed at
/// render time; the SHA256SUMS test pins the expected constants so a
/// tampered vendor file fails before ever reaching a generation dir.
pub fn engine_worker_sha256() -> String {
    sha256_hex(ENGINE_WORKER_JS)
}

/// sha256 of the loopback stub (for its embed digest).
pub fn loopback_worker_sha256() -> String {
    sha256_hex(LOOPBACK_WORKER_JS.as_bytes())
}

/// Byte content for a vendored/stub module by emit-time name.
/// `EmbedKind::VendoredModule` resolution in vrt-host goes through here.
pub fn vendored_bytes(name: &str) -> Option<&'static [u8]> {
    match name {
        "workflows-engine" => Some(ENGINE_WORKER_JS),
        "workflows-loopback" => Some(LOOPBACK_WORKER_JS.as_bytes()),
        "workflows-zod" => Some(ZOD_WORKER_JS),
        "wfops-facade" => Some(WFOPS_FACADE_JS.as_bytes()),
        "cron-dispatcher" => Some(CRON_DISPATCHER_JS.as_bytes()),
        _ => None,
    }
}

/// W3: `facade-wfops-<workerId>` module — platform-owned bridge from the
/// cell's op executor (via vrt-host `/v1/wfops` → `sock-wfops-<id>` unix
/// socket) into this tenant's per-workflow engine `WorkflowBinding`s.
/// Auth (W3-r2): every request carries `x-vrt-wfop-token` =
/// HMAC-SHA256(host process pepper, "vrt-wfop:" + uniqueKey). The
/// pepper lives only in the vrt-host process — the op channel and the
/// control-plane DB never carry token material; the token is rendered
/// ONLY into this facade's env (`WFOP_TABLE`) and injected at the
/// loopback hop from the serving spec's map. A vrt-host restart
/// rotates the pepper, invalidating every previously rendered token.
/// Tenant code cannot read another service's env and its outbound is
/// deny-all, so it can neither learn the token nor reach the socket —
/// a spoofed platform call fails closed with 403 (digest-compared).
pub const WFOPS_FACADE_JS: &str = r#"export default {
  async fetch(req, env) {
    const reply = (obj, status) =>
      new Response(JSON.stringify(obj), {
        status,
        headers: { "content-type": "application/json" },
      });
    if (req.method !== "POST") return reply({ error: "method" }, 405);
    let body;
    try {
      body = await req.json();
    } catch {
      return reply({ error: "malformed" }, 400);
    }
    const table =
      typeof env.WFOP_TABLE === "string" ? JSON.parse(env.WFOP_TABLE) : env.WFOP_TABLE;
    // W3-r2: own-property lookup — "__proto__"/"constructor" keys must
    // never resolve an entry through the prototype chain.
    const rec =
      typeof body.workflow === "string" && Object.hasOwn(table, body.workflow)
        ? table[body.workflow]
        : null;
    if (!rec) return reply({ error: "workflow.undeclared" }, 404);
    const token = req.headers.get("x-vrt-wfop-token");
    // W3-r2: compare SHA-256 digests of both sides — equal-length
    // outputs make the comparison length- and content-oblivious
    // (timing over digests leaks nothing about the token preimage).
    const dig = async (s) => {
      const d = await crypto.subtle.digest(
        "SHA-256",
        new TextEncoder().encode(String(s)),
      );
      return Array.from(new Uint8Array(d))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
    };
    if ((await dig(token)) !== (await dig(rec.token))) {
      return reply({ error: "forbidden" }, 403);
    }
    const binding = env["WFB_" + rec.binding];
    if (!binding) return reply({ error: "binding.missing" }, 500);
    try {
      switch (body.op) {
        case "create": {
          const opts = { params: body.params ?? {} };
          if (body.instanceId !== undefined) opts.id = body.instanceId;
          const h = await binding.create(opts);
          // W3-r4: vendored create() returns {id} while stub.init()
          // commits INSTANCE_METADATA asynchronously via ctx.waitUntil
          // — a workerd death inside that window leaves an id the
          // engine will never know (registry phantom). The durability
          // signal is a committed ENGINE_STATUS_KEY read: getStatus()
          // short-circuits on the IN-MEMORY accountId (init assigns it
          // at binding.worker.js:4536, BEFORE put(INSTANCE_METADATA)
          // at :4558), so a bare "queued" answer proves nothing — but
          // every non-queued status comes from a committed key written
          // strictly after the metadata put (setStatus at :4567+).
          // Poll: get() throws "instance.not_found" until the actor
          // loads; "queued" keeps polling; anything else ⇒ durable.
          const budget = Number(env.WF_CREATE_CONFIRM_MS) || 10000;
          const deadline = Date.now() + budget;
          for (;;) {
            let confirmed = null;
            try {
              using inst = await binding.get(h.id);
              const st = await inst.status();
              confirmed =
                st && st.status && st.status !== "queued" ? st.status : null;
            } catch (e) {
              const msg = String(e && e.message ? e.message : e);
              if (!msg.includes("instance.not_found")) throw e;
            }
            if (confirmed) {
              return reply({ instanceId: h.id, status: confirmed }, 200);
            }
            if (Date.now() >= deadline) {
              return reply(
                { error: "instance.create_unconfirmed" },
                504,
              );
            }
            await new Promise((r) => setTimeout(r, 50));
          }
        }
        case "get": {
          const inst = await binding.get(body.instanceId);
          // `status()` returns {status, output, error, __LOCAL_DEV_…} —
          // output/error are null until terminal (engine semantics).
          const st = await inst.status();
          // `output`/`error` are omitted when null — the wire schema
          // accepts optional text, never explicit null.
          const out = { status: st && st.status ? st.status : "unknown" };
          if (st && st.output !== undefined && st.output !== null) out.output = st.output;
          if (st && st.error !== undefined && st.error !== null) out.error = st.error;
          return reply(out, 200);
        }
        case "action": {
          if (!["pause", "resume", "restart", "terminate"].includes(body.action)) {
            return reply({ error: "action.invalid" }, 400);
          }
          const inst = await binding.get(body.instanceId);
          await inst[body.action]();
          let status = "unknown";
          try {
            const st = await inst.status();
            if (st && st.status) status = st.status;
          } catch {}
          return reply({ status }, 200);
        }
        case "sendEvent": {
          // W6: eventType bounded here too (the vendored engine
          // validates nothing) — the cell executor enforces the same
          // charset/length, so a violation means the hop was bypassed.
          if (
            typeof body.eventType !== "string" ||
            body.eventType.length === 0 ||
            body.eventType.length > 128 ||
            !/^[A-Za-z0-9_.-]+$/.test(body.eventType)
          ) {
            return reply({ error: "event.type_invalid" }, 400);
          }
          const inst = await binding.get(body.instanceId);
          await inst.sendEvent({
            type: body.eventType,
            payload: body.payload,
          });
          return reply({ status: "delivered" }, 200);
        }
        case "delete": {
          // W6-r2: logical delete is the durable boundary. Vendored
          // deleteInstance() commits `ctx.storage.deleteAll()` INSIDE
          // the actor's storage transaction BEFORE it touches any
          // loopback — those wiped frames are ordinary WAL writes the
          // host replicates, so restore/replay yields the same empty
          // DB (no resurrection) and a same-id recreate starts clean
          // BECAUSE the emptied state is what got copied. Physical
          // file unlink is owned by the host's safe-point reclaimer,
          // never by this op.
          //
          // Confirmation = bounded poll of `binding.get(id)` until it
          // throws instance.not_found — i.e. a FRESH actor reads no
          // INSTANCE_METADATA, the same condition any post-restart
          // restore would observe. A dying incumbent actor may still
          // serve status briefly between deleteAll-commit and abort,
          // so a single probe is not enough; poll until not_found or
          // the bounded deadline. NEVER a fake 200.
          const inst = await binding.get(body.instanceId);
          await inst.delete();
          const deadline =
            Date.now() + Number(env.WF_DELETE_CONFIRM_MS || 10000);
          for (;;) {
            try {
              await binding.get(body.instanceId);
            } catch (e) {
              const m = String(e && e.message ? e.message : e);
              if (m.includes("instance.not_found")) {
                return reply({ status: "deleted" }, 200);
              }
              // A non-not_found engine error is a typed upstream
              // failure — surface it, do not keep polling blindly.
              return reply(
                { error: "engine: " + m.slice(0, 160) },
                502,
              );
            }
            // get() still resolves — the incumbent actor is draining
            // (or a same-id recreate landed); keep polling.
            if (Date.now() >= deadline) {
              return reply({ error: "delete.unconfirmed" }, 502);
            }
            await new Promise((r) => setTimeout(r, 50));
          }
        }
        default:
          return reply({ error: "op.invalid" }, 400);
      }
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      if (msg.includes("instance.not_found")) {
        return reply({ error: "instance.not_found" }, 404);
      }
      return reply({ error: "engine: " + msg.slice(0, 200) }, 502);
    }
  },
};
"#;

pub fn wfops_facade_sha256() -> String {
    sha256_hex(WFOPS_FACADE_JS.as_bytes())
}

/// CR1: `facade-cron-<workerId>` module — platform-owned bridge from
/// vrt-host's `/v1/cron/trigger` (via `sock-cron-<id>` unix socket)
/// into the tenant worker's `scheduled()` handler. The delivery
/// channel is `Fetcher.scheduled()` on the `TARGET` service binding —
/// the ONLY mechanism in workerd that produces a real scheduled event
/// (verified 2026-11-12: HTTP posts to `/__vrt/scheduled` or
/// `/cdn-cgi/handler/scheduled` hit `fetch`, never `scheduled`).
///
/// Honesty invariant: workerd resolves `env.TARGET.scheduled()` even
/// when the handler throws — the failure arrives as
/// `{outcome:"exception"}` with HTTP-200 semantics at the call site.
/// Anything but `outcome == "ok"` is reported upstream as a failed
/// delivery (502 + typed outcome), never a false success. `noRetry`
/// is surfaced verbatim for observability.
pub const CRON_DISPATCHER_JS: &str = r#"export default {
  async fetch(req, env) {
    const reply = (obj, status) =>
      new Response(JSON.stringify(obj), {
        status,
        headers: { "content-type": "application/json" },
      });
    if (req.method !== "POST") return reply({ error: "method" }, 405);
    let body;
    try {
      body = await req.json();
    } catch {
      return reply({ error: "malformed" }, 400);
    }
    if (typeof body !== "object" || body === null || Array.isArray(body)) {
      return reply({ error: "malformed" }, 400);
    }
    const cron = body.cron;
    if (typeof cron !== "string" || cron.length === 0 || cron.length > 256) {
      return reply({ error: "cron.invalid" }, 400);
    }
    // scheduledTime arrives as RFC3339 (control plane) or epoch ms —
    // ScheduledController.scheduledTime is epoch ms on the wire.
    const st = body.scheduledTime;
    const ms = typeof st === "number" ? st : Date.parse(String(st));
    if (!Number.isFinite(ms) || ms < 0 || ms > 4.1e12 /* year 2100 */) {
      return reply({ error: "scheduledTime.invalid" }, 400);
    }
    let result;
    try {
      result = await env.TARGET.scheduled({ cron, scheduledTime: ms });
    } catch (e) {
      const msg = String(e && e.message ? e.message : e).slice(0, 200);
      return reply({ delivered: false, outcome: "exception", detail: msg }, 502);
    }
    const outcome = result && typeof result.outcome === "string" ? result.outcome : "unknown";
    const noRetry = !!(result && result.noRetry === true);
    if (outcome === "ok") {
      return reply({ delivered: true, outcome, noRetry }, 200);
    }
    return reply({ delivered: false, outcome, noRetry }, 502);
  },
};
"#;

/// sha256 of the cron dispatcher module (embed digest).
pub fn cron_dispatcher_sha256() -> String {
    sha256_hex(CRON_DISPATCHER_JS.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wfop_token_hmac_binds_pepper_and_key() {
        // RFC 4231 HMAC-SHA256 test vector sanity (key="key",
        // msg="The quick brown fox jumps over the lazy dog").
        assert_eq!(
            hex::encode(hmac_sha256(
                b"key",
                b"The quick brown fox jumps over the lazy dog"
            )),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
        // Same pepper+key → deterministic; rotation ⇒ new token.
        assert_eq!(wfop_token("p1", "k1"), wfop_token("p1", "k1"));
        assert_ne!(wfop_token("p1", "k1"), wfop_token("p2", "k1"));
        assert_ne!(wfop_token("p1", "k1"), wfop_token("p1", "k2"));
        assert_eq!(wfop_token("p1", "k1").len(), 64);
    }
}
