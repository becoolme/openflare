//! PT1: `vrt-heartbeat` — the in-process watchdog liveness worker.
//!
//! Rendered into the SAME workerd process as the tenant (a spinning
//! isolate stalls the shared event loop, which is exactly the signal
//! the watchdog needs: PoC7 proved a 3s spin starves every sibling
//! worker in the process). It listens ONLY on a private socket emitted
//! as `vrt-heartbeat`, carries no bindings, and gets the deny-all
//! `globalOutbound` — so it can answer the probe and nothing else.
//!
//! Isolation surface (kept identical to the facade socket rules):
//! - never registered in `worker_endpoints` → vrt-host ingress never
//!   maps a Host onto it;
//! - tenant `fetch`/`connect` to it traverses the egress worker —
//!   loopback/unix are policy-denied (PoC5/PoC6 matrix);
//! - a service binding to it would need a declared `service`
//!   binding entry, which desired validation never produces for it.

/// The emitted worker source — deliberately trivial so the response
/// is entirely event-loop-bound: any scheduling liveness answers it.
pub const HEARTBEAT_JS: &str = "export default { fetch() { return new Response(\"ok\"); } };\n";
