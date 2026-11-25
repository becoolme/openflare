//! P3c-1: durable-object emit — `durableObjectNamespaces`,
//! `durableObjectStorage.localDisk`, per-worker `disk` services, and
//! `durableObjectNamespace` bindings (self + cross-worker serviceName).
mod common;

use common::*;
use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn tenant_with_do() -> TenantDesired {
    let mut value = serde_json::to_value(tenant()).unwrap();
    // w-alpha-1 declares Counter (sqlite) + binds it; w-alpha-2 declares
    // Shared (kv surface) — bound cross-worker from w-alpha-1.
    value["workers"][0]["durableObjectClasses"] = json!([{
        "className": "Counter",
        "uniqueKey": "a".repeat(64),
        "enableSql": true
    }]);
    value["workers"][0]["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"durableObject","name":"COUNTER","className":"Counter"}));
    value["workers"][0]["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "type":"durableObject","name":"SHARED","className":"Shared",
            "serviceWorkerId":"w-alpha-2"
        }));
    value["workers"][1]["durableObjectClasses"] = json!([{
        "className": "Shared",
        "uniqueKey": "b".repeat(64),
        "enableSql": false
    }]);
    serde_json::from_value(value).unwrap()
}

#[test]
fn emit_do_namespaces_and_storage() {
    let tenant = tenant_with_do();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;

    // Namespaces: className + uniqueKey + enableSql per class, sorted.
    assert!(out.contains("durableObjectNamespaces = ["), "{out}");
    assert!(out.contains("className = \"Counter\""));
    assert!(out.contains(&format!("uniqueKey = \"{}\"", "a".repeat(64))));
    assert!(out.contains("enableSql = true"));
    assert!(out.contains("className = \"Shared\""));
    assert!(out.contains(&format!("uniqueKey = \"{}\"", "b".repeat(64))));
    assert!(out.contains("enableSql = false"));

    // localDisk storage per declaring worker, pointing at a disk service.
    assert!(out.contains("durableObjectStorage = (localDisk ="));

    // Per-worker DiskDirectory services under the tenant DO root.
    assert!(out.contains("disk = (path = "));
    assert!(out.contains("/vrt-data/do/team-alpha"));
    assert!(out.contains("w-alpha-1"));
    assert!(out.contains("writable = true"));

    // Bindings: self → className only; cross-worker → serviceName.
    assert!(out.contains("durableObjectNamespace = (className = \"Counter\")"));
    assert!(out.contains("durableObjectNamespace = (className = \"Shared\", serviceName = "));
}

#[test]
fn emit_do_binding_to_undeclared_class_fails() {
    let mut value = serde_json::to_value(tenant()).unwrap();
    value["workers"][0]["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"durableObject","name":"C","className":"Ghost"}));
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    let params = runtime_params();
    assert!(render_tenant_config(&input(&tenant, &bs(), &params)).is_err());
}

#[test]
fn emit_do_cross_tenant_service_fails() {
    let mut value = serde_json::to_value(tenant()).unwrap();
    value["workers"][0]["durableObjectClasses"] = json!([{
        "className": "Counter", "uniqueKey": "a".repeat(64), "enableSql": true
    }]);
    value["workers"][0]["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "type":"durableObject","name":"C","className":"Counter",
            "serviceWorkerId":"w-foreign-tenant"
        }));
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    let params = runtime_params();
    assert!(render_tenant_config(&input(&tenant, &bs(), &params)).is_err());
}

/// MIG-13-r2: the `vrt-switch` swap-completion marker exists ONLY
/// when the waitUntil probe socket exists — it binds the probe
/// service in the probe's own private dir. The r1 regression emitted
/// `unix:<socket_dir>/vrt-switch.sock` for every DO render (relative
/// `socket_dir` + missing dir → workerd bind loop) and fell back to
/// `workers[0]` when no probe socket was configured.
#[test]
fn switch_marker_requires_waituntil_socket() {
    let tenant = tenant_with_do();
    let mut params = runtime_params();

    // Empty waituntil_socket -> NO marker at all.
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;
    assert!(out.contains("durableObjectNamespaces"), "{out}");
    assert!(
        !out.contains("vrt-switch"),
        "marker without a probe socket: {out}"
    );

    // Probe socket set -> marker lives in the SAME dir, bound to the
    // aggregate probe service — never a tenant worker service.
    params.waituntil_socket = "sock/vrt-wu.sock".into();
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;
    assert!(
        out.contains(
            "(name = \"vrt-switch\", address = \"unix:sock/vrt-sw.sock\", http = (), service = \"vrt-wu-probe\")"
        ),
        "{out}"
    );
    // No worker-service fallback may appear anywhere.
    assert!(
        !out.contains("vrt-sw.sock\", http = (), service = \"worker-"),
        "marker bound to a tenant worker: {out}"
    );
    // The probe carries emit-site budgets: thin cpu/mem and a
    // subRequests bound at 2× the tenant-worker fan-out.
    let probe = out
        .split("(name = \"vrt-wu-probe\", worker = (")
        .nth(1)
        .unwrap_or_else(|| panic!("probe worker missing:\n{out}"));
    let probe_block = probe.split("    )),").next().unwrap();
    assert!(
        probe_block.contains("limits = (cpuMs = 2000, memoryMb = 128, subRequests = "),
        "probe carries the fan-out dispatch tier:\n{probe_block}"
    );
}

/// The marker's dir tracks the probe socket's dir exactly — a nested
/// probe dir yields the marker beside it, never at `socket_dir`.
#[test]
fn switch_marker_shares_probe_dir() {
    let tenant = tenant_with_do();
    let mut params = runtime_params();
    params.waituntil_socket = "gen-9/sock/vrt-wu.sock".into();
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;
    assert!(
        out.contains("unix:gen-9/sock/vrt-sw.sock"),
        "marker must live in the probe dir: {out}"
    );
    assert!(
        !out.contains("unix:sock/vrt-sw.sock"),
        "marker must not use socket_dir: {out}"
    );
}

/// MIG-13-r4: the marker file name must never be longer than the
/// probe socket file name — both live in the same private dir, so
/// "probe fits the AF_UNIX sun_path budget ⇒ marker fits" holds by
/// construction (compile-time twin: the const assert next to
/// `SWITCH_MARKER_FILE`; `vrt-switch.sock` was 4 bytes longer than
/// `vrt-wu.sock` and failed deploys the probe still passed).
#[test]
fn switch_marker_file_not_longer_than_probe_file() {
    // The probe file is `<enc(PROBE_SOCKET_NAME)>.sock` — for
    // "vrt-wu" the segment encoder is the identity, so this bound is
    // exact, not a lower bound.
    let probe_file = format!("{}.sock", vrt_config::waituntil::PROBE_SOCKET_NAME);
    assert!(
        vrt_config::capnp::SWITCH_MARKER_FILE.len() <= probe_file.len(),
        "marker file {} longer than probe file {probe_file}",
        vrt_config::capnp::SWITCH_MARKER_FILE
    );
}
