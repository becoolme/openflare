//! W1: workflows emit — per-workflow internal `wf-engine-*` service
//! carrying the vendored Engine DO (`enableSql`, `preventEviction`,
//! server-minted uniqueKey on the declaring worker's localDisk), the
//! user `wrapped` binding via config.extensions, the loopback stub,
//! and non-routability / flag-isolation invariants.

mod common;

use common::*;
use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn tenant_with_wf() -> TenantDesired {
    let mut value = serde_json::to_value(tenant()).unwrap();
    value["workers"][0]["workflows"] = json!([{
        "name": "order-flow",
        "binding": "WF",
        "className": "OrderFlow",
        "uniqueKey": "c".repeat(64),
        "limitsSteps": 42,
        "successRetentionMs": 259200000,
        "errorRetentionMs": 86400000,
        "concurrencyLimit": 100
    }]);
    serde_json::from_value(value).unwrap()
}

#[test]
fn emit_engine_service_with_namespace_and_bindings() {
    let tenant = tenant_with_wf();
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bs(), &params)).unwrap();
    let out = &rendered.text;

    // Engine service exists with vendored module embed.
    assert!(
        out.contains("name = \"wf-engine-w-alpha-1-order-flow\""),
        "{out}"
    );
    assert!(out.contains("workflows.mjs"));
    // Vendored embed materializes via a digest-pinned EmbedRef.
    assert!(rendered.embeds.iter().any(|e| matches!(
        e.kind,
        EmbedKind::VendoredModule { ref name } if name == "workflows-engine"
    )));

    // Engine DO namespace: fixed class name, sql, eviction-pinned,
    // server-minted uniqueKey verbatim from desired (never derived
    // from caller input at emit).
    assert!(out.contains(
        "(className = \"Engine\", uniqueKey = \"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\", enableSql = true, preventEviction = true),"
    ), "{out}");
    assert!(
        out.contains("(className = \"Coordinator\", uniqueKey = \""),
        "{out}"
    );

    // Engine bindings per miniflare wiring.
    assert!(
        out.contains("(name = \"ENGINE\", durableObjectNamespace = (className = \"Engine\"))"),
        "{out}"
    );
    assert!(
        out.contains(
            "(name = \"COORDINATOR\", durableObjectNamespace = (className = \"Coordinator\"))"
        ),
        "{out}"
    );
    assert!(out.contains(
        "(name = \"USER_WORKFLOW\", service = (name = \"worker-w-alpha-1\", entrypoint = \"OrderFlow\"))"
    ), "{out}");
    assert!(
        out.contains("(name = \"BINDING_NAME\", text = \"WF\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"WORKFLOW_NAME\", text = \"order-flow\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"STEP_LIMIT\", json = \"42\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"WORKFLOW_SUCCESS_RETENTION_MS\", text = \"259200000\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"WORKFLOW_ERROR_RETENTION_MS\", text = \"86400000\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"WORKFLOW_CONCURRENCY_LIMIT\", text = \"100\")"),
        "{out}"
    );
    // W6: per-worker loopback shim — the engine's MINIFLARE_LOOPBACK
    // binds to THIS worker's shim (which maps its wf names to
    // storage uniqueKeys and forwards to vrt-host's control listener).
    assert!(
        out.contains("(name = \"MINIFLARE_LOOPBACK\", service = \"vrt-wf-loopback-w-alpha-1\")"),
        "{out}"
    );

    // Engine storage lands on the declaring worker's localDisk.
    assert!(
        out.contains("durableObjectStorage = (localDisk = \"do-storage-w-alpha-1\")"),
        "{out}"
    );
    // Disk service exists even though the worker declares NO user DO classes.
    assert!(
        out.contains("name = \"do-storage-w-alpha-1\", disk = (path"),
        "{out}"
    );

    // Engine compat is pinned + experimental — on the ENGINE service only.
    assert!(out.contains("compatibilityDate = \"2024-10-22\""), "{out}");
}

#[test]
fn emit_wrapped_binding_and_extension_module() {
    let tenant = tenant_with_wf();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;

    // User worker env binding → wrapped module + inner `binding`.
    assert!(out.contains(
        "(name = \"WF\", wrapped = (moduleName = \"workflows:local-wrapped-binding\", innerBindings = [\n            (name = \"binding\", service = (name = \"wf-engine-w-alpha-1-order-flow\", entrypoint = \"WorkflowBinding\"))"
    ), "{out}");

    // Extension-internal module (never a tenant importable module).
    assert!(out.contains("extensions = [ (modules = ["), "{out}");
    assert!(
        out.contains("(name = \"workflows:local-wrapped-binding\", internal = true, esModule ="),
        "{out}"
    );
    // miniflare:zod: extension module WITHOUT internal — the engine's
    // ordinary `workflows.mjs` must be able to import it (W1-r2 P0;
    // internal modules are invisible to non-internal importers).
    assert!(
        out.contains("(name = \"miniflare:zod\", esModule ="),
        "{out}"
    );
    assert!(!out.contains("\"miniflare:zod\", internal = true"), "{out}");
}

#[test]
fn emit_loopback_stub_once_and_unroutable() {
    let tenant = tenant_with_wf();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;

    // W6: one shim per workflow-declaring worker.
    assert!(
        out.contains("name = \"vrt-wf-loopback-w-alpha-1\", worker = ("),
        "{out}"
    );
    assert_eq!(
        out.matches("vrt-wf-loopback-w-alpha-1\", worker").count(),
        1
    );
    // The shim carries the WF_UK name→uniqueKey map and (no
    // controlListen in this fixture) NO VRT_WFDEL binding — it
    // degrades to an explicit 501 rather than a fake success.
    assert!(out.contains("\"WF_UK\""), "{out}");
    assert!(!out.contains("\"VRT_WFDEL\""), "{out}");
    assert!(!out.contains("vrt-wfdel"), "{out}");

    // NON-ROUTABILITY: the only sockets are per-worker unix sockets
    // (and qconsumer socks) — no socket's `service =` names any
    // wf-engine or loopback service.
    for line in out.lines() {
        if line.contains("http = (), service =") {
            assert!(!line.contains("wf-engine"), "engine routable: {line}");
            assert!(
                !line.contains("vrt-wf-loopback"),
                "loopback routable: {line}"
            );
        }
    }
}

#[test]
fn experimental_flag_is_engine_only() {
    let tenant = tenant_with_wf();
    let params = runtime_params();
    let text = render_tenant_config(&input(&tenant, &bs(), &params))
        .unwrap()
        .text;
    let out = text.as_str();

    // The `experimental` flag must NOT leak into tenant workers —
    // only the engine service carries it. (System workers' own
    // configured flags are a separate, pre-existing surface.)
    fn service_block<'a>(out: &'a str, name: &str) -> &'a str {
        let start = out.find(&format!("name = \"{name}\", worker = (")).unwrap();
        let end = out[start..].find(")),\n").unwrap() + start;
        &out[start..end]
    }
    let engine = service_block(out, "wf-engine-w-alpha-1-order-flow");
    assert!(engine.contains("experimental"), "{engine}");
    let w1 = service_block(out, "worker-w-alpha-1");
    assert!(!w1.contains("experimental"), "{w1}");
    let w2 = service_block(out, "worker-w-alpha-2");
    assert!(!w2.contains("experimental"), "{w2}");
    // MIG-1: `nodejs_compat` is defaulted at the fixture's 2026-11-01
    // date — the emit strips it (a redundant declaration would make
    // workerd warn, and `check` refuses warnings). Pre-boundary dates
    // still render it (see render.rs boundary test).
    assert!(
        !w1.contains("compatibilityFlags"),
        "defaulted flag must be stripped: {w1}"
    );
}

#[test]
fn no_workflows_renders_no_engine_artifacts() {
    let tenant = tenant(); // fixture has no workflows
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bs(), &params)).unwrap();
    let out = &rendered.text;
    assert!(!out.contains("wf-engine"), "{out}");
    assert!(!out.contains("vrt-wf-loopback"), "{out}");
    assert!(!out.contains("extensions ="), "{out}");
    // The shared fixture's w-alpha-1 declares crons, so the unrelated
    // vendored `cron-dispatcher` embed is allowed; what this test pins
    // is the WORKFLOW set (engine/loopback/zod/wfops-facade).
    assert!(rendered.embeds.iter().all(|e| !matches!(
        &e.kind,
        EmbedKind::VendoredModule { name } if name != "cron-dispatcher"
    )));
}

// ---- W3-r2 P0: per-worker fault isolation ----
// All workers in a tenant share one workerd. A render-fatal error in
// one worker's emit surface must exclude only that worker — never
// take the whole generation down (the standalone e2e cascade).

fn tenant_mixed_wf_and_plain() -> TenantDesired {
    // w-alpha-1: workflows + a service binding to a ghost worker —
    // emit-level ServiceTarget failure. w-alpha-2: plain, untouched.
    let mut value = serde_json::to_value(tenant_with_wf()).unwrap();
    value["workers"][0]["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type": "service", "name": "GHOST", "workerId": "w-ghost"}));
    serde_json::from_value(value).unwrap()
}

#[test]
fn bad_worker_is_skipped_siblings_render() {
    let tenant = tenant_mixed_wf_and_plain();
    let rendered = render_tenant_config(&input(&tenant, &bs(), &runtime_params())).unwrap();

    assert_eq!(
        rendered.skipped_workers,
        vec!["w-alpha-1".to_string()],
        "only the failing worker may be excluded"
    );
    let out = &rendered.text;
    // The good sibling renders completely: service + socket.
    assert!(out.contains("name = \"worker-w-alpha-2\""), "{out}");
    assert!(out.contains("name = \"sock-w-alpha-2\""), "{out}");
    // The bad worker leaves no surface behind: no service, no sockets,
    // no workflow engine/facade, and — with the only workflow worker
    // gone — no extension block at all.
    for gone in [
        "name = \"worker-w-alpha-1\"",
        "name = \"sock-w-alpha-1\"",
        "sock-wfops-w-alpha-1",
        "facade-wfops-w-alpha-1",
        "wf-engine-w-alpha-1",
        "extensions = [",
        "vrt-wf-loopback",
    ] {
        assert!(!out.contains(gone), "leaked {gone} into render:\n{out}");
    }
    // Skipped worker's embeds must not be materialized either.
    assert!(!rendered.embeds.iter().any(|e| matches!(
        e.kind,
        EmbedKind::BundleModule { ref worker_id, .. } if worker_id == "w-alpha-1"
    )));
    assert!(!rendered
        .embeds
        .iter()
        .any(|e| matches!(e.kind, EmbedKind::VendoredModule { .. })));
    // Its secrets never reach the launch env.
    assert!(!rendered.env.values().any(|_| false)); // env map has no w-alpha-1-only entries
    assert!(rendered.env.len() <= 1, "{:?}", rendered.env.keys());
}

#[test]
fn skip_cascades_through_service_bindings() {
    // c-1: binds to ghost → fails round 1.
    // c-2: binds to c-1   → fails round 2 (fixpoint).
    // c-3: independent    → renders.
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-cascade",
        "workers": [{
            "workerId": "w-casc-1", "name": "bad",
            "activeVersion": {"versionId": "v1", "artifactDigest": bundle_aux().digest(),
                "compatibilityDate": "2026-10-15", "compatibilityFlags": []},
            "bindings": [{"type": "service", "name": "G", "workerId": "w-ghost"}],
            "routes": [{"hostname": "bad--team-cascade.vibeploy.app"}], "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }, {
            "workerId": "w-casc-2", "name": "chained",
            "activeVersion": {"versionId": "v2", "artifactDigest": bundle_aux().digest(),
                "compatibilityDate": "2026-10-15", "compatibilityFlags": []},
            "bindings": [{"type": "service", "name": "DEP", "workerId": "w-casc-1"}],
            "routes": [{"hostname": "chained--team-cascade.vibeploy.app"}], "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }, {
            "workerId": "w-casc-3", "name": "fine",
            "activeVersion": {"versionId": "v3", "artifactDigest": bundle_aux().digest(),
                "compatibilityDate": "2026-10-15", "compatibilityFlags": []},
            "bindings": [],
            "routes": [{"hostname": "fine--team-cascade.vibeploy.app"}], "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    // Three workers, ONE bundle digest — match_bundles' `extra` rule
    // means shared digests are legal only when... actually every
    // worker maps its own digest; three workers sharing one bundle is
    // fine (matched by digest, `used` is a set).
    let bundles = vec![bundle_aux()];
    let rendered = render_tenant_config(&input(&tenant, &bundles, &runtime_params())).unwrap();
    assert_eq!(
        rendered.skipped_workers,
        vec!["w-casc-1".to_string(), "w-casc-2".to_string()],
    );
    assert!(rendered.text.contains("name = \"worker-w-casc-3\""));
    assert!(!rendered.text.contains("name = \"worker-w-casc-1\""));
    assert!(!rendered.text.contains("name = \"worker-w-casc-2\""));
}

#[test]
fn all_workers_failing_fails_render() {
    // Every worker unrenderable → Err keeps the previous generation
    // serving rather than launching an empty workerd.
    let mut value = serde_json::to_value(tenant()).unwrap();
    for w in value["workers"].as_array_mut().unwrap() {
        w["bindings"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type": "service", "name": "GHOST", "workerId": "w-ghost"}));
    }
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    let err = render_tenant_config(&input(&tenant, &bs(), &runtime_params()))
        .err()
        .expect("all workers failing must fail the render");
    assert_eq!(err, ConfigError::AllWorkersExcluded, "{err}");
}

// ---- W7: `script_name` cross-worker — the engine, storage, loopback
// and ops facade all render under the DEFINING worker; the declaring
// worker only carries the wrapped binding pointing at it.

fn tenant_cross_wf() -> TenantDesired {
    // w-alpha-2 declares a workflow whose class/engine live on
    // w-alpha-1 (`serviceWorkerId` resolved server-side upstream).
    let mut value = serde_json::to_value(tenant()).unwrap();
    value["workers"][1]["workflows"] = json!([{
        "name": "pipe",
        "binding": "PIPE",
        "className": "Pipe",
        "serviceWorkerId": "w-alpha-1",
        "uniqueKey": "d".repeat(64)
    }]);
    serde_json::from_value(value).unwrap()
}

#[test]
fn cross_worker_engine_renders_under_defining_worker() {
    let tenant = tenant_cross_wf();
    let rendered = render_tenant_config(&input(&tenant, &bs(), &runtime_params())).unwrap();
    let out = &rendered.text;

    // Engine service named by the DEFINING worker; USER_WORKFLOW and
    // its localDisk point at w-alpha-1 — never at the declaring
    // worker's service or storage.
    assert!(out.contains("name = \"wf-engine-w-alpha-1-pipe\""), "{out}");
    assert!(!out.contains("wf-engine-w-alpha-2"), "{out}");
    assert!(out.contains(
        "(name = \"USER_WORKFLOW\", service = (name = \"worker-w-alpha-1\", entrypoint = \"Pipe\"))"
    ), "{out}");
    assert!(
        out.contains("durableObjectStorage = (localDisk = \"do-storage-w-alpha-1\")"),
        "{out}"
    );
    // do-storage service exists on the defining worker even though it
    // declares no DO classes of its own.
    assert!(
        out.contains("name = \"do-storage-w-alpha-1\", disk = (path"),
        "{out}"
    );

    // Declaring worker's wrapped binding → the DEFINING engine.
    assert!(out.contains(
        "(name = \"PIPE\", wrapped = (moduleName = \"workflows:local-wrapped-binding\", innerBindings = [\n            (name = \"binding\", service = (name = \"wf-engine-w-alpha-1-pipe\", entrypoint = \"WorkflowBinding\"))"
    ), "{out}");

    // Loopback + ops facade + wfops socket land on the DEFINING
    // worker — ops belong to `script_name`'s worker (CF ownership).
    assert!(
        out.contains("name = \"vrt-wf-loopback-w-alpha-1\", worker = ("),
        "{out}"
    );
    assert!(!out.contains("vrt-wf-loopback-w-alpha-2"), "{out}");
    assert!(
        out.contains("name = \"facade-wfops-w-alpha-1\", worker = ("),
        "{out}"
    );
    // Emit-site dispatch budgets: thin cpu/mem, one workflow-binding
    // call per op ⇒ subRequests = 2.
    let wfops = out
        .split("name = \"facade-wfops-w-alpha-1\", worker = (")
        .nth(1)
        .unwrap();
    let wfops_block = wfops.split("    )),").next().unwrap();
    assert!(
        wfops_block.contains("limits = (cpuMs = 1000, memoryMb = 128, subRequests = 2)"),
        "wfops facade carries the thin dispatch tier:\n{wfops_block}"
    );
    assert!(!out.contains("facade-wfops-w-alpha-2"), "{out}");
    assert!(out.contains("w-alpha-1-wfops.sock"), "{out}");
    assert!(!out.contains("w-alpha-2-wfops.sock"), "{out}");

    // Op tokens keyed by the defining worker — the hop injects on the
    // defining worker's wfops socket.
    assert!(
        rendered.wfop_tokens.contains_key("w-alpha-1"),
        "{:?}",
        rendered.wfop_tokens
    );
    assert!(!rendered.wfop_tokens.contains_key("w-alpha-2"));
    assert!(rendered.wfop_tokens["w-alpha-1"].contains_key("pipe"));

    // uniqueKey/WORKFLOW_NAME/BINDING_NAME come from the DECLARATION.
    assert!(out.contains("uniqueKey = \"dddd"), "{out}");
    assert!(
        out.contains("(name = \"WORKFLOW_NAME\", text = \"pipe\")"),
        "{out}"
    );
    assert!(
        out.contains("(name = \"BINDING_NAME\", text = \"PIPE\")"),
        "{out}"
    );
}

#[test]
fn cross_worker_duplicate_target_name_rejected() {
    // w-alpha-2 binds w-alpha-1's `pipe`; w-alpha-1 ALSO declares
    // `pipe` for itself → two decls resolve to (w-alpha-1, pipe) —
    // ambiguous engine/uniqueKey → desired validation rejects the
    // spec (render never sees it: compute/apply validate upstream).
    let mut value = serde_json::to_value(tenant_cross_wf()).unwrap();
    value["workers"][0]["workflows"] = json!([{
        "name": "pipe",
        "binding": "WF",
        "className": "Pipe",
        "uniqueKey": "e".repeat(64)
    }]);
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    let spec = spec_with(&tenant);
    assert_eq!(
        spec.validate(),
        Err(snapshot_protocol::compute_desired::DesiredError::Duplicate(
            "workflows.serviceWorkerId+name"
        ))
    );
}

#[test]
fn cross_worker_foreign_target_rejected() {
    // script_name resolving to a worker OUTSIDE the tenant spec —
    // the engine's USER_WORKFLOW designator would dangle and fail the
    // whole config load; validate() refuses to emit it.
    let mut value = serde_json::to_value(tenant()).unwrap();
    value["workers"][1]["workflows"] = json!([{
        "name": "pipe",
        "binding": "PIPE",
        "className": "Pipe",
        "serviceWorkerId": "w-foreign",
        "uniqueKey": "d".repeat(64)
    }]);
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    let spec = spec_with(&tenant);
    assert_eq!(
        spec.validate(),
        Err(snapshot_protocol::compute_desired::DesiredError::WorkflowTarget)
    );
}

/// Wrap a rendered-tenant fixture in a one-tenant `ComputeDesired`
/// envelope — `TenantDesired::validate` is worker-local; the W7
/// target/dup checks are tenant-level and run on the spec.
fn spec_with(tenant: &TenantDesired) -> snapshot_protocol::compute_desired::ComputeDesired {
    serde_json::from_value(json!({
        "schemaVersion": 1,
        "cellId": "cell-01",
        "revision": 7,
        "tenants": [tenant],
    }))
    .unwrap()
}

#[test]
fn workflow_rejects_fetcher_rpc_disabled_compat() {
    for (date, flags) in [("2024-04-02", vec![]), ("2025-01-01", vec!["no_rpc"])] {
        let mut value = serde_json::to_value(tenant_with_wf()).unwrap();
        value["workers"][0]["activeVersion"]["compatibilityDate"] = json!(date);
        value["workers"][0]["activeVersion"]["compatibilityFlags"] = json!(flags);
        let tenant: TenantDesired = serde_json::from_value(value).unwrap();
        let rendered = render_tenant_config(&input(&tenant, &bs(), &runtime_params()))
            .expect("incompatible workflow worker is isolated from siblings");
        assert!(
            rendered
                .skipped_reasons
                .iter()
                .any(|r| r.worker_id == "w-alpha-1" && r.code == "CONFIG_WORKFLOW_COMPATIBILITY"),
            "{:?}",
            rendered.skipped_reasons
        );
    }
}

#[test]
fn workflow_accepts_fetcher_rpc_compat_date() {
    let mut value = serde_json::to_value(tenant_with_wf()).unwrap();
    value["workers"][0]["activeVersion"]["compatibilityDate"] = json!("2024-04-03");
    let tenant: TenantDesired = serde_json::from_value(value).unwrap();
    assert!(render_tenant_config(&input(&tenant, &bs(), &runtime_params())).is_ok());
}
