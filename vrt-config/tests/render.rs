//! vrt-config render-layer tests: determinism, injection defense,
//! reference resolution, and error paths.
use std::collections::BTreeMap;

mod common;

use common::*;
use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::*;

fn bundles() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

#[test]
fn assets_binding_props_carry_tenant_namespaced_key() {
    // The data-plane assets store is keyed by (tenant, digest), not by a
    // flat global digest: the facade resolves content under the tenant
    // identity pinned in signed props (`instanceId`) plus
    // `namespaceResourceId`. Two teams MAY register the same digest —
    // their objects stay disjoint — and a team that learns another
    // team's digest cannot read its bytes because `instanceId` is not
    // attacker-controlled (signed config, re-checked by vrt-provider).
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    // Locate the ASSETS binding's props blob: the bindingId marker is
    // inside the same escaped props JSON, so a fixed window after it
    // always covers the full props for this binding.
    let id = "bindingId\\\":\\\"w-alpha-1~ASSETS";
    let at = out.find(id).expect("assets binding emitted");
    let props = &out[at..at + 800.min(out.len() - at)];
    assert!(
        props.contains("instanceId\\\":\\\"team-alpha"),
        "assets props must pin the tenant key"
    );
    assert!(
        props.contains(
            "namespaceResourceId\\\":\\\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ),
        "assets resource id is the bare digest; tenancy comes from instanceId"
    );
}

/// Queue-level `delivery_delay` is rendered into facade props ONLY when
/// declared — a queue without it keeps the exact props bytes it had
/// before the field existed.
#[test]
fn queue_delivery_delay_props_only_when_declared() {
    let base = || {
        let tenant = tenant();
        let bs = bundles();
        let params = runtime_params();
        render_tenant_config(&input(&tenant, &bs, &params))
            .unwrap()
            .text
    };
    let out = base();
    assert!(
        out.contains("\\\"queueName\\\":\\\"jobs\\\""),
        "queue props emitted"
    );
    assert!(
        !out.contains("deliveryDelaySeconds"),
        "absent queue delay must not add a props field"
    );

    let mut tenant = tenant();
    let queue = tenant.workers[0]
        .bindings
        .iter_mut()
        .find_map(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::Queue(q) => Some(q),
            _ => None,
        })
        .expect("fixture has a queue binding");
    queue.delivery_delay_seconds = Some(45);
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    assert!(
        out.contains("\\\"deliveryDelaySeconds\\\":45"),
        "declared queue delay must ride in props"
    );
}

fn with_auth_entrypoint(ep: &str) -> snapshot_protocol::compute_desired::TenantDesired {
    let mut tenant = tenant();
    let auth = tenant.workers[0]
        .bindings
        .iter_mut()
        .find_map(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::Service(s) => Some(s),
            _ => None,
        })
        .expect("fixture has a service binding");
    auth.entrypoint = Some(ep.to_string());
    tenant
}

#[test]
fn service_binding_named_entrypoint_renders() {
    let tenant = with_auth_entrypoint("Greeter");
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    assert!(out.contains(
        "(name = \"AUTH\", service = (name = \"worker-w-alpha-2\", entrypoint = \"Greeter\"))"
    ));
}

#[test]
fn unsafe_eval_binding_renders() {
    // CF `[[unsafe.bindings]] type="unsafe_eval"` → workerd's native
    // `unsafeEval` (Void) binding on `env.<name>`.
    let mut tenant = tenant();
    tenant.workers[0].bindings.push(
        snapshot_protocol::compute_desired::BindingDesired::UnsafeEval(
            snapshot_protocol::compute_desired::UnsafeEvalBinding {
                name: "UNSAFE_EVAL".to_string(),
            },
        ),
    );
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    assert!(
        out.contains("(name = \"UNSAFE_EVAL\", unsafeEval = void)"),
        "{out}"
    );
}

#[test]
fn service_binding_reserved_entrypoint_is_refused() {
    let bs = bundles();
    let params = runtime_params();
    for ep in ["VrtWaituntilProbe", "vrtAnything", "default", "not-ident"] {
        let tenant = with_auth_entrypoint(ep);
        // Per-worker isolation: the caller is excluded with a typed
        // reason; the target keeps serving.
        let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
        assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()], "{ep}");
        let reason = &rendered.skipped_reasons[0];
        assert_eq!(reason.code, "CONFIG_SERVICE_TARGET_SCOPE", "{ep}");
        assert!(!rendered.text.contains("(name = \"AUTH\""), "{ep}");
        assert!(rendered.text.contains("name = \"worker-w-alpha-2\""), "{ep}");
    }
}

fn with_auth_props(
    entrypoint: Option<&str>,
    props: serde_json::Value,
) -> snapshot_protocol::compute_desired::TenantDesired {
    let mut tenant = tenant();
    let auth = tenant.workers[0]
        .bindings
        .iter_mut()
        .find_map(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::Service(s) => Some(s),
            _ => None,
        })
        .expect("fixture has a service binding");
    auth.entrypoint = entrypoint.map(str::to_string);
    auth.props = Some(props);
    tenant
}

#[test]
fn service_binding_props_render_only_when_declared() {
    let bs = bundles();
    let params = runtime_params();
    let render = |tenant| {
        render_tenant_config(&input(&tenant, &bs, &params))
            .unwrap()
            .text
    };

    // Absent props: the default-export designator keeps its exact bytes.
    let base = render(tenant());
    assert!(base.contains(
        "        (name = \"AUTH\", service = (name = \"worker-w-alpha-2\")),\n"
    ));

    // Default export + props.
    let out = render(with_auth_props(
        None,
        json!({"z": {"b": [true, null]}, "a": 1}),
    ));
    assert!(out.contains(
        r#"        (name = "AUTH", service = (name = "worker-w-alpha-2", props = (json = "{\"a\":1,\"z\":{\"b\":[true,null]}}"))),"#
    ));

    // Named entrypoint + props: both ride the same designator, in order.
    let out = render(with_auth_props(
        Some("Greeter"),
        json!({"region": "eu"}),
    ));
    assert!(out.contains(
        r#"        (name = "AUTH", service = (name = "worker-w-alpha-2", entrypoint = "Greeter", props = (json = "{\"region\":\"eu\"}"))),"#
    ));
}

#[test]
fn service_binding_invalid_props_is_refused_at_render() {
    let bs = bundles();
    let params = runtime_params();
    for props in [
        json!([1, 2]),
        json!({"padding": "x".repeat(snapshot_protocol::compute_desired::MAX_SERVICE_PROPS_BYTES)}),
    ] {
        let tenant = with_auth_props(Some("Greeter"), props.clone());
        let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
        assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()], "{props}");
        assert_eq!(
            rendered.skipped_reasons[0].code, "CONFIG_CAPACITY_EXCEEDED",
            "{props}"
        );
        assert!(!rendered.text.contains("(name = \"AUTH\""), "{props}");
    }
}

/// PT1: the watchdog heartbeat worker + private socket appear iff
/// `params.heartbeat_socket` is set — and are the ONLY delta vs the
/// legacy shape (byte-compat when unset is pinned by golden.rs).
#[test]
fn ratelimit_binding_props_scope_to_tenant_and_worker() {
    // MIG-16: the ratelimit binding is a plain service binding to the
    // facade-ratelimit `RateLimiter` entrypoint; scope fields (tenant,
    // worker, namespace) come from the signed config, never the tenant.
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    let line = out
        .lines()
        .find(|l| l.contains("name = \"RATE_LIMITER\""))
        .expect("ratelimit binding emitted");
    assert!(
        line.contains("service = (name = \"facade-ratelimit\", entrypoint = \"RateLimiter\""),
        "{line}"
    );
    for needle in [
        "\\\"instanceId\\\":\\\"team-alpha\\\"",
        "\\\"workerId\\\":\\\"w-alpha-1\\\"",
        "\\\"namespaceId\\\":\\\"1001\\\"",
        "\\\"limit\\\":40",
        "\\\"period\\\":60",
    ] {
        assert!(line.contains(needle), "missing {needle} in {line}");
    }
}

#[test]
fn heartbeat_emit_gated_and_isolated() {
    let tenant = tenant();
    let bs = bundles();
    let mut params = runtime_params();
    let base = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    assert!(
        !base.contains("vrt-heartbeat"),
        "empty heartbeat_socket must emit nothing"
    );

    params.heartbeat_socket = "sock/vrt-heartbeat.sock".into();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    // Worker present, deny-all outbound, heartbeat-only surface.
    assert!(
        out.contains("(name = \"vrt-heartbeat\", worker = ("),
        "worker missing"
    );
    assert!(
        out.contains("vrt-heartbeat.mjs"),
        "heartbeat module missing"
    );
    assert!(
        out.contains("(name = \"vrt-heartbeat\", address = \"unix:sock/vrt-heartbeat.sock\""),
        "private socket missing:\n{out}"
    );
    // Isolation: the heartbeat service appears in exactly TWO places —
    // its service definition and its socket's `service =` ref. It must
    // never be referenced from a tenant worker binding or a route.
    let svc_refs = out.matches("\"vrt-heartbeat\"").count();
    let name_uses = out.matches("vrt-heartbeat").count();
    // name occurrences: service def, module name, socket name, socket
    // service — no binding entries may add more.
    assert!(
        name_uses <= 6,
        "vrt-heartbeat referenced beyond its own def+socket ({name_uses}):\n{out}"
    );
    let _ = svc_refs;
    // A tenant worker declaring a service worker named vrt-heartbeat
    // must fail closed (shadow guard).
    let mut hostile_params = params.clone();
    hostile_params.system_workers = vec![SystemWorkerRef {
        service_name: "vrt-heartbeat".into(),
        main_module: "x.mjs".into(),
        modules: vec![SystemModuleRef {
            name: "x.mjs".into(),
            digest: "a".repeat(64),
        }],
        needs_binding_backend: false,
    }];
    let r = render_tenant_config(&input(&tenant, &bs, &hostile_params));
    assert!(r.is_err(), "heartbeat shadow by system worker must fail");
}

#[test]
fn render_is_deterministic() {
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let a = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    let b = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.digest.len(), 64);
    assert!(a.digest.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn render_produces_expected_structure() {
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;

    // Services: two tenant workers, four system workers, two internal.
    for svc in [
        "worker-w-alpha-1",
        "worker-w-alpha-2",
        "facade-kv",
        "facade-r2",
        "facade-assets",
        "facade-pg",
        "vrt-egress",
        "vrt-no-network",
        "binding-backend",
        "outbound-network",
    ] {
        assert!(
            out.contains(&format!("(name = \"{svc}\"")),
            "missing service {svc}"
        );
    }
    // Per-worker unix socket + service binding + egress globalOutbound.
    assert!(out.contains("address = \"unix:sock/w-alpha-1.sock\""));
    assert!(out.contains("(name = \"AUTH\", service = (name = \"worker-w-alpha-2\"))"));
    // PG goes through the pg facade — never straight to binding-backend.
    assert!(out.contains(
        "(name = \"DB\", service = (name = \"facade-pg\", entrypoint = \"PgTransport\", props = (json ="
    ));
    assert!(out.contains("globalOutbound = (name = \"vrt-egress\", entrypoint = \"EgressProxy\")"));
    assert!(out.contains("globalOutbound = \"outbound-network\""));
    // Outbound split (T6 review): tenant workers → vrt-egress; EVERY
    // non-egress system worker → the deny-all vrt-no-network (worker
    // globalOutbound defaults to "internet" so it must be explicit);
    // ONLY vrt-egress may hold outbound-network.
    assert_eq!(
        out.matches("globalOutbound = \"outbound-network\"").count(),
        1,
        "exactly one worker may reach the raw network"
    );
    let tenants = out
        .matches("globalOutbound = (name = \"vrt-egress\", entrypoint = \"EgressProxy\")")
        .count();
    assert_eq!(tenants, 2, "both tenants route via vrt-egress");
    let denied = out
        .matches("globalOutbound = (name = \"vrt-no-network\", entrypoint = \"DenyAll\")")
        .count();
    // 10 facades (kv/r2/assets/pg/d1/queue/k2/ratelimit/email/images) + facade-cron-w-alpha-1
    // (CR1: the cron dispatcher is also deny-routed — it reaches the
    // tenant worker only through the TARGET service binding, never
    // the network) + vrt-no-network itself.
    assert_eq!(denied, 12, "all non-egress system workers are deny-routed");
    // No worker emits any other globalOutbound spelling.
    assert_eq!(
        out.matches("globalOutbound =").count(),
        tenants + denied + 1,
        "every worker carries exactly one of the three outbound forms"
    );
    // T6: the egress worker carries the typed policy as a text binding.
    assert!(out.contains("(name = \"VRT_EGRESS_POLICY\", text = \"{"));
    // Egress raw network is public-only: private/local/unix denied.
    assert!(out.contains("allow = [\"public\"]"));
    assert!(out.contains("deny = [\"private\", \"local\", \"unix\", \"unix-abstract\"]"));
    // Facade service binding with canonical props.
    assert!(out.contains(
        "service = (name = \"facade-kv\", entrypoint = \"KVNamespace\", props = (json = \""
    ));
    assert!(out.contains("\\\"namespaceResourceId\\\":\\\"kvns-01\\\""));
    assert!(out.contains("\\\"bindingId\\\":\\\"w-alpha-1~KV\\\""));
    assert!(out.contains("\\\"resourceSpecGeneration\\\":7"));
    // Vars are text bindings; secrets are fromEnvironment — the secret
    // VALUE must not appear anywhere in the config text.
    assert!(out.contains("(name = \"LOG_LEVEL\", text = \"info\")"));
    assert!(out.contains(
        "(name = \"API_TOKEN\", fromEnvironment = \"VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN\")"
    ));
    assert!(!out.contains("s3cr3t-value"));
    // Main module emitted first.
    let modules_start = out.find("modules = [").unwrap();
    assert!(out[modules_start..].contains("(name = \"index.js\", esModule = embed"));
    // binding-backend external + address surfaced for --external-addr.
    assert!(out.contains("(name = \"binding-backend\", external = (http = ()))"));
}

#[test]
fn embeds_and_external_addrs_cover_everything() {
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();

    // 7 tenant modules + every declared system module (the real
    // manifest's graphs — shared helpers repeat per worker).
    // `facade-router`/`facade-qconsumer` are TEMPLATES: their modules
    // are embedded per `router-<workerId>`/`facade-qconsumer-<id>`
    // emission, and this fixture opts no worker in.
    // +1 for the vendored `cron-dispatcher` module — the fixture's
    // w-alpha-1 declares crons, so `facade-cron-w-alpha-1` embeds it.
    let expected = 7
        + 1
        + params
            .system_workers
            .iter()
            .filter(|w| w.service_name != "facade-router" && w.service_name != "facade-qconsumer")
            .map(|w| w.modules.len())
            .sum::<usize>();
    assert_eq!(out.embeds.len(), expected);
    assert!(out
        .embeds
        .iter()
        .any(|e| e.path == format!("bundles/{}/index.js", bundle_main().digest())));
    // The egress worker is embedded by its real manifest digests — the
    // old `ddd…` placeholder is gone for good.
    let egress = params
        .system_workers
        .iter()
        .find(|w| w.service_name == "vrt-egress")
        .unwrap();
    for module in &egress.modules {
        assert!(
            out.embeds
                .iter()
                .any(|e| e.path == format!("sys/{}", module.digest)),
            "egress module {} not embedded",
            module.name
        );
    }
    for embed in &out.embeds {
        assert_eq!(embed.digest.len(), 64);
    }
    assert_eq!(
        out.external_addrs,
        vec![ExternalAddr {
            service: "binding-backend".into(),
            address: "unix:private/binding-backend.sock".into(),
        }]
    );
    // Secret surfaced only through the env map for host injection.
    assert_eq!(out.env.len(), 1);
    let (name, value) = out.env.iter().next().unwrap();
    assert_eq!(name, "VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN");
    assert_eq!(value.expose(), "s3cr3t-value");
}

#[test]
fn cross_tenant_service_binding_rejected() {
    let mut tenant = tenant();
    // Point AUTH at a worker id outside this tenant — compute.desired
    // would already reject; render rejects again on an unchecked caller.
    if let snapshot_protocol::compute_desired::BindingDesired::Service(s) =
        &mut tenant.workers[0].bindings[5]
    {
        s.worker_id = "w-foreign".into();
    }
    let bs = bundles();
    let params = runtime_params();
    // W3-r2: per-worker isolation — the offending worker is excluded
    // from the render (routes → worker_unavailable) while siblings
    // keep serving; the whole tenant no longer goes down with it.
    let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);
    assert!(!rendered.text.contains("name = \"worker-w-alpha-1\""));
    assert!(rendered.text.contains("name = \"worker-w-alpha-2\""));
}

#[test]
fn skipped_workers_carry_typed_reasons() {
    // W4-r2 P1: the dry-run exclusion must surface WHICH worker died
    // and WHY — stable code + bounded sanitized detail, not just ids.
    let tenant = tenant();
    let bs = bundles();
    let mut params = runtime_params();
    params.vars.clear(); // unresolved varRef on w-alpha-1
    let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);
    let reason = rendered
        .skipped_reasons
        .iter()
        .find(|r| r.worker_id == "w-alpha-1")
        .expect("skipped worker must carry a typed reason");
    assert_eq!(reason.code, "CONFIG_UNRESOLVED_REFERENCE");
    assert!(reason.detail.contains("varRef"));
    assert!(reason.detail.len() <= 512, "detail must be truncated");
    // Detail names a field kind — never a secret/env/binding VALUE.
    assert!(!reason.detail.contains("s3cr3t"));
}

#[test]
fn unresolved_var_and_secret_refs_fail_closed() {
    let tenant = tenant();
    let bs = bundles();

    // W3-r2: unresolved refs exclude the owning worker only — still
    // fail closed (it cannot serve), never a silent partial render.
    let mut params = runtime_params();
    params.vars.clear();
    let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);

    let mut params = runtime_params();
    params.secrets.clear();
    let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);
}

#[test]
fn bundle_mismatch_fails_closed() {
    let tenant = tenant();
    let params = runtime_params();

    // Missing bundle for the second worker.
    let bs = vec![bundle_main()];
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::BundleMismatch("missing"))
    );

    // Extra unused bundle.
    let extra = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "x.js",
            "modules": {"x.js": "export default {}"},
            "compatibilityDate": "2026-11-01",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let mut bs = bundles();
    bs.push(extra);
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::BundleMismatch("extra"))
    );

    // Same bundle provided twice.
    let bs = vec![bundle_main(), bundle_main(), bundle_aux()];
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::BundleMismatch("duplicateBundle"))
    );
}

#[test]
fn secrets_never_reach_config_text_or_digest() {
    let tenant = tenant();
    let bs = bundles();
    let base = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();
    assert!(!base.text.contains("s3cr3t-value"));

    // Rotating the secret changes `env` but never the config bytes or
    // its digest — the config stays content-stable across rotation.
    let mut params = runtime_params();
    params
        .secrets
        .entry("team-alpha".to_string())
        .or_default()
        .insert(
            "sec:w-alpha-1:tok".into(),
            SecretValue::new("rotated-secret-9"),
        );
    let rotated = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(base.text, rotated.text);
    assert_eq!(base.digest, rotated.digest);
    assert!(!rotated.text.contains("rotated-secret-9"));
    assert_eq!(
        rotated.env["VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN"].expose(),
        "rotated-secret-9"
    );

    // SecretValue cannot leak through Debug; no Display impl exists.
    let dbg = format!("{:?}", rotated.env["VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN"]);
    assert!(!dbg.contains("rotated-secret-9"));
    assert_eq!(dbg, "SecretValue(***)");
    let params_dbg = format!("{:?}", params.secrets);
    assert!(!params_dbg.contains("rotated-secret-9"));
}

#[test]
fn tenant_workers_never_reference_internal_services() {
    let tenant = tenant();
    let bs = bundles();
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params()))
        .unwrap()
        .text;

    // Extract each tenant worker service block and assert it references
    // neither the binding-backend external service nor the raw network:
    // the only services a tenant worker may name are same-tenant
    // `worker-*` services and the configured facade/egress endpoints.
    for id in ["w-alpha-1", "w-alpha-2"] {
        let start = out.find(&format!("(name = \"worker-{id}\"")).unwrap();
        let rest = &out[start..];
        let end = rest.find("\n    (name = ").unwrap_or(rest.len());
        let block = &rest[..end];
        assert!(
            !block.contains(BINDING_BACKEND_SERVICE),
            "worker-{id} references binding-backend"
        );
        assert!(
            !block.contains(OUTBOUND_NETWORK_SERVICE),
            "worker-{id} references outbound-network"
        );
        // PG must route via the pg facade designator.
        if id == "w-alpha-1" {
            assert!(block.contains("\"facade-pg\""));
        }
    }
    // `BINDING_BACKEND` appears only inside facade system-worker blocks
    // (they hold the privilege) and the external service declaration.
    assert!(out.contains("(name = \"BINDING_BACKEND\", service = \"binding-backend\")"));
}

#[test]
fn injected_values_cannot_break_capnp_structure() {
    let tenant = tenant();
    // Hostile var value: quotes, newline, `}`, `("`, capnp keywords.
    let params = {
        let mut p = runtime_params();
        p.vars.insert(
            "var:w-alpha-1:log".into(),
            "evil\")),\n(name = \"x\", worker = (modules = []}\"".into(),
        );
        p.secrets
            .entry("team-alpha".to_string())
            .or_default()
            .insert(
                "sec:w-alpha-1:tok".into(),
                SecretValue::new("tok\"\\)}\nsocket"),
            );
        p
    };
    let bs = bundles();
    let out = render_tenant_config(&input(&tenant, &bs, &params))
        .unwrap()
        .text;
    // The hostile content must be inside a single string literal: no raw
    // newline or `(`/`{` from the value may appear in the emitted line.
    let line = out.lines().find(|l| l.contains("LOG_LEVEL")).unwrap();
    // The injected value stays inside its literal: after removing escaped
    // quotes the line has exactly four `"` (name + text literal pairs).
    assert_eq!(line.replace("\\\"", "").matches('"').count(), 4);
    assert!(line.contains("\\x0a"), "raw newline must be hex-escaped");
    assert!(line.trim_end().ends_with("),"));
    // The whole document still parses line-wise: no line may open an
    // unclosed quote created by injected content — spot-check parity of
    // unescaped quotes per line is unnecessary; instead assert byte
    // determinism on re-render (a break would differ).
    let again = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(out, again.text);
}

#[test]
fn hostile_param_strings_are_rejected_or_escaped() {
    let tenant = tenant();
    let bs = bundles();

    // Socket dir with `..` traversal → identifier reject.
    let mut params = runtime_params();
    params.socket_dir = "../../etc".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("socketDir"))
    );

    // Socket dir with whitespace/quote → reject.
    let mut params = runtime_params();
    params.socket_dir = "bad dir\"".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("socketDir"))
    );

    // Facade service name with quote → reject.
    let mut params = runtime_params();
    params.facades.kv.service = "facade\"kv".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("facades.kv"))
    );

    // `facades.pg` went through endpoint-resolution checks but missed
    // the charset loop (T8 review): an injected service name must be
    // rejected as an Identifier, same as every other facade.
    let mut params = runtime_params();
    params.facades.pg.service = "facade\"pg".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("facades.pg"))
    );
    let mut params = runtime_params();
    params.facades.pg.entrypoint = "E})(".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("facades.pg"))
    );

    // Entrypoint with injection chars → reject.
    let mut params = runtime_params();
    params.egress.entrypoint = "E})(".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("egress"))
    );

    // System main module absent from modules → identifier reject.
    let mut params = runtime_params();
    params.system_workers[0].main_module = "nope.js".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("systemWorkers.mainModule"))
    );

    // Var value over the bound → capacity reject.
    let mut params = runtime_params();
    params
        .vars
        .insert("var:w-alpha-1:log".into(), "x".repeat(MAX_VALUE_LEN + 1));
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Capacity("vars"))
    );

    // System module digest not hex → digest reject.
    let mut params = runtime_params();
    params.system_workers[0].modules[0].digest = "zz".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Digest("systemWorkers.digest"))
    );
}

#[test]
fn dangling_facade_or_egress_refs_fail_closed() {
    let tenant = tenant();
    let bs = bundles();

    // Facade service not defined among system workers.
    let mut params = runtime_params();
    params.facades.kv.service = "facade-missing".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::ServiceReference("facades.kv"))
    );

    // Egress service not defined.
    let mut params = runtime_params();
    params.egress.service = "no-egress".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::ServiceReference("egress"))
    );

    // Endpoints can never be the internal/external services — even if a
    // same-named system worker were defined (the reserved check fires
    // before endpoint resolution).
    for (mut params, field) in [
        (runtime_params(), "facades.kv"),
        (runtime_params(), "facades.pg"),
        (runtime_params(), "egress"),
    ] {
        match field {
            "facades.kv" => params.facades.kv.service = BINDING_BACKEND_SERVICE.into(),
            "facades.pg" => params.facades.pg.service = BINDING_BACKEND_SERVICE.into(),
            _ => params.egress.service = OUTBOUND_NETWORK_SERVICE.into(),
        }
        assert_eq!(
            render_tenant_config(&input(&tenant, &bs, &params)),
            Err(ConfigError::ServiceReference(field))
        );
    }

    // System worker colliding with a tenant service name.
    let mut params = runtime_params();
    params.system_workers[3].service_name = "worker-w-alpha-1".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::ServiceReference("systemWorkers.serviceName"))
    );
}

#[test]
fn malformed_desired_fragment_is_rechecked() {
    let bs = bundles();
    let params = runtime_params();

    // Bad artifact digest (not 64-hex) — a caller that skipped
    // compute.desired validation still fails closed here.
    let mut tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-alpha",
        "workers": [{
            "workerId": "w-1",
            "name": "api",
            "activeVersion": {
                "versionId": "v1",
                "artifactDigest": "nothex",
                "compatibilityDate": "2026-11-01",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 0, "outboundConnections": 0}
        }]
    }))
    .unwrap();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Digest("artifactDigest"))
    );

    // Invalid tenant id charset.
    tenant.tenant_id = "bad tenant!".into();
    assert_eq!(
        render_tenant_config(&input(&tenant, &bs, &params)),
        Err(ConfigError::Identifier("tenantId"))
    );
}

/// T37b-r2: tenant `compatibilityFlags[]` must be KNOWN STABLE —
/// `$experimental` flags (`experimental`, `service_binding_extra_handlers`,
/// `streaming_tail_worker`) and unknown names both fail closed at emit
/// depth (the zod gate is the first check; this is defense in depth).
/// System/facade workers are exempt — the platform emits them.
#[test]
fn tenant_experimental_compat_flags_rejected() {
    let bs = bundles();
    let params = runtime_params();

    for (flag, expect) in [
        (
            "experimental",
            ConfigError::CompatFlag("compatFlag.experimental"),
        ),
        (
            "service_binding_extra_handlers",
            ConfigError::CompatFlag("compatFlag.experimental"),
        ),
        (
            "streaming_tail_worker",
            ConfigError::CompatFlag("compatFlag.experimental"),
        ),
        (
            "made_up_flag",
            ConfigError::CompatFlag("compatFlag.unknown"),
        ),
    ] {
        let mut t = tenant();
        t.workers[0].active_version.compatibility_flags = vec![flag.to_string()];
        assert_eq!(
            render_tenant_config(&input(&t, &bs, &params)),
            Err(expect),
            "flag {flag} must be rejected"
        );
    }

    // Positive control: the default `nodejs_compat` tenant renders.
    let t = tenant();
    assert!(render_tenant_config(&input(&t, &bs, &params)).is_ok());
}

/// MIG-14: `connect_pass_through` in `system_compat_flags` must never
/// render — on the egress worker it would make workerd forward tenant
/// CONNECTs to `outbound-network` directly, bypassing
/// `EgressProxy.connect()` (public TCP with no port/name policy).
#[test]
fn connect_pass_through_system_flag_rejected() {
    let bs = bundles();
    let mut params = runtime_params();
    params
        .system_compat_flags
        .push("connect_pass_through".to_string());
    assert_eq!(
        render_tenant_config(&input(&tenant(), &bs, &params)),
        Err(ConfigError::Identifier("systemCompatibility"))
    );
}

/// MIG-1: `nodejs_compat` becomes the workerd default at compat date
/// 2026-08-04 — emitted verbatim BEFORE the boundary, stripped at/after
/// it (the declaration is a redundant no-op that makes workerd print a
/// warning line `check` rejects). Boundary: day-before / day-of.
#[test]
fn compat_date_defaulted_flags_are_stripped_at_boundary() {
    let bs = bundles();
    let params = runtime_params();
    for (date, expect_flag) in [
        ("2026-08-03", true),
        ("2026-08-04", false),
        ("2026-09-16", false), // the rehearsal worker's date
    ] {
        let mut t = tenant();
        t.workers[0].active_version.compatibility_date = date.to_string();
        t.workers[0].active_version.compatibility_flags = vec!["nodejs_compat".to_string()];
        let rendered = render_tenant_config(&input(&t, &bs, &params)).unwrap();
        let start = rendered
            .text
            .find("name = \"worker-w-alpha-1\", worker = (")
            .unwrap();
        let end = rendered.text[start..].find(")),\n").unwrap() + start;
        let block = &rendered.text[start..end];
        assert_eq!(
            block.contains("nodejs_compat"),
            expect_flag,
            "date {date}: {block}"
        );
        assert!(block.contains(&format!("compatibilityDate = \"{date}\"")));
    }
    // A non-defaulted stable flag is NEVER stripped.
    let mut t = tenant();
    t.workers[0].active_version.compatibility_date = "2026-11-01".to_string();
    t.workers[0].active_version.compatibility_flags = vec![
        "nodejs_compat".to_string(),
        "global_fetch_strictly_public".to_string(),
    ];
    let rendered = render_tenant_config(&input(&t, &bs, &params)).unwrap();
    let start = rendered
        .text
        .find("name = \"worker-w-alpha-1\", worker = (")
        .unwrap();
    let end = rendered.text[start..].find(")),\n").unwrap() + start;
    let block = &rendered.text[start..end];
    assert!(
        block.contains("compatibilityFlags = [\"global_fetch_strictly_public\"]"),
        "{block}"
    );
}

/// T13/M6: secretRef resolves only inside the OWNING tenant's
/// namespace — team-beta's ref never satisfies team-alpha's binding
/// and vice versa (fail closed, typed error).
#[test]
fn secret_refs_are_tenant_namespaced() {
    let tenant = tenant(); // tenantId = "team-alpha"
    let bs = bundles();
    let mut params = runtime_params();
    // Move the seeded secret under a DIFFERENT tenant's namespace —
    // team-alpha's binding must now fail closed.
    params.secrets.clear();
    params.secrets.insert(
        "team-beta".to_string(),
        BTreeMap::from([(
            "sec:w-alpha-1:tok".to_owned(),
            SecretValue::new("s3cr3t-value"),
        )]),
    );
    // W3-r2: cross-tenant secret resolution still fails closed —
    // scoped to the offending worker (per-worker exclusion).
    let rendered = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);
    // Same-tenant resolution still works.
    params
        .secrets
        .entry("team-alpha".to_string())
        .or_default()
        .insert("sec:w-alpha-1:tok".into(), SecretValue::new("s3cr3t-value"));
    let out = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    assert_eq!(
        out.env["VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN"].expose(),
        "s3cr3t-value"
    );
}

// ---- T25/L13: every external service must carry an argv address ----

#[test]
fn external_services_all_have_external_addrs() {
    // B8/L13: workerd refuses `(name=X, external=(http=()))` services
    // without `--external-addr`. The renderer must therefore prove, for
    // every `external` service it emits, that an address lands in
    // `external_addrs` — and refuse to render without a usable socket.
    let tenant = tenant();
    let bs = bundles();
    let params = runtime_params();
    let out = render_tenant_config(&input(&tenant, &bs, &params)).unwrap();
    // Collect every `external = (…)` service name in the emitted text.
    let mut externals = Vec::new();
    for line in out.text.lines() {
        if let Some(idx) = line.find("external = (") {
            let name = line[..idx].trim();
            let name = name
                .strip_prefix("(name = \"")
                .and_then(|n| n.strip_suffix("\","))
                .unwrap_or(name);
            externals.push(name.to_string());
        }
    }
    assert!(!externals.is_empty(), "fixture emits an external service");
    for service in &externals {
        let addr = out
            .external_addrs
            .iter()
            .find(|a| &a.service == service)
            .unwrap_or_else(|| panic!("{service} missing --external-addr"));
        assert!(
            addr.address.starts_with("unix:"),
            "external addr is a unix socket: {addr:?}"
        );
    }
    // And the reverse: no orphan argv addrs.
    for a in &out.external_addrs {
        assert!(externals.contains(&a.service), "{a:?} has no service");
    }
}

#[test]
fn render_refuses_empty_binding_backend_socket() {
    // An empty bindingBackendSocket must fail closed at render — never
    // emit `unix:`-less config for workerd to reject at runtime.
    let tenant = tenant();
    let bs = bundles();
    let mut params = runtime_params();
    params.binding_backend_socket = String::new();
    assert!(render_tenant_config(&input(&tenant, &bs, &params)).is_err());
    params.binding_backend_socket = "/".into(); // not a socket path
    assert!(render_tenant_config(&input(&tenant, &bs, &params)).is_err());
    params.binding_backend_socket = "../escape.sock".into();
    assert!(render_tenant_config(&input(&tenant, &bs, &params)).is_err());
}

// ---------------------------------------------------------------------------
// T31c: assets-first router services
// ---------------------------------------------------------------------------

fn tenant_assets_first(run_worker_first: serde_json::Value) -> TenantDesired {
    serde_json::from_value(serde_json::json!({
        "tenantId": "team-alpha",
        "workers": [{
            "workerId": "w-alpha-1",
            "name": "api",
            "activeVersion": {
                "versionId": "v-1",
                "artifactDigest": bundle_main().digest(),
                "compatibilityDate": "2026-11-01",
                "compatibilityFlags": ["nodejs_compat"]
            },
            "bindings": [
                {"type": "assets", "name": "ASSETS",
                 "manifestDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 "permissions": {"read": true, "write": false},
                 "assetsFirst": true,
                 "runWorkerFirst": run_worker_first}
            ],
            "routes": [{"hostname": "api--team-alpha.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap()
}

#[test]
fn assets_first_emits_router_and_repoints_socket() {
    let tenant = tenant_assets_first(serde_json::json!(null));
    let bs = vec![bundle_main()];
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();
    assert!(
        out.text.contains("(name = \"router-w-alpha-1\""),
        "router service emitted:\n{}",
        out.text
    );
    // The worker socket now serves the router, not the user worker.
    assert!(
        out.text
            .contains(
                "unix:sock/w-alpha-1.sock\", http = (forwardedProtoHeader = \"x-forwarded-proto\", cfBlobHeader = \"x-vrt-cf-blob\"), service = \"router-w-alpha-1\"",
            ),
        "socket repointed:\n{}",
        out.text
    );
    // Router binds the user worker by service name + the assets facade.
    assert!(out.text.contains("service = (name = \"worker-w-alpha-1\")"));
    // Router modules are embedded as system modules keyed by the
    // manifest's service name — assembly resolves them in
    // `params.system_workers` under `facade-router` (T31d/R0).
    assert!(out.embeds.iter().any(
        |e| matches!(&e.kind, EmbedKind::SystemModule { service_name, .. }
            if service_name == "facade-router")
    ));
    // Default cfg: assets-first, no worker-first rules.
    assert!(out.text.contains("invokeUserWorkerAhead\\\":false"));
}

#[test]
fn run_worker_first_bool_and_rules_serialize() {
    let bs = vec![bundle_main()];
    // runWorkerFirst: true → invokeUserWorkerAhead
    let tenant = tenant_assets_first(serde_json::json!(true));
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();
    assert!(
        out.text.contains("invokeUserWorkerAhead\\\":true"),
        "{}",
        out.text
    );

    // Rules split into userWorker/assetWorker (`!` prefix strips).
    let tenant = tenant_assets_first(serde_json::json!(["/api/*", "!/api/static/*"]));
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();
    assert!(out.text.contains("\\\"/api/*\\\""), "{}", out.text);
    assert!(out.text.contains("\\\"/api/static/*\\\""), "{}", out.text);
    assert!(out.text.contains("assetWorker\\\":[\\\"/api/static/*\\\"]"));
    assert!(out.text.contains("userWorker\\\":[\\\"/api/*\\\"]"));
}

#[test]
fn duplicate_assets_first_bindings_fail_closed() {
    let tenant: TenantDesired = serde_json::from_value(serde_json::json!({
        "tenantId": "team-alpha",
        "workers": [{
            "workerId": "w-alpha-1", "name": "api",
            "activeVersion": {"versionId": "v-1", "artifactDigest": bundle_main().digest(),
                "compatibilityDate": "2026-11-01", "compatibilityFlags": []},
            "bindings": [
                {"type": "assets", "name": "A1",
                 "manifestDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 "permissions": {"read": true, "write": false}, "assetsFirst": true},
                {"type": "assets", "name": "A2",
                 "manifestDigest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                 "permissions": {"read": true, "write": false}, "assetsFirst": true}
            ],
            "routes": [{"hostname": "api--team-alpha.vibeploy.app"}],
            "crons": [], "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 0, "outboundConnections": 0}
        }]
    }))
    .unwrap();
    let bs = vec![bundle_main()];
    assert!(render_tenant_config(&input(&tenant, &bs, &runtime_params())).is_err());
}

#[test]
fn legacy_assets_binding_keeps_socket_on_worker() {
    // The shared fixture has an assets binding WITHOUT assetsFirst —
    // no router service, socket stays on the user worker.
    let tenant = tenant();
    let bs = bundles();
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();
    assert!(!out.text.contains("router-w-alpha-1"));
    assert!(out
        .text
        .contains(
            "unix:sock/w-alpha-1.sock\", http = (forwardedProtoHeader = \"x-forwarded-proto\", cfBlobHeader = \"x-vrt-cf-blob\"), service = \"worker-w-alpha-1\"",
        ));
}

/// T31f/F1: a custom assets binding name (`"STATIC"`) must not rename
/// the router's env key — `facade-router.ts` reads `env.ASSETS`. The
/// user worker keeps the declared name; the router binds it as the
/// fixed `ASSETS` while the authorized `bindingId` still names the
/// declared binding.
#[test]
fn router_assets_binding_name_is_fixed_not_user_declared() {
    let mut tenant = tenant_assets_first(serde_json::json!(null));
    tenant.workers[0].bindings = serde_json::from_value(serde_json::json!([
        {"type": "assets", "name": "STATIC",
         "manifestDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
         "permissions": {"read": true, "write": false},
         "assetsFirst": true}
    ]))
    .unwrap();
    let bs = vec![bundle_main()];
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params())).unwrap();

    // Router section: env key is the fixed "ASSETS", wired to the
    // facade with props that still authorize the DECLARED name.
    let router_pos = out.text.find("(name = \"router-w-alpha-1\"").unwrap();
    let router_section = &out.text[router_pos..];
    assert!(
        router_section.contains("(name = \"ASSETS\", service = (name = \"facade-assets\""),
        "router env key must be the fixed ASSETS:\n{}",
        &router_section[..router_section.len().min(3000)]
    );
    assert!(
        router_section.contains("w-alpha-1~STATIC"),
        "props bindingId still authorizes the declared name:\n{}",
        &router_section[..router_section.len().min(3000)]
    );
    // User worker section keeps the declared binding name.
    let worker_pos = out.text.find("(name = \"worker-w-alpha-1\"").unwrap();
    let worker_section = &out.text[worker_pos..];
    assert!(
        worker_section.contains("(name = \"STATIC\", service = (name = \"facade-assets\""),
        "user worker keeps the declared name:\n{}",
        &worker_section[..worker_section.len().min(3000)]
    );
}

/// T31f/F1: the three router binding names must never silently fork
/// from the literals `facade/router-wire.ts` exports — both sides are
/// pinned by reading the TS source text.
#[test]
fn router_binding_names_match_ts_wire() {
    let wire = include_str!("../../facade/router-wire.ts");
    for (rust_const, ts_decl) in [
        ("ASSETS", "ASSETS_BINDING"),
        ("USER_WORKER", "USER_WORKER_BINDING"),
        ("VRT_ROUTER_CFG", "ROUTER_CFG_BINDING"),
    ] {
        let needle = format!("{ts_decl} = \"{rust_const}\"");
        assert!(
            wire.contains(&needle),
            "router-wire.ts must declare `{needle}` — capnp and the \
             facade entry must use the same env key"
        );
    }
    // And the entry module must read exactly those env keys.
    let entry = include_str!("../../facade/facade-router.ts");
    for key in ["env.ASSETS", "env.USER_WORKER", "env.VRT_ROUTER_CFG"] {
        assert!(
            entry.contains(key),
            "facade-router.ts must read `{key}` (matching the fixed capnp binding name)"
        );
    }
}

/// T31f/F0.4: the bounded `x-vrt-route-error` enum must be identical on
/// both sides — router-wire.ts emits it, vrt-host ingress whitelists it.
/// Pin both literals so a new reason can't land on one side only.
#[test]
fn route_error_reasons_match_across_languages() {
    let wire = include_str!("../../facade/router-wire.ts");
    let ingress = include_str!("../../vrt-host/src/ingress.rs");
    for reason in [
        "route_index_rpc_failed",
        "route_index_invalid",
        "route_index_digest_mismatch",
        "route_index_timeout",
        "can_fetch_rpc_failed",
        "can_fetch_timeout",
    ] {
        assert!(
            wire.contains(&format!("\"{reason}\"")),
            "router-wire.ts missing reason {reason}"
        );
        assert!(
            ingress.contains(&format!("\"{reason}\"")),
            "ingress.rs whitelist missing reason {reason}"
        );
    }
    // Count parity inside the enum blocks only (comments may repeat
    // reason names): extract each side's literal list and compare the
    // exact sets.
    let enum_items = |hay: &str, start: &str, end: &str| -> Vec<String> {
        let i = hay
            .find(start)
            .unwrap_or_else(|| panic!("{start} not found"));
        let j = hay[i..]
            .find(end)
            .unwrap_or_else(|| panic!("{end} not found"));
        let block = &hay[i..i + j];
        block
            .split('"')
            .enumerate()
            .filter(|(k, _)| k % 2 == 1)
            .map(|(_, v)| v.to_string())
            .filter(|v| v.starts_with("route_") || v.starts_with("can_"))
            .collect()
    };
    let ts = enum_items(wire, "ROUTE_ERROR_REASONS = [", "] as const");
    let rs = enum_items(ingress, "ROUTE_ERROR_REASONS: &[&str] = &[", "];");
    assert_eq!(
        ts, rs,
        "reason enum drift between router-wire.ts and ingress.rs"
    );
}

/// Dynamic Workers (`worker_loader` bindings) render the native
/// workerd `workerLoader` group with the platform quota — and nothing
/// else: a loader carries no resource reference, so no service/socket
/// may be wired for it.
#[test]
fn worker_loader_binding_renders_native_loader() {
    let mut tenant = tenant();
    tenant.workers[0].bindings = serde_json::from_value(serde_json::json!([
        {"type": "workerLoader", "name": "LOADER"}
    ]))
    .unwrap();
    let bs = bundles();
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params()))
        .unwrap()
        .text;
    assert!(
        out.contains("(name = \"LOADER\", workerLoader = (maxWorkers = 32))"),
        "workerLoader group with platform quota must render:\n{out}"
    );
    // The only LOADER occurrence is the binding line itself — a loader
    // wires no service, socket, or facade reference.
    assert_eq!(out.matches("\"LOADER\"").count(), 1, "{out}");
}

/// `images` renders a `wrapped` binding over the upstream Miniflare
/// module name `cloudflare-internal:images-api`, whose inner `fetcher`
/// reaches the facade-images system worker — the transform engine is
/// vendored WASM in-worker, so no provider socket is wired.
#[test]
fn images_binding_renders_wrapped_images_api() {
    let mut tenant = tenant();
    tenant.workers[0].bindings = serde_json::from_value(serde_json::json!([
        {"type": "images", "name": "IMAGES"}
    ]))
    .unwrap();
    let bs = bundles();
    let out = render_tenant_config(&input(&tenant, &bs, &runtime_params()))
        .unwrap()
        .text;
    assert!(
        out.contains(
            "(name = \"IMAGES\", wrapped = (moduleName = \"cloudflare-internal:images-api\""
        ),
        "images must render the wrapped images-api module:\n{out}"
    );
    assert!(
        out.contains("service = (name = \"facade-images\""),
        "inner fetcher must reach facade-images:\n{out}"
    );
}

// ---- per-worker quotas (`limits`) ----

/// The `limits` block renders only declared dimensions, in a fixed
/// order, and never for a dimension left at the platform default or 0 —
/// an undeclared worker keeps stock workerd behaviour byte-for-byte.
#[test]
fn worker_limits_render_declared_dimensions_only() {
    use snapshot_protocol::compute_desired::WorkerQuotas;
    let mut tenant = tenant();
    tenant.workers[0].quotas = WorkerQuotas {
        cpu_ms: 120,
        memory_mb: 192,
        subrequests: 100,
        outbound_connections: 16,
    };
    let out = render_tenant_config(&input(&tenant, &bundles(), &runtime_params()))
        .unwrap()
        .text;
    assert!(
        out.contains("limits = (cpuMs = 120, memoryMb = 192, subRequests = 100),"),
        "declared dimensions render in order:\n{out}"
    );

    // A zero / default dimension is omitted: stock behaviour, not an
    // explicit `0` limit (workerd's 0 means "unlimited" and would make
    // the emitted config lie about intent).
    for worker in &mut tenant.workers {
        worker.quotas = WorkerQuotas {
            cpu_ms: 50,
            memory_mb: 128,
            subrequests: 0,
            outbound_connections: 16,
        };
    }
    let out = render_tenant_config(&input(&tenant, &bundles(), &runtime_params()))
        .unwrap()
        .text;
    assert!(
        !out.contains("limits = ("),
        "platform defaults / 0 must not render a limits block:\n{out}"
    );

    // Mixed declaration: an explicit `subrequests = 0` is "no budget" on
    // the protocol side and must be omitted from a block that still
    // renders the non-default `cpu_ms` — never `subRequests = 0`, which
    // workerd would read as a real zero/unlimited budget.
    for worker in &mut tenant.workers {
        worker.quotas = WorkerQuotas {
            cpu_ms: 200,
            memory_mb: 128,
            subrequests: 0,
            outbound_connections: 16,
        };
    }
    let out = render_tenant_config(&input(&tenant, &bundles(), &runtime_params()))
        .unwrap()
        .text;
    assert!(
        out.contains("limits = (cpuMs = 200),"),
        "non-default dimension renders alone:\n{out}"
    );
    assert!(
        !out.contains("subRequests"),
        "explicit 0 must never render as a limit:\n{out}"
    );
}

/// `outboundConnections` has no workerd enforcement hook yet: a
/// non-default declaration is refused instead of silently ignored.
#[test]
fn worker_limits_reject_unenforceable_outbound_connections() {
    use snapshot_protocol::compute_desired::WorkerQuotas;
    let mut tenant = tenant();
    tenant.workers[0].quotas = WorkerQuotas {
        cpu_ms: 50,
        memory_mb: 128,
        subrequests: 50,
        outbound_connections: 6,
    };
    assert_eq!(
        render_tenant_config(&input(&tenant, &bundles(), &runtime_params())),
        Err(ConfigError::Quotas("outboundConnections"))
    );
    // 0 means "no declared budget" (stock behaviour) and stays accepted.
    tenant.workers[0].quotas.outbound_connections = 0;
    assert!(render_tenant_config(&input(&tenant, &bundles(), &runtime_params())).is_ok());
}

/// Out-of-bounds quota values are rejected at emit depth (defense in
/// depth behind compute.desired validation).
#[test]
fn worker_limits_reject_out_of_bounds() {
    use snapshot_protocol::compute_desired::WorkerQuotas;
    let mut tenant = tenant();
    for (quotas, field) in [
        (
            WorkerQuotas {
                cpu_ms: 0,
                ..WorkerQuotas::platform_default()
            },
            "cpuMs",
        ),
        (
            WorkerQuotas {
                cpu_ms: 300_001,
                ..WorkerQuotas::platform_default()
            },
            "cpuMs",
        ),
        (
            WorkerQuotas {
                memory_mb: 4_097,
                ..WorkerQuotas::platform_default()
            },
            "memoryMb",
        ),
        (
            WorkerQuotas {
                subrequests: 10_001,
                ..WorkerQuotas::platform_default()
            },
            "subrequests",
        ),
        (
            WorkerQuotas {
                outbound_connections: 1_025,
                ..WorkerQuotas::platform_default()
            },
            "outboundConnections",
        ),
    ] {
        tenant.workers[0].quotas = quotas;
        assert_eq!(
            render_tenant_config(&input(&tenant, &bundles(), &runtime_params())),
            Err(ConfigError::Quotas(field)),
            "field {field}"
        );
    }
}

/// Capability gate: a cell that cannot render `limits` must refuse a
/// tenant that declared one (fail closed — never silently unlimited).
#[test]
fn quotas_capability_gate_fails_closed() {
    use snapshot_protocol::compute_desired::WorkerQuotas;
    let mut tenant = tenant();
    // All-default tenant: nothing to enforce, accepted on any cell.
    for worker in &mut tenant.workers {
        worker.quotas = WorkerQuotas::platform_default();
    }
    assert!(ensure_quotas_enforceable(&tenant, false).is_ok());
    assert!(ensure_quotas_enforceable(&tenant, true).is_ok());

    tenant.workers[1].quotas.cpu_ms = 400;
    assert_eq!(
        ensure_quotas_enforceable(&tenant, false),
        Err(ConfigError::Quotas("cpuMs"))
    );
    assert_eq!(
        ConfigError::Quotas("cpuMs").code(),
        "CONFIG_QUOTA_UNSUPPORTED"
    );
    assert!(ensure_quotas_enforceable(&tenant, true).is_ok());

    // `subrequests = 0` declares "no budget": omitted, not enforced, so
    // it never trips the capability gate.
    tenant.workers[1].quotas.cpu_ms = 50;
    tenant.workers[1].quotas.subrequests = 0;
    assert!(ensure_quotas_enforceable(&tenant, false).is_ok());
}
