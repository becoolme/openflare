//! Shared fixtures for vrt-config integration tests.

use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use vrt_config::*;

pub fn bundle_main() -> VersionBundle {
    VersionBundle::build(DeployManifest::from_pack(&json!({
        "mainModule": "index.js",
        "modules": {
            "index.js": "export default {\n  async fetch(req, env, ctx) {\n    return env.AUTH.fetch(req);\n  }\n}",
            "lib/util.js": "export function helper() { return 42 }",
            "vendor/legacy.cjs": {"cjs": "module.exports = { v: 1 }"},
            "cfg.json": {"json": {"a": 1, "b": [2, 3]}},
            "blob.bin": {"data_b64": "AAECAwQ="},
            "mod.wasm": {"wasm_b64": "AGFzbQEAAAA="}
        },
        "compatibilityDate": "2026-11-01",
        "compatibilityFlags": ["nodejs_compat"]
    }))
    .unwrap())
    .unwrap()
}

pub fn bundle_aux() -> VersionBundle {
    VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "auth.js",
            "modules": {
                "auth.js": "export default { fetch() { return new Response('ok') } }"
            },
            "compatibilityDate": "2026-10-15",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap()
}

/// A tenant covering every binding kind, two workers, one cross-worker
/// service binding.
pub fn tenant() -> TenantDesired {
    serde_json::from_value(json!({
        "tenantId": "team-alpha",
        "workers": [{
            "workerId": "w-alpha-1",
            "name": "api",
            "activeVersion": {
                "versionId": "v-2026-11-08-1",
                "artifactDigest": bundle_main().digest(),
                "compatibilityDate": "2026-11-01",
                "compatibilityFlags": ["nodejs_compat"]
            },
            "bindings": [
                {"type": "var", "name": "LOG_LEVEL", "varRef": "var:w-alpha-1:log"},
                {"type": "secret", "name": "API_TOKEN", "secretRef": "sec:w-alpha-1:tok"},
                {"type": "kv", "name": "KV", "namespaceId": "kvns-01",
                 "permissions": {"read": true, "write": false}},
                {"type": "r2", "name": "MEDIA", "bucket": "media-bucket-01",
                 "permissions": {"read": true, "write": true}},
                {"type": "pg", "name": "DB", "databaseId": "db-01",
                 "permissions": {"read": false, "write": false}},
                {"type": "service", "name": "AUTH", "workerId": "w-alpha-2"},
                {"type": "d1", "name": "DB1", "databaseId": "abcdefghijklmnopqrstuvwxyz",
                 "epoch": 1, "permissions": {"read": true, "write": true}},
                {"type": "queue", "name": "JOBS", "queueId": "qqqqqqqqqqqqqqqqqqqqqqqqqq",
                 "queueName": "jobs", "permissions": {"read": true, "write": true}},
                {"type": "ratelimit", "name": "RATE_LIMITER", "namespaceId": "1001",
                 "limit": 40, "period": 60},
                {"type": "assets", "name": "ASSETS", "manifestDigest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 "permissions": {"read": true, "write": false}}
            ],
            "routes": [{"hostname": "api--team-alpha.vibeploy.app"}],
            "crons": [{"scheduleId": "cron-1", "expression": "*/15 * * * *"}],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }, {
            "workerId": "w-alpha-2",
            "name": "auth",
            "activeVersion": {
                "versionId": "v-2026-11-08-2",
                "artifactDigest": bundle_aux().digest(),
                "compatibilityDate": "2026-10-15",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "auth--team-alpha.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 10, "memoryMb": 64, "subrequests": 0, "outboundConnections": 0}
        }]
    }))
    .unwrap()
}

/// The checked-in `facade/manifest.json` — system workers and service
/// endpoints come from the real generated artifact, so render tests
/// exercise the same module digests vrt-host will embed (no `ddd…`
/// placeholder for vrt-egress).
const FACADE_MANIFEST_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../facade/manifest.json"
));

pub fn facade_manifest() -> FacadeManifest {
    parse_facade_manifest(FACADE_MANIFEST_JSON).expect("facade manifest parses")
}

pub fn runtime_params() -> RuntimeParams {
    let manifest = facade_manifest();
    RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        // Non-default fixture so the golden exercises allow/denyNames
        // serialization end to end.
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![EgressAllowEntry {
                host: "api.vibeploy.internal".into(),
                port: 8443,
            }],
            deny_names: vec![
                EgressNameRule::Suffix("tenant.example".into()),
                EgressNameRule::Exact("blocked.example".into()),
            ],
            request_timeout_ms: 30_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::from([("var:w-alpha-1:log".to_owned(), "info".to_owned())]),
        secrets: BTreeMap::from([(
            "team-alpha".to_owned(),
            BTreeMap::from([(
                "sec:w-alpha-1:tok".to_owned(),
                SecretValue::new("s3cr3t-value"),
            )]),
        )]),
        do_storage_dir: "/vrt-data/do/team-alpha".into(),
        wfop_pepper: "vrt-config-test-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

pub fn input<'a>(
    tenant: &'a TenantDesired,
    bundles: &'a [VersionBundle],
    params: &'a RuntimeParams,
) -> TenantRenderInput<'a> {
    TenantRenderInput {
        tenant,
        revision: 7,
        bundles,
        params,
    }
}
