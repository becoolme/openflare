//! Secrets Store emit — `{"type":"secretsStore"}` bindings render as a
//! service binding to the per-worker `worker-<id>-secrets-store`
//! bundled-module worker (`/workerd/secrets-store.js`, entrypoint
//! `SecretsStoreSecret`), the CF `{store_id, secret_name}` props in
//! canonical JSON. The secret VALUE never enters the config: the
//! service worker carries one `env:<secret_name>` `fromEnvironment`
//! binding per distinct name, resolved from the tenant's secret map —
//! missing refs and a name mapped to two different refs fail closed.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{
    secrets_store_secret_ref, BindingDesired, SecretsStoreBinding, TenantDesired,
};
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

/// One `SECRET`-named binding targeting `store_id`/`secret_name`;
/// the derived ref is provisioned into `params.secrets`.
fn tenant_with_secrets_store(store_id: &str, secret_name: &str) -> TenantDesired {
    let mut t = tenant();
    t.workers[0]
        .bindings
        .push(BindingDesired::SecretsStore(SecretsStoreBinding {
            name: "SECRET".into(),
            store_id: store_id.into(),
            secret_name: secret_name.into(),
            secret_ref: secrets_store_secret_ref(store_id, secret_name),
        }));
    t
}

fn params_with_secret(store_id: &str, secret_name: &str, value: &str) -> RuntimeParams {
    let mut params = runtime_params();
    params
        .secrets
        .entry("team-alpha".to_string())
        .or_default()
        .insert(
            secrets_store_secret_ref(store_id, secret_name),
            SecretValue::new(value),
        );
    params
}

#[test]
fn secrets_store_absent_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("secrets-store"), "{}", r.text);
}

#[test]
fn secrets_store_emits_service_binding_and_worker() {
    let t = tenant_with_secrets_store("default", "api-key");
    let params = params_with_secret("default", "api-key", "hunter42");
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;

    // Tenant binding → SecretsStoreSecret entrypoint with CF props
    // (canonical JSON — keys sorted).
    assert!(
        out.contains(
            "(name = \"SECRET\", service = (name = \"worker-w-alpha-1-secrets-store\", entrypoint = \"SecretsStoreSecret\", props = (json = \"{\\\"secret_name\\\":\\\"api-key\\\",\\\"store_id\\\":\\\"default\\\"}\")))",
        ),
        "{out}"
    );

    // The bundled-module worker: `env:api-key` fromEnvironment, deny
    // outbound, no tenant route.
    assert!(
        out.contains("(name = \"worker-w-alpha-1-secrets-store\", worker = ("),
        "{out}"
    );
    assert!(
        out.contains("(name = \"secrets-store.js\", bundled = \"/workerd/secrets-store.js\")"),
        "{out}"
    );
    assert!(out.contains("name = \"env:api-key\""), "{out}");
    assert!(out.contains("fromEnvironment"), "{out}");

    // The value reaches the child env only — never the config text.
    let env_name =
        secret_env_name("w-alpha-1", "secrets-store:api-key");
    assert!(out.contains(&env_name), "{out}");
    assert_eq!(r.env[&env_name].expose(), "hunter42");
    assert!(!out.contains("hunter42"), "{out}");
}

#[test]
fn secrets_store_same_name_two_bindings_share_env() {
    // CF: one secret_name has one value — two bindings on the same
    // worker naming the same secret share the `env:<name>` slot.
    let mut t = tenant_with_secrets_store("default", "api-key");
    t.workers[0]
        .bindings
        .push(BindingDesired::SecretsStore(SecretsStoreBinding {
            name: "SECRET2".into(),
            store_id: "other-store".into(),
            secret_name: "api-key".into(),
            secret_ref: secrets_store_secret_ref("default", "api-key"),
        }));
    let params = params_with_secret("default", "api-key", "hunter42");
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert_eq!(r.text.matches("name = \"env:api-key\"").count(), 1);
    assert!(r.text.contains("(name = \"SECRET2\", service = ("), "{}", r.text);
}

#[test]
fn secrets_store_divergent_ref_for_same_name_skips_worker() {
    // Same `secret_name` mapped to a different ref would make
    // `env:api-key` ambiguous — the worker is excluded (fail closed
    // at worker scope; upstream `check_bindings` makes this fatal at
    // parse time, VRT's per-worker exclusion keeps siblings serving).
    let mut t = tenant_with_secrets_store("default", "api-key");
    t.workers[0]
        .bindings
        .push(BindingDesired::SecretsStore(SecretsStoreBinding {
            name: "SECRET2".into(),
            store_id: "other-store".into(),
            secret_name: "api-key".into(),
            secret_ref: secrets_store_secret_ref("other-store", "api-key"),
        }));
    // Both refs resolve — the divergence must surface from the
    // `env:<name>` collision check, not resolution.
    let mut params = params_with_secret("default", "api-key", "hunter42");
    params
        .secrets
        .entry("team-alpha".to_string())
        .or_default()
        .insert(
            secrets_store_secret_ref("other-store", "api-key"),
            SecretValue::new("different-value"),
        );
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(
        r.skipped_workers.iter().any(|w| w == "w-alpha-1"),
        "{:?}",
        r.skipped_workers
    );
    assert!(
        r.skipped_reasons
            .iter()
            .any(|s| s.code == "CONFIG_INVALID_IDENTIFIER"),
        "{:?}",
        r.skipped_reasons
    );
    assert!(!r.text.contains("secrets-store"), "{}", r.text);
}

#[test]
fn secrets_store_missing_secret_skips_worker() {
    // An unresolvable ref must never render a half-wired binding —
    // the worker is excluded, siblings keep serving.
    let t = tenant_with_secrets_store("default", "api-key");
    let params = runtime_params(); // ref not provisioned
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(
        r.skipped_workers.iter().any(|w| w == "w-alpha-1"),
        "{:?}",
        r.skipped_workers
    );
    assert!(!r.text.contains("secrets-store"), "{}", r.text);
}

#[test]
fn secrets_store_resolve_tenant_env_covers_stub_path() {
    // The stub/standalone env path resolves through the same
    // `resolve_tenant_env` — a SecretsStore binding contributes its
    // `env:<name>`-scoped var there too.
    let t = tenant_with_secrets_store("default", "api-key");
    let params = params_with_secret("default", "api-key", "hunter42");
    let env = resolve_tenant_env(&t, &params.vars, &params.secrets).unwrap();
    assert_eq!(
        env[&secret_env_name("w-alpha-1", "secrets-store:api-key")].expose(),
        "hunter42"
    );
}
