//! Flagship emit — `{"type":"flagship"}` bindings resolve their
//! `appId` against the tenant's `flagshipApps` and render ONE
//! `wrapped` binding over `cloudflare-internal:flagship`, carrying the
//! app config verbatim in the `config` innerBinding (the flag model
//! was proven at the desired boundary, so the JSON can ride
//! unescaped-into-capnp). A bound app with no tenant entry fails the
//! whole render closed.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{
    BindingDesired, FlagshipAppDesired, FlagshipBinding, TenantDesired,
};
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

const APP_CONFIG: &str = r#"{"appId":"app-01","flags":{"new-checkout":{"enabled":true,"type":"boolean","variants":{"on":true,"off":false},"defaultVariant":"off"}}}"#;

fn tenant_with_flagship(entries: &[(&str, &str)]) -> TenantDesired {
    let mut t = tenant();
    t.flagship_apps.push(FlagshipAppDesired {
        app_id: "app-01".into(),
        config: APP_CONFIG.into(),
    });
    for (name, app_id) in entries {
        t.workers[0]
            .bindings
            .push(BindingDesired::Flagship(FlagshipBinding {
                name: (*name).into(),
                app_id: (*app_id).into(),
            }));
    }
    t
}

#[test]
fn flagship_absent_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("flagship"), "{}", r.text);
}

#[test]
fn flagship_emits_wrapped_binding_with_config() {
    let t = tenant_with_flagship(&[("FLAGS", "app-01")]);
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;

    // One wrapped binding named FLAGS over the internal module, with
    // the app config verbatim in a `config` json innerBinding.
    assert!(out.contains("(name = \"FLAGS\", wrapped = ("), "{out}");
    assert!(
        out.contains("moduleName = \"cloudflare-internal:flagship\""),
        "{out}"
    );
    assert!(out.contains("name = \"config\""), "{out}");
    assert!(out.contains("new-checkout"), "{out}");
    assert!(out.contains("app-01"), "{out}");
}

#[test]
fn flagship_multiple_bindings_share_one_app() {
    // CF: one worker may consume several Flagship apps — each binding
    // resolves its own appId; here two bindings share one app.
    let t = tenant_with_flagship(&[("FLAGS_A", "app-01"), ("FLAGS_B", "app-01")]);
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;
    assert!(out.contains("(name = \"FLAGS_A\", wrapped = ("), "{out}");
    assert!(out.contains("(name = \"FLAGS_B\", wrapped = ("), "{out}");
    assert_eq!(out.matches("cloudflare-internal:flagship").count(), 2);
}

#[test]
fn flagship_unknown_app_excludes_the_worker() {
    // Defense in depth behind compute-desired validation: a binding
    // naming an app absent from flagshipApps never renders — the
    // offender is excluded with a typed reason (W3-r2 per-worker
    // isolation) rather than silently evaluating defaults.
    let t = tenant_with_flagship(&[("FLAGS", "app-02")]);
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert_eq!(rendered.skipped_workers, vec!["w-alpha-1".to_string()]);
    assert_eq!(
        rendered.skipped_reasons[0].code,
        "CONFIG_UNRESOLVED_REFERENCE"
    );
    assert!(!rendered.text.contains("(name = \"FLAGS\""));
    assert!(rendered.text.contains("name = \"worker-w-alpha-2\""));
}
