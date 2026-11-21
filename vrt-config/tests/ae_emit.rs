//! Analytics Engine emit — `analyticsEngine` bindings render ONE
//! external service per distinct dataset (the service name IS the
//! event `dataset` field workerd fills from the designator name),
//! every dataset points at the same per-tenant AE socket, and an
//! empty `ae_addr` with AE bindings present fails the whole render
//! closed.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{AnalyticsEngineBinding, BindingDesired, TenantDesired};
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn tenant_with_ae(entries: &[(&str, &str)]) -> TenantDesired {
    let mut t = tenant();
    for (name, dataset) in entries {
        t.workers[0]
            .bindings
            .push(BindingDesired::AnalyticsEngine(AnalyticsEngineBinding {
                name: (*name).into(),
                dataset: (*dataset).into(),
            }));
    }
    t
}

#[test]
fn ae_absent_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("analyticsEngine"), "{}", r.text);
    assert!(
        r.external_addrs.iter().all(|a| a.service != "weather"),
        "{:?}",
        r.external_addrs
    );
}

#[test]
fn ae_emits_binding_service_and_addr() {
    let t = tenant_with_ae(&[("AE", "weather")]);
    let mut params = runtime_params();
    params.ae_addr = "unix:/run/vrt/ae/team-alpha.sock".into();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;

    // Binding on worker w-alpha-1: service designator = dataset name.
    assert!(
        out.contains("(name = \"AE\", analyticsEngine = \"weather\")"),
        "{out}"
    );
    // Exactly ONE external service for the dataset.
    assert_eq!(
        out.matches("name = \"weather\", external = (").count(),
        1,
        "{out}"
    );
    // One ExternalAddr routing the dataset service to the tenant socket.
    let addr: Vec<_> = r
        .external_addrs
        .iter()
        .filter(|a| a.service == "weather")
        .collect();
    assert_eq!(addr.len(), 1, "{:?}", r.external_addrs);
    assert_eq!(addr[0].address, "unix:/run/vrt/ae/team-alpha.sock");
    // Worker WITHOUT the binding (w-alpha-2) must not reference it.
    let w2 = out
        .find("name = \"worker-w-alpha-2\", worker = (")
        .expect("worker block");
    let w2_block = &out[w2..];
    assert!(
        !w2_block.contains("analyticsEngine"),
        "unbound worker sees AE: {w2_block}"
    );
}

#[test]
fn ae_two_bindings_same_dataset_share_service() {
    let t = tenant_with_ae(&[("AE1", "metrics"), ("AE2", "metrics")]);
    let mut params = runtime_params();
    params.ae_addr = "unix:/run/vrt/ae/t.sock".into();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;

    assert!(out.contains("(name = \"AE1\", analyticsEngine = \"metrics\")"));
    assert!(out.contains("(name = \"AE2\", analyticsEngine = \"metrics\")"));
    assert_eq!(out.matches("name = \"metrics\", external = (").count(), 1);
    assert_eq!(
        r.external_addrs
            .iter()
            .filter(|a| a.service == "metrics")
            .count(),
        1
    );
}

#[test]
fn ae_two_datasets_two_services_one_addr_each() {
    let t = tenant_with_ae(&[("AE", "weather"), ("METRICS", "metrics")]);
    let mut params = runtime_params();
    params.ae_addr = "unix:/run/vrt/ae/t.sock".into();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();

    for ds in ["weather", "metrics"] {
        let addrs: Vec<_> = r
            .external_addrs
            .iter()
            .filter(|a| a.service == ds)
            .collect();
        assert_eq!(addrs.len(), 1, "dataset {ds}");
        assert_eq!(addrs[0].address, "unix:/run/vrt/ae/t.sock");
    }
}

#[test]
fn ae_binding_without_ae_addr_fails_closed() {
    let t = tenant_with_ae(&[("AE", "weather")]);
    let params = runtime_params(); // ae_addr empty
    let err = render_tenant_config(&input(&t, &bs(), &params)).unwrap_err();
    assert!(matches!(err, ConfigError::Identifier("aeAddr")), "{err:?}");
}

#[test]
fn ae_invalid_dataset_fails_closed() {
    for bad in ["has space", "has/slash", "eq=sign", "dot.name", ""] {
        let t = tenant_with_ae(&[("AE", bad)]);
        let mut params = runtime_params();
        params.ae_addr = "unix:/run/vrt/ae/t.sock".into();
        assert!(
            render_tenant_config(&input(&t, &bs(), &params)).is_err(),
            "dataset {bad:?} must fail render"
        );
    }
}
