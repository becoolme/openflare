//! RV-R93: the canonical distributed-lease key is `[cron_lease]`.
//! `[cron.lease]` (the old doc spelling) must fail loudly with a hint —
//! never be tolerated, which would silently fall back to the
//! single-replica memory lease.

fn load(extra: &str) -> Result<cell_controller::prod::ProdConfig, String> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.toml");
    let text = format!(
        "cell_id = \"c1\"\ncontrol_plane_url = \"https://cp.example\"\ntoken_file = \"/t\"\ntrusted_keys = []\n{extra}\n"
    );
    std::fs::write(&path, text).unwrap();
    cell_controller::prod::load_config(path.to_str().unwrap())
}

#[test]
fn canonical_cron_lease_table_parses() {
    let cfg = load("replicas = 2\n[cron_lease]\ntype = \"kubernetes\"\nnamespace = \"vrt-system\"")
        .expect("canonical key loads");
    let lease = cfg.cron_lease.expect("lease configured");
    assert_eq!(lease.kind, "kubernetes");
    assert_eq!(lease.namespace, "vrt-system");
}

#[test]
fn dotted_cron_lease_is_rejected_with_hint() {
    // `[cron.lease]` must be placed after top-level keys; TOML makes it
    // a top-level `cron` table, an unknown field.
    let err = load("[cron.lease]\ntype = \"kubernetes\"\nnamespace = \"vrt-system\"")
        .expect_err("old spelling must fail");
    assert!(err.contains("cron_lease"), "hint names the right key: {err}");
}

#[test]
fn unsupported_lease_type_rejected() {
    let err = load("[cron_lease]\ntype = \"etcd\"\nnamespace = \"x\"").expect_err("bad type");
    assert!(err.contains("unsupported cron_lease type"), "{err}");
}
