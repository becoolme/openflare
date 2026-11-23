//! T17/F1 tests — hot render inputs: per-converge reload, permission
//! fail-closed, fingerprint turnover, and the `render-inputs-unavailable`
//! typed reason. Proves a var/secret change is a NEW expected spec
//! ("received ≠ loaded" stays false even without a desired update).

mod common;

use common::*;
use std::sync::Arc;
use vrt_host::artifact::BundleCache;
use vrt_host::assemble::{AssembleError, Assembler, EmitMode};
use vrt_host::render_inputs::{FileRenderInputs, RenderInputsError, RenderInputsProvider};
use vrt_host::supervisor::TenantAssembler;

fn write_mode(path: &std::path::Path, bytes: &[u8], mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn tmpfile(dir: &tempfile::TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let p = dir.path().join(name);
    write_mode(&p, bytes, 0o600);
    p
}

#[test]
fn file_provider_loads_and_enforces_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let vars = tmpfile(&dir, "vars.json", br#"{"t1":{"var:w:vhash:K":"v1"}}"#);
    let secrets = tmpfile(&dir, "secrets.json", br#"{"t1":{"api-key":"s3cr3t"}}"#);
    let provider = FileRenderInputs::new(Some(vars.clone()), Some(secrets.clone()));
    let inputs = provider.load().unwrap();
    assert_eq!(inputs.vars["t1"]["var:w:vhash:K"], "v1");
    assert_eq!(inputs.secrets["t1"]["api-key"].expose(), "s3cr3t");

    // 0644 → fail closed on EVERY load (not just startup).
    write_mode(&secrets, br#"{"t1":{"api-key":"s3cr3t"}}"#, 0o644);
    assert!(matches!(
        provider.load(),
        Err(RenderInputsError::Permissions)
    ));
    write_mode(&secrets, br#"{"t1":{"api-key":"s3cr3t"}}"#, 0o600);
    assert!(provider.load().is_ok());

    // Missing / malformed / oversized all fail closed.
    std::fs::remove_file(&vars).unwrap();
    assert!(matches!(provider.load(), Err(RenderInputsError::Io)));
    write_mode(&vars, b"{not json", 0o600);
    assert!(matches!(provider.load(), Err(RenderInputsError::Parse)));
    write_mode(&vars, b"[]", 0o600);
    assert!(matches!(provider.load(), Err(RenderInputsError::Parse)));
    let big = vec![b'x'; 4 * 1024 * 1024 + 1];
    write_mode(&vars, &big, 0o600);
    assert!(matches!(provider.load(), Err(RenderInputsError::TooLarge)));
}

fn stub_assembler(fx: &common::Fixture, inputs: Arc<dyn RenderInputsProvider>) -> Assembler {
    Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        std::sync::Arc::new(vrt_host::artifact::MemoryBundleSource::default()),
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        inputs,
    )
}

/// Real `VersionBundle` in a memory source — stub assembly verifies
/// every declared digest (T15/B2), so a synthetic hex won't do.
fn seeded_bundle() -> (Arc<vrt_host::artifact::MemoryBundleSource>, String) {
    let source = Arc::new(vrt_host::artifact::MemoryBundleSource::default());
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    (source, bundle.digest().to_string())
}

#[test]
fn inputs_change_flips_params_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let vars = tmpfile(&dir, "vars.json", br#"{"t1":{"var:w:vhash:K":"v1"}}"#);
    let provider: Arc<dyn RenderInputsProvider> =
        Arc::new(FileRenderInputs::new(Some(vars.clone()), None));
    let fx = common::Fixture::new();
    let asm = stub_assembler(&fx, provider);
    let fp1 = asm.params_fingerprint().unwrap();
    // Same content → same fingerprint (stable across reloads).
    assert_eq!(asm.params_fingerprint().unwrap(), fp1);
    // Value change → new fingerprint → new expected spec.
    write_mode(&vars, br#"{"t1":{"var:w:vhash:K":"v2"}}"#, 0o600);
    let fp2 = asm.params_fingerprint().unwrap();
    assert_ne!(fp1, fp2);
    // Broken file → fingerprint uncomputable (fail closed).
    std::fs::remove_file(&vars).unwrap();
    assert!(matches!(
        asm.params_fingerprint(),
        Err(AssembleError::Inputs(_))
    ));
}

#[test]
fn stub_assemble_resolves_env_from_fresh_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let vars = tmpfile(&dir, "vars.json", br#"{"t1":{"var:w:vh:GREETING":"hello"}}"#);
    let secrets = tmpfile(&dir, "secrets.json", br#"{"t1":{"api-key":"s3cr3t"}}"#);
    let provider: Arc<dyn RenderInputsProvider> =
        Arc::new(FileRenderInputs::new(Some(vars), Some(secrets)));
    let fx = common::Fixture::new();
    let (source, digest) = seeded_bundle();
    let asm = Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        source,
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        provider,
    );
    let mut w = worker("w1", "a.test", &digest);
    w.bindings = vec![
        snapshot_protocol::compute_desired::BindingDesired::Var(
            snapshot_protocol::compute_desired::VarBinding {
                name: "GREETING".into(),
                var_ref: "var:w:vh:GREETING".into(),
            },
        ),
        snapshot_protocol::compute_desired::BindingDesired::Secret(
            snapshot_protocol::compute_desired::SecretBinding {
                name: "API_KEY".into(),
                secret_ref: "api-key".into(),
            },
        ),
    ];
    let t = tenant("t1", vec![w]);
    let spec = asm.assemble(&t, 1, 1).unwrap();
    let env_name = vrt_config::secret_env_name("w1", "API_KEY");
    assert_eq!(
        spec.env.get(&env_name).map(|v| v.expose()),
        Some("s3cr3t"),
        "secret binding resolves into child env only"
    );
    assert_eq!(
        spec.env.get("VRT_VAR_GREETING").map(|v| v.expose()),
        Some("hello"),
        "var injected for stub observability"
    );

    // Missing secret ref → typed render failure (→ render-inputs-unavailable).
    let mut w2 = worker("w2", "b.test", &digest);
    w2.bindings = vec![snapshot_protocol::compute_desired::BindingDesired::Secret(
        snapshot_protocol::compute_desired::SecretBinding {
            name: "MISSING".into(),
            secret_ref: "absent".into(),
        },
    )];
    let t2 = tenant("t1", vec![w2]);
    assert!(matches!(
        asm.assemble(&t2, 2, 2),
        Err(AssembleError::Render(
            vrt_config::ConfigError::UnresolvedReference("secretRef")
        ))
    ));
}

/// Assemble whose inputs load always fails — used to prove the
/// converge-level sentinel + typed reason.
struct BrokenInputsAssembler;

impl TenantAssembler for BrokenInputsAssembler {
    fn params_fingerprint(&self) -> Result<String, AssembleError> {
        Err(AssembleError::Inputs(RenderInputsError::Io))
    }
    fn assemble(
        &self,
        _tenant: &snapshot_protocol::compute_desired::TenantDesired,
        _revision: u64,
        _generation: u64,
    ) -> Result<vrt_host::spec::TenantLaunchSpec, AssembleError> {
        Err(AssembleError::Inputs(RenderInputsError::Io))
    }
}

#[test]
fn converge_marks_render_inputs_unavailable_fail_closed() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: Arc<dyn TenantAssembler> = Arc::new(BrokenInputsAssembler);
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![worker("w1", "a.test", "d1")])],
        deletions: vec![],
};
    let outcome = host.converge(&desired, &assembler);
    assert!(outcome.failed.contains(&"t1".to_string()));
    let rt = registry.get("t1").unwrap();
    assert_eq!(
        rt.assemble_error().as_deref(),
        Some("render-inputs-unavailable"),
        "typed reason surfaces to readyz/ACK"
    );
    // Sentinel expected can never equal a real serving digest.
    let expected = rt.expected_digest().unwrap();
    assert!(rt.serving_digest().is_none() || rt.serving_digest() != Some(expected));
}

#[test]
fn inputs_material_binds_vars_to_tenant_identity() {
    // T13/M6: the SAME var value under a different tenant key must
    // change the params fingerprint — otherwise one tenant's vars
    // rotation would silently alias another tenant's spec.
    let pepper = [9u8; 32];
    let mk = |tenant: &str, value: &str| vrt_host::render_inputs::RenderInputs {
        vars: std::collections::BTreeMap::from([(
            tenant.to_string(),
            std::collections::BTreeMap::from([(
                "var:w:k".to_string(),
                value.to_string(),
            )]),
        )]),
        secrets: std::collections::BTreeMap::new(),
    };
    let mut m1 = Vec::new();
    vrt_host::render_inputs::inputs_material(&mk("t1", "v"), &pepper, &mut m1);
    let mut m2 = Vec::new();
    vrt_host::render_inputs::inputs_material(&mk("t2", "v"), &pepper, &mut m2);
    assert_ne!(
        m1, m2,
        "tenant identity must be part of the fingerprint material"
    );
    let mut m3 = Vec::new();
    vrt_host::render_inputs::inputs_material(&mk("t1", "v2"), &pepper, &mut m3);
    assert_ne!(m1, m3, "value changes still move the fingerprint");
}
