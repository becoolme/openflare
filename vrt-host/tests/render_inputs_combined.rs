//! T19b/B7 — cross-crate consistency: `cell-controller`'s
//! `write_render_inputs` output is exactly what `vrt-host`'s
//! `FileRenderInputs::new_combined` loads — one consistent snapshot,
//! and a second write changes the fingerprint material (i.e. the new
//! generation actually picks up the new secret).

use cell_controller::render_inputs::{write_render_inputs, RENDER_INPUTS_FILE};
use vrt_host::render_inputs::{FileRenderInputs, RenderInputsProvider};

#[test]
fn controller_write_then_host_load_roundtrip_and_fingerprint_moves() {
    let dir = std::env::temp_dir().join(format!("xcrate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let payload1: cell_controller::render_inputs::RenderInputsPayload = serde_json::from_str(
        r#"{"fingerprint":"fp1","tenants":{"t1":{"vars":{"vrtvar:w:k":"v1"},"secrets":{"vrtsec:w:s":"s1"}}}}"#,
    )
    .unwrap();
    write_render_inputs(&dir, &payload1).unwrap();

    let provider = FileRenderInputs::new_combined(dir.join(RENDER_INPUTS_FILE));
    let inputs1 = provider.load().unwrap();
    assert_eq!(inputs1.vars["t1"]["vrtvar:w:k"], "v1");

    let pepper = [7u8; 32];
    let mut m1 = Vec::new();
    vrt_host::render_inputs::inputs_material(&inputs1, &pepper, &mut m1);

    let payload2: cell_controller::render_inputs::RenderInputsPayload = serde_json::from_str(
        r#"{"fingerprint":"fp2","tenants":{"t1":{"vars":{"vrtvar:w:k":"v1"},"secrets":{"vrtsec:w:s":"s2-rotated"}}}}"#,
    )
    .unwrap();
    write_render_inputs(&dir, &payload2).unwrap();
    let inputs2 = provider.load().unwrap();
    let mut m2 = Vec::new();
    vrt_host::render_inputs::inputs_material(&inputs2, &pepper, &mut m2);
    assert_ne!(m1, m2, "secret rotation must move the params fingerprint");

    // permissions: host must fail closed when the file is not 0600
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            dir.join(RENDER_INPUTS_FILE),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(provider.load().is_err());
    }
    let _ = std::fs::remove_dir_all(&dir);
}
