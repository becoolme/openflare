//! Operation log — one JSON object per line (`state/oplog.jsonl`,
//! `0600`). Mirrors the platform's operation-log semantics for
//! standalone mode; entries never carry secret values (callers pass
//! field identifiers only — `putSecret` logs the ref, not the value).

use crate::LocalError;
use std::io::Write;
use std::path::Path;

pub fn entry(op: &str, fields: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "op": op,
        "fields": fields,
    })
}

pub fn append(root: &Path, line: &serde_json::Value) -> Result<(), LocalError> {
    let path = crate::layout::state_dir(root).join("oplog.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|_| LocalError::Layout("oplog"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    let mut bytes = serde_json::to_vec(line).map_err(|_| LocalError::State("oplog"))?;
    bytes.push(b'\n');
    file.write_all(&bytes).map_err(LocalError::Io)?;
    file.sync_all().map_err(LocalError::Io)?;
    Ok(())
}
