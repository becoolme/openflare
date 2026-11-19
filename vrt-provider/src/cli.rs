//! CLI/config-file surface for the `vrt-provider` binary — kept in
//! the lib so contract tests exercise the EXACT parsing the binary
//! runs (T25/L7/L8).
//!
//! `vrt-provider` resolves its config path from (in order):
//! `VRT_PROVIDER_CONFIG` → `--config <path>` → a single positional
//! path. Anything else (unknown flags, both flag+positional, extra
//! args) is an error — silent mis-parsing is worse than a crash.

use std::io;
use std::path::PathBuf;

/// Resolve the config file path from process args (argv WITHOUT
/// argv[0]). `VRT_PROVIDER_CONFIG` (read by the caller) takes
/// precedence; this function covers argv only.
///
/// Accepted forms: `["--config", "<path>"]`, `["<path>"]`, `[]` (env
/// fallback handled by the caller). `--config` without a value, an
/// unknown flag, multiple positionals, or flag+positional mix all
/// fail closed.
pub fn resolve_config_path(args: &[String]) -> Result<Option<PathBuf>, String> {
    let mut positional: Option<&str> = None;
    let mut flag: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--config" => {
                if flag.is_some() || i + 1 >= args.len() {
                    return Err("--config requires a path argument".to_string());
                }
                flag = Some(&args[i + 1]);
                i += 2;
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown flag: {arg}"));
            }
            _ => {
                if positional.is_some() {
                    return Err(format!("unexpected extra argument: {arg}"));
                }
                positional = Some(arg);
                i += 1;
            }
        }
    }
    match (flag, positional) {
        (Some(path), None) => Ok(Some(PathBuf::from(path))),
        (None, Some(path)) => Ok(Some(PathBuf::from(path))),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => {
            Err("--config and a positional path are mutually exclusive".to_string())
        }
    }
}

/// Load a service secret (cursor secret, vrt-sql auth secret) from
/// `path` via the shared RV-R72 loader: 0600 + owner check, ≤4KiB, only
/// one trailing newline stripped, no internal whitespace, ≥32 bytes, no
/// placeholder/uniform values. Byte-identical to what vrt-sql and
/// cell-controller derive from the same file.
pub fn load_cursor_secret(path: &std::path::Path) -> io::Result<Vec<u8>> {
    Ok(vrt_securefs::load_service_secret(path, vrt_securefs::SecretPurpose::ServiceHmac)?
        .into_bytes())
}

/// Same validation for a secret delivered through an environment
/// variable (platform mode).
pub fn parse_cursor_secret_env(value: &str) -> io::Result<Vec<u8>> {
    Ok(vrt_securefs::parse_service_secret(
        value.as_bytes(),
        vrt_securefs::SecretPurpose::ServiceHmac,
    )?
    .into_bytes())
}
