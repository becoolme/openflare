//! KV binding-backend wire format: request DTOs, put frame, KVS1/KVB1
//! encoders and the JSON list response. Mirrors upstream
//! `binding_backend/kv.rs` + `storage/src/kv/engine.rs` semantics.

use crate::error::ProviderError;
use crate::limits::{
    KV_MAX_KEY_BYTES, KV_MAX_METADATA_BYTES, KV_MAX_VALUE_BYTES, KV_MIN_EXPIRATION_TTL_SECONDS,
    KV_PUT_HEADER_MAX_BYTES,
};
use serde::Deserialize;
use serde_json::Value;

use super::KvPutOptions;

/// `{"key": ...}` — delete request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRequest {
    /// Key string (validated against the KV key grammar).
    pub key: String,
}

/// `{"keys": [...], "cacheTtl": <secs>?}` — get/get-with-metadata/get-many.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FrameGetRequest {
    /// Keys to fetch (exactly 1 for single gets, `<= 100` for get-many).
    pub keys: Vec<String>,
    /// Optional edge cache TTL in seconds (`>= 30` when present).
    pub cache_ttl: Option<u64>,
}

/// Put frame JSON header (`<u32 len><json><value bytes>`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FramePutHeader {
    /// Key string.
    pub key: String,
    /// Absolute expiration in unix seconds.
    pub expiration: Option<u64>,
    /// Relative expiration TTL in seconds (`>= 60`).
    pub expiration_ttl: Option<u64>,
    /// Arbitrary JSON metadata, canonicalized server-side.
    pub metadata: Option<Value>,
    /// Whether `metadata` should be stored (`metadata: null` canonicalizes
    /// to `null` when this is set).
    #[serde(default)]
    pub metadata_present: bool,
}

/// `{"prefix"?, "limit", "cursor"?}` — list request body.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FrameListRequest {
    /// Key prefix filter (`""` when absent).
    #[serde(default)]
    pub prefix: Option<String>,
    /// Page size, `1..=1000`.
    pub limit: u16,
    /// Opaque signed cursor from a previous page.
    pub cursor: Option<String>,
}

/// KV key grammar: non-empty, not `.`/`..`, `<= 512` UTF-8 bytes.
pub fn validate_key(key: &str) -> Result<Vec<u8>, ProviderError> {
    let bytes = key.as_bytes();
    if key.is_empty() || key == "." || key == ".." {
        return Err(ProviderError::KvKeyInvalid);
    }
    if bytes.len() > KV_MAX_KEY_BYTES {
        return Err(ProviderError::KvKeyTooLarge);
    }
    Ok(bytes.to_vec())
}

/// Canonical JSON-compatible metadata with lexicographically ordered
/// object keys; `<= 1024` bytes after canonicalization.
pub fn canonical_metadata(value: &Value) -> Result<Vec<u8>, ProviderError> {
    let canonical = canonical_value(value)?;
    let bytes = serde_json::to_vec(&canonical).map_err(|_| ProviderError::KvMetadataInvalid)?;
    if bytes.len() > KV_MAX_METADATA_BYTES {
        return Err(ProviderError::KvMetadataTooLarge);
    }
    Ok(bytes)
}

fn canonical_value(value: &Value) -> Result<Value, ProviderError> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(value.clone()),
        Value::Number(number) if number.as_f64().is_some_and(f64::is_finite) => Ok(value.clone()),
        Value::Number(_) => Err(ProviderError::KvMetadataInvalid),
        Value::Array(values) => values
            .iter()
            .map(canonical_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => {
            let mut ordered = serde_json::Map::new();
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            for key in keys {
                ordered.insert(key.clone(), canonical_value(&values[key])?);
            }
            Ok(Value::Object(ordered))
        }
    }
}

/// Resolve a put frame header into typed options.
///
/// - `expirationTtl` must be `>= 60` s → `now + ttl`.
/// - `expiration` (unix seconds) must be `>= now + 60` s.
/// - `metadataPresent` canonicalizes `metadata` (`null` when absent).
pub fn prepare_put_options(
    now_ms: i64,
    header: &FramePutHeader,
) -> Result<KvPutOptions, ProviderError> {
    let expires_at_ms = if let Some(ttl) = header.expiration_ttl {
        if ttl < KV_MIN_EXPIRATION_TTL_SECONDS {
            return Err(ProviderError::KvInvalidOptions);
        }
        let delta = i64::try_from(ttl)
            .ok()
            .and_then(|v| v.checked_mul(1000))
            .ok_or(ProviderError::KvInvalidOptions)?;
        Some(
            now_ms
                .checked_add(delta)
                .ok_or(ProviderError::KvInvalidOptions)?,
        )
    } else if let Some(seconds) = header.expiration {
        let millis = i64::try_from(seconds)
            .ok()
            .and_then(|v| v.checked_mul(1000))
            .ok_or(ProviderError::KvInvalidOptions)?;
        if millis < now_ms.saturating_add(60_000) {
            return Err(ProviderError::KvInvalidOptions);
        }
        Some(millis)
    } else {
        None
    };
    let metadata_json = if header.metadata_present {
        Some(canonical_metadata(
            header.metadata.as_ref().unwrap_or(&Value::Null),
        )?)
    } else {
        None
    };
    Ok(KvPutOptions {
        expires_at_ms,
        metadata_json,
    })
}

/// Parse a put frame body: `u32-BE headerLen | header JSON | value`.
/// Bounded variant of the upstream streaming stager — the caller caps the
/// collected body at `KV_MAX_VALUE_BYTES + 64 KiB` before invoking this.
pub fn parse_put_frame(bytes: &[u8]) -> Result<(FramePutHeader, &[u8]), ProviderError> {
    if bytes.len() < 4 {
        return Err(ProviderError::KvInternalProtocolError);
    }
    let header_len = usize::try_from(u32::from_be_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| ProviderError::KvInternalProtocolError)?,
    ))
    .map_err(|_| ProviderError::KvInternalProtocolError)?;
    let header_end = 4_usize
        .checked_add(header_len)
        .ok_or(ProviderError::KvInternalProtocolError)?;
    if header_len > KV_PUT_HEADER_MAX_BYTES || header_end > bytes.len() {
        return Err(ProviderError::KvInternalProtocolError);
    }
    let header: FramePutHeader =
        serde_json::from_slice(&bytes[4..header_end]).map_err(|_| ProviderError::Protocol)?;
    validate_key(&header.key)?;
    let value = &bytes[header_end..];
    if value.len() > KV_MAX_VALUE_BYTES {
        return Err(ProviderError::KvValueTooLarge);
    }
    Ok((header, value))
}

/// Append one entry record: presence byte, i64 expires-ms (-1),
/// u32 metadata len (`u32::MAX` absent) + bytes, u32 value len + bytes.
pub fn encode_entry(output: &mut Vec<u8>, entry: Option<&super::KvEntry>) {
    let Some(entry) = entry else {
        output.push(0);
        output.extend_from_slice(&(-1_i64).to_be_bytes());
        output.extend_from_slice(&u32::MAX.to_be_bytes());
        output.extend_from_slice(&u32::MAX.to_be_bytes());
        return;
    };
    output.push(1);
    output.extend_from_slice(&entry.expires_at_ms.unwrap_or(-1).to_be_bytes());
    match &entry.metadata_json {
        Some(metadata) => {
            output.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
            output.extend_from_slice(metadata);
        }
        None => output.extend_from_slice(&u32::MAX.to_be_bytes()),
    }
    output.extend_from_slice(&(entry.value.len() as u32).to_be_bytes());
    output.extend_from_slice(&entry.value);
}

/// `KVS1` + one entry record.
pub fn encode_kvs1(entry: Option<&super::KvEntry>) -> Vec<u8> {
    let mut out = b"KVS1".to_vec();
    encode_entry(&mut out, entry);
    out
}

/// `KVB1` + u16 count + entry records.
pub fn encode_kvb1(entries: &[Option<super::KvEntry>]) -> Vec<u8> {
    let mut out = b"KVB1".to_vec();
    out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    for entry in entries {
        encode_entry(&mut out, entry.as_ref());
    }
    out
}

/// Decode one entry record starting at `bytes[0]`; returns
/// `(entry, consumed)` or `None` on truncation. Test/tooling helper used
/// by the protocol tests.
pub fn decode_entry(bytes: &[u8]) -> Option<(Option<super::KvEntry>, usize)> {
    let (&presence, rest) = bytes.split_first()?;
    if presence > 1 {
        return None;
    }
    if presence != 1 {
        if rest.len() < 16 {
            return None;
        }
        let expires = i64::from_be_bytes(rest[..8].try_into().ok()?);
        let meta_len = u32::from_be_bytes(rest[8..12].try_into().ok()?);
        let value_len = u32::from_be_bytes(rest[12..16].try_into().ok()?);
        if expires != -1 || meta_len != u32::MAX || value_len != u32::MAX {
            return None;
        }
        return Some((None, 17));
    }
    if rest.len() < 12 {
        return None;
    }
    let expires = i64::from_be_bytes(rest[..8].try_into().ok()?);
    let meta_len = u32::from_be_bytes(rest[8..12].try_into().ok()?) as usize;
    let mut cursor = 12usize;
    let metadata_json = if meta_len == u32::MAX as usize {
        None
    } else {
        if rest.len() < cursor + meta_len + 4 {
            return None;
        }
        let meta = rest[cursor..cursor + meta_len].to_vec();
        cursor += meta_len;
        Some(meta)
    };
    if rest.len() < cursor + 4 {
        return None;
    }
    let value_len = u32::from_be_bytes(rest[cursor..cursor + 4].try_into().ok()?) as usize;
    if value_len == u32::MAX as usize {
        return None;
    }
    cursor += 4;
    if rest.len() < cursor + value_len {
        return None;
    }
    let value = rest[cursor..cursor + value_len].to_vec();
    Some((
        Some(super::KvEntry {
            value,
            metadata_json,
            expires_at_ms: if expires == -1 { None } else { Some(expires) },
        }),
        1 + cursor + value_len,
    ))
}

/// JSON list response `{keys, list_complete, cursor}` — expiration is
/// emitted in unix *seconds* like upstream.
pub fn encode_list_response(
    rows: &[super::KvListRow],
    complete: bool,
    cursor: Option<&str>,
) -> Result<Vec<u8>, ProviderError> {
    let keys = rows
        .iter()
        .map(|row| {
            let name = String::from_utf8(row.key.clone()).map_err(|_| ProviderError::KvCorrupt)?;
            let metadata = row
                .metadata_json
                .as_ref()
                .map(|bytes| {
                    serde_json::from_slice::<Value>(bytes).map_err(|_| ProviderError::KvCorrupt)
                })
                .transpose()?;
            Ok(serde_json::json!({
                "name": name,
                "expiration": row.expires_at_ms.map(|v| v / 1000),
                "metadata": metadata,
            }))
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    serde_json::to_vec(&serde_json::json!({
        "keys": keys,
        "list_complete": complete,
        "cursor": cursor,
    }))
    .map_err(|_| ProviderError::KvInternalProtocolError)
}
