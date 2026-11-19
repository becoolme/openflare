//! Strict JSON parsing and canonical serialization.
//!
//! This module is an equivalent reimplementation of the rules in
//! `postgres-tenant-gateway/src/config_protocol.rs` (`StrictValue`,
//! `StrictValue::parse`, `StrictValue::canonical`, `validate_numeric_lexemes`,
//! `utf16_cmp`). It exists so the snapshot protocol can be shared without
//! modifying the production gateway. Equivalence is proven by the golden
//! vectors in this crate's tests, which are taken from
//! `shared/postgres-tenant-gateway-hot-config-protocol-golden.json` — a
//! fixture the gateway's own test suite verifies.
//!
//! Strict rules (identical to the gateway):
//!
//! - Duplicate object members are rejected.
//! - `__proto__` object keys are rejected.
//! - Only integer JSON numbers are accepted; any numeric token containing
//!   `.`, `e` or `E` is rejected before typed deserialization. Magnitudes
//!   beyond the JavaScript safe-integer range (`±2^53-1`) are rejected.
//!   A literal `-0` is accepted and canonicalizes to `0`.
//! - Non-canonical inputs such as unpaired UTF-16 surrogates are rejected
//!   by the underlying serde parser.
//!
//! Canonical serialization:
//!
//! - Object members are ordered by UTF-16 code-unit order of their keys
//!   (matching `JSON.stringify` + `Object.keys().sort()` semantics used by
//!   the TypeScript control-plane signer).
//! - Strings are serialized with `serde_json::to_string` escaping rules.
//! - Integers serialize as decimal; booleans/null as literals; no
//!   insignificant whitespace anywhere.

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt};

/// Largest integer exactly representable by JavaScript (2^53 - 1), identical
/// to `MAX_SAFE_INTEGER` in the gateway implementation.
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// A JSON representation that rejects duplicate members, unsafe object keys,
/// fractional numbers, and values outside JavaScript's exact integer range
/// before canonicalization. Mirrors the gateway's `StrictValue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StrictValue {
    Null,
    Bool(bool),
    Integer(i64),
    Unsigned(u64),
    String(String),
    Array(Vec<StrictValue>),
    Object(BTreeMap<String, StrictValue>),
}

impl StrictValue {
    /// Parse raw JSON bytes under the strict rules. Equivalent to the
    /// gateway's `StrictValue::parse`.
    pub(crate) fn parse(raw: &[u8]) -> Result<Self, serde_json::Error> {
        validate_numeric_lexemes(raw).map_err(<serde_json::Error as de::Error>::custom)?;
        let mut deserializer = serde_json::Deserializer::from_slice(raw);
        let value = StrictValue::deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(value)
    }

    /// Serialize a typed value to JSON and re-parse it under strict rules so
    /// canonicalization and digests apply identical constraints to produced
    /// snapshots as to consumed ones (e.g. a u64 beyond 2^53-1 fails).
    pub(crate) fn from_serializable<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(value)?;
        Self::parse(&bytes)
    }

    pub(crate) fn get(&self, key: &str) -> Option<&StrictValue> {
        match self {
            Self::Object(object) => object.get(key),
            _ => None,
        }
    }

    pub(crate) fn as_object(&self) -> Option<&BTreeMap<String, StrictValue>> {
        match self {
            Self::Object(object) => Some(object),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Integer(value) => u64::try_from(*value).ok(),
            Self::Unsigned(value) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn remove_object_key(&mut self, key: &str) -> Option<StrictValue> {
        match self {
            Self::Object(object) => object.remove(key),
            _ => None,
        }
    }

    /// Canonical bytes per the protocol rules. Equivalent to the gateway's
    /// `StrictValue::canonical`.
    pub(crate) fn canonical(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::String(value) => serde_json::to_string(value).expect("strings serialize"),
            Self::Array(items) => format!(
                "[{}]",
                items
                    .iter()
                    .map(Self::canonical)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Object(items) => {
                let mut entries = items.iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| utf16_cmp(left, right));
                format!(
                    "{{{}}}",
                    entries
                        .into_iter()
                        .map(|(key, value)| format!(
                            "{}:{}",
                            serde_json::to_string(key).expect("object keys serialize"),
                            value.canonical()
                        ))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }

    pub(crate) fn to_json_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::Integer(value) => Value::Number((*value).into()),
            Self::Unsigned(value) => Value::Number((*value).into()),
            Self::String(value) => Value::String(value.clone()),
            Self::Array(items) => Value::Array(items.iter().map(Self::to_json_value).collect()),
            Self::Object(items) => Value::Object(
                items
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json_value()))
                    .collect(),
            ),
        }
    }
}

/// UTF-16 code-unit ordering, identical to the gateway's `utf16_cmp` and to
/// JavaScript's default string sort used by the control-plane signer.
fn utf16_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueVisitor;

impl<'de> de::Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("strict JSON value with safe integer numbers")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.unsigned_abs() > MAX_SAFE_INTEGER {
            return Err(E::custom("integer is outside the JavaScript safe range"));
        }
        Ok(StrictValue::Integer(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value > MAX_SAFE_INTEGER {
            return Err(E::custom("integer is outside the JavaScript safe range"));
        }
        Ok(StrictValue::Unsigned(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value == 0.0 && value.is_sign_negative() {
            // `validate_numeric_lexemes` has already proved the raw token was
            // exactly `-0`; decimal and exponent zero forms fail there.
            Ok(StrictValue::Integer(0))
        } else {
            Err(E::custom(
                "fractional or exponent-form JSON numbers are not allowed",
            ))
        }
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue::String(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element()? {
            values.push(value);
        }
        Ok(StrictValue::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if key == "__proto__" {
                return Err(de::Error::custom("dangerous object key is not allowed"));
            }
            let value = map.next_value()?;
            if values.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate object member"));
            }
        }
        Ok(StrictValue::Object(values))
    }
}

/// serde_json intentionally normalizes `-0` and `-0.0` to the same f64
/// visitor call. The wire protocol accepts the former (JSON.stringify emits
/// `0`) but rejects every decimal/exponent token, so inspect number lexemes
/// before handing the document to the typed deserializer. Equivalent to the
/// gateway's `validate_numeric_lexemes`.
fn validate_numeric_lexemes(raw: &[u8]) -> Result<(), &'static str> {
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < raw.len() {
        let byte = raw[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < raw.len()
                && (raw[index].is_ascii_digit()
                    || matches!(raw[index], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                index += 1;
            }
            let token = std::str::from_utf8(&raw[start..index]).map_err(|_| "invalid number")?;
            if token.contains(['.', 'e', 'E']) {
                return Err("decimal and exponent numbers are not allowed");
            }
            if let Some(negative) = token.strip_prefix('-') {
                if negative.is_empty() || !negative.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err("invalid integer number");
                }
                if negative != "0" {
                    let value = token
                        .parse::<i64>()
                        .map_err(|_| "integer is out of range")?;
                    if value.unsigned_abs() > MAX_SAFE_INTEGER {
                        return Err("integer is outside the JavaScript safe range");
                    }
                }
            } else {
                let value = token
                    .parse::<u64>()
                    .map_err(|_| "integer is out of range")?;
                if value > MAX_SAFE_INTEGER {
                    return Err("integer is outside the JavaScript safe range");
                }
            }
            continue;
        }
        index += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Canonical vectors 1:1 from
    // shared/postgres-tenant-gateway-hot-config-protocol-golden.json
    // (`canonicalVectors` / `invalidVectors`), which the gateway test
    // `shared_golden_canonical_vectors_match` verifies against its own
    // `StrictValue` implementation.
    const CANONICAL_VECTORS: &[(&str, &str)] = &[
        (
            "{\"\\ue000\": \"bmp\", \"\\ud800\\udc00\": \"supplementary\", \"a\": \"\\u00e9\\ud83d\\ude3a\"}",
            "{\"a\":\"é😺\",\"𐀀\":\"supplementary\",\"\":\"bmp\"}",
        ),
        (
            "{\"b\": [true, false, null], \"a\": {\"\\ud834\\udd1e\": 0, \"x\": null}}",
            "{\"a\":{\"x\":null,\"𝄞\":0},\"b\":[true,false,null]}",
        ),
        (
            "[-9007199254740991, 0, 9007199254740991]",
            "[-9007199254740991,0,9007199254740991]",
        ),
    ];

    const INVALID_VECTORS: &[&str] = &[
        "{\"value\":1,\"value\":2}",
        "{\"__proto__\":null}",
        "{\"value\":1.5}",
        "{\"value\":1e0}",
        "{\"value\":9007199254740992}",
        "{\"value\":\"\\ud800\"}",
    ];

    #[test]
    fn shared_golden_canonical_vectors_match() {
        for (raw, expected) in CANONICAL_VECTORS {
            let value = StrictValue::parse(raw.as_bytes())
                .unwrap_or_else(|e| panic!("vector {raw} failed to parse: {e}"));
            assert_eq!(value.canonical(), *expected, "vector {raw}");
        }
        for raw in INVALID_VECTORS {
            assert!(
                StrictValue::parse(raw.as_bytes()).is_err(),
                "accepted {raw}"
            );
        }
    }

    #[test]
    fn canonical_object_keys_use_utf16_code_unit_order() {
        // Same assertion as the gateway test of the same name.
        let value =
            StrictValue::parse("{\"\\ud800\\udc00\":1,\"\\ue000\":2,\"a\":3}".as_bytes()).unwrap();
        assert_eq!(
            value.canonical(),
            "{\"a\":3,\"\u{10000}\":1,\"\u{e000}\":2}"
        );
    }

    #[test]
    fn strict_json_rejects_duplicate_dangerous_fractional_and_unsafe_values() {
        for raw in [
            br#"{"a":1,"a":2}"#.as_slice(),
            br#"{"__proto__":null}"#.as_slice(),
            br#"{"a":1.0}"#.as_slice(),
            br#"{"a":-0.0}"#.as_slice(),
            br#"{"a":1e0}"#.as_slice(),
            br#"{"a":9007199254740992}"#.as_slice(),
            br#"{"a":"\ud800"}"#.as_slice(),
        ] {
            assert!(StrictValue::parse(raw).is_err(), "accepted {raw:?}");
        }
        assert_eq!(StrictValue::parse(b"[-0]").unwrap().canonical(), "[0]");
    }

    #[test]
    fn strict_json_rejects_trailing_content_and_non_object_top_level_is_allowed() {
        assert!(StrictValue::parse(b"{} {}").is_err());
        assert!(StrictValue::parse(b"{} trailing").is_err());
        // Scalars are valid strict values; envelope parsing rejects non-objects.
        assert!(StrictValue::parse(b"42").is_ok());
    }

    #[test]
    fn safe_integer_boundaries_accepted() {
        let value = StrictValue::parse(b"[-9007199254740991,9007199254740991]").unwrap();
        assert_eq!(value.canonical(), "[-9007199254740991,9007199254740991]");
        assert!(StrictValue::parse(b"[-9007199254740992]").is_err());
        assert!(StrictValue::parse(b"[9007199254740992]").is_err());
    }
}
