//! Deterministic JSON serialization for digest-bearing documents.
//!
//! Object members are emitted in **UTF-16 code unit order** — the same
//! ordering rule as `snapshot-protocol`'s canonical form and the
//! TypeScript `Object.keys().sort()` of the signing side — with no
//! insignificant whitespace. Leaf values are written by `serde_json`,
//! which is itself deterministic (shortest round-trip float
//! representation, fixed escape set).
//!
//! Unlike `snapshot-protocol`'s `StrictValue`, this writer accepts any
//! in-memory `serde_json::Value` (including non-integer numbers): it is
//! used to canonicalize `.json` module contents, where real-world JSON
//! legitimately contains floats. Digest-bearing documents built by this
//! crate only ever contain strings, arrays and objects — so output is a
//! pure function of the inputs.

use serde_json::Value;

/// Compare two object keys by UTF-16 code unit order (matches the
/// snapshot-protocol canonical ordering and JS `sort()` semantics).
pub fn utf16_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Serialize `value` to canonical bytes: no whitespace, object keys in
/// UTF-16 code unit order, deterministic leaf encoding.
pub fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => out.extend_from_slice(
            serde_json::to_string(s)
                .expect("string serialization")
                .as_bytes(),
        ),
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut members: Vec<(&String, &Value)> = map.iter().collect();
            members.sort_by(|a, b| utf16_cmp(a.0, b.0));
            out.push(b'{');
            for (index, (key, item)) in members.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("string serialization")
                        .as_bytes(),
                );
                out.push(b':');
                write_canonical(item, out);
            }
            out.push(b'}');
        }
    }
}

/// Serialize `value` to a canonical byte vector.
pub fn canonical_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_sort_by_utf16_order() {
        // "𝄞" (U+1D11E) sorts after BMP chars by UTF-16 units even though
        // its UTF-8 leading byte differs from surrogate ordering traps.
        let v = json!({"b": 1, "a": 2, "𝄞": 3, "z": 4});
        assert_eq!(
            String::from_utf8(canonical_bytes(&v)).unwrap(),
            "{\"a\":2,\"b\":1,\"z\":4,\"𝄞\":3}"
        );
    }

    #[test]
    fn nesting_and_numbers_are_stable() {
        let v = json!({"x": [1, -2, 1.5], "s": "q\"\\\n", "n": null, "t": true});
        let a = canonical_bytes(&v);
        let b = canonical_bytes(&v);
        assert_eq!(a, b);
        assert_eq!(
            String::from_utf8(a).unwrap(),
            "{\"n\":null,\"s\":\"q\\\"\\\\\\n\",\"t\":true,\"x\":[1,-2,1.5]}"
        );
    }
}
