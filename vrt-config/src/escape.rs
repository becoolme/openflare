//! capnp text-format escaping for `Text` and `Data` literals.
//!
//! `Text` fields (module sources, var/secret values, embedded `props`
//! JSON) are emitted as double-quoted literals. Only printable ASCII
//! (`0x20..=0x7E`) is emitted raw except `"` and `\`, which are escaped;
//! every other byte — including all multi-byte UTF-8 sequences — is
//! emitted as `\xNN`. The result is pure ASCII, so the generated config
//! is deterministic regardless of input encoding edge cases and cannot
//! smuggle structure through quote/newline/`}` injection.
//!

/// Escape `text` into a capnp double-quoted `Text` literal (including the
/// surrounding quotes).
pub fn capnp_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for byte in text.bytes() {
        match byte {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x20..=0x7e => out.push(byte as char),
            _ => {
                out.push_str("\\x");
                out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
                out.push(char::from_digit((byte & 0xf) as u32, 16).unwrap());
            }
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_quotes_backslash_and_control() {
        assert_eq!(capnp_text("a\"b\\c\nd}e"), "\"a\\\"b\\\\c\\x0ad}e\"");
        assert_eq!(capnp_text(""), "\"\"");
        assert_eq!(capnp_text("plain ascii-ok_123"), "\"plain ascii-ok_123\"");
    }

    #[test]
    fn escapes_non_ascii_as_hex_bytes() {
        // '中' is UTF-8 E4 B8 AD → three \xNN escapes; no raw multibyte
        // output, so a homoglyph or stray continuation byte cannot smuggle
        // structure into the text format.
        assert_eq!(capnp_text("中"), "\"\\xe4\\xb8\\xad\"");
        assert_eq!(capnp_text("\t\r"), "\"\\x09\\x0d\"");
        assert_eq!(capnp_text("\u{7f}"), "\"\\x7f\"");
    }
}
