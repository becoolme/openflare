//! Segment encoding for backend key layout. Tenant ids allow `.`/`:` which
//! collide with our `:`/`/` separators, so every embedded segment is
//! percent-escaped: `[A-Za-z0-9_-]` literal, anything else `%XX` (uppercase
//! hex). The encoding is injective — two distinct raw strings never map to
//! the same encoded segment.
//!
//! Implementation lives in the shared `vrt-securefs` crate (extracted so
//! `vrt-host` applies the identical encoding to tenant paths); these
//! re-exports keep `crate::encoding::*` callers unchanged.

pub use vrt_securefs::{segment_decode, segment_encode};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_injection_safety() {
        for raw in [
            "team-a",
            "a.b:c",
            "..",
            "a/b",
            "a%2Fb",
            "\u{79df}\u{6237}",
            "x y",
            "\0nul",
            "vrt:kv:other:key",
        ] {
            let encoded = segment_encode(raw);
            assert!(!encoded.contains(':'));
            assert!(!encoded.contains('/'));
            assert_eq!(segment_decode(&encoded).as_deref(), Some(raw));
        }
        assert_eq!(segment_decode("a%2"), None);
        assert_eq!(segment_decode("a%zz"), None);
    }
}
