//! RV-R72: one range-check helper shared by every binary's startup
//! config validator (instead of scattered `if` chains).
//!
//! `ensure_range!` produces a *static* error message that names the
//! field and the allowed range, so it fits the `&'static str` error
//! payloads the existing `ConfigError` enums use.

/// Timeout floor / ceiling for every `*_ms` timeout field (ms).
pub const TIMEOUT_MS_MIN: u64 = 100;
pub const TIMEOUT_MS_MAX: u64 = 300_000;
/// Upper bound for TTL-style durations (30 days, ms).
pub const TTL_MS_MAX: i64 = 30 * 24 * 3600 * 1000;

/// `ensure_range!(value, "field.name", min, max, |msg| Err(...))`:
/// when `value` is outside `min..=max` evaluates the closure with the
/// static message `"<field> out of range [<min>, <max>]"` and returns it
/// from the enclosing function.
#[macro_export]
macro_rules! ensure_range {
    ($value:expr, $field:literal, $min:expr, $max:expr, $mk_err:expr) => {
        if !($min..=$max).contains(&$value) {
            return ($mk_err)(concat!(
                $field,
                " out of range [",
                stringify!($min),
                ", ",
                stringify!($max),
                "]"
            ));
        }
    };
}

/// Timeout (ms) bounded to `TIMEOUT_MS_MIN..=TIMEOUT_MS_MAX`.
#[macro_export]
macro_rules! ensure_timeout_ms {
    ($value:expr, $field:literal, $mk_err:expr) => {
        // Literals (not the constants) so `stringify!` prints numbers;
        // `timeout_literals_match_constants` pins them together.
        $crate::ensure_range!($value, $field, 100, 300_000, $mk_err)
    };
}

#[cfg(test)]
mod tests {
    fn check(v: u64) -> Result<(), &'static str> {
        crate::ensure_timeout_ms!(v, "x.timeoutMs", |m| Err(m));
        Ok(())
    }

    #[test]
    fn timeout_literals_match_constants() {
        assert_eq!(super::TIMEOUT_MS_MIN, 100);
        assert_eq!(super::TIMEOUT_MS_MAX, 300_000);
    }

    #[test]
    fn range_message_names_field_and_bounds() {
        assert!(check(100).is_ok());
        assert!(check(300_000).is_ok());
        for bad in [0u64, 99, 300_001, u64::MAX] {
            let e = check(bad).unwrap_err();
            assert!(e.starts_with("x.timeoutMs out of range ["), "{e}");
            assert!(e.ends_with("[100, 300_000]"), "{e}");
        }
    }
}
