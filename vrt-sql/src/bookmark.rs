//! D1 session bookmark (section 5): fixed-width zero-padded hex + HMAC.
//!
//! Format: `{epoch:016x}-{lsn:016x}-{hmac16hex}`. `localeCompare` lexical order
//! is isomorphic to commit order (the worker d1-api client takes the max by `localeCompare`).
//! The HMAC covers `databaseId|epoch|lsn`, preventing clients from forging commit positions.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct BookmarkSigner {
    key: [u8; 32],
}

impl BookmarkSigner {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Mint a commit-position bookmark: `{epoch:016x}-{lsn:016x}-{hmac16hex}`.
    pub fn sign(&self, database_id: &str, epoch: u64, lsn: u64) -> String {
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(database_id.as_bytes());
        mac.update(b"|");
        mac.update(format!("{epoch:016x}").as_bytes());
        mac.update(b"|");
        mac.update(format!("{lsn:016x}").as_bytes());
        let tag = mac.finalize().into_bytes();
        format!("{epoch:016x}-{lsn:016x}-{}", hex::encode(&tag[..8]))
    }

    /// Verify the signature and parse out (epoch, lsn); forged/expired -> error text aligned with
    /// `D1_SESSION_ERROR: invalid bookmark or constraint` (section 5).
    pub fn verify(&self, database_id: &str, bookmark: &str) -> Result<(u64, u64), &'static str> {
        const ERR: &str = "invalid bookmark or constraint";
        let parts: Vec<&str> = bookmark.split('-').collect();
        if parts.len() != 3 || parts[0].len() != 16 || parts[1].len() != 16 || parts[2].len() != 16
        {
            return Err(ERR);
        }
        let epoch = u64::from_str_radix(parts[0], 16).map_err(|_| ERR)?;
        let lsn = u64::from_str_radix(parts[1], 16).map_err(|_| ERR)?;
        if self.sign(database_id, epoch, lsn) != bookmark {
            return Err(ERR);
        }
        Ok((epoch, lsn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bookmark_roundtrip_and_forgery() {
        let s = BookmarkSigner::new([7u8; 32]);
        let b = s.sign("db1", 3, 0x2f);
        assert_eq!(b.len(), 16 + 1 + 16 + 1 + 16);
        assert_eq!(s.verify("db1", &b).unwrap(), (3, 0x2f));
        // Forged / cross-db / tampered lsn all fail
        assert!(s.verify("db2", &b).is_err());
        assert!(s.verify("db1", &b.replace('f', "a")).is_err());
        assert!(s.verify("db1", "0-0-0").is_err());
        // Lexical order = commit order
        let (a, c) = (s.sign("d", 1, 9), s.sign("d", 1, 10));
        assert!(a < c);
        assert!(s.sign("d", 2, 0) > s.sign("d", 1, u64::MAX));
    }
}
