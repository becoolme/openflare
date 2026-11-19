//! D1 session bookmark（§5）：定宽零填充十六进制 + HMAC。
//!
//! 格式：`{epoch:016x}-{lsn:016x}-{hmac16hex}`。`localeCompare` 字典序
//! 与提交序同构（worker d1-api 客户端按 `localeCompare` 取最大）。
//! HMAC 覆盖 `databaseId|epoch|lsn`，防客户端伪造提交位点。

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

    /// 生成提交位点书签：`{epoch:016x}-{lsn:016x}-{hmac16hex}`。
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

    /// 验签并解析出 (epoch, lsn)；伪造/过期 → 错误文本与
    /// `D1_SESSION_ERROR: invalid bookmark or constraint` 对齐（§5）。
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
        // 伪造 / 跨库 / 篡改 lsn 均失败
        assert!(s.verify("db2", &b).is_err());
        assert!(s.verify("db1", &b.replace('f', "a")).is_err());
        assert!(s.verify("db1", "0-0-0").is_err());
        // 字典序 = 提交序
        let (a, c) = (s.sign("d", 1, 9), s.sign("d", 1, 10));
        assert!(a < c);
        assert!(s.sign("d", 2, 0) > s.sign("d", 1, u64::MAX));
    }
}
