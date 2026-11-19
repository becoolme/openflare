//! SQLite WAL 解析(§6.11 WAL1)— 供外部尾随者验证帧链完整性。
//! 格式依 sqlite.org/fileformat2;**不**信任文件元数据,逐帧验证
//! salt 连续性与累计 checksum(与 PoC `do_wal_poc.rs` 同款语义)。

/// WAL 文件头长。
pub const WAL_HDR: usize = 32;
/// WAL 帧头长。
pub const WAL_FRAME_HDR: usize = 24;

fn be32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().unwrap())
}

/// WAL 头字段(全部大端)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    /// 0x377f0682(LE 校验和)或 0x377f0683(BE 校验和)。
    pub magic: u32,
    /// 页大小(可为 1 表示 65536)。
    pub page_size: u32,
    /// checkpoint 序号(诊断用)。
    pub ckpt_seq: u32,
    /// WAL 代际盐对——重置(truncate/重建)即变。
    pub salt1: u32,
    /// 盐2。
    pub salt2: u32,
}

impl WalHeader {
    /// checksum 是否用大端读 u32(magic 末位 = 1)。
    pub fn cksum_be(&self) -> bool {
        self.magic & 1 == 1
    }
    /// 有效页大小(1 → 65536)。
    pub fn page_size(&self) -> u32 {
        if self.page_size == 1 {
            65536
        } else {
            self.page_size
        }
    }
}

/// 解析并验证 WAL 头(magic + 头自带 checksum)。
pub fn wal_header(buf: &[u8]) -> Option<WalHeader> {
    if buf.len() < WAL_HDR {
        return None;
    }
    let magic = be32(buf, 0);
    if magic != 0x377f_0682 && magic != 0x377f_0683 {
        return None;
    }
    let h = WalHeader {
        magic,
        page_size: be32(buf, 8),
        ckpt_seq: be32(buf, 12),
        salt1: be32(buf, 16),
        salt2: be32(buf, 20),
    };
    let ps = h.page_size();
    if !(512..=65536).contains(&ps) || !ps.is_power_of_two() {
        return None;
    }
    // 头 checksum 覆盖前 24 字节(magic..salt2),自包含可验。
    let ck = wal_cksum(h.cksum_be(), (0, 0), &buf[..24]);
    if ck != (be32(buf, 24), be32(buf, 28)) {
        return None;
    }
    Some(h)
}

/// SQLite WAL 累计校验和:每 8 字节一对 u32。
/// s1 += x0 + s2; s2 += x1 + s1。
pub fn wal_cksum(be: bool, seed: (u32, u32), data: &[u8]) -> (u32, u32) {
    let (mut s1, mut s2) = seed;
    for c in data.chunks_exact(8) {
        let (x0, x1) = if be {
            (
                u32::from_be_bytes(c[0..4].try_into().unwrap()),
                u32::from_be_bytes(c[4..8].try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(c[0..4].try_into().unwrap()),
                u32::from_le_bytes(c[4..8].try_into().unwrap()),
            )
        };
        s1 = s1.wrapping_add(x0).wrapping_add(s2);
        s2 = s2.wrapping_add(x1).wrapping_add(s1);
    }
    (s1, s2)
}

/// 一帧头解析结果。
#[derive(Debug, Clone, Copy)]
pub struct FrameHead {
    /// 页号(>=1)。
    pub page_no: u32,
    /// 提交后 db 页数(非提交帧 = 0)。
    pub commit_db_size: u32,
    /// 该帧后的累计 checksum。
    pub cksum: (u32, u32),
}

/// 从 `pos`(帧边界字节偏移)向前扫描,返回最后一个 checksum
/// 验证通过的帧末尾偏移及逐帧头列表。
/// `salt` 为期望的当前代际盐;`cksum` 为 `pos` 处累计校验和起点。
/// 返回的 frames 只含完整且校验通过的帧——不完整尾部留给下次读。
/// `be` = WAL 头 checksum 字节序(`header.cksum_be()`)。
/// 从 `pos` 起逐帧验证:salt 不符(代际变化)或 checksum 断裂
/// (撕裂/损坏尾)即停——返回已验证终点,不完整尾部留给下次。
pub fn scan_frames(
    buf: &[u8],
    pos: u64,
    page_size: u32,
    be: bool,
    salt: (u32, u32),
    cksum: (u32, u32),
) -> (u64, Vec<FrameHead>, (u32, u32)) {
    let fsz = WAL_FRAME_HDR + page_size as usize;
    let mut off = pos as usize;
    let mut ck = cksum;
    let mut frames = Vec::new();
    while off + fsz <= buf.len() {
        let f = &buf[off..off + fsz];
        if be32(f, 8) != salt.0 || be32(f, 12) != salt.1 {
            break; // 代际变化——停在前一有效帧
        }
        ck = wal_cksum(be, ck, &f[..8]);
        ck = wal_cksum(be, ck, &f[WAL_FRAME_HDR..]);
        if ck != (be32(f, 16), be32(f, 20)) {
            break; // 撕裂/损坏尾帧——留到下次
        }
        frames.push(FrameHead {
            page_no: be32(f, 0),
            commit_db_size: be32(f, 4),
            cksum: ck,
        });
        off += fsz;
    }
    (off as u64, frames, ck)
}

// ---------- 读事务钉位(WalPin)----------

use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// 外部尾随者的 WAL 钉位:打开只读连接并保持一个读事务——checkpoint
/// 无法把 WAL 回收/重置到本读者位点之下(PoC P2 已证实)。持有期
/// WAL 只增不缩,调用方必须按上限管理钉住窗口。
pub struct WalPin {
    conn: Connection,
}

impl WalPin {
    /// 只读打开 db 并 `BEGIN` + 一次实际读(物化 read mark)。
    /// 返回钉位;workerd `locking_mode=EXCLUSIVE` 或 shm 缺失时
    /// 打开/读取失败——调用方视为"不可尾随"(fail closed)。
    pub fn acquire(db: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_with_flags(
            db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("BEGIN")?;
        // 物化 read mark——空 BEGIN 不钉任何位置。
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        Ok(Self { conn })
    }

    /// 结束当前读事务并立刻重钉(短暂释放窗口:checkpoint 可推进到
    /// 新位点之下;窗口内被回收的帧必须是调用方已上传的)。
    pub fn repin(&mut self) -> Result<(), rusqlite::Error> {
        let _ = self.conn.execute_batch("ROLLBACK");
        self.conn.execute_batch("BEGIN")?;
        let _: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        Ok(())
    }
}

impl Drop for WalPin {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch("ROLLBACK");
    }
}

/// Fold a restored image's journal back into the main file so a
/// raw page-copy of `db` is the complete image. Returns false when
/// the checkpoint could not complete (busy/corrupt) — callers must
/// then treat the image as foreign and fail closed.
pub fn fold_journal(db: &Path) -> Result<bool, rusqlite::Error> {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let (busy, _log, _ckpt): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    Ok(busy == 0)
}

/// `PRAGMA integrity_check` on a staged db file — replay gate.
pub fn integrity_ok(db: &Path) -> Result<bool, rusqlite::Error> {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    Ok(ok == "ok")
}
