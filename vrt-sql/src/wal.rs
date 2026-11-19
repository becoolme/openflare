//! SQLite WAL parsing (section 6.11 WAL1) -- lets an external tailer verify frame-chain integrity.
//! Format per sqlite.org/fileformat2; does **not** trust file metadata, verifies frame by frame
//! salt continuity and the cumulative checksum (same semantics as the PoC `do_wal_poc.rs`).

/// WAL file header length.
pub const WAL_HDR: usize = 32;
/// WAL frame header length.
pub const WAL_FRAME_HDR: usize = 24;

fn be32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().unwrap())
}

/// WAL header fields (all big-endian).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    /// 0x377f0682 (LE checksum) or 0x377f0683 (BE checksum).
    pub magic: u32,
    /// Page size (1 means 65536).
    pub page_size: u32,
    /// checkpoint sequence number (diagnostic).
    pub ckpt_seq: u32,
    /// WAL generation salt pair -- changes whenever the WAL is reset (truncate/rebuild).
    pub salt1: u32,
    /// salt 2.
    pub salt2: u32,
}

impl WalHeader {
    /// Whether the checksum reads u32 big-endian (last magic bit = 1).
    pub fn cksum_be(&self) -> bool {
        self.magic & 1 == 1
    }
    /// Effective page size (1 -> 65536).
    pub fn page_size(&self) -> u32 {
        if self.page_size == 1 {
            65536
        } else {
            self.page_size
        }
    }
}

/// Parse and verify the WAL header (magic + the header's own checksum).
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
    // The header checksum covers the first 24 bytes (magic..salt2), self-contained and verifiable.
    let ck = wal_cksum(h.cksum_be(), (0, 0), &buf[..24]);
    if ck != (be32(buf, 24), be32(buf, 28)) {
        return None;
    }
    Some(h)
}

/// SQLite WAL cumulative checksum: one pair of u32 every 8 bytes.
/// s1 += x0 + s2; s2 += x1 + s1.
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

/// Parse result of one frame header.
#[derive(Debug, Clone, Copy)]
pub struct FrameHead {
    /// Page number (>=1).
    pub page_no: u32,
    /// db page count after commit (non-commit frame = 0).
    pub commit_db_size: u32,
    /// Cumulative checksum after this frame.
    pub cksum: (u32, u32),
}

/// Scan forward from `pos` (a frame-boundary byte offset), returning the end offset of the last frame whose checksum
/// verified plus the list of per-frame headers.
/// `salt` is the expected current generation salt; `cksum` is the cumulative checksum start at `pos`.
/// The returned frames contain only complete, verified frames -- an incomplete tail is left for the next read.
/// `be` = the WAL header checksum byte order (`header.cksum_be()`).
/// Verify frame by frame from `pos`: stop on salt mismatch (generation change) or checksum break
/// (torn/corrupt tail) -- return the verified end point; the incomplete tail is left for next time.
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
            break; // generation change -- stop at the last valid frame
        }
        ck = wal_cksum(be, ck, &f[..8]);
        ck = wal_cksum(be, ck, &f[WAL_FRAME_HDR..]);
        if ck != (be32(f, 16), be32(f, 20)) {
            break; // torn/corrupt tail frame -- leave for next time
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

// ---------- read-transaction pin (WalPin) ----------

use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// WAL pin for an external tailer: opens a read-only connection and holds a read transaction -- checkpoint
/// cannot recycle/reset the WAL below this reader's position (confirmed by PoC P2). While held,
/// the WAL only grows and never shrinks; the caller must bound the pin window by a cap.
pub struct WalPin {
    conn: Connection,
}

impl WalPin {
    /// Open the db read-only and `BEGIN` + do one actual read (materializing the read mark).
    /// Returns the pin; with workerd `locking_mode=EXCLUSIVE` or a missing shm,
    /// open/read fails -- the caller treats it as "cannot tail" (fail closed).
    pub fn acquire(db: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_with_flags(
            db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("BEGIN")?;
        // Materialize the read mark -- an empty BEGIN pins no position.
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))?;
        Ok(Self { conn })
    }

    /// End the current read transaction and re-pin immediately (brief release window: checkpoint may advance
    /// below the new position; frames recycled within the window must already have been uploaded by the caller).
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
