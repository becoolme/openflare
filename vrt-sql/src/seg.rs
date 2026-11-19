//! `vrt-seg v1` segment format (T36 / section 4.2 option 1).
//!
//! **Custom format, not claimed to be LTX-compatible**. After each local commit, the WAL frames
//! produced by that transaction are parsed and packed into a segment for upload; on restore the page frames are applied directly on top of the snapshot
//! (equivalent to WAL replay, but without depending on WAL header/salt semantics).
//!
//! Layout (all big-endian):
//! ```text
//! magic      8B   "VRTSEG01"
//! version    u32  = 1
//! flags      u32  reserved = 0
//! databaseId 32B  ASCII, right-padded with \0
//! epoch      u64
//! lsn        u64
//! prev_lsn   u64  lsn of the previous committed segment (chain check; 0 or the snapshot baseline for the first segment)
//! page_size  u32
//! frame_cnt  u32
//! wal_begin  u64  frame index of this segment's first frame within the WAL (for debugging, may be 0)
//! frames     frame_cnt × { page_no u32, commit_db_size u32, data[page_size] }
//! sha256     32B  checksum over all of the above bytes
//! ```
//!
//! A frame with `commit_db_size != 0` is a commit record: after applying it on restore, truncate the db to
//! that page count. The salt/checksum in the WAL frame header are not stored -- the segment-level sha256 is the sole
//! integrity source.

use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"VRTSEG01";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + 4 + 32 + 8 + 8 + 8 + 4 + 4 + 8;
const SHA_LEN: usize = 32;
/// Per-segment cap (frame data): 64 MiB -- a defensive gate before segmentation gaps appear.
pub const SEG_MAX_BYTES: usize = 64 * 1024 * 1024;

/// One frame: page number + db page count after commit (0 for non-commit frames) + page image.
#[derive(Debug, Clone)]
pub struct SegFrame {
    pub page_no: u32,
    pub commit_db_size: u32,
    pub data: Vec<u8>,
}

/// Segment content.
#[derive(Debug, Clone)]
pub struct Segment {
    pub database_id: String,
    pub epoch: u64,
    pub lsn: u64,
    pub prev_lsn: u64,
    pub page_size: u32,
    pub wal_begin: u64,
    pub frames: Vec<SegFrame>,
}

/// Segment error (fail-closed signal).
#[derive(Debug, thiserror::Error)]
pub enum SegError {
    #[error("segment too short")]
    Truncated,
    #[error("segment bad magic")]
    BadMagic,
    #[error("segment version {0} unsupported")]
    Version(u32),
    #[error("segment sha256 mismatch")]
    Checksum,
    #[error("segment field invalid: {0}")]
    Invalid(&'static str),
}

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}

/// Encode a segment (including the trailing sha256).
pub fn encode(seg: &Segment) -> Vec<u8> {
    let mut b =
        Vec::with_capacity(HEADER_LEN + seg.frames.len() * (8 + seg.page_size as usize) + SHA_LEN);
    b.extend_from_slice(MAGIC);
    put_u32(&mut b, VERSION);
    put_u32(&mut b, 0);
    let mut id = [0u8; 32];
    let idb = seg.database_id.as_bytes();
    id[..idb.len().min(32)].copy_from_slice(&idb[..idb.len().min(32)]);
    b.extend_from_slice(&id);
    put_u64(&mut b, seg.epoch);
    put_u64(&mut b, seg.lsn);
    put_u64(&mut b, seg.prev_lsn);
    put_u32(&mut b, seg.page_size);
    put_u32(&mut b, seg.frames.len() as u32);
    put_u64(&mut b, seg.wal_begin);
    for f in &seg.frames {
        put_u32(&mut b, f.page_no);
        put_u32(&mut b, f.commit_db_size);
        debug_assert_eq!(f.data.len(), seg.page_size as usize);
        b.extend_from_slice(&f.data);
    }
    let digest = Sha256::digest(&b);
    b.extend_from_slice(&digest);
    b
}

/// Decode + verify sha256. Any mismatch -> explicit error (fail closed).
pub fn decode(buf: &[u8]) -> Result<Segment, SegError> {
    if buf.len() < HEADER_LEN + SHA_LEN {
        return Err(SegError::Truncated);
    }
    if buf.len() > SEG_MAX_BYTES {
        return Err(SegError::Invalid("oversize"));
    }
    let (body, sha) = buf.split_at(buf.len() - SHA_LEN);
    if Sha256::digest(body).as_slice() != sha {
        return Err(SegError::Checksum);
    }
    if &body[..8] != MAGIC {
        return Err(SegError::BadMagic);
    }
    let mut p = 8usize;
    let u32_at = |p: &mut usize| -> Result<u32, SegError> {
        let v = u32::from_be_bytes(
            body[*p..*p + 4]
                .try_into()
                .map_err(|_| SegError::Truncated)?,
        );
        *p += 4;
        Ok(v)
    };
    let u64_at = |p: &mut usize| -> Result<u64, SegError> {
        let v = u64::from_be_bytes(
            body[*p..*p + 8]
                .try_into()
                .map_err(|_| SegError::Truncated)?,
        );
        *p += 8;
        Ok(v)
    };
    let version = u32_at(&mut p)?;
    if version != VERSION {
        return Err(SegError::Version(version));
    }
    let _flags = u32_at(&mut p)?;
    let id_raw = &body[p..p + 32];
    p += 32;
    let id_end = id_raw.iter().position(|b| *b == 0).unwrap_or(32);
    let database_id = std::str::from_utf8(&id_raw[..id_end])
        .map_err(|_| SegError::Invalid("database_id utf8"))?
        .to_string();
    let epoch = u64_at(&mut p)?;
    let lsn = u64_at(&mut p)?;
    let prev_lsn = u64_at(&mut p)?;
    let page_size = u32_at(&mut p)?;
    if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(SegError::Invalid("page_size"));
    }
    let frame_cnt = u32_at(&mut p)? as usize;
    let wal_begin = u64_at(&mut p)?;
    let need = HEADER_LEN + frame_cnt * (8 + page_size as usize) + SHA_LEN;
    if buf.len() != need {
        return Err(SegError::Invalid("frame count/length mismatch"));
    }
    let mut frames = Vec::with_capacity(frame_cnt);
    for _ in 0..frame_cnt {
        let page_no = u32_at(&mut p)?;
        let commit_db_size = u32_at(&mut p)?;
        if page_no == 0 {
            return Err(SegError::Invalid("page_no 0"));
        }
        let data = body[p..p + page_size as usize].to_vec();
        p += page_size as usize;
        frames.push(SegFrame {
            page_no,
            commit_db_size,
            data,
        });
    }
    Ok(Segment {
        database_id,
        epoch,
        lsn,
        prev_lsn,
        page_size,
        wal_begin,
        frames,
    })
}

/// Apply segment frames to the db image file (pwrite pages + commit-frame truncation).
/// The caller guarantees lsn order and a consistent baseline.
pub fn apply_frames(file: &mut std::fs::File, seg: &Segment) -> Result<(), SegError> {
    use std::io::{Seek, SeekFrom, Write};
    let ps = seg.page_size as u64;
    for f in &seg.frames {
        if f.data.len() != seg.page_size as usize {
            return Err(SegError::Invalid("frame data len"));
        }
        file.seek(SeekFrom::Start((f.page_no as u64 - 1) * ps))
            .map_err(|_| SegError::Invalid("seek"))?;
        file.write_all(&f.data)
            .map_err(|_| SegError::Invalid("write"))?;
        if f.commit_db_size != 0 {
            file.set_len(f.commit_db_size as u64 * ps)
                .map_err(|_| SegError::Invalid("truncate"))?;
        }
    }
    file.sync_all().map_err(|_| SegError::Invalid("fsync"))?;
    Ok(())
}

/// Segment object key: `{kind.prefix}/{id}/{epoch:016x}/{lsn:016x}.seg`
/// (T37r: D1 `db/...` layout unchanged, Queue `q/...`).
pub fn seg_key(
    kind: crate::paths::ResourceKind,
    database_id: &str,
    epoch: u64,
    lsn: u64,
) -> String {
    format!(
        "{}/{database_id}/{epoch:016x}/{lsn:016x}.seg",
        kind.object_prefix()
    )
}

/// Snapshot object key: `{kind.prefix}/{id}/{epoch:016x}/snap-{lsn:016x}.snap`.
pub fn snap_key(
    kind: crate::paths::ResourceKind,
    database_id: &str,
    epoch: u64,
    lsn: u64,
) -> String {
    format!(
        "{}/{database_id}/{epoch:016x}/snap-{lsn:016x}.snap",
        kind.object_prefix()
    )
}

/// Manifest object key.
pub fn manifest_key(kind: crate::paths::ResourceKind, database_id: &str) -> String {
    format!("{}/{database_id}/manifest", kind.object_prefix())
}
