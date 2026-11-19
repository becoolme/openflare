//! `vrt-seg v1` 段格式（T36 / §4.2 方案 1）。
//!
//! **自定义格式，不冒称 LTX 兼容**。每次本地提交后，本次事务产生
//! 的 WAL 帧被解析、打包为段上传；恢复时把页帧直接应用到快照之上
//! （等效 WAL replay，但不依赖 WAL 文件头/盐值语义）。
//!
//! 布局（全部大端）：
//! ```text
//! magic      8B   "VRTSEG01"
//! version    u32  = 1
//! flags      u32  reserved = 0
//! databaseId 32B  ASCII，右补 \0
//! epoch      u64
//! lsn        u64
//! prev_lsn   u64  前一已提交段 lsn（链校验；首段为 0 或快照基线）
//! page_size  u32
//! frame_cnt  u32
//! wal_begin  u64  本段首帧在 WAL 中的帧下标（调试用，可为 0）
//! frames     frame_cnt × { page_no u32, commit_db_size u32, data[page_size] }
//! sha256     32B  对以上全部字节的校验
//! ```
//!
//! `commit_db_size != 0` 的帧是提交记录：恢复时应用后把 db 截断到
//! 该页数。WAL 帧头里的 salt/checksum 不存储——段级 sha256 是唯一
//! 完整性来源。

use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"VRTSEG01";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + 4 + 32 + 8 + 8 + 8 + 4 + 4 + 8;
const SHA_LEN: usize = 32;
/// 单段上限（帧数据）：64 MiB——超出即分段缺失前的防御闸。
pub const SEG_MAX_BYTES: usize = 64 * 1024 * 1024;

/// 一帧：页号 + 提交后 db 页数（非提交帧为 0）+ 页镜像。
#[derive(Debug, Clone)]
pub struct SegFrame {
    pub page_no: u32,
    pub commit_db_size: u32,
    pub data: Vec<u8>,
}

/// 段内容。
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

/// 段错误（fail closed 信号）。
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

/// 编码段（含尾部 sha256）。
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

/// 解码 + 校验 sha256。任何不符 → 明确错误（fail closed）。
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

/// 把段帧应用到 db 镜像文件（pwrite 页 + 提交帧截断）。
/// 调用方保证按 lsn 顺序、基线一致。
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

/// 段对象 key：`{kind.prefix}/{id}/{epoch:016x}/{lsn:016x}.seg`
/// （T37r：D1 `db/…` 布局不变，Queue `q/…`）。
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

/// 快照对象 key：`{kind.prefix}/{id}/{epoch:016x}/snap-{lsn:016x}.snap`。
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

/// manifest 对象 key。
pub fn manifest_key(kind: crate::paths::ResourceKind, database_id: &str) -> String {
    format!("{}/{database_id}/manifest", kind.object_prefix())
}
