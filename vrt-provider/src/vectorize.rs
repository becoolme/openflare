//! Vectorize data plane — `facade-vectorize` -> provider -> per-index
//! sqlite-vec files. Exact-KNN backend (recall = 1.0, a strict
//! superset of CF's ANN recall): `vec0` virtual table for the
//! cosine/l2 fast path, Rust exact scan over stored f32 blobs for
//! `dot-product` (vec0 has no dot metric) and for filtered queries.
//!
//! Mutations apply synchronously, then return a `mutationId` — CF's
//! async visibility lag is an implementation detail, not the contract.
//! Layout: `<dir>/<tenant>/<indexId>.vec.db`.

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, Once};

/// CF caps: vectors per insert/upsert batch, vector id, metadata blob.
pub const MAX_VECTORS_PER_BATCH: usize = 1000;
const MAX_ID_BYTES: usize = 64;
const MAX_META_BYTES: usize = 1024;
const MAX_NAMESPACE_BYTES: usize = 63;
/// `topK` bound — CF documents a max well above this; we bound the
/// result set since our scans are exact.
const MAX_TOP_K: u32 = 10_000;
/// Bounded conn cache: one index db per entry, closed on store drop.
const MAX_OPEN_CONNS: usize = 64;

// `sqlite3_auto_extension` — declared directly rather than via
// `rusqlite::ffi`: with `loadable_extension` enabled rusqlite's
// bindgen routes this through the extension-side `sqlite3_api`
// function table, which is null in a host process. The bundled
// sqlite always exports the real symbol.
unsafe extern "C" {
    fn sqlite3_auto_extension(cb: Option<unsafe extern "C" fn()>) -> std::ffi::c_int;
}

/// sqlite-vec is a C auto-extension — register once per process; every
/// new `Connection` picks it up.
static REGISTER: Once = Once::new();
fn ensure_extension() {
    REGISTER.call_once(|| unsafe {
        sqlite3_auto_extension(Some(std::mem::transmute::<
            *const (),
            unsafe extern "C" fn(),
        >(sqlite_vec::sqlite3_vec_init as *const ())));
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Cosine,
    Euclidean,
    DotProduct,
}

impl Metric {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "cosine" => Some(Self::Cosine),
            "euclidean" => Some(Self::Euclidean),
            "dot-product" => Some(Self::DotProduct),
            _ => None,
        }
    }
}

struct Index {
    conn: Connection,
    dims: u32,
    metric: Metric,
}

/// Per-tenant store: lazily-opened sqlite files keyed `tenant/indexId`.
pub struct VectorizeStore {
    root: PathBuf,
    conns: Mutex<HashMap<String, std::sync::Arc<Mutex<Index>>>>,
}

/// `{code,error}` wire envelope — the upstream shim turns a non-200
/// `{code:number,error:string}` body into `<OP>_ERROR (code = N)`.
fn vec_err(code: u32, error: impl Into<String>) -> (u16, Value) {
    (400, json!({ "code": code, "error": error.into() }))
}

impl VectorizeStore {
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        ensure_extension();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            conns: Mutex::new(HashMap::new()),
        })
    }

    fn index(
        &self,
        tenant: &str,
        id: &str,
        dims: u32,
        metric: Metric,
    ) -> Result<std::sync::Arc<Mutex<Index>>, (u16, Value)> {
        let key = format!("{tenant}/{id}");
        {
            let conns = self.conns.lock().unwrap();
            if let Some(ix) = conns.get(&key) {
                let ix_guard = ix.lock().unwrap();
                if ix_guard.dims == dims && ix_guard.metric == metric {
                    drop(ix_guard);
                    return Ok(ix.clone());
                }
                drop(ix_guard);
                return Err(vec_err(
                    3101,
                    "index spec drifted from signed desired state",
                ));
            }
        }
        let dir = self.root.join(tenant);
        std::fs::create_dir_all(&dir).map_err(|_| vec_err(3100, "index store unavailable"))?;
        let path = dir.join(format!("{id}.vec.db"));
        let conn = Connection::open(&path).map_err(|_| vec_err(3100, "index store unavailable"))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|_| vec_err(3100, "index store unavailable"))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta(
               key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS ids(
               rowid INTEGER PRIMARY KEY,
               vid   TEXT NOT NULL UNIQUE,
               ns    TEXT NOT NULL DEFAULT '',
               meta  TEXT NOT NULL DEFAULT '{}',
               vals  BLOB NOT NULL);",
        )
        .map_err(|_| vec_err(3100, "index init failed"))?;
        // Signed spec is the source of truth; the on-disk index must
        // agree — a rebuilt file with the wrong dims would corrupt
        // ranking silently otherwise.
        let stored_dims: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key='dims'", [], |r| r.get(0))
            .optional()
            .map_err(|_| vec_err(3100, "index init failed"))?;
        match stored_dims.as_deref() {
            Some(v) if v != dims.to_string() => {
                return Err(vec_err(3101, "index dimensions mismatch"))
            }
            None => {
                conn.execute(
                    "INSERT INTO meta(key,value) VALUES('dims',?1),
                     ('metric',?2),('mut_seq','0')",
                    params![dims.to_string(), format!("{:?}", metric)],
                )
                .map_err(|_| vec_err(3100, "index init failed"))?;
            }
            _ => {}
        }
        // vec0 has no dot-product metric — dot indexes rank via the
        // Rust exact scan over `ids.vals` instead.
        if metric != Metric::DotProduct {
            let dm = if metric == Metric::Cosine {
                "cosine"
            } else {
                "L2"
            };
            conn.execute_batch(&format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS vecs USING \
                 vec0(embedding float[{dims}] distance_metric={dm});"
            ))
            .map_err(|_| vec_err(3100, "vector index init failed"))?;
        }
        let mut conns = self.conns.lock().unwrap();
        if conns.len() >= MAX_OPEN_CONNS {
            conns.clear();
        }
        let ix = std::sync::Arc::new(Mutex::new(Index { conn, dims, metric }));
        conns.insert(key, ix.clone());
        Ok(ix)
    }

    /// One op, blocking — callers wrap in `spawn_blocking`.
    pub fn op(
        &self,
        tenant: &str,
        id: &str,
        dims: u32,
        metric: &str,
        op: &str,
        body: &[u8],
    ) -> (u16, Value) {
        let Some(metric) = Metric::parse(metric) else {
            return vec_err(3102, "unknown index metric");
        };
        let ix = match self.index(tenant, id, dims, metric) {
            Ok(ix) => ix,
            Err(e) => return e,
        };
        let mut ix = ix.lock().unwrap();
        let r = match op {
            "info" => ix.info(),
            "query" => ix.query(body),
            "insert" => ix.mutate(body, false),
            "upsert" => ix.mutate(body, true),
            "getByIds" => ix.get_by_ids(body),
            "deleteByIds" => ix.delete(body),
            _ => return vec_err(3000, "unsupported vectorize op"),
        };
        r.unwrap_or_else(|e| e)
    }
}

fn decode_vals(blob: &[u8]) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

fn encode_vals(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn parse_vec(v: &Value, dims: u32) -> Result<Vec<f32>, (u16, Value)> {
    let arr = v
        .as_array()
        .ok_or_else(|| vec_err(3010, "vector must be a number array"))?;
    if arr.len() != dims as usize {
        return Err(vec_err(3010, "vector dimensions mismatch"));
    }
    arr.iter()
        .map(|x| {
            x.as_f64()
                .filter(|f| f.is_finite())
                .map(|f| f as f32)
                .ok_or_else(|| vec_err(3010, "vector values must be finite numbers"))
        })
        .collect()
}

fn validate_meta(meta: &Value) -> Result<(), (u16, Value)> {
    let obj = meta
        .as_object()
        .ok_or_else(|| vec_err(3011, "metadata must be an object"))?;
    let mut bytes = 0usize;
    for (k, v) in obj {
        bytes += k.len();
        let ok = match v {
            Value::String(s) => {
                bytes += s.len();
                true
            }
            Value::Number(n) => n.is_f64() || n.is_i64() || n.is_u64(),
            Value::Bool(_) => true,
            Value::Array(a) => {
                a.iter().all(|x| x.is_string())
                    && a.iter().map(|x| x.as_str().unwrap().len()).sum::<usize>() < 1024
            }
            _ => false,
        };
        if !ok {
            return Err(vec_err(3011, "unsupported metadata value type"));
        }
    }
    if bytes > MAX_META_BYTES {
        return Err(vec_err(3011, "metadata too large"));
    }
    Ok(())
}

fn new_mutation(ix: &Index) -> Result<String, (u16, Value)> {
    let seq: i64 = ix
        .conn
        .query_row(
            "UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='mut_seq' \
             RETURNING CAST(value AS INTEGER)",
            [],
            |r| r.get(0),
        )
        .map_err(|_| vec_err(3100, "index write failed"))?;
    // Opaque to callers; mutationIds only need uniqueness for clients
    // polling `describe` lag — not part of CF's response contract.
    Ok(format!("{seq:016x}"))
}

impl Index {
    fn info(&self) -> Result<(u16, Value), (u16, Value)> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM ids", [], |r| r.get(0))
            .map_err(|_| vec_err(3100, "index read failed"))?;
        let mut_seq: String = self
            .conn
            .query_row("SELECT value FROM meta WHERE key='mut_seq'", [], |r| {
                r.get(0)
            })
            .unwrap_or_else(|_| "0".into());
        Ok((
            200,
            json!({
                "vectorCount": count,
                "dimensions": self.dims,
                "processedUpToDatetime": 0,
                "processedUpToMutation": mut_seq,
            }),
        ))
    }

    /// `{vectors:[…]}` insert (replace=false) / upsert (replace=true).
    fn mutate(&mut self, body: &[u8], replace: bool) -> Result<(u16, Value), (u16, Value)> {
        let parsed: Value =
            serde_json::from_slice(body).map_err(|_| vec_err(3000, "malformed request body"))?;
        let vectors = parsed
            .get("vectors")
            .and_then(|v| v.as_array())
            .ok_or_else(|| vec_err(3000, "missing vectors array"))?;
        if vectors.is_empty() || vectors.len() > MAX_VECTORS_PER_BATCH {
            return Err(vec_err(3000, "batch size out of bounds"));
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| vec_err(3100, "index write failed"))?;
        for v in vectors {
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty() && s.len() <= MAX_ID_BYTES)
                .ok_or_else(|| vec_err(3012, "invalid vector id"))?;
            let vals = parse_vec(
                v.get("values")
                    .ok_or_else(|| vec_err(3010, "missing values"))?,
                self.dims,
            )?;
            let ns = v
                .get("namespace")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if ns.len() > MAX_NAMESPACE_BYTES {
                return Err(vec_err(3013, "namespace too long"));
            }
            let meta = match v.get("metadata") {
                Some(m) => {
                    validate_meta(m)?;
                    m.to_string()
                }
                None => "{}".to_string(),
            };
            if replace {
                // Keep the rowid so vecs stays aligned across the
                // update; delete the stale vector row first.
                if let Some(rowid) = tx
                    .query_row("SELECT rowid FROM ids WHERE vid=?1", params![id], |r| {
                        r.get::<_, i64>(0)
                    })
                    .optional()
                    .map_err(|_| vec_err(3100, "index write failed"))?
                {
                    if self.metric != Metric::DotProduct {
                        tx.execute("DELETE FROM vecs WHERE rowid=?1", params![rowid])
                            .map_err(|_| vec_err(3100, "index write failed"))?;
                    }
                    tx.execute(
                        "UPDATE ids SET ns=?2, meta=?3, vals=?4 WHERE rowid=?1",
                        params![rowid, ns, meta, encode_vals(&vals)],
                    )
                    .map_err(|_| vec_err(3100, "index write failed"))?;
                    if self.metric != Metric::DotProduct {
                        tx.execute(
                            "INSERT INTO vecs(rowid,embedding) VALUES(?1,?2)",
                            params![rowid, serde_json::to_string(&vals).unwrap()],
                        )
                        .map_err(|_| vec_err(3100, "index write failed"))?;
                    }
                    continue;
                }
            }
            let blob = encode_vals(&vals);
            let inserted = tx.execute(
                "INSERT INTO ids(vid,ns,meta,vals) VALUES(?1,?2,?3,?4)",
                params![id, ns, meta, blob],
            );
            match inserted {
                Ok(_) => {
                    let rowid = tx.last_insert_rowid();
                    if self.metric != Metric::DotProduct {
                        tx.execute(
                            "INSERT INTO vecs(rowid,embedding) VALUES(?1,?2)",
                            params![rowid, serde_json::to_string(&vals).unwrap()],
                        )
                        .map_err(|_| vec_err(3100, "index write failed"))?;
                    }
                }
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    return Err(vec_err(3020, "duplicate vector id"))
                }
                Err(_) => return Err(vec_err(3100, "index write failed")),
            }
        }
        tx.commit()
            .map_err(|_| vec_err(3100, "index write failed"))?;
        let mutation_id = new_mutation(self)?;
        Ok((200, json!({ "mutationId": mutation_id })))
    }

    fn query(&self, body: &[u8]) -> Result<(u16, Value), (u16, Value)> {
        let parsed: Value =
            serde_json::from_slice(body).map_err(|_| vec_err(3000, "malformed request body"))?;
        let top_k = parsed
            .get("topK")
            .and_then(|v| v.as_u64())
            .unwrap_or(5)
            .clamp(1, MAX_TOP_K as u64) as usize;
        let return_values = parsed
            .get("returnValues")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let return_metadata = parsed
            .get("returnMetadata")
            .and_then(|v| v.as_str())
            .unwrap_or("none");
        if !matches!(return_metadata, "none" | "all" | "indexed") {
            return Err(vec_err(3000, "bad returnMetadata"));
        }
        let ns = parsed.get("namespace").and_then(|v| v.as_str());
        let filter = parsed.get("filter");
        // Resolve the query vector: raw `vector` or `vectorId` lookup.
        let qvec = if let Some(q) = parsed.get("vector") {
            parse_vec(q, self.dims)?
        } else if let Some(vid) = parsed.get("vectorId").and_then(|v| v.as_str()) {
            let blob: Option<Vec<u8>> = self
                .conn
                .query_row("SELECT vals FROM ids WHERE vid=?1", params![vid], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(|_| vec_err(3100, "index read failed"))?;
            let Some(blob) = blob else {
                return Err(vec_err(3030, "query vector id not found"));
            };
            decode_vals(&blob)
        } else {
            return Err(vec_err(3000, "missing query vector"));
        };

        // Candidates = namespace filter (if any), else the full index.
        let mut stmt = self
            .conn
            .prepare(if ns.is_some() {
                "SELECT rowid, vid, meta, vals FROM ids WHERE ns=?1"
            } else {
                "SELECT rowid, vid, meta, vals FROM ids"
            })
            .map_err(|_| vec_err(3100, "index read failed"))?;
        let rows: Vec<(i64, String, String, Vec<u8>)> = match ns {
            Some(n) => stmt
                .query_map(params![n], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })
                .map_err(|_| vec_err(3100, "index read failed"))?
                .collect::<Result<_, _>>()
                .map_err(|_| vec_err(3100, "index read failed"))?,
            None => stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .map_err(|_| vec_err(3100, "index read failed"))?
                .collect::<Result<_, _>>()
                .map_err(|_| vec_err(3100, "index read failed"))?,
        };
        drop(stmt);

        // Score = CF semantics: cosine → similarity in [-1,1];
        // euclidean → L2 distance; dot-product → NEGATIVE dot.
        let mut scored: Vec<(f64, i64, String, String, Vec<u8>)> = rows
            .into_iter()
            .filter(|(_, _, meta, _)| meta_matches(meta, filter))
            .map(|(rowid, vid, meta, vals)| {
                let v = decode_vals(&vals);
                let score = match self.metric {
                    Metric::Cosine => {
                        let dot: f64 = qvec
                            .iter()
                            .zip(&v)
                            .map(|(a, b)| (*a as f64) * (*b as f64))
                            .sum();
                        let qa: f64 = qvec.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
                        let va: f64 = v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
                        if qa == 0.0 || va == 0.0 {
                            0.0
                        } else {
                            dot / (qa * va)
                        }
                    }
                    Metric::Euclidean => qvec
                        .iter()
                        .zip(&v)
                        .map(|(a, b)| ((*a as f64) - (*b as f64)).powi(2))
                        .sum::<f64>()
                        .sqrt(),
                    Metric::DotProduct => -qvec
                        .iter()
                        .zip(&v)
                        .map(|(a, b)| (*a as f64) * (*b as f64))
                        .sum::<f64>(),
                };
                (score, rowid, vid, meta, vals)
            })
            .collect();
        match self.metric {
            // Similarity metrics: higher score first.
            Metric::Cosine => {
                scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal))
            }
            // Distance / negative-dot: lower score first.
            Metric::Euclidean | Metric::DotProduct => {
                scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            }
        }
        scored.truncate(top_k);
        let count = scored.len();
        let matches: Vec<Value> = scored
            .into_iter()
            .map(|(score, _, vid, meta, vals)| {
                let mut m = Map::new();
                m.insert("id".into(), json!(vid));
                m.insert("score".into(), json!(score));
                if return_values {
                    m.insert("values".into(), json!(decode_vals(&vals)));
                }
                if return_metadata != "none" {
                    let meta: Value = serde_json::from_str(&meta).unwrap_or_else(|_| json!({}));
                    m.insert("metadata".into(), meta);
                }
                Value::Object(m)
            })
            .collect();
        Ok((200, json!({ "count": count, "matches": matches })))
    }

    fn get_by_ids(&self, body: &[u8]) -> Result<(u16, Value), (u16, Value)> {
        let parsed: Value =
            serde_json::from_slice(body).map_err(|_| vec_err(3000, "malformed request body"))?;
        let ids = parsed
            .get("ids")
            .and_then(|v| v.as_array())
            .ok_or_else(|| vec_err(3000, "missing ids array"))?;
        if ids.is_empty() || ids.len() > MAX_VECTORS_PER_BATCH {
            return Err(vec_err(3000, "ids out of bounds"));
        }
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let id = id
                .as_str()
                .ok_or_else(|| vec_err(3000, "ids must be strings"))?;
            let row: Option<(String, String, Vec<u8>)> = self
                .conn
                .query_row(
                    "SELECT vid, meta, vals FROM ids WHERE vid=?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(|_| vec_err(3100, "index read failed"))?;
            if let Some((vid, meta, vals)) = row {
                out.push(json!({
                    "id": vid,
                    "values": decode_vals(&vals),
                    "metadata": serde_json::from_str::<Value>(&meta).unwrap_or_else(|_| json!({})),
                }));
            }
        }
        Ok((200, json!(out)))
    }

    fn delete(&mut self, body: &[u8]) -> Result<(u16, Value), (u16, Value)> {
        let parsed: Value =
            serde_json::from_slice(body).map_err(|_| vec_err(3000, "malformed request body"))?;
        let ids = parsed
            .get("ids")
            .and_then(|v| v.as_array())
            .ok_or_else(|| vec_err(3000, "missing ids array"))?;
        if ids.is_empty() || ids.len() > MAX_VECTORS_PER_BATCH {
            return Err(vec_err(3000, "ids out of bounds"));
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| vec_err(3100, "index write failed"))?;
        for id in ids {
            let id = id
                .as_str()
                .ok_or_else(|| vec_err(3000, "ids must be strings"))?;
            if let Some(rowid) = tx
                .query_row("SELECT rowid FROM ids WHERE vid=?1", params![id], |r| {
                    r.get::<_, i64>(0)
                })
                .optional()
                .map_err(|_| vec_err(3100, "index write failed"))?
            {
                if self.metric != Metric::DotProduct {
                    tx.execute("DELETE FROM vecs WHERE rowid=?1", params![rowid])
                        .map_err(|_| vec_err(3100, "index write failed"))?;
                }
                tx.execute("DELETE FROM ids WHERE rowid=?1", params![rowid])
                    .map_err(|_| vec_err(3100, "index write failed"))?;
            }
        }
        tx.commit()
            .map_err(|_| vec_err(3100, "index write failed"))?;
        let mutation_id = new_mutation(self)?;
        Ok((200, json!({ "mutationId": mutation_id })))
    }
}

/// `{key: value}` (implicit `$eq`) or `{key: {$op: value}}` filters
/// over stored metadata. Array-valued fields satisfy a scalar op when
/// ANY element does (CF string[] semantics).
fn meta_matches(meta: &str, filter: Option<&Value>) -> bool {
    let Some(filter) = filter else { return true };
    let Some(fobj) = filter.as_object() else {
        return true;
    };
    let meta: Value = serde_json::from_str(meta).unwrap_or_else(|_| json!({}));
    let mobj = meta.as_object().cloned().unwrap_or_default();
    for (k, cond) in fobj {
        let field = mobj.get(k).cloned().unwrap_or(Value::Null);
        let ops: Vec<(&str, Value)> = match cond {
            Value::Object(o) if o.keys().next().is_some_and(|k| k.starts_with('$')) => {
                o.iter().map(|(op, v)| (op.as_str(), v.clone())).collect()
            }
            v => vec![("$eq", v.clone())],
        };
        for (op, target) in ops {
            let elems: Vec<&Value> = match &field {
                Value::Array(a) => a.iter().collect(),
                v => vec![v],
            };
            let hit = elems.iter().any(|e| cmp_meta(op, e, &target));
            if !hit {
                return false;
            }
        }
    }
    true
}

fn cmp_meta(op: &str, field: &Value, target: &Value) -> bool {
    use std::cmp::Ordering;
    // Numeric compare when both sides are numbers; string compare for
    // string-vs-string; otherwise no ordering exists.
    let ord: Option<Ordering> = match (field.as_f64(), target.as_f64()) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ => match (field.as_str(), target.as_str()) {
            (Some(a), Some(b)) => Some(a.cmp(b)),
            _ => None,
        },
    };
    match op {
        "$eq" => field == target,
        "$ne" => field != target,
        "$lt" => ord == Some(Ordering::Less),
        "$lte" => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        "$gt" => ord == Some(Ordering::Greater),
        "$gte" => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
        "$in" => target
            .as_array()
            .is_some_and(|a| a.iter().any(|t| field == t)),
        "$nin" => !target
            .as_array()
            .is_some_and(|a| a.iter().any(|t| field == t)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, VectorizeStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = VectorizeStore::new(dir.path().join("vx")).unwrap();
        (dir, store)
    }

    fn body(v: Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }

    fn insert(store: &VectorizeStore, vectors: Value) -> (u16, Value) {
        store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "insert",
            &body(json!({ "vectors": vectors })),
        )
    }

    #[test]
    fn insert_query_roundtrip_cosine() {
        let (_dir, store) = store();
        let (s, _) = insert(
            &store,
            json!([
              {"id":"a","values":[1.0,0.0,0.0],"metadata":{"k":"x"}},
              {"id":"b","values":[0.0,1.0,0.0]},
              {"id":"c","values":[0.9,0.1,0.0]}
            ]),
        );
        assert_eq!(s, 200);
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "query",
            &body(json!({"vector":[1.0,0.0,0.0],"topK":2,"returnValues":true,
                         "returnMetadata":"all"})),
        );
        assert_eq!(s, 200);
        assert_eq!(out["count"], 2);
        let m = &out["matches"];
        // Cosine similarity ordering: a (1.0) before c (~0.994).
        assert_eq!(m[0]["id"], "a");
        assert!((m[0]["score"].as_f64().unwrap() - 1.0).abs() < 1e-6);
        assert_eq!(m[1]["id"], "c");
        assert_eq!(m[0]["values"], json!([1.0, 0.0, 0.0]));
        assert_eq!(m[0]["metadata"], json!({"k":"x"}));
        assert!(m[0].get("namespace").is_none());
    }

    #[test]
    fn duplicate_insert_rejected_upsert_replaces() {
        let (_dir, store) = store();
        assert_eq!(
            insert(&store, json!([{"id":"a","values":[1.0,0.0,0.0]}])).0,
            200
        );
        let (s, e) = insert(&store, json!([{"id":"a","values":[0.0,1.0,0.0]}]));
        assert_eq!(s, 400);
        assert_eq!(e["code"], 3020);
        let (s, _) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "upsert",
            &body(json!({"vectors":[{"id":"a","values":[0.0,1.0,0.0]}]})),
        );
        assert_eq!(s, 200);
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "getByIds",
            &body(json!({"ids":["a"]})),
        );
        assert_eq!(s, 200);
        assert_eq!(out[0]["values"], json!([0.0, 1.0, 0.0]));
    }

    #[test]
    fn dims_mismatch_and_bad_metric_fail() {
        let (_dir, store) = store();
        let (s, e) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "insert",
            &body(json!({"vectors":[{"id":"a","values":[1.0,2.0]}]})),
        );
        assert_eq!(s, 400);
        assert_eq!(e["code"], 3010);
        let (s, _) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "bogus",
            "insert",
            b"{}",
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn namespace_and_metadata_filter() {
        let (_dir, store) = store();
        insert(
            &store,
            json!([
              {"id":"a","values":[1.0,0.0,0.0],"namespace":"n1","metadata":{"tier":"gold"}},
              {"id":"b","values":[1.0,0.0,0.0],"namespace":"n2","metadata":{"tier":"free"}},
              {"id":"c","values":[1.0,0.0,0.0],"namespace":"n1","metadata":{"tier":"free"}}
            ]),
        );
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "query",
            &body(json!({"vector":[1.0,0.0,0.0],"topK":5,"namespace":"n1"})),
        );
        assert_eq!(s, 200);
        assert_eq!(out["count"], 2);
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "query",
            &body(json!({"vector":[1.0,0.0,0.0],"topK":5,
                         "filter":{"tier":{"$eq":"gold"}}})),
        );
        assert_eq!(s, 200);
        assert_eq!(out["count"], 1);
        assert_eq!(out["matches"][0]["id"], "a");
    }

    #[test]
    fn dot_product_negative_score_ordering() {
        let (_dir, store) = store();
        store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "dot-product",
            "insert",
            &body(json!({"vectors":[
              {"id":"big","values":[10.0,0.0,0.0]},
              {"id":"small","values":[1.0,0.0,0.0]}
            ]})),
        );
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "dot-product",
            "query",
            &body(json!({"vector":[1.0,0.0,0.0],"topK":2})),
        );
        assert_eq!(s, 200);
        // CF: score = negative dot product; most negative first.
        assert_eq!(out["matches"][0]["id"], "big");
        assert_eq!(out["matches"][0]["score"], -10.0);
        assert_eq!(out["matches"][1]["score"], -1.0);
    }

    #[test]
    fn delete_and_query_by_id() {
        let (_dir, store) = store();
        insert(
            &store,
            json!([{"id":"a","values":[1.0,0.0,0.0]},{"id":"b","values":[0.0,1.0,0.0]}]),
        );
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "query",
            &body(json!({"vectorId":"a","topK":2})),
        );
        assert_eq!(s, 200);
        assert_eq!(out["matches"][0]["id"], "a");
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "deleteByIds",
            &body(json!({"ids":["a"]})),
        );
        assert_eq!(s, 200);
        assert!(out.get("mutationId").is_some());
        let (s, out) = store.op(
            "t1",
            "abcdefghijklmnopqrstuvwxyz",
            3,
            "cosine",
            "getByIds",
            &body(json!({"ids":["a","b"]})),
        );
        assert_eq!(s, 200);
        assert_eq!(out.as_array().unwrap().len(), 1);
        assert_eq!(out[0]["id"], "b");
    }

    #[test]
    fn info_reports_count_and_dims() {
        let (_dir, store) = store();
        insert(&store, json!([{"id":"a","values":[1.0,0.0,0.0]}]));
        let (s, out) = store.op("t1", "abcdefghijklmnopqrstuvwxyz", 3, "cosine", "info", b"");
        assert_eq!(s, 200);
        assert_eq!(out["vectorCount"], 1);
        assert_eq!(out["dimensions"], 3);
    }

    #[test]
    fn spec_drift_fails_closed() {
        let (_dir, store) = store();
        insert(&store, json!([{"id":"a","values":[1.0,0.0,0.0]}]));
        // Same index id, different dims — signed desired says 3, caller
        // claims 4 → refuse rather than corrupt the index.
        let (s, e) = store.op("t1", "abcdefghijklmnopqrstuvwxyz", 4, "cosine", "info", b"");
        assert_eq!(s, 400);
        assert_eq!(e["code"], 3101);
    }
}
