//! W5: multipart conformance — one semantic contract exercised against
//! the memory, filesystem, and mock-S3 backends. The server layer wraps
//! the backend-internal upload id in a signed token; provider-level
//! tests pass that internal id directly.

mod support;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::r2::fs::FsR2Provider;
use vrt_provider::r2::s3::{S3Config, S3R2Provider, SecretString};
use vrt_provider::r2::wire::{
    R2HttpMetadata, R2MultipartCreateOptions, R2SsecKey, R2UploadedPart,
    R2_MULTIPART_MIN_PART_BYTES,
};
use vrt_provider::r2::{MemoryR2Provider, R2GetResult, R2Provider};
use vrt_provider::BindingContext;

const PART: usize = R2_MULTIPART_MIN_PART_BYTES as usize;
const BUCKET2: &str = "bucket-2";

fn ctx_for(tid: &str, bucket: &str) -> BindingContext {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![
                tenant(
                    TENANT,
                    vec![worker(
                        WORKER,
                        VERSION,
                        DIGEST,
                        vec![r2_binding("BUCKET", BUCKET), r2_binding("BUCKET2", BUCKET2)],
                    )],
                ),
                tenant(
                    OTHER_TENANT,
                    vec![worker(
                        "w-other",
                        VERSION,
                        DIGEST,
                        vec![r2_binding("BUCKET", BUCKET)],
                    )],
                ),
            ],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(
        if tid == TENANT { WORKER } else { "w-other" },
        if bucket == BUCKET {
            "BUCKET"
        } else {
            "BUCKET2"
        },
        if tid == TENANT { WORKER } else { "w-other" },
        VERSION,
        DIGEST,
        bucket,
        7,
    );
    authorize(&auth, tid, &id, ResourceKind::R2).unwrap()
}

fn create_opts() -> R2MultipartCreateOptions {
    R2MultipartCreateOptions {
        http_metadata: R2HttpMetadata {
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        },
        custom_metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
        ..Default::default()
    }
}

fn part(n: i32, etag: &str) -> R2UploadedPart {
    R2UploadedPart {
        part_number: n,
        etag: etag.to_string(),
    }
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// The full shared contract — identical on every backend.
async fn conformance(p: &dyn R2Provider) {
    let c = ctx_for(TENANT, BUCKET);
    let big = vec![0xABu8; PART];
    let small = b"tail".to_vec();

    // -- happy path: create → 3 parts → complete → object --
    // Window covers create→complete: S3 records `uploaded` at create
    // time while the local backends stamp it at commit; both are inside.
    let started_ms = unix_ms();
    let up = p
        .create_multipart(&c, "mp/obj", &create_opts())
        .await
        .unwrap()
        .upload_id;
    let p1 = p
        .upload_part(&c, "mp/obj", &up, 1, &big, None)
        .await
        .unwrap();
    let p2 = p
        .upload_part(&c, "mp/obj", &up, 2, &big, None)
        .await
        .unwrap();
    let p3 = p
        .upload_part(&c, "mp/obj", &up, 3, &small, None)
        .await
        .unwrap();
    assert!(p1.etag.len() == 32 && p1.etag.chars().all(|c| c.is_ascii_hexdigit()));
    let (e1, e2, e3) = (p1.etag.clone(), p2.etag.clone(), p3.etag.clone());
    let meta = p
        .complete_multipart(&c, "mp/obj", &up, &[p1, p2, p3])
        .await
        .unwrap()
        .expect("complete returns metadata");
    let finished_ms = unix_ms();
    assert_eq!(meta.size, (2 * PART + small.len()) as u64, "object size");
    assert!(meta.etag.ends_with("-3"), "composite etag: {}", meta.etag);
    assert_eq!(meta.http_etag, format!("\"{}\"", meta.etag));
    assert!(
        (started_ms..=finished_ms).contains(&meta.uploaded),
        "uploaded {} outside [{started_ms}, {finished_ms}]",
        meta.uploaded
    );
    assert_eq!(
        meta.custom_metadata
            .as_ref()
            .unwrap()
            .get("k")
            .map(String::as_str),
        Some("v"),
        "custom metadata preserved"
    );
    assert_eq!(
        meta.http_metadata
            .as_ref()
            .and_then(|h| h.content_type.as_deref()),
        Some("text/plain"),
        "http metadata preserved"
    );
    match p.get(&c, "mp/obj", None, None, None).await.unwrap() {
        R2GetResult::Body(stored, body) => {
            assert_eq!(body.len(), 2 * PART + small.len());
            assert_eq!(&body[..4], &[0xAB; 4]);
            assert_eq!(&body[2 * PART..], small.as_slice());
            // `complete()` is the authority: later reads must agree.
            assert_eq!(stored, meta, "complete() metadata matches later get()");
        }
        _ => panic!("completed object must be readable"),
    }

    // W5-r4: retrying the SAME complete is idempotent across backends —
    // the consumed upload plus an object whose etag equals the
    // request-derived composite ⇒ stored metadata, not
    // MULTIPART_INVALID. (The existing assert below covers the
    // different-parts case.)
    let retried = p
        .complete_multipart(
            &c,
            "mp/obj",
            &up,
            &[part(1, &e1), part(2, &e2), part(3, &e3)],
        )
        .await
        .unwrap()
        .expect("idempotent complete retry returns metadata");
    assert_eq!(
        retried, meta,
        "idempotent retry returns the same object metadata, metadata included"
    );

    // -- terminal state: complete consumed the upload --
    assert!(
        matches!(
            p.upload_part(&c, "mp/obj", &up, 4, b"x", None).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "uploadPart after complete"
    );
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/obj", &up, &[part(1, "00")])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "second complete (NoSuchUpload semantics)"
    );
    assert!(
        matches!(
            p.abort_multipart(&c, "mp/obj", &up).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "abort after complete"
    );

    // -- complete() result contract, no metadata supplied --
    // Cloudflare documents `complete()` as `Promise<R2Object>` and both
    // metadata fields are non-optional on `R2Object`; workerd's non-list
    // parser materializes them as `{}` when the object has none. Every
    // backend must return them present, so a caller never re-reads.
    let up_empty = p
        .create_multipart(&c, "mp/empty-meta", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let empty_part = p
        .upload_part(&c, "mp/empty-meta", &up_empty, 1, &big, None)
        .await
        .unwrap();
    let empty = p
        .complete_multipart(&c, "mp/empty-meta", &up_empty, &[empty_part])
        .await
        .unwrap()
        .expect("complete returns metadata");
    assert_eq!(
        empty.http_metadata,
        Some(R2HttpMetadata::default()),
        "httpMetadata present (empty) when the upload supplied none"
    );
    assert_eq!(
        empty.custom_metadata,
        Some(BTreeMap::new()),
        "customMetadata present (empty) when the upload supplied none"
    );
    assert_eq!(empty.range, None);
    assert_eq!(empty.size, PART as u64);
    assert!(!empty.etag.is_empty());
    assert!(empty.uploaded > 0);

    // -- abort flow + post-abort ops --
    let up2 = p
        .create_multipart(&c, "mp/abort", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "mp/abort", &up2, 1, b"x", None)
        .await
        .unwrap();
    p.abort_multipart(&c, "mp/abort", &up2).await.unwrap();
    assert!(
        matches!(
            p.upload_part(&c, "mp/abort", &up2, 2, b"x", None).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "part after abort"
    );
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/abort", &up2, &[part(1, "00")])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "complete after abort"
    );
    assert!(
        matches!(
            p.abort_multipart(&c, "mp/abort", &up2).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "double abort"
    );
    assert!(
        matches!(
            p.get(&c, "mp/abort", None, None, None).await.unwrap(),
            R2GetResult::Missing
        ),
        "aborted upload must not commit"
    );

    // -- part rules --
    let up3 = p
        .create_multipart(&c, "mp/rules", &Default::default())
        .await
        .unwrap()
        .upload_id;
    assert!(
        matches!(
            p.upload_part(&c, "mp/rules", &up3, 0, b"x", None).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "partNumber 0"
    );
    assert!(
        matches!(
            p.upload_part(&c, "mp/rules", &up3, 10_001, b"x", None)
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "partNumber 10001"
    );
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/rules", &up3, &[]).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "empty parts"
    );
    let pa = p
        .upload_part(&c, "mp/rules", &up3, 1, &big, None)
        .await
        .unwrap();
    let pb = p
        .upload_part(&c, "mp/rules", &up3, 2, b"t", None)
        .await
        .unwrap();
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/rules", &up3, &[pb.clone(), pa.clone()])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "descending parts"
    );
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/rules", &up3, &[pa.clone(), pa.clone()])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "duplicate parts"
    );
    assert!(
        matches!(
            p.complete_multipart(
                &c,
                "mp/rules",
                &up3,
                &[part(1, "00000000000000000000000000000000"), pb]
            )
            .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "etag mismatch"
    );
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/rules", &up3, &[part(9, &pa.etag)])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "unknown part number"
    );

    // -- minimum part size: non-final < 5MiB rejected, single small ok --
    let up4 = p
        .create_multipart(&c, "mp/minpart", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let s1 = p
        .upload_part(&c, "mp/minpart", &up4, 1, b"tiny", None)
        .await
        .unwrap();
    let s2 = p
        .upload_part(&c, "mp/minpart", &up4, 2, b"tail", None)
        .await
        .unwrap();
    assert!(
        matches!(
            p.complete_multipart(&c, "mp/minpart", &up4, &[s1, s2])
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "non-final part below 5MiB"
    );
    let up5 = p
        .create_multipart(&c, "mp/single", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let sp = p
        .upload_part(&c, "mp/single", &up5, 1, b"one", None)
        .await
        .unwrap();
    assert!(
        p.complete_multipart(&c, "mp/single", &up5, &[sp])
            .await
            .unwrap()
            .is_some(),
        "single small part allowed"
    );

    // -- same partNumber re-upload replaces --
    let up6 = p
        .create_multipart(&c, "mp/replace", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "mp/replace", &up6, 1, b"first", None)
        .await
        .unwrap();
    let second = p
        .upload_part(&c, "mp/replace", &up6, 1, b"second", None)
        .await
        .unwrap();
    p.complete_multipart(&c, "mp/replace", &up6, &[second])
        .await
        .unwrap();
    match p.get(&c, "mp/replace", None, None, None).await.unwrap() {
        R2GetResult::Body(_, body) => assert_eq!(body, b"second", "part replace"),
        _ => panic!("replaced part object missing"),
    }

    // -- unknown/cross-scope upload ids --
    assert!(matches!(
        p.upload_part(&c, "k", "no-such-upload", 1, b"x", None)
            .await,
        Err(ProviderError::R2MultipartInvalid)
    ));
    let up7 = p
        .create_multipart(&c, "mp/keyA", &Default::default())
        .await
        .unwrap()
        .upload_id;
    assert!(
        matches!(
            p.upload_part(&c, "mp/keyB", &up7, 1, b"x", None).await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "key mismatch"
    );
    let other_tenant = ctx_for(OTHER_TENANT, BUCKET);
    assert!(
        matches!(
            p.upload_part(&other_tenant, "mp/keyA", &up7, 1, b"x", None)
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "cross-tenant id"
    );
    let other_bucket = ctx_for(TENANT, BUCKET2);
    assert!(
        matches!(
            p.upload_part(&other_bucket, "mp/keyA", &up7, 1, b"x", None)
                .await,
            Err(ProviderError::R2MultipartInvalid)
        ),
        "cross-bucket id"
    );

    // -- complete/abort race: exactly one terminal op wins --
    let up8 = p
        .create_multipart(&c, "mp/race", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let rp = p
        .upload_part(&c, "mp/race", &up8, 1, b"x", None)
        .await
        .unwrap();
    let parts = [rp];
    let (cres, ares) = tokio::join!(
        p.complete_multipart(&c, "mp/race", &up8, &parts),
        p.abort_multipart(&c, "mp/race", &up8)
    );
    let wins = [cres.is_ok(), ares.is_ok()]
        .into_iter()
        .filter(|b| *b)
        .count();
    assert_eq!(wins, 1, "exactly one terminal op must win the race");
}

#[tokio::test]
async fn memory_conformance() {
    conformance(&MemoryR2Provider::new()).await;
}

#[tokio::test]
async fn memory_active_upload_cap() {
    // Per-tenant (cross-bucket) cap: uploads opened in ANY bucket of
    // the tenant count, and the count+insert is atomic under the
    // shared mutex — concurrency cannot slip past the ceiling.
    let p = MemoryR2Provider::new().with_max_active_uploads(4);
    let a = ctx_for(TENANT, BUCKET);
    let b = ctx_for(TENANT, BUCKET2);
    for i in 0..2 {
        p.create_multipart(&a, &format!("k{i}"), &Default::default())
            .await
            .unwrap();
        p.create_multipart(&b, &format!("k{i}"), &Default::default())
            .await
            .unwrap();
    }
    assert!(matches!(
        p.create_multipart(&a, "overflow", &Default::default())
            .await,
        Err(ProviderError::R2Overloaded)
    ));
    // A different tenant's ledger is unaffected.
    p.create_multipart(&ctx_for(OTHER_TENANT, BUCKET), "k", &Default::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn memory_create_cap_atomic_under_concurrency() {
    // 16 concurrent creates against a cap of 4: exactly 4 win — the
    // check lives inside the same Mutex as the insert.
    let p = Arc::new(MemoryR2Provider::new().with_max_active_uploads(4));
    let mut handles = Vec::new();
    for i in 0..16 {
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            p.create_multipart(
                &ctx_for(TENANT, BUCKET),
                &format!("k{i}"),
                &Default::default(),
            )
            .await
        }));
    }
    let mut ok = 0usize;
    for h in handles {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 4, "concurrent creates exceeded the tenant cap");
}

#[tokio::test]
async fn memory_staging_byte_quota() {
    // Boundary: staged bytes exactly at the tenant cap admit; one
    // byte over rejects as R2Overloaded; abort releases the budget.
    let p = MemoryR2Provider::new().with_staging_max_bytes_per_tenant(10);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up, 1, &[0u8; 10], None)
        .await
        .unwrap();
    assert!(matches!(
        p.upload_part(&c, "k", &up, 2, &[0u8; 1], None).await,
        Err(ProviderError::R2Overloaded)
    ));
    p.abort_multipart(&c, "k", &up).await.unwrap();
    let up2 = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up2, 1, &[0u8; 10], None)
        .await
        .unwrap();
}

#[tokio::test]
async fn memory_total_size_cap() {
    let p = MemoryR2Provider::new().with_max_object_bytes(PART as u64 + 4);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    // A part exceeding the cap outright.
    assert!(matches!(
        p.upload_part(&c, "k", &up, 1, &vec![0u8; PART + 8], None)
            .await,
        Err(ProviderError::R2ObjectTooLarge)
    ));
    // Two parts individually legal but exceeding the cumulative cap.
    let up2 = p
        .create_multipart(&c, "k2", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k2", &up2, 1, &vec![0u8; PART], None)
        .await
        .unwrap();
    assert!(
        matches!(
            p.upload_part(&c, "k2", &up2, 2, &vec![0u8; 8], None).await,
            Err(ProviderError::R2ObjectTooLarge)
        ),
        "cumulative cap"
    );
    // Replacing part 1 with smaller bytes must subtract the old size.
    let up3 = p
        .create_multipart(&c, "k3", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k3", &up3, 1, &vec![0u8; PART], None)
        .await
        .unwrap();
    p.upload_part(&c, "k3", &up3, 1, &vec![0u8; 4], None)
        .await
        .unwrap();
    p.upload_part(&c, "k3", &up3, 2, &vec![0u8; 4], None)
        .await
        .unwrap();
}

#[tokio::test]
async fn fs_conformance() {
    let dir = support::TestDir::new();
    let p = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024).unwrap();
    conformance(&p).await;
}

#[tokio::test]
async fn fs_rejects_malformed_internal_id() {
    let dir = support::TestDir::new();
    let p = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024).unwrap();
    let c = ctx_for(TENANT, BUCKET);
    // Internal ids are provider-issued uuids; a forged/traversal id
    // reaching the backend still fails closed before any path join.
    for bad in ["../escape", "a/b", "..", "x".repeat(64).as_str(), ""] {
        let r = p.upload_part(&c, "k", bad, 1, b"x", None).await;
        assert!(
            matches!(r, Err(ProviderError::R2MultipartInvalid)),
            "accepted internal id {bad:?}"
        );
    }
}

#[tokio::test]
async fn fs_gc_reaps_expired_keeps_active() {
    let dir = support::TestDir::new();
    let p = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024).unwrap();
    let c = ctx_for(TENANT, BUCKET);
    let stale = p
        .create_multipart(&c, "k-stale", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k-stale", &stale, 1, b"x", None)
        .await
        .unwrap();
    // Deterministic expiry: rewrite the staged meta with a past
    // expiresMs — no wall-clock dependence under parallel tests.
    let meta_path = dir
        .path()
        .join("r2")
        .join(vrt_securefs::segment_encode(TENANT))
        .join(vrt_securefs::segment_encode(BUCKET))
        .join(".multipart")
        .join(&stale)
        .join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta["expiresMs"] = serde_json::json!(1);
    std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    // Stale upload is reaped; ops on it now fail.
    let report = p.sweep_staging().unwrap();
    let (removed, freed) = (report.expired, report.bytes_freed);
    assert_eq!(removed, 1);
    assert!(freed > 0);
    assert!(matches!(
        p.upload_part(&c, "k-stale", &stale, 2, b"y", None).await,
        Err(ProviderError::R2MultipartInvalid)
    ));
    // A fresh upload is untouched by the sweep.
    let live = p
        .create_multipart(&c, "k-live", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k-live", &live, 1, b"x", None)
        .await
        .unwrap();
    assert_eq!(p.sweep_staging().unwrap().expired, 0, "active upload swept");
    p.abort_multipart(&c, "k-live", &live).await.unwrap();
}

#[tokio::test]
async fn fs_restart_resumes_staging() {
    let dir = support::TestDir::new();
    let root = dir.path().join("r2");
    let c = ctx_for(TENANT, BUCKET);
    let up = {
        let p = FsR2Provider::new(root.clone(), 64 * 1024 * 1024).unwrap();
        let up = p
            .create_multipart(&c, "k", &create_opts())
            .await
            .unwrap()
            .upload_id;
        p.upload_part(&c, "k", &up, 1, &vec![0xAA; PART], None)
            .await
            .unwrap();
        up
    };
    // Provider restart: staging dirs persist; the upload resumes.
    let p2 = FsR2Provider::new(root, 64 * 1024 * 1024).unwrap();
    let last = p2
        .upload_part(&c, "k", &up, 2, b"tail", None)
        .await
        .unwrap();
    let expected = part_md5(&vec![0xAAu8; PART]);
    let meta = p2
        .complete_multipart(&c, "k", &up, &[part(1, &expected), last])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.size, PART as u64 + 4);
}

fn part_md5(b: &[u8]) -> String {
    format!("{:x}", md5::compute(b))
}

#[tokio::test]
async fn fs_staging_bytes_gauge_source() {
    let dir = support::TestDir::new();
    let p = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024).unwrap();
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up, 1, b"12345", None)
        .await
        .unwrap();
    let rows = p.staging_bytes().unwrap();
    let row = rows
        .iter()
        .find(|(t, b, _)| t == TENANT && b == BUCKET)
        .expect("staging row");
    assert_eq!(row.2, 5);
    p.abort_multipart(&c, "k", &up).await.unwrap();
    let row = p
        .staging_bytes()
        .unwrap()
        .into_iter()
        .find(|(t, b, _)| t == TENANT && b == BUCKET)
        .map(|r| r.2)
        .unwrap_or(0);
    assert_eq!(row, 0, "abort frees staged bytes");
}

// ------------------------------------------------------------- mock S3

#[derive(Default)]
struct MockMp {
    /// physical key → (vrt headers captured at create, etag, body).
    objects: HashMapCompat,
    /// uploadId → staging.
    uploads: BTreeMap<String, MockUpload>,
    next: usize,
    /// When set, a complete answers `200` with an embedded `<Error>`.
    embedded_error: Option<String>,
    /// Commit-then-drop or plain-drop for the next complete.
    drop_next_complete: Option<bool>,
    /// (fault, commit-first?) for the next complete response — W5-r3:
    /// exercises the service-fault → confirm-HEAD classification.
    complete_fault: Option<(CompleteFault, bool)>,
    /// Drop the next uploadPart once (transport failure).
    drop_next_part: bool,
    /// Drop the next HEAD once (confirmation unreachable → UNKNOWN).
    drop_next_head: bool,
    /// Every request as `(method, path+query, headers)`.
    log: Vec<(String, String, Vec<(String, String)>)>,
}

/// Complete-response fault modes the mock can inject (W5-r3).
#[derive(Clone, Copy, Debug)]
enum CompleteFault {
    /// 500 + `<Error><Code>InternalError</Code></Error>`.
    Status500,
    /// 200 + embedded `<Error><Code>InternalError</Code></Error>`.
    EmbeddedInternal,
    /// 200 + non-XML garbage body.
    GarbageXml,
    /// 200 + `<CompleteMultipartUploadResult><ETag></ETag>…`.
    EmptyEtag,
}
type HashMapCompat = BTreeMap<String, (Vec<(String, String)>, String, Vec<u8>)>;

struct MockUpload {
    key: String,
    meta: Vec<(String, String)>,
    parts: BTreeMap<i32, (String, Vec<u8>)>,
}

fn read_req(reader: &mut BufReader<impl Read>) -> (String, String, Vec<(String, String)>, Vec<u8>) {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut it = line.split_whitespace();
    let method = it.next().unwrap().to_string();
    let path = it.next().unwrap().to_string();
    let mut headers = Vec::new();
    let mut len = 0usize;
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).unwrap();
        let h = h.trim();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                len = v.trim().parse().unwrap_or(0);
            }
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).unwrap();
    (method, path, headers, body)
}

fn respond(s: &mut impl Write, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let declared = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| body.len().to_string());
    let mut out =
        format!("HTTP/1.1 {status}\r\ncontent-length: {declared}\r\nconnection: close\r\n");
    for (k, v) in headers {
        if !k.eq_ignore_ascii_case("content-length") {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    out.push_str("\r\n");
    s.write_all(out.as_bytes()).unwrap();
    s.write_all(body).unwrap();
}

fn query(path: &str) -> BTreeMap<String, String> {
    path.split_once('?')
        .map(|(_, q)| {
            q.split('&')
                .filter_map(|kv| {
                    kv.split_once('=')
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn physical(path: &str) -> String {
    path.split('?')
        .next()
        .unwrap()
        .trim_start_matches('/')
        .split_once('/')
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_default()
}

/// Mock S3 honoring the multipart API surface the provider uses.
fn mock_s3() -> (std::net::SocketAddr, Arc<Mutex<MockMp>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(MockMp::default()));
    let st = state.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let mut stream = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let st = st.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let (method, path, headers, body) = read_req(&mut reader);
                let q = query(&path);
                let key = physical(&path);
                let mut g = st.lock().unwrap();
                g.log.push((method.clone(), path.clone(), headers.clone()));
                if method == "POST" && q.contains_key("uploads") {
                    g.next += 1;
                    let id = format!("mu-{}", g.next);
                    let meta = headers
                        .iter()
                        .filter(|(k, _)| k.starts_with("x-amz-meta-vrt-"))
                        .cloned()
                        .collect();
                    g.uploads.insert(
                        id.clone(),
                        MockUpload {
                            key,
                            meta,
                            parts: BTreeMap::new(),
                        },
                    );
                    let xml = format!(
                        "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                    );
                    respond(&mut stream, "200 OK", &[], xml.as_bytes());
                    return;
                }
                if let Some(upload_id) = q.get("uploadId").cloned() {
                    match (method.as_str(), q.contains_key("partNumber")) {
                        ("PUT", true) => {
                            if g.drop_next_part {
                                g.drop_next_part = false;
                                return; // connection drop
                            }
                            let n: i32 = q["partNumber"].parse().unwrap();
                            match g.uploads.get_mut(&upload_id) {
                                Some(u) if u.key == key => {
                                    let etag = part_md5(&body);
                                    u.parts.insert(n, (etag.clone(), body));
                                    respond(
                                        &mut stream,
                                        "200 OK",
                                        &[("etag", &format!("\"{etag}\""))],
                                        &[],
                                    );
                                }
                                _ => respond(
                                    &mut stream,
                                    "404 Not Found",
                                    &[],
                                    b"<Error><Code>NoSuchUpload</Code></Error>",
                                ),
                            }
                        }
                        ("GET", false) => match g.uploads.get(&upload_id) {
                            Some(u) => {
                                let mut xml =
                                    "<ListPartsResult><IsTruncated>false</IsTruncated>".to_string();
                                for (n, (etag, bytes)) in &u.parts {
                                    xml.push_str(&format!(
                                        "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag><Size>{}</Size></Part>",
                                        bytes.len()
                                    ));
                                }
                                xml.push_str("</ListPartsResult>");
                                respond(&mut stream, "200 OK", &[], xml.as_bytes());
                            }
                            None => respond(
                                &mut stream,
                                "404 Not Found",
                                &[],
                                b"<Error><Code>NoSuchUpload</Code></Error>",
                            ),
                        },
                        ("POST", false) => {
                            if let Some(commit) = g.drop_next_complete.take() {
                                if commit {
                                    commit_upload(&mut g, &upload_id, &key, &body);
                                }
                                return; // connection drop — ambiguous
                            }
                            if let Some(code) = g.embedded_error.take() {
                                let xml = format!("<Error><Code>{code}</Code></Error>");
                                respond(&mut stream, "200 OK", &[], xml.as_bytes());
                                return;
                            }
                            if let Some((fault, commit)) = g.complete_fault.take() {
                                if commit {
                                    commit_upload(&mut g, &upload_id, &key, &body);
                                }
                                match fault {
                                    CompleteFault::Status500 => respond(
                                        &mut stream,
                                        "500 Internal Server Error",
                                        &[],
                                        b"<Error><Code>InternalError</Code></Error>",
                                    ),
                                    CompleteFault::EmbeddedInternal => respond(
                                        &mut stream,
                                        "200 OK",
                                        &[],
                                        b"<Error><Code>InternalError</Code></Error>",
                                    ),
                                    CompleteFault::GarbageXml => respond(
                                        &mut stream,
                                        "200 OK",
                                        &[],
                                        b"this is not valid xml <<<",
                                    ),
                                    CompleteFault::EmptyEtag => respond(
                                        &mut stream,
                                        "200 OK",
                                        &[],
                                        b"<CompleteMultipartUploadResult><ETag></ETag></CompleteMultipartUploadResult>",
                                    ),
                                }
                                return;
                            }
                            if g.uploads.get(&upload_id).map(|u| &u.key) != Some(&key) {
                                respond(
                                    &mut stream,
                                    "404 Not Found",
                                    &[],
                                    b"<Error><Code>NoSuchUpload</Code></Error>",
                                );
                                return;
                            }
                            let etag = commit_upload(&mut g, &upload_id, &key, &body);
                            let xml = format!(
                                "<CompleteMultipartUploadResult><ETag>\"{etag}\"</ETag></CompleteMultipartUploadResult>"
                            );
                            respond(&mut stream, "200 OK", &[], xml.as_bytes());
                        }
                        ("DELETE", false) => {
                            if g.uploads.remove(&upload_id).is_some() {
                                respond(&mut stream, "204 No Content", &[], &[]);
                            } else {
                                respond(
                                    &mut stream,
                                    "404 Not Found",
                                    &[],
                                    b"<Error><Code>NoSuchUpload</Code></Error>",
                                );
                            }
                        }
                        _ => respond(&mut stream, "400 Bad Request", &[], &[]),
                    }
                    return;
                }
                match method.as_str() {
                    "HEAD" if g.drop_next_head => {
                        g.drop_next_head = false;
                    }
                    "HEAD" => match g.objects.get(&key) {
                        Some((meta, etag, body)) => {
                            let mut hs: Vec<(String, String)> = meta.clone();
                            hs.push(("etag".into(), format!("\"{etag}\"")));
                            hs.push(("content-length".into(), body.len().to_string()));
                            let pairs: Vec<(&str, &str)> =
                                hs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                            respond(&mut stream, "200 OK", &pairs, &[]);
                        }
                        None => respond(&mut stream, "404 Not Found", &[], &[]),
                    },
                    "GET" => match g.objects.get(&key) {
                        Some((meta, etag, body)) => {
                            let mut hs: Vec<(String, String)> = meta.clone();
                            hs.push(("etag".into(), format!("\"{etag}\"")));
                            let pairs: Vec<(&str, &str)> =
                                hs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                            respond(&mut stream, "200 OK", &pairs, body);
                        }
                        None => respond(&mut stream, "404 Not Found", &[], &[]),
                    },
                    _ => respond(&mut stream, "405 Method Not Allowed", &[], &[]),
                }
            });
        }
    });
    (addr, state)
}

/// S3 CompleteMultipartUpload: concat listed parts into the object,
/// composite etag `md5(concat(md5s))-N`, headers from create.
fn commit_upload(g: &mut MockMp, upload_id: &str, key: &str, xml: &[u8]) -> String {
    let mut upload = g.uploads.remove(upload_id).expect("upload exists");
    let text = String::from_utf8_lossy(xml);
    let mut md5s = Vec::new();
    let mut body = Vec::new();
    for seg in text.split("<Part>").skip(1) {
        let num: i32 = seg
            .split("<PartNumber>")
            .nth(1)
            .and_then(|s| s.split("</PartNumber>").next())
            .unwrap()
            .parse()
            .unwrap();
        let (etag, bytes) = upload.parts.remove(&num).expect("part stored");
        md5s.extend_from_slice(&md5::compute(&bytes).0);
        body.extend_from_slice(&bytes);
        let _ = etag;
    }
    let etag = format!(
        "{:x}-{}",
        md5::compute(&md5s),
        text.matches("<Part>").count()
    );
    g.objects
        .insert(key.to_string(), (upload.meta.clone(), etag.clone(), body));
    etag
}

fn s3_provider(addr: std::net::SocketAddr) -> S3R2Provider {
    S3R2Provider::new(S3Config {
        endpoint: format!("http://{addr}"),
        region: "auto".to_string(),
        bucket: "b".to_string(),
        access_key: "ak".to_string(),
        secret_key: SecretString::new("sk".to_string()),
        timeout: std::time::Duration::from_secs(10),
        max_object_bytes: 64 * 1024 * 1024,
        conditional_writes: true,
        multipart_ttl_ms: 60_000,
    })
    .unwrap()
}

#[tokio::test]
async fn s3_conformance() {
    let (addr, _state) = mock_s3();
    conformance(&s3_provider(addr)).await;
}

#[tokio::test]
async fn s3_complete_embedded_error_is_typed() {
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up, 1, b"x", None).await.unwrap();
    // 200 + embedded InternalError → service fault, not multipart-invalid.
    state.lock().unwrap().embedded_error = Some("InternalError".into());
    assert!(matches!(
        p.complete_multipart(&c, "k", &up, &[part(1, &part_md5(b"x"))])
            .await,
        Err(ProviderError::R2ProviderUnavailable)
    ));
    // 200 + embedded NoSuchUpload → multipart-invalid.
    let up2 = p
        .create_multipart(&c, "k2", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let pp = p.upload_part(&c, "k2", &up2, 1, b"x", None).await.unwrap();
    state.lock().unwrap().embedded_error = Some("NoSuchUpload".into());
    assert!(matches!(
        p.complete_multipart(&c, "k2", &up2, &[pp]).await,
        Err(ProviderError::R2MultipartInvalid)
    ));
}

#[tokio::test]
async fn s3_complete_ambiguous_confirmed_by_head() {
    // Commit-then-drop: HEAD confirms the object with the composite
    // etag derived from the request → committed metadata returned.
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
    state.lock().unwrap().drop_next_complete = Some(true);
    let meta = p
        .complete_multipart(&c, "k", &up, &[pp])
        .await
        .unwrap()
        .expect("confirmed commit");
    assert_eq!(meta.size, 4);
}

#[tokio::test]
async fn s3_complete_ambiguous_uncommitted_is_unavailable() {
    // Drop without commit → HEAD 404 proves the object did NOT commit
    // → honest retryable `R2_PROVIDER_UNAVAILABLE` (W5-r3 semantics:
    // UNKNOWN is reserved for when confirmation itself cannot run).
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
    state.lock().unwrap().drop_next_complete = Some(false);
    assert!(matches!(
        p.complete_multipart(&c, "k", &up, &[pp]).await,
        Err(ProviderError::R2ProviderUnavailable)
    ));
}

#[tokio::test]
async fn s3_complete_ambiguous_unconfirmable_is_result_unknown() {
    // Commit-then-drop + unreachable HEAD → cannot confirm either way
    // → R2_RESULT_UNKNOWN (never a false success, never a false
    // failure).
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
    {
        let mut g = state.lock().unwrap();
        g.drop_next_complete = Some(true);
        g.drop_next_head = true;
    }
    assert!(matches!(
        p.complete_multipart(&c, "k", &up, &[pp]).await,
        Err(ProviderError::R2ResultUnknown)
    ));
}

#[tokio::test]
async fn s3_complete_retry_after_committed_fault_is_idempotent() {
    // W5-r4: first attempt commits but 500s and HEAD is unreachable →
    // UNKNOWN (not a false Unavailable). The RETRY then meets a
    // consumed upload (NoSuchUpload) — HEAD confirms the request's
    // own composite etag is committed → idempotent Ok(metadata).
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
    {
        let mut g = state.lock().unwrap();
        g.complete_fault = Some((CompleteFault::Status500, true));
        g.drop_next_head = true;
    }
    assert!(matches!(
        p.complete_multipart(&c, "k", &up, std::slice::from_ref(&pp))
            .await,
        Err(ProviderError::R2ResultUnknown)
    ));

    // Retry with the SAME parts → idempotent success.
    let meta = p
        .complete_multipart(&c, "k", &up, std::slice::from_ref(&pp))
        .await
        .unwrap()
        .expect("retry confirmed by HEAD");
    assert_eq!(meta.size, 4);

    // Retry with DIFFERENT parts on the consumed upload — composite
    // can't match the committed object → typed invalid stands.
    let other = part(1, &part_md5(b"unrelated-bytes"));
    assert!(matches!(
        p.complete_multipart(&c, "k", &up, &[other]).await,
        Err(ProviderError::R2MultipartInvalid)
    ));
}

#[tokio::test]
async fn s3_complete_service_faults_route_through_head() {
    // W5-r3/P0-1: every ambiguous Complete outcome — 5xx, embedded
    // InternalError, garbage XML, empty ETag — is confirmed via HEAD:
    // committed → Ok(metadata); not committed → retryable Unavailable.
    for fault in [
        CompleteFault::Status500,
        CompleteFault::EmbeddedInternal,
        CompleteFault::GarbageXml,
        CompleteFault::EmptyEtag,
    ] {
        // Committed branch → confirmed success.
        let (addr, state) = mock_s3();
        let p = s3_provider(addr);
        let c = ctx_for(TENANT, BUCKET);
        let up = p
            .create_multipart(&c, "k", &Default::default())
            .await
            .unwrap()
            .upload_id;
        let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
        state.lock().unwrap().complete_fault = Some((fault, true));
        let meta = p
            .complete_multipart(&c, "k", &up, &[pp])
            .await
            .unwrap_or_else(|e| panic!("fault {fault:?} committed → {e:?}"))
            .expect("committed metadata");
        assert_eq!(meta.size, 4);

        // Not-committed branch → retryable Unavailable, never
        // MULTIPART_INVALID (which would misreport a live upload) nor
        // UNKNOWN (HEAD proved absence).
        let up = p
            .create_multipart(&c, "k2", &Default::default())
            .await
            .unwrap()
            .upload_id;
        let pp = p
            .upload_part(&c, "k2", &up, 1, b"data", None)
            .await
            .unwrap();
        state.lock().unwrap().complete_fault = Some((fault, false));
        let res = p.complete_multipart(&c, "k2", &up, &[pp]).await;
        assert!(
            matches!(res, Err(ProviderError::R2ProviderUnavailable)),
            "fault {fault:?} uncommitted → {res:?}"
        );
    }
}

#[tokio::test]
async fn s3_upload_part_retries_transport() {
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    state.lock().unwrap().drop_next_part = true;
    let pp = p.upload_part(&c, "k", &up, 1, b"data", None).await.unwrap();
    assert_eq!(pp.part_number, 1);
}

#[tokio::test]
async fn s3_abort_missing_upload_is_invalid() {
    let (addr, _s) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    assert!(matches!(
        p.abort_multipart(&c, "k", "no-such").await,
        Err(ProviderError::R2MultipartInvalid)
    ));
}

// ------------------------------------------------- upload token scope

#[test]
fn upload_token_scope_and_expiry() {
    use vrt_provider::cursor::CursorSigner;
    let signer = CursorSigner::new(b"secret");
    let token = signer.sign_r2_upload(TENANT, BUCKET, "k", b"internal-1", 60_000);
    // Wrong tenant/bucket/key all fail closed.
    assert!(signer.verify_r2_upload(TENANT, BUCKET, "k", &token).is_ok());
    assert!(signer
        .verify_r2_upload(OTHER_TENANT, BUCKET, "k", &token)
        .is_err());
    assert!(signer
        .verify_r2_upload(TENANT, "bucket-2", "k", &token)
        .is_err());
    assert!(signer
        .verify_r2_upload(TENANT, BUCKET, "k2", &token)
        .is_err());
    // Tampering any byte fails.
    let mut tampered = token.clone().into_bytes();
    tampered[10] ^= 1;
    assert!(signer
        .verify_r2_upload(TENANT, BUCKET, "k", std::str::from_utf8(&tampered).unwrap())
        .is_err());
    // Expired token fails.
    let old = signer.sign_r2_upload_with_times(TENANT, BUCKET, "k", b"i", 1, 2);
    assert!(signer.verify_r2_upload(TENANT, BUCKET, "k", &old).is_err());
    // A different signer key cannot verify.
    let other = CursorSigner::new(b"other-secret");
    assert!(other.verify_r2_upload(TENANT, BUCKET, "k", &token).is_err());
    // Garbage input fails closed, never panics.
    assert!(signer
        .verify_r2_upload(TENANT, BUCKET, "k", "not-a-token")
        .is_err());
    assert!(signer.verify_r2_upload(TENANT, BUCKET, "k", "").is_err());
}

// ------------------------------------------------- W5-r2: quota, self-heal, GC, spawn_blocking

fn fs_upload_dir(
    root: &std::path::Path,
    tenant: &str,
    bucket: &str,
    id: &str,
) -> std::path::PathBuf {
    root.join(vrt_securefs::segment_encode(tenant))
        .join(vrt_securefs::segment_encode(bucket))
        .join(".multipart")
        .join(id)
}

#[tokio::test]
async fn fs_active_upload_cap_is_per_tenant() {
    // The count spans every bucket under the tenant dir, not just the
    // request's own bucket.
    let dir = support::TestDir::new();
    let p = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024)
        .unwrap()
        .with_max_active_uploads(3);
    let a = ctx_for(TENANT, BUCKET);
    let b = ctx_for(TENANT, BUCKET2);
    p.create_multipart(&a, "k1", &Default::default())
        .await
        .unwrap();
    p.create_multipart(&b, "k2", &Default::default())
        .await
        .unwrap();
    p.create_multipart(&a, "k3", &Default::default())
        .await
        .unwrap();
    assert!(matches!(
        p.create_multipart(&b, "overflow", &Default::default())
            .await,
        Err(ProviderError::R2Overloaded)
    ));
    // A different tenant is unaffected.
    p.create_multipart(&ctx_for(OTHER_TENANT, BUCKET), "k", &Default::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn fs_create_count_atomic_under_concurrency() {
    let dir = support::TestDir::new();
    let p = Arc::new(
        FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024)
            .unwrap()
            .with_max_active_uploads(4),
    );
    let mut handles = Vec::new();
    for i in 0..16 {
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            p.create_multipart(
                &ctx_for(TENANT, BUCKET),
                &format!("k{i}"),
                &Default::default(),
            )
            .await
        }));
    }
    let mut ok = 0usize;
    for h in handles {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 4, "concurrent creates exceeded the tenant cap");
}

#[tokio::test]
async fn fs_staging_byte_quota_boundary_and_release() {
    // Boundary: staged bytes at the tenant cap admit; one byte over
    // rejects; abort and GC both release the budget. Uses small
    // bodies — the 5MiB floor only applies at complete.
    let dir = support::TestDir::new();
    let root = dir.path().join("r2");
    let p = FsR2Provider::new(root.clone(), 64 * 1024 * 1024)
        .unwrap()
        .with_staging_max_bytes_per_tenant(10);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up, 1, &[0u8; 10], None)
        .await
        .unwrap();
    // +1 byte pushes the tenant ledger past the cap.
    assert!(matches!(
        p.upload_part(&c, "k", &up, 2, &[0u8; 1], None).await,
        Err(ProviderError::R2Overloaded)
    ));
    // ...and a second upload's part is equally gated.
    let up2 = p
        .create_multipart(&c, "k2", &Default::default())
        .await
        .unwrap()
        .upload_id;
    assert!(matches!(
        p.upload_part(&c, "k2", &up2, 1, &[0u8; 1], None).await,
        Err(ProviderError::R2Overloaded)
    ));
    // abort releases the staged bytes.
    p.abort_multipart(&c, "k", &up).await.unwrap();
    p.upload_part(&c, "k2", &up2, 1, &[0u8; 10], None)
        .await
        .unwrap();
    // GC on an expired upload releases the budget too.
    let meta_path = fs_upload_dir(&root, TENANT, BUCKET, &up2).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta["expiresMs"] = serde_json::json!(1);
    std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let report = p.sweep_staging().unwrap();
    assert_eq!(report.expired, 1);
    p.upload_part(
        &c,
        "k3",
        &p.create_multipart(&c, "k3", &Default::default())
            .await
            .unwrap()
            .upload_id,
        1,
        &[0u8; 9],
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn fs_upload_part_self_heals_meta_drift() {
    // P1-1: a part file whose meta write failed leaves totalBytes
    // under-counted (or inverted). The ledger is recomputed from
    // disk under the locks — never a u64 underflow, and the meta is
    // healed on write-back.
    let dir = support::TestDir::new();
    let root = dir.path().join("r2");
    let p = FsR2Provider::new(root.clone(), 64 * 1024 * 1024)
        .unwrap()
        .with_staging_max_bytes_per_tenant(20);
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p.upload_part(&c, "k", &up, 1, &[0u8; 8], None)
        .await
        .unwrap();
    // Simulate the meta-write failure: ledger on disk says 0 while a
    // real 8-byte part file exists.
    let meta_path = fs_upload_dir(&root, TENANT, BUCKET, &up).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta["totalBytes"] = serde_json::json!(0);
    std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    // Re-uploading the SAME partNumber must not panic (replaced=8 >
    // stale total=0) and must land the correct ledger on disk.
    p.upload_part(&c, "k", &up, 1, &[0u8; 6], None)
        .await
        .unwrap();
    let meta2: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    assert_eq!(meta2["totalBytes"], serde_json::json!(6));
}

#[tokio::test]
async fn fs_orphan_gc_reaps_meta_less_dirs() {
    // P2: a staging dir whose meta.json never landed (or is corrupt)
    // would pin bytes forever — the sweep reaps it once the dir is
    // older than the TTL. ttl=0 makes any elapsed age count; the
    // live-upload case is covered by fs_gc_reaps_expired_keeps_active.
    let dir = support::TestDir::new();
    let root = dir.path().join("r2");
    // ttl=0 makes any elapsed mtime count as "aged" for the orphan
    // fallback; the live upload is created with a normal TTL provider
    // and survives the ttl=0 sweep because its meta says future.
    let p = FsR2Provider::new(root.clone(), 64 * 1024 * 1024)
        .unwrap()
        .with_upload_ttl_ms(0);
    let c = ctx_for(TENANT, BUCKET);
    // Hand-craft an orphan: valid upload-id name, no meta.json.
    let staging = root
        .join(vrt_securefs::segment_encode(TENANT))
        .join(vrt_securefs::segment_encode(BUCKET))
        .join(".multipart");
    // ensure_secure_dir demands 0700 on the whole chain — hand-made
    // orphans must match or create() fails before GC ever runs.
    use std::os::unix::fs::PermissionsExt;
    let orphan = staging.join(uuid::Uuid::now_v7().hyphenated().to_string());
    std::fs::create_dir_all(orphan.join("parts")).unwrap();
    std::fs::write(orphan.join("parts").join("00000001"), b"junk").unwrap();
    // A corrupt-meta sibling.
    let corrupt = staging.join(uuid::Uuid::now_v7().hyphenated().to_string());
    std::fs::create_dir_all(corrupt.join("parts")).unwrap();
    std::fs::write(corrupt.join("meta.json"), b"{corrupt").unwrap();
    for d in [
        &staging,
        &orphan,
        &orphan.join("parts"),
        &corrupt,
        &corrupt.join("parts"),
        &staging.parent().unwrap().to_path_buf(),
        &staging.parent().unwrap().parent().unwrap().to_path_buf(),
        &root,
    ] {
        std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    // A live upload (valid meta, future expiry) survives a ttl=0
    // sweep — the orphan fallback never touches well-formed uploads.
    let p_live = FsR2Provider::new(root.clone(), 64 * 1024 * 1024)
        .unwrap()
        .with_upload_ttl_ms(3_600_000);
    let live = p_live
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    p_live
        .upload_part(&c, "k", &live, 1, b"x", None)
        .await
        .unwrap();
    let report = p.sweep_staging().unwrap();
    assert_eq!(report.orphans, 2, "meta-less/corrupt dirs not reaped");
    assert_eq!(report.expired, 0, "live upload reaped as expired");
    assert!(!orphan.exists() && !corrupt.exists());
    // The live upload is still fully operational.
    p_live
        .upload_part(&c, "k", &live, 2, b"y", None)
        .await
        .unwrap();
    // With a huge TTL nothing ages — a fresh orphan is kept.
    let p2 = FsR2Provider::new(root.clone(), 64 * 1024 * 1024)
        .unwrap()
        .with_upload_ttl_ms(i64::MAX / 2);
    let orphan2 = staging.join(uuid::Uuid::now_v7().hyphenated().to_string());
    std::fs::create_dir_all(orphan2.join("parts")).unwrap();
    std::fs::set_permissions(&orphan2, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(
        orphan2.join("parts"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let report2 = p2.sweep_staging().unwrap();
    assert_eq!(report2.orphans, 0);
    assert!(orphan2.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn fs_spawn_blocking_keeps_runtime_responsive() {
    // P1-3: staging IO + the std Mutexes live on the blocking pool —
    // eight concurrent parts on the SAME upload plus an unrelated op
    // must all finish on a single-worker runtime. If the locks ran on
    // the worker this join would hang.
    let dir = support::TestDir::new();
    let p = Arc::new(FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024).unwrap());
    let c = ctx_for(TENANT, BUCKET);
    let up = p
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap()
        .upload_id;
    let mut handles = Vec::new();
    for n in 1..=8 {
        let p = p.clone();
        let up = up.clone();
        handles.push(tokio::spawn(async move {
            p.upload_part(
                &ctx_for(TENANT, BUCKET),
                "k",
                &up,
                n,
                &[n as u8; 1024],
                None,
            )
            .await
        }));
    }
    // An unrelated lightweight op must not starve behind the parts.
    let p2 = p.clone();
    let head = tokio::spawn(async move { p2.head(&ctx_for(TENANT, BUCKET), "other").await });
    let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async move {
        let mut ok = 0;
        for h in handles {
            if h.await.unwrap().is_ok() {
                ok += 1;
            }
        }
        (ok, head.await.unwrap())
    })
    .await;
    let (ok, head_result) = joined.expect("runtime starved by blocking fs ops");
    assert_eq!(ok, 8);
    assert!(matches!(head_result, Ok(None)));
}

#[test]
fn complete_parts_body_fits_metadata_limit() {
    // The complete request body is capped at R2_MAX_METADATA_BYTES —
    // the practical part ceiling is min(10000, cap/5MiB+1) well below
    // the JSON limit. Assert the arithmetic stays true at the current
    // 16KiB limit and 256MiB object cap: 52 parts ≈ 2.4KiB.
    use vrt_provider::limits::R2_MAX_METADATA_BYTES;
    let cap = 256 * 1024 * 1024u64;
    let max_parts = cap / R2_MULTIPART_MIN_PART_BYTES + 1; // 52
    let body = serde_json::json!({
        "key": "k",
        "uploadId": "t".repeat(64),
        "parts": (1..=max_parts)
            .map(|n| serde_json::json!({"partNumber": n, "etag": "a".repeat(32)}))
            .collect::<Vec<_>>(),
    });
    let bytes = serde_json::to_vec(&body).unwrap();
    assert!(
        bytes.len() <= R2_MAX_METADATA_BYTES,
        "max-parts complete body {} exceeds {} — raise the body limit or document",
        bytes.len(),
        R2_MAX_METADATA_BYTES
    );
}

#[test]
fn upload_token_follows_provider_expiry() {
    // P1-2: the token lifetime comes from the backend's
    // `expires_at_ms`, not a compile-time constant — a short-TTL
    // backend must not hand out over-long tokens.
    use vrt_provider::cursor::CursorSigner;
    let signer = CursorSigner::new(b"secret");
    // Expired tokens fail closed (deterministic via explicit times).
    let dead = signer.sign_r2_upload_with_times(TENANT, BUCKET, "k", b"i", 1, 2);
    assert!(signer.verify_r2_upload(TENANT, BUCKET, "k", &dead).is_err());
    // Provider-decided future expiry is honored verbatim — a long fs
    // TTL yields a long-lived token, not the 7-day default.
    let long = signer.sign_r2_upload_until(TENANT, BUCKET, "k", b"i", now_ms_pub() + 3_600_000);
    assert!(signer.verify_r2_upload(TENANT, BUCKET, "k", &long).is_ok());
    // A past expiry clamps to issue-time (never negative): the token
    // dies with the current millisecond, not "in the past forever" —
    // same-millisecond verification may pass, which is why the
    // expired case above uses explicit times.
    let clamped = signer.sign_r2_upload_until(TENANT, BUCKET, "k", b"i", -1);
    let _ = signer.verify_r2_upload(TENANT, BUCKET, "k", &clamped); // never panics
}

#[tokio::test]
async fn create_returns_backend_expiry() {
    // The provider advertises its own horizon; the token must inherit
    // it (P1-2). memory/fs use their configured TTL; s3 uses its
    // configured lifecycle mirror (60_000 in the mock fixture).
    let now = now_ms_pub();
    let mem = MemoryR2Provider::new();
    let c = ctx_for(TENANT, BUCKET);
    let r = mem
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap();
    assert!(r.expires_at_ms > now, "memory expiry not in the future");
    let dir = support::TestDir::new();
    let fs = FsR2Provider::new(dir.path().join("r2"), 64 * 1024 * 1024)
        .unwrap()
        .with_upload_ttl_ms(123_456);
    let r = fs
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap();
    assert!(
        (r.expires_at_ms - now - 123_456).abs() < 5_000,
        "fs expiry must track its configured TTL, got {}",
        r.expires_at_ms
    );
    // s3 fixture wires multipart_ttl_ms=60_000.
    let (addr, _state) = mock_s3();
    let s3 = s3_provider(addr);
    let r = s3
        .create_multipart(&c, "k", &Default::default())
        .await
        .unwrap();
    assert!(
        (r.expires_at_ms - now - 60_000).abs() < 5_000,
        "s3 expiry must track its configured lifecycle mirror, got {}",
        r.expires_at_ms
    );
}

fn now_ms_pub() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn logged_header<'a>(
    request: &'a (String, String, Vec<(String, String)>),
    name: &str,
) -> Option<&'a str> {
    request
        .2
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// SSE-C on the S3 multipart chain: create → uploadPart → complete all
/// present the customer key, metadata round-trips the key MD5, and a
/// later keyed get still reads the object. ListParts is not an SSE-C
/// operation and must stay header-free.
#[tokio::test]
async fn s3_multipart_ssec_headers_and_metadata() {
    let (addr, state) = mock_s3();
    let p = s3_provider(addr);
    let c = ctx_for(TENANT, BUCKET);
    let key = R2SsecKey::parse(&"ef".repeat(32)).unwrap();
    let create = R2MultipartCreateOptions {
        ssec_key: Some(key.clone()),
        ..create_opts()
    };
    let up = p.create_multipart(&c, "mp/ssec", &create).await.unwrap();
    let part = p
        .upload_part(&c, "mp/ssec", &up.upload_id, 1, b"payload", Some(&key))
        .await
        .unwrap();
    let meta = p
        .complete_multipart(&c, "mp/ssec", &up.upload_id, &[part])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.ssec_key_md5.as_deref(), Some(key.md5_hex().as_str()));

    match p.get(&c, "mp/ssec", None, None, Some(&key)).await.unwrap() {
        R2GetResult::Body(meta, body) => {
            assert_eq!(body, b"payload");
            assert_eq!(meta.ssec_key_md5.as_deref(), Some(key.md5_hex().as_str()));
        }
        other => panic!("expected body, got {other:?}"),
    }

    let g = state.lock().unwrap();
    let create_req = g
        .log
        .iter()
        .find(|(m, path, _)| m == "POST" && path.contains("uploads="))
        .expect("create request");
    let part_req = g
        .log
        .iter()
        .find(|(m, path, _)| m == "PUT" && path.contains("partNumber="))
        .expect("upload part request");
    let complete_req = g
        .log
        .iter()
        .find(|(m, path, _)| {
            m == "POST" && path.contains("uploadId=") && !path.contains("partNumber=")
        })
        .expect("complete request");
    for (name, request) in [
        ("create", create_req),
        ("uploadPart", part_req),
        ("complete", complete_req),
    ] {
        assert_eq!(
            logged_header(request, "x-amz-server-side-encryption-customer-algorithm"),
            Some("AES256"),
            "{name}: {request:?}"
        );
        assert_eq!(
            logged_header(request, "x-amz-server-side-encryption-customer-key"),
            Some(key.s3_key_b64().as_str()),
            "{name}: {request:?}"
        );
        assert_eq!(
            logged_header(request, "x-amz-server-side-encryption-customer-key-MD5"),
            Some(key.s3_key_md5_b64().as_str()),
            "{name}: {request:?}"
        );
    }
    // ListParts must not carry SSE-C headers.
    if let Some(list_parts) = g
        .log
        .iter()
        .find(|(m, path, _)| m == "GET" && path.contains("uploadId="))
    {
        assert!(
            !list_parts
                .2
                .iter()
                .any(|(k, _)| k.starts_with("x-amz-server-side-encryption-customer-")),
            "ListParts carried SSE-C headers: {list_parts:?}"
        );
    }
}
