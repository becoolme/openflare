//! Per-tenant unix-socket hardening and serving.
//!
//! - The listener directory must be a real directory owned by the
//!   current euid with mode `0700` (`0711` under per-tenant uid
//!   isolation — traverse, never list) — anything else refuses to
//!   bind (fail closed: a loose directory would let another local
//!   principal drop a same-named socket and impersonate the tenant
//!   listener).
//! - A pre-existing path must be a socket (stale leftovers are removed);
//!   a regular file or directory at the path is a hard error.
//! - The bound socket is chmod'd `0600` (`0666` under isolation —
//!   authorization then comes from the SO_PEERCRED gate).
//! - Serving uses `axum::serve` over `tokio::net::UnixListener` with
//!   caller-supplied graceful shutdown.

use std::io;
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;

/// Compute the socket path for `tenant_id` under `dir`. Delegates to the
/// shared `vrt-securefs` helper — the same segment-encoded layout vrt-host
/// uses to derive the per-tenant `binding-backend` address.
pub fn socket_path(dir: &Path, tenant_id: &str) -> PathBuf {
    vrt_securefs::socket_path(dir, tenant_id)
}

/// Validate that `dir` is a directory owned by us with mode `0700`.
/// Delegates to `vrt_securefs::validate_secure_dir` (extracted shared
/// check, no copy-paste).
pub fn validate_listener_dir(dir: &Path) -> io::Result<()> {
    vrt_securefs::validate_secure_dir(dir)
}

/// Bind a per-tenant unix listener under `dir` — T25/L6: the dir is
/// created when absent (0700, owned by us); an existing one must pass
/// the same secure-dir validation. Stale sockets at the target path
/// are removed; a non-socket file there is a hard error.
pub fn bind_listener(dir: &Path, tenant_id: &str) -> io::Result<UnixListener> {
    bind_listener_inner(dir, tenant_id, false)
}

fn bind_listener_inner(dir: &Path, tenant_id: &str, traversable: bool) -> io::Result<UnixListener> {
    vrt_securefs::ensure_socket_dir(dir, traversable)?;
    let path = socket_path(dir, tenant_id);
    // T34e: fail closed BEFORE bind — a bare ENAMETOOLONG surfaces as
    // "tenant listener bind failed" with zero hint of the cause.
    vrt_securefs::check_socket_path_len(&path)?;
    vrt_securefs::remove_stale_socket(&path)?;
    // F1: from birth (staged bind + rename). `0600` owner-only in the
    // shared-uid model; `0666` when the dir is traversable — under
    // per-tenant uid isolation the connecting workerd is a different
    // uid by design, and the peer gate (PeerGuardListener, kernel
    // SO_PEERCRED) carries the authorization DAC can't express.
    let mode = if traversable { 0o666 } else { 0o600 };
    let std_listener = vrt_securefs::bind_unix_private(&path, mode)?;
    std_listener.set_nonblocking(true)?;
    UnixListener::from_std(std_listener)
}

/// Peer-uid gate on a per-tenant unix listener — SO_PEERCRED on every
/// accepted stream must equal `expected_uid`. Tenant identity for the
/// listener is the socket NAME; the kernel-reported peer uid is what
/// proves the connecting workerd IS that tenant under per-tenant uid
/// isolation (T13/M2). A cross-tenant connect is dropped before a
/// single byte is read — closing the A→B dial path that socket-file
/// DAC cannot cover once `listenerDir` is traversable (0711) and
/// sockets are connectable by other uids (0666).
///
/// In the shared-uid model (`expected_uid` = our own euid) the gate
/// only blocks DIFFERENT-uid processes — useful against non-runtime
/// tenants on the host. It does NOT separate co-uid tenants: when
/// workerd for tenant A and tenant B run under the same uid (the k8s
/// pod model today), a connect from A's workerd to B's socket carries
/// the expected peer uid and is indistinguishable at this layer.
/// There the cross-tenant boundary is rendered-address scoping plus
/// the workerd sandbox, NOT this gate — see
/// `doc/platform/workers-runtime-deploy.md` known-limits.
pub struct PeerGuardListener {
    inner: UnixListener,
    expected_uid: u32,
}

impl axum::serve::Listener for PeerGuardListener {
    type Io = tokio::net::UnixStream;
    type Addr = tokio::net::unix::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, addr)) => match stream.peer_cred() {
                    Ok(cred) if cred.uid() == self.expected_uid => {
                        return (stream, addr);
                    }
                    Ok(cred) => {
                        tracing::warn!(
                            peer_uid = cred.uid(),
                            expected_uid = self.expected_uid,
                            "vrt-provider: dropped cross-tenant binding-backend peer"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "vrt-provider: peer_cred failed — dropped peer"
                        );
                    }
                },
                Err(error) => {
                    // Transient accept failure (fd pressure) — brief
                    // backoff then retry; a Listener's accept is not
                    // allowed to fail, per the axum contract.
                    tracing::warn!(error = %error, "vrt-provider: accept failed — retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Bind a peer-guarded per-tenant listener.
///
/// `expected_uid = Some(uid)` — isolation mode (T13/M2): the dir is
/// `0711` (traverse, never list), the socket `0666`, and accepted
/// peers must carry exactly `uid` — pass
/// `vrt_securefs::tenant_uid(iso.base, iso.span, tenant_id)` so the
/// gate derives the SAME uid the host setuids the tenant to.
/// `None` — shared-uid model: `0700` dir + `0600` socket, peers must
/// equal our own euid (pod-uid deployments).
pub fn bind_listener_guarded(
    dir: &Path,
    tenant_id: &str,
    expected_uid: Option<u32>,
) -> io::Result<PeerGuardListener> {
    let isolated = expected_uid.is_some();
    let listener = bind_listener_inner(dir, tenant_id, isolated)?;
    Ok(PeerGuardListener {
        inner: listener,
        expected_uid: expected_uid.unwrap_or_else(|| unsafe { libc::geteuid() }),
    })
}

/// Serve `router` on `listener` until `shutdown` resolves, then drain
/// in-flight requests. Returns when the listener has fully drained.
/// Generic over [`axum::serve::Listener`] so the same drain path is
/// exercised over `TcpListener` in tests (unix `bind()` is unavailable
/// there) and `UnixListener` in production.
pub async fn serve<L>(
    listener: L,
    router: axum::Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> io::Result<()>
where
    L: axum::serve::Listener,
    L::Addr: std::fmt::Debug + Send + 'static,
    axum::routing::IntoMakeService<axum::Router>: Send + 'static,
    for<'a> axum::routing::IntoMakeService<axum::Router>: tower::Service<
        axum::serve::IncomingStream<'a, L>,
        Error = std::convert::Infallible,
        Response = axum::Router,
    >,
    for<'a> <axum::routing::IntoMakeService<axum::Router> as tower::Service<
        axum::serve::IncomingStream<'a, L>,
    >>::Future: Send,
{
    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    /// T34e: a too-long socket path fails closed BEFORE bind —
    /// never a bare ENAMETOOLONG.
    #[test]
    fn bind_listener_rejects_overlong_socket_path() {
        let dir = std::env::temp_dir().join(format!("vrt-usock-{}", std::process::id()));
        let tenant = "t".repeat(200);
        let err = super::bind_listener(&dir, &tenant).unwrap_err();
        assert!(
            err.to_string().contains("bytes") || err.to_string().contains("socket"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// unix `bind()` is denied in the dev sandbox — same gate as the
    /// crate's other unix tests (`VRT_HOST_UNIX_TESTS=1` outside).
    fn unix_enabled() -> bool {
        if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() == Some("1") {
            true
        } else {
            eprintln!("SKIP: unix bind unavailable — set VRT_HOST_UNIX_TESTS=1");
            false
        }
    }

    /// Panic-safe cleanup for unix-bind test dirs — a failed assert
    /// must not leave socket files/dirs behind in temp.
    struct RmDirOnDrop(std::path::PathBuf);
    impl RmDirOnDrop {
        fn new(tag: &str) -> Self {
            Self(std::env::temp_dir().join(format!("vrt-{tag}-{}", std::process::id())))
        }
    }
    impl Drop for RmDirOnDrop {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// H5-r1: socket/dir modes track the deployment's isolation
    /// contract — shared-uid stays 0700/0600; isolation mode needs
    /// the traversable 0711 dir + connectable 0666 socket (peer-uid
    /// gate carries the auth DAC can't express across uids).
    #[tokio::test]
    #[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
    async fn bind_modes_track_isolation() {
        if !unix_enabled() {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        // Exclusive-per-test dir under a drop guard — panics still
        // clean up (bind_listener_guarded calls tokio's
        // `UnixListener::from_std`, hence `#[tokio::test]`).
        let base = RmDirOnDrop::new("iso");
        let shared = base.0.join("shared");
        let iso = base.0.join("iso");

        let _l1 = super::bind_listener_guarded(&shared, "t1", None).unwrap();
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&shared), 0o700, "shared-uid dir stays 0700");
        assert_eq!(
            mode(&vrt_securefs::socket_path(&shared, "t1")),
            0o600,
            "shared-uid socket stays 0600"
        );

        let _l2 = super::bind_listener_guarded(&iso, "t1", Some(4242)).unwrap();
        assert_eq!(mode(&iso), 0o711, "isolated dir traverses, never lists");
        assert_eq!(
            mode(&vrt_securefs::socket_path(&iso, "t1")),
            0o666,
            "isolated socket connectable — peer uid gates"
        );
    }

    /// H5-r1: the peer gate drops a connection whose SO_PEERCRED uid
    /// differs from the listener's expected tenant uid — before a
    /// single byte is read. Same-euid peers pass. (A real
    /// different-uid connect can't be simulated without root/user
    /// namespaces — the wrong-uid drop is proven by expecting a uid
    /// that can never match our own process's credentials.)
    #[tokio::test]
    #[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
    async fn peer_guard_accepts_own_uid_and_drops_others() {
        if !unix_enabled() {
            return;
        }
        use axum::serve::Listener as _;
        use std::os::unix::fs::PermissionsExt;
        use tokio::io::AsyncReadExt;
        use tokio::net::UnixStream;

        let guard = RmDirOnDrop::new("pg");
        let dir = guard.0.clone();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let euid = unsafe { libc::geteuid() };
        let path = vrt_securefs::socket_path(&dir, "t1");

        // 1) Expected uid == our euid → the connection is accepted.
        let mut guard = super::bind_listener_guarded(&dir, "t1", Some(euid)).unwrap();
        let conn = UnixStream::connect(&path).await.unwrap();
        let _accepted = tokio::time::timeout(std::time::Duration::from_secs(2), guard.accept())
            .await
            .expect("own-uid peer must be accepted");
        drop(conn);

        // 2) Expected uid == euid+1 (never matches any peer this
        //    process can produce) → the connection is dropped at
        //    accept: the client's socket sees EOF.
        let mut guard = super::bind_listener_guarded(&dir.join("x"), "t1", Some(euid + 1)).unwrap();
        let path2 = vrt_securefs::socket_path(&dir.join("x"), "t1");
        let mut conn = UnixStream::connect(&path2).await.unwrap();
        let accept = tokio::spawn(async move { guard.accept().await });
        let mut buf = [0u8; 4];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), conn.read(&mut buf))
            .await
            .expect("wrong-uid peer must be closed, not hang")
            .expect("wrong-uid read");
        assert_eq!(n, 0, "wrong-uid peer must see EOF");
        drop(accept);
    }
}
