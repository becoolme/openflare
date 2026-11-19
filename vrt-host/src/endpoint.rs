//! `WorkerEndpoint` — where one tenant worker's ingress socket lives.
//!
//! Production workerd binds `unix:<socketDir>/<workerId>.sock` (declared in
//! the rendered capnp). Tests and local dev may bind loopback TCP instead
//! (the sandbox forbids unix `bind()`); `Tcp` endpoints are restricted to
//! loopback addresses so a stub can never advertise a routable address.

use std::fmt;
use std::io;
#[cfg(feature = "dev-stub")]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(feature = "dev-stub")]
use tokio::net::TcpStream;
use tokio::net::UnixStream;

/// The concrete socket a worker listens on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEndpoint {
    /// `unix:/abs/path` — the only address family real workerd uses.
    Unix(PathBuf),
    /// Loopback TCP — stub/dev path only; never routable. Compiled out
    /// unless the `dev-stub` feature is enabled.
    #[cfg(feature = "dev-stub")]
    Tcp(SocketAddr),
}

impl WorkerEndpoint {
    /// The endpoint's `address` field as reported on the control channel.
    pub fn control_address(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix:{}", path.display()),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(addr) => format!("tcp:{addr}"),
        }
    }

    /// Connect to the endpoint.
    pub async fn connect(&self) -> io::Result<UpstreamIo> {
        match self {
            Self::Unix(path) => Ok(UpstreamIo::Unix(UnixStream::connect(path).await?)),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(addr) => Ok(UpstreamIo::Tcp(TcpStream::connect(addr).await?)),
        }
    }
}

/// Parse a control-channel `address` field. Accepted forms:
/// `unix:/abs/path` and `tcp:<ipv4|ipv6>:<port>` where the address must be
/// loopback. Anything else is rejected — the control channel is trusted
/// host-side but still fail-closed on malformed values.
pub fn parse_endpoint_address(raw: &str) -> Option<WorkerEndpoint> {
    if let Some(path) = raw.strip_prefix("unix:") {
        if path.starts_with('/') && path.len() <= 4096 && !path.contains('\0') {
            return Some(WorkerEndpoint::Unix(PathBuf::from(path)));
        }
        return None;
    }
    #[cfg(feature = "dev-stub")]
    if let Some(rest) = raw.strip_prefix("tcp:") {
        let addr: SocketAddr = rest.parse().ok()?;
        if addr.ip().is_loopback() {
            return Some(WorkerEndpoint::Tcp(addr));
        }
        return None;
    }
    None
}

/// A connected upstream stream (unix or TCP) unified under one
/// `AsyncRead + AsyncWrite` type for the hyper handshake.
pub enum UpstreamIo {
    /// Unix domain stream (production).
    Unix(UnixStream),
    /// TCP stream (`dev-stub` only).
    #[cfg(feature = "dev-stub")]
    Tcp(TcpStream),
}

impl AsyncRead for UpstreamIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unix(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Unix(s) => s.is_write_vectored(),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(s) => s.is_write_vectored(),
        }
    }
}

impl fmt::Display for WorkerEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
            #[cfg(feature = "dev-stub")]
            Self::Tcp(addr) => write!(f, "tcp:{addr}"),
        }
    }
}
