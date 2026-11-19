//! Readiness: control-fd listen events **and** a bounded probe per
//! worker socket. Both must hold before a generation is installed —
//! the task contract ("control-fd 收到就绪消息 + 每个 worker socket
//! 可连接").
//!
//! Real workerd 1.20261103.1 emits
//! `{"event":"listen","socket":"sock-<id>","port":0}` with NO address
//! for unix sockets (T25/M1, measured on B8). The event only CONFIRMS
//! that a named socket is listening — the endpoint is always resolved
//! from the spec the host itself rendered, never from event data.
//! The parser is injected ([`ControlEventParser`]) so the contract
//! stays swappable.

use crate::control::ControlEventParser;
use crate::endpoint::{parse_endpoint_address, WorkerEndpoint};
use crate::launch::{terminate_child, LaunchedWorkerd};
use crate::spec::TenantLaunchSpec;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

/// Default per-socket connect-probe timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Interval between connect retries inside the overall deadline.
const PROBE_RETRY: Duration = Duration::from_millis(50);

/// The worker a `sock-<workerId>` event refers to.
pub fn socket_worker_name(socket: &str) -> Option<&str> {
    socket.strip_prefix("sock-")
}

/// The worker a `sock-qconsumer-<workerId>` event refers to (T37b —
/// checked BEFORE `sock-` so `qconsumer-<id>` never reads as a worker).
pub fn socket_qconsumer_name(socket: &str) -> Option<&str> {
    socket.strip_prefix("sock-qconsumer-")
}

/// W3: `sock-wfops-<workerId>` — MUST be classified before `sock-`
/// (same reason as qconsumer: `wfops-<id>` is not a worker id).
pub fn socket_wfops_name(socket: &str) -> Option<&str> {
    socket.strip_prefix("sock-wfops-")
}

/// CR1: `sock-cron-<workerId>` — classified before `sock-` (same
/// reason: `cron-<id>` is not a worker id).
pub fn socket_cron_name(socket: &str) -> Option<&str> {
    socket.strip_prefix("sock-cron-")
}

/// Resolved endpoints after a generation proves ready.
#[derive(Debug)]
pub struct ReadyEndpoints {
    /// `workerId → sock-<id>` ingress endpoints.
    pub ingress: BTreeMap<String, WorkerEndpoint>,
    /// `workerId → sock-qconsumer-<id>` dispatch endpoints (T37b).
    pub qconsumer: BTreeMap<String, WorkerEndpoint>,
    /// `workerId → sock-wfops-<id>` wfops facade endpoints (W3).
    pub wfops: BTreeMap<String, WorkerEndpoint>,
    /// `workerId → sock-cron-<id>` cron dispatcher endpoints (CR1).
    pub cron: BTreeMap<String, WorkerEndpoint>,
}

/// Readiness failures — constant payloads only.
#[derive(Debug, thiserror::Error)]
pub enum ReadyError {
    /// Overall deadline expired.
    #[error("ready deadline expired")]
    Timeout,
    /// The child exited before becoming ready.
    #[error("child exited before ready")]
    Exited,
    /// Control channel was malformed.
    #[error("control-fd: {0}")]
    Control(#[from] crate::control::ControlError),
    /// A listen event named an unexpected socket.
    #[error("unexpected socket name in control event")]
    UnexpectedSocket,
    /// An event's address conflicts with the spec endpoint.
    #[error("endpoint mismatch between spec and control event")]
    EndpointMismatch,
    /// A unix-socket event reported a non-zero port.
    #[error("unix socket event reported a non-zero port")]
    UnexpectedPort,
    /// A control channel read failed.
    #[error("control-fd read failed")]
    Io,
}

impl ReadyError {
    /// Stable reason token for structured logs/metrics.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Exited => "exited",
            Self::Control(_) => "control",
            Self::UnexpectedSocket => "unexpected_socket",
            Self::EndpointMismatch => "endpoint_mismatch",
            Self::UnexpectedPort => "unexpected_port",
            Self::Io => "io",
        }
    }
}

/// Resolve the endpoint to probe/connect for one worker — ALWAYS from
/// the spec, never guessed from event data (T25/M1).
///
/// - `Unix` spec: the event's `port` MUST be 0 (measured: real workerd
///   sends 0 for unix sockets). An absent `address` resolves to the
///   spec path; a present one must equal `unix:<spec path>` exactly.
/// - `Tcp` spec (dev-stub only), port 0: the ephemeral port can only be
///   learned from the event — `address` is required and must be a
///   loopback `tcp:` address.
/// - `Tcp` spec with a concrete port: use it; a present `address` must
///   equal it exactly.
fn resolve_endpoint(
    _worker_id: &str,
    spec_ep: &WorkerEndpoint,
    event: &crate::control::ListenEvent,
) -> Result<WorkerEndpoint, ReadyError> {
    // An address that is PRESENT but unparseable/unmatching is a
    // mismatch — it is never silently treated as "absent" (M1: only a
    // truly absent address means "trust the spec").
    match spec_ep {
        WorkerEndpoint::Unix(path) => {
            if event.port != 0 {
                return Err(ReadyError::UnexpectedPort);
            }
            match &event.address {
                None => Ok(spec_ep.clone()),
                Some(raw) => match parse_endpoint_address(raw) {
                    Some(WorkerEndpoint::Unix(rep)) if rep == *path => Ok(spec_ep.clone()),
                    _ => Err(ReadyError::EndpointMismatch),
                },
            }
        }
        #[cfg(feature = "dev-stub")]
        WorkerEndpoint::Tcp(spec_addr) => match &event.address {
            None if spec_addr.port() == 0 => Err(ReadyError::EndpointMismatch),
            None => Ok(spec_ep.clone()),
            Some(raw) => match parse_endpoint_address(raw) {
                Some(WorkerEndpoint::Tcp(rep_addr)) => {
                    let ip_ok = spec_addr.ip().is_unspecified() || rep_addr.ip() == spec_addr.ip();
                    if spec_addr.port() == 0 {
                        // Ephemeral spec: the event reports the real
                        // port — same host, nonzero port required.
                        if rep_addr.port() != 0 && ip_ok {
                            Ok(WorkerEndpoint::Tcp(rep_addr))
                        } else {
                            Err(ReadyError::EndpointMismatch)
                        }
                    } else if rep_addr == *spec_addr {
                        Ok(spec_ep.clone())
                    } else {
                        Err(ReadyError::EndpointMismatch)
                    }
                }
                _ => Err(ReadyError::EndpointMismatch),
            },
        },
    }
}

/// Wait until every expected worker socket has reported `listen` AND
/// accepted a probe connection, bounded by `deadline`. On failure the
/// caller kills the child — `wait_ready` itself does not.
pub async fn wait_ready(
    live: &mut LaunchedWorkerd,
    spec: &TenantLaunchSpec,
    parser: &mut dyn ControlEventParser,
    deadline: Duration,
    probe_timeout: Duration,
) -> Result<ReadyEndpoints, ReadyError> {
    let expected: BTreeSet<String> = spec.worker_endpoints.keys().cloned().collect();
    let expected_q: BTreeSet<String> = spec.qconsumer_endpoints.keys().cloned().collect();
    let expected_w: BTreeSet<String> = spec.wfops_endpoints.keys().cloned().collect();
    let expected_c: BTreeSet<String> = spec.cron_endpoints.keys().cloned().collect();
    let mut seen: BTreeMap<String, WorkerEndpoint> = BTreeMap::new();
    let mut seen_q: BTreeMap<String, WorkerEndpoint> = BTreeMap::new();
    let mut seen_w: BTreeMap<String, WorkerEndpoint> = BTreeMap::new();
    let mut seen_c: BTreeMap<String, WorkerEndpoint> = BTreeMap::new();
    let end = Instant::now() + deadline;
    let mut buf = [0u8; 4096];

    loop {
        if Instant::now() >= end {
            return Err(ReadyError::Timeout);
        }
        match live.child.try_wait() {
            Ok(Some(_)) => return Err(ReadyError::Exited),
            Ok(None) => {}
            Err(_) => return Err(ReadyError::Io),
        }
        if seen.len() == expected.len()
            && seen_q.len() == expected_q.len()
            && seen_w.len() == expected_w.len()
            && seen_c.len() == expected_c.len()
        {
            // All events seen — verify connectability per endpoint.
            let remain = end.saturating_duration_since(Instant::now());
            let ingress = probe_all(&seen, remain.min(probe_timeout)).await?;
            let remain = end.saturating_duration_since(Instant::now());
            let qconsumer = probe_all(&seen_q, remain.min(probe_timeout)).await?;
            let remain = end.saturating_duration_since(Instant::now());
            let wfops = probe_all(&seen_w, remain.min(probe_timeout)).await?;
            let remain = end.saturating_duration_since(Instant::now());
            let cron = probe_all(&seen_c, remain.min(probe_timeout)).await?;
            return Ok(ReadyEndpoints {
                ingress,
                qconsumer,
                wfops,
                cron,
            });
        }
        let remain = end.saturating_duration_since(Instant::now());
        if remain.is_zero() {
            return Err(ReadyError::Timeout);
        }
        match tokio::time::timeout(
            remain.min(Duration::from_millis(100)),
            live.control.read(&mut buf),
        )
        .await
        {
            Ok(Ok(0)) => {
                // Control channel closed early.
                match live.child.try_wait() {
                    Ok(Some(_)) => return Err(ReadyError::Exited),
                    _ => return Err(ReadyError::Io),
                }
            }
            Ok(Ok(n)) => {
                for event in parser.push(&buf[..n])? {
                    // T37b: `sock-qconsumer-` MUST be classified before
                    // `sock-` — the former contains the latter's payload
                    // as a suffix (`qconsumer-<id>` is not a worker).
                    if let Some(worker_id) = socket_qconsumer_name(&event.socket) {
                        if !expected_q.contains(worker_id) {
                            return Err(ReadyError::UnexpectedSocket);
                        }
                        let resolved = resolve_endpoint(
                            worker_id,
                            &spec.qconsumer_endpoints[worker_id],
                            &event,
                        )?;
                        seen_q.insert(worker_id.to_string(), resolved);
                        continue;
                    }
                    if let Some(worker_id) = socket_wfops_name(&event.socket) {
                        if !expected_w.contains(worker_id) {
                            return Err(ReadyError::UnexpectedSocket);
                        }
                        let resolved =
                            resolve_endpoint(worker_id, &spec.wfops_endpoints[worker_id], &event)?;
                        seen_w.insert(worker_id.to_string(), resolved);
                        continue;
                    }
                    if let Some(worker_id) = socket_cron_name(&event.socket) {
                        if !expected_c.contains(worker_id) {
                            return Err(ReadyError::UnexpectedSocket);
                        }
                        let resolved =
                            resolve_endpoint(worker_id, &spec.cron_endpoints[worker_id], &event)?;
                        seen_c.insert(worker_id.to_string(), resolved);
                        continue;
                    }
                    let Some(worker_id) = socket_worker_name(&event.socket) else {
                        // Non-tenant sockets (e.g. a diagnostic listener)
                        // are ignored; only `sock-*` names carry meaning.
                        continue;
                    };
                    if !expected.contains(worker_id) {
                        return Err(ReadyError::UnexpectedSocket);
                    }
                    let resolved =
                        resolve_endpoint(worker_id, &spec.worker_endpoints[worker_id], &event)?;
                    seen.insert(worker_id.to_string(), resolved);
                }
            }
            Ok(Err(_)) => return Err(ReadyError::Io),
            Err(_) => {} // read timeout slice — re-loop
        }
    }
}

/// Connect-probe every resolved endpoint once (with bounded retries
/// inside `budget`). Unix endpoints are additionally lstat-checked:
/// the expected path must be an actual socket inode — a symlinked or
/// regular file at the path fails the probe (T25/M1).
pub(crate) async fn probe_all(
    seen: &BTreeMap<String, WorkerEndpoint>,
    budget: Duration,
) -> Result<BTreeMap<String, WorkerEndpoint>, ReadyError> {
    let end = Instant::now() + budget;
    let mut out = BTreeMap::new();
    for (worker, endpoint) in seen.iter() {
        loop {
            match probe_one(endpoint).await {
                Ok(()) => {
                    out.insert(worker.clone(), endpoint.clone());
                    break;
                }
                Err(_) if Instant::now() + PROBE_RETRY < end => {
                    tokio::time::sleep(PROBE_RETRY).await;
                }
                Err(_) => return Err(ReadyError::Timeout),
            }
        }
    }
    Ok(out)
}

/// One probe attempt: unix-socket inode check (no symlink follow)
/// then a connect.
async fn probe_one(endpoint: &WorkerEndpoint) -> std::io::Result<()> {
    match endpoint {
        WorkerEndpoint::Unix(path) => {
            use std::os::unix::fs::FileTypeExt;
            let meta = std::fs::symlink_metadata(path)?;
            if !meta.file_type().is_socket() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "endpoint path is not a socket inode",
                ));
            }
        }
        #[cfg(feature = "dev-stub")]
        WorkerEndpoint::Tcp(_) => {}
    }
    endpoint.connect().await.map(|_| ())
}

/// Convenience: wait ready with a parser factory and, on any failure,
/// terminate the child (SIGTERM → `kill_grace` → SIGKILL). Returns the
/// resolved endpoints on success.
pub async fn launch_until_ready(
    live: &mut LaunchedWorkerd,
    spec: &TenantLaunchSpec,
    parser: &mut dyn ControlEventParser,
    ready_timeout: Duration,
    probe_timeout: Duration,
    kill_grace: Duration,
) -> Result<ReadyEndpoints, ReadyError> {
    match wait_ready(live, spec, parser, ready_timeout, probe_timeout).await {
        Ok(endpoints) => Ok(endpoints),
        Err(error) => {
            terminate_child(&mut live.child, live.pid, kill_grace).await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unix_spec() -> WorkerEndpoint {
        WorkerEndpoint::Unix(std::path::PathBuf::from("/gen-1/sock/w1.sock"))
    }

    fn ev(port: u16, address: Option<&str>) -> crate::control::ListenEvent {
        crate::control::ListenEvent {
            socket: "sock-w1".to_string(),
            port,
            address: address.map(|a| a.to_string()),
        }
    }

    #[test]
    fn unix_no_address_uses_spec_endpoint() {
        // The real workerd 1.20261103.1 line: port 0, no address.
        let got = resolve_endpoint("w1", &unix_spec(), &ev(0, None)).unwrap();
        assert_eq!(got, unix_spec());
    }

    #[test]
    fn unix_address_matching_spec_accepted() {
        let got =
            resolve_endpoint("w1", &unix_spec(), &ev(0, Some("unix:/gen-1/sock/w1.sock"))).unwrap();
        assert_eq!(got, unix_spec());
    }

    #[test]
    fn unix_address_mismatch_rejected() {
        for bad in [
            "unix:/gen-1/sock/w2.sock",
            "unix:/elsewhere/w1.sock",
            "unix:/gen-1/sock/w1.evil.sock",
        ] {
            let err = resolve_endpoint("w1", &unix_spec(), &ev(0, Some(bad))).unwrap_err();
            assert!(matches!(err, ReadyError::EndpointMismatch), "{bad}: {err}");
        }
    }

    #[test]
    fn unix_address_wrong_scheme_rejected() {
        // TCP address reported for a unix spec → mismatch, not accept.
        let err =
            resolve_endpoint("w1", &unix_spec(), &ev(0, Some("tcp:127.0.0.1:9000"))).unwrap_err();
        assert!(matches!(err, ReadyError::EndpointMismatch));
    }

    #[test]
    fn unix_nonzero_port_rejected() {
        for port in [1u16, 8080, u16::MAX] {
            let err = resolve_endpoint(
                "w1",
                &unix_spec(),
                &ev(port, Some("unix:/gen-1/sock/w1.sock")),
            )
            .unwrap_err();
            assert!(matches!(err, ReadyError::UnexpectedPort), "{port}");
            let err = resolve_endpoint("w1", &unix_spec(), &ev(port, None)).unwrap_err();
            assert!(matches!(err, ReadyError::UnexpectedPort), "{port}");
        }
    }

    #[test]
    fn unix_unparseable_address_rejected() {
        // A non-empty address that isn't a recognized endpoint form
        // never silently becomes "matching".
        for bad in ["unix:relative/path.sock", "bogus", "tcp:8.8.8.8:53"] {
            let err = resolve_endpoint("w1", &unix_spec(), &ev(0, Some(bad))).unwrap_err();
            assert!(matches!(err, ReadyError::EndpointMismatch), "{bad}: {err}");
        }
    }

    #[cfg(feature = "dev-stub")]
    mod tcp {
        use super::*;
        use std::net::SocketAddr;

        fn spec() -> WorkerEndpoint {
            WorkerEndpoint::Tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        }

        #[test]
        fn tcp_reported_port_resolves_endpoint() {
            // Stub tcp: spec binds :0; the event's address carries the
            // real port — same host required.
            let got =
                resolve_endpoint("w1", &spec(), &ev(43210, Some("tcp:127.0.0.1:43210"))).unwrap();
            assert_eq!(got, WorkerEndpoint::Tcp("127.0.0.1:43210".parse().unwrap()));
        }

        #[test]
        fn tcp_wrong_host_rejected() {
            let err = resolve_endpoint("w1", &spec(), &ev(43210, Some("tcp:10.1.2.3:43210")))
                .unwrap_err();
            assert!(matches!(err, ReadyError::EndpointMismatch));
        }

        #[test]
        fn tcp_missing_address_rejected() {
            // A tcp event can't resolve without an address.
            let err = resolve_endpoint("w1", &spec(), &ev(8080, None)).unwrap_err();
            assert!(matches!(err, ReadyError::EndpointMismatch));
        }

        #[test]
        fn tcp_port_zero_reported_rejected() {
            let err = resolve_endpoint("w1", &spec(), &ev(0, Some("tcp:127.0.0.1:0"))).unwrap_err();
            assert!(matches!(err, ReadyError::EndpointMismatch));
        }
    }
}
