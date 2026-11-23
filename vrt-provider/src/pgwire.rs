//! MIG-15: pg-wire protocol primitives shared by the Hyperdrive proxy
//! listener ([`crate::pgproxy`]) and its connection pool
//! ([`crate::pgpool`]).
//!
//! Upstream auth supports AuthenticationOk, cleartext and
//! SCRAM-SHA-256; md5/GSS fail closed. Every read is length-bounded
//! ([`PG_MESSAGE_MAX`]) and no credential material is ever logged.
//! [`RelayState`] tracks just enough frame structure on both directions
//! of a relayed session to decide whether the upstream socket sits on a
//! clean `ReadyForQuery` boundary when the client goes away — that is
//! the precondition for returning a connection to the pool.

use std::time::Duration;

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::pg::PgTcpCredential;

/// Protocol 3.0 startup version word.
pub const PROTO_V3: u32 = 196_608;
/// Per-message body cap — a hostile upstream cannot grow memory past it.
pub const PG_MESSAGE_MAX: u32 = 16 * 1024 * 1024;
/// Simple-query text cap for the pool's own statements
/// (`SELECT 1`, `ROLLBACK`, `DISCARD ALL`).
const QUERY_MAX: usize = 64 * 1024;

/// Typed upstream failures — mapped to session denials by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// TCP connect failed, timed out, or the socket died mid-frame.
    Connect,
    /// Authentication was rejected or used an unsupported mechanism.
    Auth,
    /// The peer sent a frame that cannot belong to this protocol state.
    Protocol,
}

/// An authenticated upstream connection with a clean
/// `ReadyForQuery` on both sides (the caller owns framing from here).
pub struct PgConn {
    pub rd: OwnedReadHalf,
    pub wr: OwnedWriteHalf,
}

/// One backend message: tag byte plus body (length word stripped).
#[derive(Debug)]
pub struct Message {
    pub tag: u8,
    pub body: Vec<u8>,
}

/// `ReadyForQuery` reply: transaction status (`I`/`T`/`E`) and whether
/// any `ErrorResponse` was seen before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ready {
    pub status: u8,
    pub errored: bool,
}

/// Connect + authenticate, bounded by `timeout` for the whole
/// connect→`ReadyForQuery` phase. The deadline is what keeps a stalled or
/// silent upstream from pinning a session slot forever.
pub async fn connect(tcp: &PgTcpCredential, timeout: Duration) -> Result<PgConn, WireError> {
    match tokio::time::timeout(timeout, connect_inner(tcp)).await {
        Ok(result) => result,
        Err(_) => Err(WireError::Connect),
    }
}

async fn connect_inner(tcp: &PgTcpCredential) -> Result<PgConn, WireError> {
    let sock = TcpStream::connect((tcp.host.as_str(), tcp.port))
        .await
        .map_err(|_| WireError::Connect)?;
    let _ = sock.set_nodelay(true);
    let (mut rd, mut wr) = sock.into_split();
    upstream_handshake(&mut rd, &mut wr, tcp).await?;
    Ok(PgConn { rd, wr })
}

/// Read one backend message. EOF/IO before a whole frame is
/// [`WireError::Connect`]; an impossible length word is
/// [`WireError::Protocol`].
pub async fn read_message(rd: &mut OwnedReadHalf) -> Result<Message, WireError> {
    let tag = rd.read_u8().await.map_err(|_| WireError::Connect)?;
    let len = rd.read_u32().await.map_err(|_| WireError::Connect)?;
    if !(4..=PG_MESSAGE_MAX).contains(&len) {
        return Err(WireError::Protocol);
    }
    let mut body = vec![0u8; (len - 4) as usize];
    rd.read_exact(&mut body)
        .await
        .map_err(|_| WireError::Connect)?;
    Ok(Message { tag, body })
}

/// Send one simple-protocol `Query` frame.
pub async fn send_query(wr: &mut OwnedWriteHalf, sql: &str) -> Result<(), WireError> {
    if sql.len() > QUERY_MAX || sql.contains('\0') {
        return Err(WireError::Protocol);
    }
    let mut frame = Vec::with_capacity(6 + sql.len());
    frame.push(b'Q');
    frame.extend_from_slice(&((sql.len() + 5) as u32).to_be_bytes());
    frame.extend_from_slice(sql.as_bytes());
    frame.push(0);
    wr.write_all(&frame).await.map_err(|_| WireError::Connect)
}

/// Read up to and including the next `ReadyForQuery`. Notices,
/// parameter status and notifications are skipped; an `ErrorResponse`
/// is recorded but the loop still runs to the ready frame (the
/// protocol always ends there).
pub async fn read_until_ready(rd: &mut OwnedReadHalf) -> Result<Ready, WireError> {
    let mut errored = false;
    loop {
        let msg = read_message(rd).await?;
        match msg.tag {
            b'Z' => {
                let status = *msg.body.first().ok_or(WireError::Protocol)?;
                return Ok(Ready { status, errored });
            }
            b'E' => errored = true,
            _ => {}
        }
    }
}

/// Simple query + read to `ReadyForQuery`.
pub async fn query(
    rd: &mut OwnedReadHalf,
    wr: &mut OwnedWriteHalf,
    sql: &str,
) -> Result<Ready, WireError> {
    send_query(wr, sql).await?;
    read_until_ready(rd).await
}

type HmacSha = Hmac<Sha256>;
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = <HmacSha as Mac>::new_from_slice(key).expect("hmac key");
    m.update(data);
    m.finalize().into_bytes().into()
}
fn pbkdf2(password: &[u8], salt: &[u8], iters: u32) -> [u8; 32] {
    // PBKDF2-HMAC-SHA256, dkLen == 32 → single block.
    let mut block = hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut out = block;
    for _ in 1..iters {
        block = hmac_sha256(password, &block);
        for (o, b) in out.iter_mut().zip(block.iter()) {
            *o ^= b;
        }
    }
    out
}

/// Upstream startup + auth. Fails closed on any unsupported mode.
async fn upstream_handshake(
    urd: &mut OwnedReadHalf,
    uwr: &mut OwnedWriteHalf,
    tcp: &PgTcpCredential,
) -> Result<(), WireError> {
    let mut start = Vec::with_capacity(128);
    let mut params = Vec::new();
    for (k, v) in [
        ("user", tcp.user.as_str()),
        ("database", tcp.database.as_str()),
        ("application_name", "vrt-hyperdrive-proxy"),
    ] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    start.extend_from_slice(&((params.len() + 8) as u32).to_be_bytes());
    start.extend_from_slice(&PROTO_V3.to_be_bytes());
    start.extend_from_slice(&params);
    uwr.write_all(&start)
        .await
        .map_err(|_| WireError::Connect)?;

    let mut scram: Option<ScramClient> = None;
    loop {
        let tag = urd.read_u8().await.map_err(|_| WireError::Connect)?;
        let len = urd.read_u32().await.map_err(|_| WireError::Connect)?;
        if !(4..=PG_MESSAGE_MAX).contains(&len) {
            return Err(WireError::Auth);
        }
        let mut body = vec![0u8; (len - 4) as usize];
        urd.read_exact(&mut body)
            .await
            .map_err(|_| WireError::Connect)?;
        match tag {
            b'Z' => return Ok(()),
            b'S' | b'K' | b'N' => continue,
            b'E' => return Err(WireError::Auth),
            b'R' => {
                if body.len() < 4 {
                    return Err(WireError::Auth);
                }
                match u32::from_be_bytes(body[..4].try_into().unwrap()) {
                    0 => continue,
                    3 => send_password(uwr, &tcp.password).await?,
                    10 => {
                        let mechs: Vec<&[u8]> = body[4..]
                            .split(|b| *b == 0)
                            .filter(|s| !s.is_empty())
                            .collect();
                        if !mechs.iter().any(|m| *m == b"SCRAM-SHA-256") {
                            return Err(WireError::Auth);
                        }
                        let c = ScramClient::new(&tcp.user, &tcp.password);
                        uwr.write_all(&c.initial_response())
                            .await
                            .map_err(|_| WireError::Connect)?;
                        scram = Some(c);
                    }
                    11 => {
                        let Some(c) = scram.as_mut() else {
                            return Err(WireError::Auth);
                        };
                        let resp = c.server_first(&body[4..]).ok_or(WireError::Auth)?;
                        uwr.write_all(&resp).await.map_err(|_| WireError::Connect)?;
                    }
                    12 => {
                        let Some(c) = scram.as_mut() else {
                            return Err(WireError::Auth);
                        };
                        if !c.verify_server_final(&body[4..]) {
                            return Err(WireError::Auth);
                        }
                    }
                    _ => return Err(WireError::Auth),
                }
            }
            _ => return Err(WireError::Auth),
        }
    }
}

async fn send_password(uwr: &mut OwnedWriteHalf, password: &str) -> Result<(), WireError> {
    let mut m = Vec::with_capacity(9 + password.len());
    m.push(b'p');
    m.extend_from_slice(&((password.len() + 5) as u32).to_be_bytes());
    m.extend_from_slice(password.as_bytes());
    m.push(0);
    uwr.write_all(&m).await.map_err(|_| WireError::Connect)
}

/// SCRAM-SHA-256 client (RFC 5802/7677), channel binding off (`n,,` —
/// the cluster channel is loopback+service-account, not TLS).
struct ScramClient {
    password: String,
    cnonce: String,
    client_first_bare: String,
    server_signature: Option<[u8; 32]>,
}

impl ScramClient {
    fn new(user: &str, password: &str) -> Self {
        let nonce_src = format!(
            "{}:{}:{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            "vrt",
        );
        let cnonce = hex::encode(Sha256::digest(nonce_src.as_bytes()))[..24].to_string();
        let client_first_bare = format!("n={},r={}", user, cnonce);
        Self {
            password: password.to_string(),
            cnonce,
            client_first_bare,
            server_signature: None,
        }
    }

    fn initial_response(&self) -> Vec<u8> {
        let first = format!("n,,{}", self.client_first_bare);
        let data = first.as_bytes();
        let mut body = Vec::with_capacity(24 + data.len());
        body.extend_from_slice(b"SCRAM-SHA-256\0");
        body.extend_from_slice(&(data.len() as i32).to_be_bytes());
        body.extend_from_slice(data);
        let mut m = Vec::with_capacity(5 + body.len());
        m.push(b'p');
        m.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(&body);
        m
    }

    fn server_first(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        let s = String::from_utf8_lossy(data);
        let mut nonce = "";
        let mut salt64 = "";
        let mut iters = "";
        for part in s.split(',') {
            match &part[..2.min(part.len())] {
                "r=" => nonce = &part[2..],
                "s=" => salt64 = &part[2..],
                "i=" => iters = &part[2..],
                _ => {}
            }
        }
        if nonce.is_empty() || !nonce.starts_with(&self.cnonce) {
            return None;
        }
        let salt = base64::engine::general_purpose::STANDARD
            .decode(salt64)
            .ok()?;
        let iters: u32 = iters.parse().ok()?;
        // MIG-15 r2: upstream-controlled iteration count — cap it so a
        // hostile/buggy upstream can't pin the CPU (fail closed →
        // WireError::Auth).
        if iters == 0 || iters > 100_000 {
            return None;
        }
        let cf_no_proof = format!("c=biws,r={}", nonce);
        let auth = format!("{},{},{}", self.client_first_bare, s, cf_no_proof);
        let salted = pbkdf2(self.password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored = Sha256::digest(client_key);
        let sig = hmac_sha256(&stored, auth.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(sig.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac_sha256(&salted, b"Server Key");
        self.server_signature = Some(hmac_sha256(&server_key, auth.as_bytes()));
        let final_msg = format!(
            "{},p={}",
            cf_no_proof,
            base64::engine::general_purpose::STANDARD.encode(&proof)
        );
        let data = final_msg.as_bytes();
        let mut m = Vec::with_capacity(5 + data.len());
        m.push(b'p');
        m.extend_from_slice(&((data.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(data);
        Some(m)
    }

    fn verify_server_final(&self, data: &[u8]) -> bool {
        let Some(expected) = self.server_signature else {
            return false;
        };
        let s = String::from_utf8_lossy(data);
        let Some(v) = s.split(',').find_map(|p| p.strip_prefix("v=")) else {
            return false;
        };
        base64::engine::general_purpose::STANDARD
            .decode(v)
            .ok()
            .map(|b| b == expected.to_vec())
            .unwrap_or(false)
    }
}

// ------------------------------------------------- relay state tracking

/// Incremental frame scanner for ONE direction of a relayed session.
/// Bytes are consumed as they are forwarded; only the tag and (for
/// `Z`) the first body byte are retained, so a 16 MiB row never gets
/// buffered here.
#[derive(Debug, Default)]
struct FrameScan {
    header: [u8; 5],
    header_len: usize,
    body_left: u32,
    tag: u8,
    first: Option<u8>,
    /// Offset of the in-progress tag byte within the chunk being fed —
    /// `None` when the frame started in an earlier chunk.
    frame_start: Option<usize>,
    in_body: bool,
    poisoned: bool,
}

impl FrameScan {
    /// True when every byte so far ended exactly on a message boundary.
    fn at_boundary(&self) -> bool {
        !self.poisoned && !self.in_body && self.header_len == 0
    }

    /// Feed one chunk. `on_frame(tag, first_body_byte, start_offset)`
    /// runs per completed frame; when `stop` names a tag, scanning ends
    /// at the first completed frame with that tag and the offset at
    /// which that frame starts inside `bytes` is returned (`0` when the
    /// frame began in an earlier chunk, i.e. nothing of this chunk may
    /// be forwarded).
    fn feed(
        &mut self,
        bytes: &[u8],
        stop: Option<u8>,
        mut on_frame: impl FnMut(u8, Option<u8>, Option<usize>),
    ) -> Option<usize> {
        let total = bytes.len();
        if self.header_len > 0 || self.in_body {
            // A frame carried over from the previous chunk: its start is
            // not addressable inside this one.
            self.frame_start = None;
        }
        let mut consumed = 0usize;
        while consumed < total && !self.poisoned {
            if !self.in_body {
                if self.header_len == 0 {
                    self.frame_start = Some(consumed);
                }
                let n = (5 - self.header_len).min(total - consumed);
                self.header[self.header_len..self.header_len + n]
                    .copy_from_slice(&bytes[consumed..consumed + n]);
                self.header_len += n;
                consumed += n;
                if self.header_len < 5 {
                    break;
                }
                self.tag = self.header[0];
                let len = u32::from_be_bytes(self.header[1..5].try_into().unwrap());
                self.header_len = 0;
                self.first = None;
                if !(4..=PG_MESSAGE_MAX).contains(&len) {
                    // Desynchronised stream: never reusable.
                    self.poisoned = true;
                    break;
                }
                self.body_left = len - 4;
                self.in_body = true;
                if self.body_left == 0 {
                    self.in_body = false;
                    let start = self.frame_start;
                    self.frame_start = None;
                    on_frame(self.tag, None, start);
                    if Some(self.tag) == stop {
                        return Some(start.unwrap_or(0));
                    }
                }
            } else {
                let n = (self.body_left as usize).min(total - consumed);
                if self.first.is_none() && n > 0 {
                    self.first = Some(bytes[consumed]);
                }
                self.body_left -= n as u32;
                consumed += n;
                if self.body_left == 0 {
                    self.in_body = false;
                    let start = self.frame_start;
                    self.frame_start = None;
                    on_frame(self.tag, self.first, start);
                    if Some(self.tag) == stop {
                        return Some(start.unwrap_or(0));
                    }
                }
            }
        }
        None
    }
}

/// What the relay learned about protocol boundaries.
#[derive(Debug)]
pub struct RelayState {
    up: FrameScan,
    client: FrameScan,
    upstream_at_ready: bool,
    client_idle: bool,
    ready_status: Option<u8>,
}

impl Default for RelayState {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayState {
    /// A freshly checked-out upstream connection is authenticated and
    /// idle, so both sides start on a boundary.
    pub fn new() -> Self {
        Self {
            up: FrameScan::default(),
            client: FrameScan::default(),
            upstream_at_ready: true,
            client_idle: true,
            ready_status: Some(b'I'),
        }
    }

    /// Bytes flowing upstream→client.
    pub fn feed_upstream(&mut self, bytes: &[u8]) {
        let mut at_ready = self.upstream_at_ready;
        let mut status = self.ready_status;
        let mut idle = self.client_idle;
        self.up.feed(bytes, None, |tag, first, _| match tag {
            // ReadyForQuery answers every outstanding client request and
            // closes the current cycle.
            b'Z' => {
                at_ready = true;
                idle = true;
                status = first;
            }
            // Asynchronous frames may arrive at any time and do not
            // move the session off its boundary.
            b'N' | b'S' | b'A' => {}
            _ => at_ready = false,
        });
        self.upstream_at_ready = at_ready;
        self.client_idle = idle;
        self.ready_status = status;
    }

    /// Bytes flowing client→upstream. When the chunk holds a
    /// `Terminate` frame the number of bytes to forward *before* it is
    /// returned; the frame itself is never forwarded, because
    /// swallowing it is what keeps the backend socket alive for the
    /// next session of the same key.
    pub fn feed_client(&mut self, bytes: &[u8]) -> Option<usize> {
        let mut idle = self.client_idle;
        let terminated = self.client.feed(bytes, Some(b'X'), |tag, _, _| {
            // Every frontend message except Terminate starts (or
            // continues) an exchange that only ends at ReadyForQuery.
            if tag != b'X' {
                idle = false;
            }
        });
        self.client_idle = idle;
        terminated
    }

    /// True when the upstream sits exactly on a `ReadyForQuery`
    /// boundary with no client request outstanding — the only state in
    /// which a connection may be reset and pooled.
    pub fn reusable(&self) -> bool {
        self.upstream_at_ready
            && self.client_idle
            && self.up.at_boundary()
            && self.client.at_boundary()
    }

    /// Transaction status of the last `ReadyForQuery` (`None` before
    /// the first one).
    pub fn ready_status(&self) -> Option<u8> {
        self.ready_status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_proof_vector() {
        // RFC 7677 example: user "user", pass "pencil".
        let mut c = ScramClient::new("user", "pencil");
        c.cnonce = "rOprNGfwEbeRWgbNEkqO".into();
        c.client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO".into();
        let sf = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let resp = c.server_first(sf.as_bytes()).expect("resp");
        let body = String::from_utf8_lossy(&resp[5..]);
        assert!(body.contains("p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="));
        assert!(c.verify_server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="));
    }

    #[test]
    fn scram_rejects_bad_nonce_and_signature() {
        let mut c = ScramClient::new("u", "p");
        assert!(c.server_first(b"s=eQ==,i=4096").is_none()); // no r=
        assert!(!c.verify_server_final(b"v=AAAA"));
    }

    #[test]
    fn scram_iters_cap() {
        // Upstream-controlled iteration count: >100_000 → fail closed.
        for bad in ["i=0", "i=100001", "i=99999999"] {
            let sf = format!("r=abcdefX,s=AAAA,{}", bad);
            let mut cc = ScramClient::new("u", "p");
            cc.cnonce = "abcdef".into();
            cc.client_first_bare = "n=u,r=abcdef".into();
            assert!(cc.server_first(sf.as_bytes()).is_none(), "{}", bad);
        }
        let mut c = ScramClient::new("u", "p");
        c.cnonce = "abcdef".into();
        c.client_first_bare = "n=u,r=abcdef".into();
        assert!(c.server_first(b"r=abcdefX,s=AAAA,i=4096").is_some());
    }

    #[test]
    fn pbkdf2_known() {
        // PBKDF2-HMAC-SHA256("password","salt",1) = 120fb6cf...
        let k = pbkdf2(b"password", b"salt", 1);
        assert_eq!(
            hex::encode(k),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
    }

    /// Backend frame: tag + length word + body.
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn relay_state_pools_only_on_a_clean_boundary() {
        let mut s = RelayState::new();
        assert!(s.reusable(), "a fresh connection is on a boundary");

        // Client query, upstream in-flight: not reusable.
        s.feed_client(&frame(b'Q', b"select 1\0"));
        assert!(!s.reusable());
        // Partial upstream frame must not flip the state.
        let rows = frame(b'D', b"x");
        s.feed_upstream(&rows[..2]);
        assert!(!s.reusable());
        s.feed_upstream(&rows[2..]);
        assert!(!s.reusable(), "row description is not a boundary");
        s.feed_upstream(&frame(b'Z', b"I"));
        assert!(s.reusable(), "ReadyForQuery closes the exchange");
        assert_eq!(s.ready_status(), Some(b'I'));

        // In a transaction block the boundary is not safe to reuse as
        // is; the status byte carries that.
        s.feed_upstream(&frame(b'Z', b"T"));
        assert!(s.reusable());
        assert_eq!(s.ready_status(), Some(b'T'));
    }

    #[test]
    fn relay_state_extends_through_sync_and_ignores_async_frames() {
        let mut s = RelayState::new();
        for tag in [b'P', b'B', b'D', b'E'] {
            s.feed_client(&frame(tag, b""));
            assert!(!s.reusable(), "{} expects a reply", tag as char);
            // Async frames never move the upstream off its boundary.
            s.feed_upstream(&frame(b'N', b"Sx\0Mnotice\0\0"));
            s.feed_upstream(&frame(b'S', b"k\0v\0"));
        }
        s.feed_client(&frame(b'S', b""));
        s.feed_upstream(&frame(b'Z', b"I"));
        assert!(s.reusable());

        // Terminate expects no reply.
        s.feed_client(&frame(b'X', b""));
        assert!(s.reusable());
    }

    #[test]
    fn relay_state_poisons_on_a_corrupt_length() {
        let mut s = RelayState::new();
        s.feed_upstream(&[b'D', 0xff, 0xff, 0xff, 0xff]);
        assert!(!s.reusable());
    }

    #[test]
    fn relay_state_survives_split_frames() {
        let mut s = RelayState::new();
        let frames = [frame(b'C', b"SELECT 1\0"), frame(b'Z', b"I")];
        for f in &frames {
            for chunk in f.chunks(1) {
                s.feed_upstream(chunk);
            }
        }
        assert!(s.reusable());
    }

    #[tokio::test]
    async fn query_round_trip_reads_errors_before_ready() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            s.into_split()
        });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut crd, mut cwr) = client.into_split();
        let (mut srd, mut swr) = server.await.unwrap();
        let srv = tokio::spawn(async move {
            let msg = read_message(&mut srd).await.unwrap();
            assert_eq!(msg.tag, b'Q');
            assert_eq!(msg.body, b"SELECT 1\0");
            swr.write_all(&frame(b'C', b"SELECT 1\0")).await.unwrap();
            swr.write_all(&frame(b'E', b"SERROR\0C42P01\0Mboom\0\0"))
                .await
                .unwrap();
            swr.write_all(&frame(b'Z', b"I")).await.unwrap();
        });
        let ready = query(&mut crd, &mut cwr, "SELECT 1").await.unwrap();
        assert_eq!(
            ready,
            Ready {
                status: b'I',
                errored: true
            }
        );
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn read_message_rejects_impossible_length() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let _ = s.write_all(&[b'R', 0x00, 0x00, 0x00, 0x02]).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let c = TcpStream::connect(addr).await.unwrap();
        let (mut rd, _wr) = c.into_split();
        assert_eq!(
            read_message(&mut rd).await.unwrap_err(),
            WireError::Protocol
        );
    }
}
