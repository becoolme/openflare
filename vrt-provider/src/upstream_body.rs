//! Upstream (vrt-sql / PG gateway) response body reader with a hard
//! byte cap (RV-R8 #8). Chunked reads so a lying or missing
//! `Content-Length` can never grow the buffer past the cap — the
//! response dies mid-stream instead of allocating unbounded memory.

/// Why an upstream body read failed.
#[derive(Debug)]
pub(crate) enum UpstreamBodyErr {
    /// Declared or emitted more than the cap.
    TooLarge,
    /// Transport-level failure mid-body.
    Transport,
}

/// Read `resp`'s body, hard-capped at `cap` bytes.
pub(crate) async fn read_body_capped(
    mut resp: reqwest::Response,
    cap: usize,
) -> Result<bytes::Bytes, UpstreamBodyErr> {
    if resp.content_length().is_some_and(|n| n as usize > cap) {
        return Err(UpstreamBodyErr::TooLarge);
    }
    let mut out = bytes::BytesMut::new();
    while let Some(chunk) = resp.chunk().await.map_err(|_| UpstreamBodyErr::Transport)? {
        if out.len() + chunk.len() > cap {
            return Err(UpstreamBodyErr::TooLarge);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    fn serve(raw: Vec<u8>) -> String {
        let lis = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = lis.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in lis.incoming() {
                let mut stream = match stream {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let mut r = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                let _ = r.read_line(&mut line);
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).unwrap_or(0) == 0 || h.trim().is_empty() {
                        break;
                    }
                }
                let _ = stream.write_all(&raw);
            }
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn oversized_chunked_body_is_cut_mid_stream() {
        // No Content-Length — chunks of 1MiB; the cap must trip during
        // accumulation, not after buffering.
        let mut raw = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        for _ in 0..8 {
            let chunk = vec![b'x'; 1024 * 1024];
            raw.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            raw.extend_from_slice(&chunk);
            raw.extend_from_slice(b"\r\n");
        }
        raw.extend_from_slice(b"0\r\n\r\n");
        let url = serve(raw);
        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await.unwrap();
        let err = read_body_capped(resp, 3 * 1024 * 1024).await.unwrap_err();
        assert!(matches!(err, UpstreamBodyErr::TooLarge));
    }

    #[tokio::test]
    async fn lying_content_length_is_rejected_before_read() {
        let mut raw = b"HTTP/1.1 200 OK\r\ncontent-length: 99999999\r\n\r\n".to_vec();
        raw.extend_from_slice(&vec![b'y'; 4096]);
        let url = serve(raw);
        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await.unwrap();
        let err = read_body_capped(resp, 1024 * 1024).await.unwrap_err();
        assert!(matches!(err, UpstreamBodyErr::TooLarge));
    }

    #[tokio::test]
    async fn within_cap_body_passes() {
        let mut raw = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
        for chunk in [&b"hello"[..], &b"world"[..]] {
            raw.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            raw.extend_from_slice(chunk);
            raw.extend_from_slice(b"\r\n");
        }
        raw.extend_from_slice(b"0\r\n\r\n");
        let url = serve(raw);
        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await.unwrap();
        let body = read_body_capped(resp, 1024).await.unwrap();
        assert_eq!(&body[..], b"helloworld");
    }
}
