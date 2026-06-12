//! Real end-to-end SPDY/3.1 exec test.
//!
//! This drives the server over the wire with an INDEPENDENT, self-contained
//! SPDY/3.1 client (its own frame codec + its own zlib codec installing the
//! canonical SPDY dictionary) — NOT the crate's own `spdy` module. That makes
//! it a true cross-implementation check of the wire path, not a dict round-trip
//! against itself.
//!
//! The flow mirrors client-go's `remotecommand` SPDY executor:
//!   1. HTTP/1.1 `Upgrade: SPDY/3.1` POST → expect `101 Switching Protocols`.
//!   2. Open the error + stdout streams via SYN_STREAM, header block compressed
//!      with the SPDY dictionary (`streamType=error`, `streamType=stdout`).
//!   3. Read the server's SYN_REPLY for each stream and DECOMPRESS its header
//!      block — proving the server emits a *well-formed* zlib SYN_REPLY block.
//!   4. Half-close so the server's read loop ends, then assert:
//!        - stdout DATA frames carry the process output, and
//!        - the v4 `metav1.Status` JSON lands on the error stream.
//!
//! Why this catches the bug a self-round-trip cannot: the client compresses
//! SYN_STREAM against the CANONICAL dictionary (sha256-pinned below). If the
//! server's dictionary were drifted (e.g. a spurious trailing NUL appended),
//! the server's `inflateSetDictionary` would `Z_DATA_ERROR`, `streamType` would
//! never parse, no output would be pumped and no exit Status delivered — and
//! the assertions here would fail. Proven against the broken dict in
//! `dictionary_drift_breaks_the_wire` below.

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};
use lightr_cri_backend::{ExitWaiter, StreamSession};
use lightr_cri_stream::{serve, SessionFactory, StreamParams, StreamVerb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ── canonical SPDY/3 dictionary (independent copy, sha256-verified) ──────────
//
// Built the same human-readable way the crate's `dict.rs` is, then pinned by
// sha256 so this test's client provably compresses against the canonical bytes
// (moby/spdystream `headerDictionary`, 1423 bytes, ends `enq=0.`, no NUL).
const CANON_DICT: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\x0dcache-control\x00\x00\x00\x0aconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\x0dcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\x0dif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\x0dlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\x0auser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\x0aset-cookie\x00\x00\x00\x0akeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0.";

const CANON_DICT_SHA256: &str = "51d27341373f923f3cd88e1eb7162aeaa3723d7585ff2399201dc06498407f02";

// ── minimal SPDY/3.1 wire codec (client side, independent of the crate) ──────

const STREAM_STDOUT: u32 = 1;
const STREAM_ERROR: u32 = 3;

/// A client header compressor: one continuous zlib stream, dictionary preset.
struct ClientComp {
    z: Compress,
}
impl ClientComp {
    fn new(dict: &[u8]) -> Self {
        let mut z = Compress::new(Compression::default(), true);
        z.set_dictionary(dict).expect("client set dict");
        ClientComp { z }
    }
    fn compress(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        let mut consumed = 0usize;
        loop {
            let bi = self.z.total_in();
            let bo = self.z.total_out();
            self.z
                .compress(&input[consumed..], &mut buf, FlushCompress::Sync)
                .expect("client compress");
            let read = (self.z.total_in() - bi) as usize;
            let wrote = (self.z.total_out() - bo) as usize;
            consumed += read;
            out.extend_from_slice(&buf[..wrote]);
            if consumed >= input.len() && wrote < buf.len() {
                break;
            }
            if read == 0 && wrote == 0 {
                break;
            }
        }
        out
    }
}

/// A client header decompressor mirroring the server's lazy-dict install.
struct ClientDecomp {
    z: Decompress,
    dict_installed: bool,
    dict: Vec<u8>,
}
impl ClientDecomp {
    fn new(dict: &[u8]) -> Self {
        ClientDecomp {
            z: Decompress::new(true),
            dict_installed: false,
            dict: dict.to_vec(),
        }
    }
    fn decompress(&mut self, input: &[u8]) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        let mut consumed = 0usize;
        loop {
            let bi = self.z.total_in();
            let bo = self.z.total_out();
            match self
                .z
                .decompress(&input[consumed..], &mut buf, FlushDecompress::None)
            {
                Ok(_) => {}
                Err(e) => {
                    if !self.dict_installed {
                        self.z
                            .set_dictionary(&self.dict)
                            .map_err(|e| format!("client set dict: {e}"))?;
                        self.dict_installed = true;
                        consumed += (self.z.total_in() - bi) as usize;
                        continue;
                    }
                    return Err(format!("client decompress: {e}"));
                }
            }
            let read = (self.z.total_in() - bi) as usize;
            let wrote = (self.z.total_out() - bo) as usize;
            consumed += read;
            out.extend_from_slice(&buf[..wrote]);
            if consumed >= input.len() && wrote < buf.len() {
                break;
            }
            if read == 0 && wrote == 0 {
                break;
            }
        }
        Ok(out)
    }
}

fn serialize_headers(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (k, v) in pairs {
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v.as_bytes());
    }
    out
}

/// Build a SYN_STREAM control frame (version 3, type 1).
fn syn_stream(stream_id: u32, compressed_block: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let payload_len = 10 + compressed_block.len();
    out.push(0x80); // control bit set + version high byte (0)
    out.push(0x03); // version low (SPDY/3)
    out.extend_from_slice(&1u16.to_be_bytes()); // type SYN_STREAM
    out.push(0x01); // FLAG_FIN — half-close immediately (k8s exec stream pattern)
    out.push((payload_len >> 16) as u8);
    out.push((payload_len >> 8) as u8);
    out.push(payload_len as u8);
    out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes()); // assoc stream id
    out.push(0); // priority
    out.push(0); // slot
    out.extend_from_slice(compressed_block);
    out
}

/// Parsed control/data frame header essentials.
struct WireFrame {
    is_control: bool,
    ctrl_type: u16,
    stream_id: u32,
    payload: Vec<u8>,
}

/// Read exactly one frame off the stream (8-byte header + payload).
async fn read_frame<R: AsyncReadExt + Unpin>(s: &mut R) -> Option<WireFrame> {
    let mut head = [0u8; 8];
    s.read_exact(&mut head).await.ok()?;
    let length = ((head[5] as usize) << 16) | ((head[6] as usize) << 8) | head[7] as usize;
    let mut payload = vec![0u8; length];
    if length > 0 {
        s.read_exact(&mut payload).await.ok()?;
    }
    let is_control = head[0] & 0x80 != 0;
    if is_control {
        let ctrl_type = ((head[2] as u16) << 8) | head[3] as u16;
        Some(WireFrame {
            is_control,
            ctrl_type,
            stream_id: 0,
            payload,
        })
    } else {
        let stream_id = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) & 0x7fff_ffff;
        Some(WireFrame {
            is_control,
            ctrl_type: 0,
            stream_id,
            payload,
        })
    }
}

// ── test fixtures ────────────────────────────────────────────────────────────

struct CodeWaiter(i32);
impl ExitWaiter for CodeWaiter {
    fn wait(self: Box<Self>) -> lightr_cri_backend::Result<i32> {
        Ok(self.0)
    }
}

/// Build a throwaway, already-unlinked temp file seeded with `bytes`, rewound
/// to the start. No external temp-file crate: create under `temp_dir()` with a
/// unique name, then `remove_file` so only the open fd keeps it alive (the same
/// "anonymous handle" shape the real backend hands the streamer).
fn seeded_temp_file(bytes: &[u8]) -> std::fs::File {
    use std::io::{Seek, SeekFrom};
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let path = std::env::temp_dir().join(format!("lightr-spdy-e2e-{pid}-{n}.tmp"));
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("open temp file");
    f.write_all(bytes).expect("write stdout fixture");
    f.seek(SeekFrom::Start(0)).expect("rewind");
    // Unlink immediately; the open fd keeps the data readable until close.
    let _ = std::fs::remove_file(&path);
    f
}

/// Factory yielding a session whose stdout is a temp file preloaded with
/// `stdout_bytes` and whose waiter returns `exit_code`.
fn fixture_factory(stdout_bytes: &'static [u8], exit_code: i32) -> Arc<dyn SessionFactory> {
    Arc::new(move |_v: StreamVerb, _p: &StreamParams| {
        Ok(StreamSession {
            stdin: None,
            stdout: Some(seeded_temp_file(stdout_bytes)),
            stderr: None,
            pty_master: None,
            waiter: Box::new(CodeWaiter(exit_code)),
        })
    })
}

fn exec_params() -> StreamParams {
    StreamParams {
        container: Some("c0".into()),
        sandbox: None,
        cmd: vec!["echo".into()],
        tty: false,
        stdin: false,
        ports: vec![],
        dial_target: None,
    }
}

async fn upgrade_spdy(addr: &str, token: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST /exec/{token} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: SPDY/3.1\r\n\
         X-Stream-Protocol-Version: v4.channel.k8s.io\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.expect("write req");

    // Read until end of HTTP headers (\r\n\r\n).
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut byte))
            .await
            .expect("head timeout")
            .expect("head read");
        assert_ne!(n, 0, "EOF before 101 headers");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head_str = String::from_utf8_lossy(&head);
    assert!(
        head_str.starts_with("HTTP/1.1 101"),
        "expected 101, got: {head_str}"
    );
    s
}

/// Drive a full exec stream and return (stdout collected, error-stream bytes,
/// whether every server SYN_REPLY decompressed cleanly).
async fn run_exec_client(s: TcpStream, stdout_content_expected: usize) -> (Vec<u8>, Vec<u8>, bool) {
    let mut comp = ClientComp::new(CANON_DICT);
    let mut decomp = ClientDecomp::new(CANON_DICT);
    let (mut rd, mut wr) = tokio::io::split(s);

    // Open the error stream (id 3) then stdout (id 1), each with a streamType
    // header compressed against the SPDY dictionary.
    let err_block = comp.compress(&serialize_headers(&[("streamType", "error")]));
    wr.write_all(&syn_stream(STREAM_ERROR, &err_block))
        .await
        .expect("write err syn");
    let out_block = comp.compress(&serialize_headers(&[("streamType", "stdout")]));
    wr.write_all(&syn_stream(STREAM_STDOUT, &out_block))
        .await
        .expect("write out syn");
    wr.flush().await.expect("flush syns");
    // Half-close: signal EOF so the server's read loop ends, reaps the process
    // and delivers the v4 Status on the error stream (client-go closes its
    // write side once all input streams are opened/finished).
    wr.shutdown().await.expect("shutdown write half");

    let mut stdout_buf = Vec::new();
    let mut error_buf = Vec::new();
    let mut all_replies_ok = true;

    loop {
        let f = match tokio::time::timeout(Duration::from_secs(10), read_frame(&mut rd)).await {
            Ok(Some(f)) => f,
            Ok(None) | Err(_) => break, // EOF (server closed) or timeout
        };
        if f.is_control {
            if f.ctrl_type == 2 {
                // SYN_REPLY: stream_id(4) + compressed header block. The block
                // MUST decompress cleanly — that is the SYN_REPLY validity proof.
                if f.payload.len() >= 4 {
                    let block = &f.payload[4..];
                    if decomp.decompress(block).is_err() {
                        all_replies_ok = false;
                    }
                }
            }
            // ignore PING/SETTINGS/etc.
        } else {
            match f.stream_id {
                STREAM_STDOUT => stdout_buf.extend_from_slice(&f.payload),
                STREAM_ERROR => error_buf.extend_from_slice(&f.payload),
                _ => {}
            }
        }
        // Once we have all stdout AND the error-stream status, we can stop.
        if stdout_buf.len() >= stdout_content_expected && !error_buf.is_empty() {
            break;
        }
    }
    (stdout_buf, error_buf, all_replies_ok)
}

// ── the real end-to-end tests ────────────────────────────────────────────────

#[test]
fn client_dictionary_is_canonical() {
    // Guard: the test's own client dict must be the canonical bytes, else the
    // E2E below would be testing the wrong thing.
    assert_eq!(CANON_DICT.len(), 1423);
    assert_eq!(*CANON_DICT.last().unwrap(), b'.');
    assert_eq!(sha256_hex(CANON_DICT), CANON_DICT_SHA256);
}

#[tokio::test]
async fn spdy_exec_pumps_stdout_and_delivers_exit_status() {
    let stdout_content: &[u8] = b"hello from exec\n";
    let exit_code = 7;
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        fixture_factory(stdout_content, exit_code),
    )
    .await
    .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let s = upgrade_spdy(&addr, &token).await;
    let (stdout_buf, error_buf, replies_ok) = run_exec_client(s, stdout_content.len()).await;

    assert!(
        replies_ok,
        "server SYN_REPLY header block failed to decompress (malformed reply)"
    );
    assert_eq!(
        stdout_buf, stdout_content,
        "stdout was not pumped over the wire"
    );
    assert!(
        !error_buf.is_empty(),
        "no v4 Status delivered on the error stream"
    );
    let status: serde_json::Value =
        serde_json::from_slice(&error_buf).expect("error stream is metav1.Status JSON");
    assert_eq!(status["status"], "Failure");
    assert_eq!(status["reason"], "NonZeroExitCode");
    assert_eq!(status["details"]["causes"][0]["message"], "7");

    handle.shutdown().await;
}

#[tokio::test]
async fn spdy_exec_success_status_on_zero_exit() {
    let stdout_content: &[u8] = b"ok";
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        fixture_factory(stdout_content, 0),
    )
    .await
    .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let s = upgrade_spdy(&addr, &token).await;
    let (stdout_buf, error_buf, replies_ok) = run_exec_client(s, stdout_content.len()).await;

    assert!(replies_ok, "SYN_REPLY decompress failed");
    assert_eq!(stdout_buf, stdout_content);
    let status: serde_json::Value = serde_json::from_slice(&error_buf).expect("status json");
    assert_eq!(status["status"], "Success");

    handle.shutdown().await;
}

/// Proof the E2E catches the exact bug under repair: if the CLIENT compresses
/// SYN_STREAM against a DRIFTED dictionary (canonical + a spurious trailing
/// NUL, the critic's proposed "fix"), the server — holding the correct dict —
/// fails to decode, `streamType` never parses, and NO stdout is pumped. This is
/// the wire-break a self-round-trip cannot see. With the canonical dict on both
/// sides (the passing tests above) it works; with drift it does not.
#[tokio::test]
async fn dictionary_drift_breaks_the_wire() {
    let stdout_content: &[u8] = b"should-not-arrive";
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        fixture_factory(stdout_content, 0),
    )
    .await
    .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let mut s = upgrade_spdy(&addr, &token).await;

    // DRIFTED dict: canonical + trailing NUL (the wrong "fix").
    let mut drifted = CANON_DICT.to_vec();
    drifted.push(0x00);
    let mut comp = ClientComp::new(&drifted);

    let out_block = comp.compress(&serialize_headers(&[("streamType", "stdout")]));
    s.write_all(&syn_stream(STREAM_STDOUT, &out_block))
        .await
        .expect("write syn");
    s.flush().await.expect("flush");

    // Collect any DATA on the stdout stream for a bounded window.
    let mut stdout_buf = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), read_frame(&mut s)).await {
            Ok(Some(f)) if !f.is_control && f.stream_id == STREAM_STDOUT => {
                stdout_buf.extend_from_slice(&f.payload);
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
        if stdout_buf.len() >= stdout_content.len() {
            break;
        }
    }
    assert!(
        stdout_buf.is_empty(),
        "drifted-dict SYN_STREAM should NOT decode server-side; got {stdout_buf:?}"
    );

    handle.shutdown().await;
}

// ── dependency-free SHA-256 (FIPS 180-4), to verify the client dict bytes ────

fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bitlen = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bitlen.to_be_bytes());
    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut v = h;
        for i in 0..64 {
            let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
            let ch = (v[4] & v[5]) ^ ((!v[4]) & v[6]);
            let t1 = v[7]
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
            let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
            let t2 = s0.wrapping_add(maj);
            v[7] = v[6];
            v[6] = v[5];
            v[5] = v[4];
            v[4] = v[3].wrapping_add(t1);
            v[3] = v[2];
            v[2] = v[1];
            v[1] = v[0];
            v[0] = t1.wrapping_add(t2);
        }
        for (hi, vi) in h.iter_mut().zip(v.iter()) {
            *hi = hi.wrapping_add(*vi);
        }
    }
    let mut out = String::with_capacity(64);
    for word in h {
        out.push_str(&format!("{word:08x}"));
    }
    out
}
