//! SPDY/3.1 connection driver for exec/attach and portforward.
//!
//! Runs over a single upgraded byte stream. The server is passive: the client
//! opens streams (SYN_STREAM); the server SYN_REPLYs each, then multiplexes
//! DATA frames. Header blocks use the connection-scoped zlib codecs with the
//! SPDY dictionary (`zlib`/`dict`).
//!
//! Exec/attach: streams identified by the `streamType` header
//! (stdin|stdout|stderr|error|resize) drive the `StreamSession`. The v4
//! `metav1.Status` exit lands on the error stream.
//! PortForward: stream PAIRS (`streamType` data|error, `port`, `requestID`)
//! dial `dial_target:port` and pipe; RST both on dial failure.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use super::frame::{self, Frame, ParseResult};
use super::headers::{self, get};
use super::zlib::{HeaderCompressor, HeaderDecompressor};
use crate::bridge;
use crate::channel as ch;
use crate::status;
use crate::{SessionFactory, StreamParams, StreamVerb};

/// Shared write side: the upgraded stream + the (server→client) header
/// compressor, behind one mutex so frame writes are atomic.
struct Writer<W> {
    out: W,
    comp: HeaderCompressor,
}

impl<W: AsyncWrite + Unpin> Writer<W> {
    async fn write_frame(&mut self, f: &Frame) -> std::io::Result<()> {
        let bytes = f.serialize();
        self.out.write_all(&bytes).await?;
        self.out.flush().await
    }

    /// SYN_REPLY with an (empty) compressed header block — acknowledges a
    /// stream the client opened.
    async fn syn_reply(&mut self, stream_id: u32) -> std::io::Result<()> {
        let block = headers::serialize_headers(&[]);
        let compressed = self.comp.compress(&block);
        self.write_frame(&Frame::SynReply {
            stream_id,
            flags: 0,
            header_block: compressed,
        })
        .await
    }

    async fn data(&mut self, stream_id: u32, flags: u8, data: Vec<u8>) -> std::io::Result<()> {
        self.write_frame(&Frame::Data {
            stream_id,
            flags,
            data,
        })
        .await
    }

    async fn rst(&mut self, stream_id: u32, status: u32) -> std::io::Result<()> {
        self.write_frame(&Frame::RstStream { stream_id, status })
            .await
    }
}

/// Read one full frame from `buf`/`stream`, growing `buf` as needed.
async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<Frame>> {
    loop {
        match frame::parse_frame(buf) {
            ParseResult::Frame(f, n) => {
                buf.drain(..n);
                return Ok(Some(f));
            }
            ParseResult::Error(e) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
            ParseResult::NeedMore => {
                let mut tmp = [0u8; 16384];
                let n = stream.read(&mut tmp).await?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

/// Drive an exec/attach SPDY session.
pub async fn run_exec<S>(
    stream: S,
    verb: StreamVerb,
    params: StreamParams,
    factory: Arc<dyn SessionFactory>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(Writer {
        out: wr,
        comp: HeaderCompressor::new(),
    }));
    let mut decomp = HeaderDecompressor::new();
    let mut buf = Vec::with_capacity(16384);

    // Open the session up front (blocking factory off the async path).
    let f2 = factory.clone();
    let p2 = params.clone();
    let session = match tokio::task::spawn_blocking(move || f2.open_session(verb, &p2)).await {
        Ok(Ok(s)) => Some(s),
        _ => None,
    };
    let mut session = session;

    // stream_id → role
    let mut roles: HashMap<u32, u8> = HashMap::new();
    // role → stream_id (for output routing)
    let mut by_role: HashMap<u8, u32> = HashMap::new();

    // output pump tasks spawned once we know stdout/stderr stream ids
    let mut stdin = session
        .as_mut()
        .and_then(|s| s.stdin.take())
        .map(bridge::adopt);
    let pty_master = session.as_mut().and_then(|s| s.pty_master.take());
    // hold raw output files until their stream is opened
    let mut stdout_raw = session.as_mut().and_then(|s| s.stdout.take());
    let mut stderr_raw = session.as_mut().and_then(|s| s.stderr.take());
    let waiter = session.map(|s| s.waiter);

    let mut output_tasks = Vec::new();

    loop {
        let f = match read_frame(&mut rd, &mut buf).await {
            Ok(Some(f)) => f,
            Ok(None) | Err(_) => break,
        };
        match f {
            Frame::SynStream {
                stream_id,
                header_block,
                ..
            } => {
                let hdr = decomp
                    .decompress(&header_block)
                    .ok()
                    .and_then(|b| headers::deserialize_headers(&b))
                    .unwrap_or_default();
                let role = match get(&hdr, "streamType") {
                    Some("stdin") => ch::STDIN,
                    Some("stdout") => ch::STDOUT,
                    Some("stderr") => ch::STDERR,
                    Some("error") => ch::ERROR,
                    Some("resize") => ch::RESIZE,
                    _ => 0xff,
                };
                {
                    let mut w = writer.lock().await;
                    let _ = w.syn_reply(stream_id).await;
                }
                roles.insert(stream_id, role);
                by_role.insert(role, stream_id);

                // When an output stream opens, spawn its pump.
                if role == ch::STDOUT {
                    if let Some(file) = stdout_raw.take() {
                        output_tasks.push(spawn_output(
                            writer.clone(),
                            stream_id,
                            bridge::adopt(file),
                        ));
                    }
                } else if role == ch::STDERR {
                    if let Some(file) = stderr_raw.take() {
                        output_tasks.push(spawn_output(
                            writer.clone(),
                            stream_id,
                            bridge::adopt(file),
                        ));
                    }
                }
            }
            Frame::Data {
                stream_id,
                flags,
                data,
            } => {
                match roles.get(&stream_id).copied() {
                    Some(ch::STDIN) => {
                        if let Some(f) = stdin.as_mut() {
                            let _ = f.write_all(&data).await;
                            let _ = f.flush().await;
                        }
                    }
                    Some(ch::RESIZE) => {
                        if let (Some(sz), Some(pty)) =
                            (ch::parse_resize(&data), pty_master.as_ref())
                        {
                            bridge::set_winsize(pty, sz.width, sz.height);
                        }
                    }
                    _ => {}
                }
                if flags & frame::FLAG_FIN != 0 {
                    // client half-closed this stream
                    if roles.get(&stream_id).copied() == Some(ch::STDIN) {
                        // drop stdin so the process sees EOF
                        stdin = None;
                    }
                }
            }
            Frame::Ping { id } => {
                let mut w = writer.lock().await;
                let _ = w.write_frame(&Frame::Ping { id }).await;
            }
            Frame::RstStream { .. } | Frame::GoAway { .. } => break,
            Frame::Settings { .. } | Frame::WindowUpdate { .. } | Frame::Headers { .. } => {}
            Frame::SynReply { .. } => {}
        }
    }

    // Reap the process and emit the v4 Status on the error stream.
    if let Some(waiter) = waiter {
        let exit = tokio::task::spawn_blocking(move || waiter.wait())
            .await
            .unwrap_or(Ok(-1))
            .unwrap_or(-1);
        if let Some(err_id) = by_role.get(&ch::ERROR).copied() {
            let json = status::exit_status_json(exit);
            let mut w = writer.lock().await;
            let _ = w.data(err_id, frame::FLAG_FIN, json).await;
        }
    }
    for t in output_tasks {
        t.abort();
    }
}

/// Pump a session output file → DATA frames on `stream_id` until EOF.
fn spawn_output<W>(
    writer: Arc<Mutex<Writer<W>>>,
    stream_id: u32,
    mut file: tokio::fs::File,
) -> tokio::task::JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match file.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let mut w = writer.lock().await;
                    if w.data(stream_id, 0, buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

/// Drive a SPDY portforward session. Streams arrive in PAIRS keyed by
/// `requestID`; the `data` stream carries bytes to/from the dialed TCP socket,
/// the `error` stream carries plaintext failures. RST both on dial failure.
pub async fn run_portforward<S>(stream: S, dial_host: String)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(Writer {
        out: wr,
        comp: HeaderCompressor::new(),
    }));
    let mut decomp = HeaderDecompressor::new();
    let mut buf = Vec::with_capacity(16384);

    struct Pair {
        port: u16,
        data_stream: Option<u32>,
        error_stream: Option<u32>,
        tcp: Option<Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>,
    }
    // requestID → pair
    let mut pairs: HashMap<String, Pair> = HashMap::new();
    // data stream_id → requestID (route DATA frames)
    let mut data_route: HashMap<u32, String> = HashMap::new();
    let mut tcp_tasks = Vec::new();

    loop {
        let f = match read_frame(&mut rd, &mut buf).await {
            Ok(Some(f)) => f,
            Ok(None) | Err(_) => break,
        };
        match f {
            Frame::SynStream {
                stream_id,
                header_block,
                ..
            } => {
                let hdr = decomp
                    .decompress(&header_block)
                    .ok()
                    .and_then(|b| headers::deserialize_headers(&b))
                    .unwrap_or_default();
                let stype = get(&hdr, "streamType").unwrap_or("").to_string();
                let port: u16 = get(&hdr, "port").and_then(|p| p.parse().ok()).unwrap_or(0);
                // requestID may be absent — fall back to the port as the key.
                let req_id = get(&hdr, "requestID")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("port-{port}"));

                {
                    let mut w = writer.lock().await;
                    let _ = w.syn_reply(stream_id).await;
                }

                let entry = pairs.entry(req_id.clone()).or_insert(Pair {
                    port,
                    data_stream: None,
                    error_stream: None,
                    tcp: None,
                });
                if entry.port == 0 {
                    entry.port = port;
                }
                if stype == "error" {
                    entry.error_stream = Some(stream_id);
                } else {
                    // default/"data" stream
                    entry.data_stream = Some(stream_id);
                    data_route.insert(stream_id, req_id.clone());
                    // Dial now and start the TCP→SPDY pump.
                    match TcpStream::connect((dial_host.as_str(), entry.port)).await {
                        Ok(sock) => {
                            let (rd_half, wr_half) = sock.into_split();
                            entry.tcp = Some(Arc::new(Mutex::new(wr_half)));
                            tcp_tasks.push(spawn_tcp_to_spdy(writer.clone(), stream_id, rd_half));
                        }
                        Err(_) => {
                            let mut w = writer.lock().await;
                            if let Some(eid) = entry.error_stream {
                                let _ = w.data(eid, frame::FLAG_FIN, b"dial failed".to_vec()).await;
                            }
                            let _ = w.rst(stream_id, frame::RST_REFUSED_STREAM).await;
                            if let Some(eid) = entry.error_stream {
                                let _ = w.rst(eid, frame::RST_REFUSED_STREAM).await;
                            }
                        }
                    }
                }
            }
            Frame::Data {
                stream_id, data, ..
            } => {
                if let Some(req) = data_route.get(&stream_id) {
                    if let Some(pair) = pairs.get(req) {
                        if let Some(tcp) = pair.tcp.clone() {
                            let mut g = tcp.lock().await;
                            let _ = g.write_all(&data).await;
                            let _ = g.flush().await;
                        }
                    }
                }
            }
            Frame::Ping { id } => {
                let mut w = writer.lock().await;
                let _ = w.write_frame(&Frame::Ping { id }).await;
            }
            Frame::RstStream { .. } | Frame::GoAway { .. } => break,
            _ => {}
        }
    }
    for t in tcp_tasks {
        t.abort();
    }
}

/// Pump the TCP read half → DATA frames on the data stream.
fn spawn_tcp_to_spdy<W>(
    writer: Arc<Mutex<Writer<W>>>,
    stream_id: u32,
    mut rd: tokio::net::tcp::OwnedReadHalf,
) -> tokio::task::JoinHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let mut w = writer.lock().await;
                    let _ = w.data(stream_id, frame::FLAG_FIN, Vec::new()).await;
                    break;
                }
                Ok(n) => {
                    let mut w = writer.lock().await;
                    if w.data(stream_id, 0, buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}
