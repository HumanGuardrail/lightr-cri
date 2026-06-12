//! PortForward — WebSocket (legacy channel) + the shared TCP dial/pipe used by
//! both WS and SPDY paths (r1-streaming.md items 2 & 4).
//!
//! The streamer dials `StreamParams.dial_target:<port>` (contract §B: pod IP,
//! or 127.0.0.1 for host_network). WS framing: ports from the `?port=` query;
//! 2 channels per port (2i = data RW, 2i+1 = error W); the FIRST 2 bytes the
//! server writes on EACH channel are the port number as u16 LITTLE-endian.

use std::collections::HashMap;

use axum::extract::ws::{Message, WebSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Parse the `port` query parameters (repeated `port=` keys, also tolerates a
/// comma-joined single value). Returns ports in request order.
pub fn parse_ports(query: &str) -> Vec<u16> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if k != "port" && k != "ports" {
            continue;
        }
        for tok in v.split(',') {
            if let Ok(p) = tok.trim().parse::<u16>() {
                out.push(p);
            }
        }
    }
    out
}

/// Dial the target host:port for a forwarded connection.
pub async fn dial(host: &str, port: u16) -> std::io::Result<TcpStream> {
    TcpStream::connect((host, port)).await
}

/// One forwarded port pairing: the target port plus a lazily-dialed TCP stream.
struct PortConn {
    port: u16,
    stream: Option<TcpStream>,
}

/// Poll all connected TCP streams; return `(data_channel, Some(bytes))` for the
/// first with data, `(ch, None)` on EOF/err, or never resolve when none are
/// connected (the caller guards this arm so that is unreachable).
async fn read_any(
    conns: &mut HashMap<u8, PortConn>,
    bufs: &mut HashMap<u8, [u8; 8192]>,
) -> (u8, Option<Vec<u8>>) {
    for (ch, conn) in conns.iter_mut() {
        if let Some(s) = conn.stream.as_mut() {
            let buf = bufs.entry(*ch).or_insert([0u8; 8192]);
            return match s.read(buf).await {
                Ok(0) => (*ch, None),
                Ok(n) => (*ch, Some(buf[..n].to_vec())),
                Err(_) => (*ch, None),
            };
        }
    }
    std::future::pending().await
}

/// WS portforward: ports come from the query, channels are 2-per-port. We
/// support a single active port pairing per connection at a time (the common
/// crictl/kubectl case) but route by channel for any number of declared ports.
pub async fn run_ws(mut socket: WebSocket, ports: Vec<u16>, dial_host: String) {
    if ports.is_empty() {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }

    // Map data-channel index -> port pairing. We connect lazily on the first
    // data byte for that channel, matching client behaviour (it may not use
    // every declared port).
    let mut conns: HashMap<u8, PortConn> = HashMap::new();
    for (i, p) in ports.iter().enumerate() {
        let data_ch = (i * 2) as u8;
        conns.insert(
            data_ch,
            PortConn {
                port: *p,
                stream: None,
            },
        );
        // Per the protocol the server writes the port prefix on BOTH channels.
        let prefix = p.to_le_bytes().to_vec();
        let mut data_frame = vec![data_ch];
        data_frame.extend_from_slice(&prefix);
        let _ = socket.send(Message::Binary(data_frame)).await;
        let err_ch = (i * 2 + 1) as u8;
        let mut err_frame = vec![err_ch];
        err_frame.extend_from_slice(&prefix);
        let _ = socket.send(Message::Binary(err_frame)).await;
    }

    // Read loop: inbound data frames carry [channel | payload]; even channel =
    // data toward the TCP target. We pump TCP→WS inline after each connect via
    // a per-connection reader task feeding back through a channel.
    let mut read_bufs: HashMap<u8, [u8; 8192]> = HashMap::new();
    loop {
        // Drive any connected TCP streams' reads alongside the socket.
        let any_open = conns.values().any(|c| c.stream.is_some());
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Binary(frame))) => {
                        let Some((ch, payload)) = frame.split_first() else { continue; };
                        // even = data channel
                        if ch % 2 == 0 {
                            if let Some(conn) = conns.get_mut(ch) {
                                if conn.stream.is_none() {
                                    match dial(&dial_host, conn.port).await {
                                        Ok(s) => conn.stream = Some(s),
                                        Err(_) => {
                                            // signal failure on the error channel
                                            let mut ef = vec![ch + 1];
                                            ef.extend_from_slice(b"dial failed");
                                            let _ = socket.send(Message::Binary(ef)).await;
                                            continue;
                                        }
                                    }
                                }
                                if let Some(s) = conn.stream.as_mut() {
                                    if s.write_all(payload).await.is_err() {
                                        conn.stream = None;
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            // Poll connected TCP streams for data to forward back.
            (ch, payload) = read_any(&mut conns, &mut read_bufs), if any_open => {
                match payload {
                    Some(bytes) => {
                        let mut f = vec![ch];
                        f.extend_from_slice(&bytes);
                        if socket.send(Message::Binary(f)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // TCP EOF/err on this channel
                        if let Some(c) = conns.get_mut(&ch) { c.stream = None; }
                    }
                }
            }
        }
    }

    let _ = socket.send(Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ports_repeated() {
        assert_eq!(parse_ports("port=80&port=443"), vec![80, 443]);
    }

    #[test]
    fn parse_ports_comma() {
        assert_eq!(parse_ports("ports=80,443"), vec![80, 443]);
    }

    #[test]
    fn parse_ports_ignores_other_keys() {
        assert_eq!(parse_ports("foo=bar&port=22"), vec![22]);
    }

    #[test]
    fn parse_ports_empty() {
        assert_eq!(parse_ports(""), Vec::<u16>::new());
    }

    #[test]
    fn port_prefix_is_little_endian() {
        // port 443 = 0x01BB → LE bytes [0xBB, 0x01]
        assert_eq!(443u16.to_le_bytes(), [0xBB, 0x01]);
    }
}
