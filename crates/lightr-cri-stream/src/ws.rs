//! WebSocket exec/attach (≤ v5) — r1-streaming.md item 3.
//!
//! Subprotocol negotiation from the allowed set:
//!   "", channel.k8s.io, base64.channel.k8s.io,
//!   v4.channel.k8s.io, v4.base64.channel.k8s.io,
//!   v5.channel.k8s.io, v5.base64.channel.k8s.io
//! v5 is wire-identical to v4 on the stream handler (apimachinery
//! `streamProtocolV5` embeds `streamProtocolV4` and only delegates; v5's change
//! is client-side close signaling, not framing). crictl v1.33's WebSocket
//! executor (client-go `NewWebSocketExecutor`) offers ONLY `v5.channel.k8s.io`,
//! so the server MUST select v5 to complete the handshake — it is then driven
//! over the same channel framing and v4 metav1.Status exit delivery as v4.
//! Binary subprotocols frame `[channel | payload]`; base64 subprotocols send
//! an ASCII channel digit + standard-base64 payload over TEXT frames.
//!
//! On open the server writes one EMPTY message on the lowest writable channel
//! (stdout=1 if stdout requested, else stderr=2). resize JSON drives the pty.
//! On exit the v4 metav1.Status lands on the error channel (3); close 1000.

use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bridge;
use crate::channel;
use crate::status;
use crate::{SessionFactory, StreamParams, StreamVerb};

/// The WS subprotocols this server accepts, in server-preference order
/// (highest first). v5 is wire-identical to v4 (same channel framing + v4
/// metav1.Status exit), so we select it when offered and drive the v4 path —
/// crictl v1.33's WebSocket executor offers ONLY `v5.channel.k8s.io`.
pub const ALLOWED_PROTOCOLS: &[&str] = &[
    "v5.channel.k8s.io",
    "v5.base64.channel.k8s.io",
    "v4.channel.k8s.io",
    "v4.base64.channel.k8s.io",
    "channel.k8s.io",
    "base64.channel.k8s.io",
];

/// Negotiate a subprotocol from the client's offered list. Returns the chosen
/// protocol string (`""` when the client offered none / no match → bare
/// binary framing, which the empty-subprotocol case mandates).
pub fn negotiate(offered: Option<&str>) -> &'static str {
    let Some(offered) = offered else {
        return "";
    };
    for cand in ALLOWED_PROTOCOLS {
        if offered.split(',').any(|p| p.trim() == *cand) {
            return cand;
        }
    }
    ""
}

/// True when the negotiated protocol uses base64 text framing.
fn is_base64(proto: &str) -> bool {
    proto.contains("base64")
}

/// Drive an exec/attach session over an upgraded WebSocket.
pub async fn run_session(
    mut socket: WebSocket,
    proto: String,
    verb: StreamVerb,
    params: StreamParams,
    factory: Arc<dyn SessionFactory>,
) {
    let base64 = is_base64(&proto);

    // Open the backend session (blocking factory off the async path).
    let factory2 = factory.clone();
    let params2 = params.clone();
    let session = tokio::task::spawn_blocking(move || factory2.open_session(verb, &params2)).await;
    let mut session = match session {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            send_frame(&mut socket, base64, channel::ERROR, e.as_bytes()).await;
            close(&mut socket).await;
            return;
        }
        Err(_) => {
            send_frame(
                &mut socket,
                base64,
                channel::ERROR,
                b"session open panicked",
            )
            .await;
            close(&mut socket).await;
            return;
        }
    };

    // Initial empty write on the lowest writable channel (stdout, else stderr).
    let lowest = if session.stdout.is_some() {
        channel::STDOUT
    } else {
        channel::STDERR
    };
    send_frame(&mut socket, base64, lowest, b"").await;

    // Adopt the output handles. For tty, stdout carries the pty stream and
    // stderr is None (contract §B).
    let mut stdout = session.stdout.take().map(bridge::adopt);
    let mut stderr = session.stderr.take().map(bridge::adopt);
    let mut stdin = session.stdin.take().map(bridge::adopt);
    // Keep the raw pty master for TIOCSWINSZ (resize) — do NOT adopt it; we
    // only ioctl it. dup it from the output side is unnecessary: the contract
    // hands pty_master separately.
    let pty_master = session.pty_master.take();

    let mut out_buf = [0u8; 8192];
    let mut err_buf = [0u8; 8192];
    let mut stdout_open = stdout.is_some();
    let mut stderr_open = stderr.is_some();

    loop {
        tokio::select! {
            // stdout → STDOUT channel
            r = read_some(&mut stdout, &mut out_buf), if stdout_open => {
                match r {
                    Some(n) if n > 0 => {
                        send_frame(&mut socket, base64, channel::STDOUT, &out_buf[..n]).await;
                    }
                    _ => { stdout_open = false; }
                }
            }
            // stderr → STDERR channel
            r = read_some(&mut stderr, &mut err_buf), if stderr_open => {
                match r {
                    Some(n) if n > 0 => {
                        send_frame(&mut socket, base64, channel::STDERR, &err_buf[..n]).await;
                    }
                    _ => { stderr_open = false; }
                }
            }
            // client → stdin / resize
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if !handle_inbound(&data, base64, &mut stdin, &pty_master).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if !handle_inbound(text.as_bytes(), base64, &mut stdin, &pty_master).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(_))) => { /* axum auto-pongs */ }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            else => break,
        }
        if !stdout_open && !stderr_open {
            // both output streams drained; wait for process exit below
            break;
        }
    }

    // Wait for the process exit and deliver the v4 metav1.Status on ERROR.
    let waiter = session.waiter;
    let exit = tokio::task::spawn_blocking(move || waiter.wait())
        .await
        .unwrap_or(Ok(-1))
        .unwrap_or(-1);
    let status_json = status::exit_status_json(exit);
    send_frame(&mut socket, base64, channel::ERROR, &status_json).await;
    close(&mut socket).await;
}

/// Handle one inbound frame; returns false when the connection should close.
async fn handle_inbound(
    frame: &[u8],
    base64: bool,
    stdin: &mut Option<tokio::fs::File>,
    pty_master: &Option<std::fs::File>,
) -> bool {
    let (ch, payload): (u8, std::borrow::Cow<[u8]>) = if base64 {
        match channel::decode_base64(frame) {
            Some((c, p)) => (c, std::borrow::Cow::Owned(p)),
            None => return true,
        }
    } else {
        match channel::decode_binary(frame) {
            Some((c, p)) => (c, std::borrow::Cow::Borrowed(p)),
            None => return true,
        }
    };
    match ch {
        channel::STDIN => {
            if let Some(f) = stdin.as_mut() {
                if f.write_all(&payload).await.is_err() {
                    return false;
                }
                let _ = f.flush().await;
            }
        }
        channel::RESIZE => {
            if let (Some(sz), Some(pty)) = (channel::parse_resize(&payload), pty_master.as_ref()) {
                bridge::set_winsize(pty, sz.width, sz.height);
            }
        }
        _ => {}
    }
    true
}

/// Read from an optional async file into `buf`; `None` on EOF/err/absent.
async fn read_some(file: &mut Option<tokio::fs::File>, buf: &mut [u8]) -> Option<usize> {
    match file.as_mut() {
        Some(f) => f.read(buf).await.ok(),
        None => std::future::pending().await,
    }
}

/// Send a channel frame, choosing binary vs base64 framing.
async fn send_frame(socket: &mut WebSocket, base64: bool, ch: u8, payload: &[u8]) {
    let msg = if base64 {
        Message::Text(channel::encode_base64(ch, payload))
    } else {
        Message::Binary(channel::encode_binary(ch, payload))
    };
    let _ = socket.send(msg).await;
}

/// Close with code 1000 (normal), per the spec.
async fn close(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: std::borrow::Cow::Borrowed(""),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_prefers_v5_binary() {
        // v5 is wire-identical to v4; server preference selects v5 when offered.
        let got = negotiate(Some("v5.channel.k8s.io, v4.channel.k8s.io, channel.k8s.io"));
        assert_eq!(got, "v5.channel.k8s.io");
    }

    #[test]
    fn negotiate_picks_v5_alone() {
        // crictl v1.33's WebSocket executor offers ONLY v5 — must be selected
        // (returning "" would dead-end the gorilla/websocket client handshake).
        assert_eq!(negotiate(Some("v5.channel.k8s.io")), "v5.channel.k8s.io");
    }

    #[test]
    fn negotiate_falls_back_to_v4_when_no_v5() {
        let got = negotiate(Some("v4.channel.k8s.io, channel.k8s.io"));
        assert_eq!(got, "v4.channel.k8s.io");
    }

    #[test]
    fn negotiate_base64_variant() {
        assert_eq!(
            negotiate(Some("v4.base64.channel.k8s.io")),
            "v4.base64.channel.k8s.io"
        );
        assert!(is_base64("v4.base64.channel.k8s.io"));
        assert!(!is_base64("v4.channel.k8s.io"));
    }

    #[test]
    fn negotiate_none_is_empty() {
        assert_eq!(negotiate(None), "");
    }

    #[test]
    fn negotiate_unknown_is_empty() {
        assert_eq!(negotiate(Some("graphql-ws")), "");
    }
}
