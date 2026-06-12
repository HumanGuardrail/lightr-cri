//! Integration tests for the HTTP surface: token 404, the SPDY/3.1 upgrade
//! handshake (101 + protocol echo), the 403 protocol-mismatch path, and the
//! ServerHandle URL law. These use a raw TCP client (no WS/SPDY client crate
//! of record); the full wire E2E against a real client is WP-F (critest).

use std::sync::Arc;

use lightr_cri_backend::{ExitWaiter, StreamSession};
use lightr_cri_stream::{serve, SessionFactory, StreamParams, StreamVerb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct NullWaiter;
impl ExitWaiter for NullWaiter {
    fn wait(self: Box<Self>) -> lightr_cri_backend::Result<i32> {
        Ok(0)
    }
}

/// A factory that opens an empty session (no real process).
fn null_factory() -> Arc<dyn SessionFactory> {
    Arc::new(|_verb: StreamVerb, _p: &StreamParams| {
        Ok(StreamSession {
            stdin: None,
            stdout: None,
            stderr: None,
            pty_master: None,
            waiter: Box::new(NullWaiter),
        })
    })
}

fn exec_params() -> StreamParams {
    StreamParams {
        container: Some("c0".into()),
        sandbox: None,
        cmd: vec!["echo".into()],
        tty: false,
        stdin: true,
        ports: vec![],
        dial_target: None,
    }
}

async fn read_response_head(stream: &mut TcpStream) -> String {
    // read until the end of headers (\r\n\r\n) or EOF
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut tmp))
            .await
            .expect("read timeout")
            .expect("read");
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn unknown_token_is_404() {
    let handle = serve("127.0.0.1:0".parse().unwrap(), null_factory())
        .await
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let mut s = TcpStream::connect(&addr).await.unwrap();
    let req = "GET /exec/deadbeef HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    s.write_all(req.as_bytes()).await.unwrap();
    let head = read_response_head(&mut s).await;
    assert!(head.starts_with("HTTP/1.1 404"), "got: {head}");

    handle.shutdown().await;
}

#[tokio::test]
async fn base_url_and_stream_url_law() {
    let handle = serve("127.0.0.1:0".parse().unwrap(), null_factory())
        .await
        .unwrap();
    assert!(handle.base_url().starts_with("http://127.0.0.1:"));
    let url = handle.stream_url(StreamVerb::Exec, "tok12345");
    assert!(url.ends_with("/exec/tok12345"));
    assert!(url.starts_with("http://127.0.0.1:"));
    handle.shutdown().await;
}

#[tokio::test]
async fn spdy_upgrade_handshake_101_and_protocol_echo() {
    let handle = serve("127.0.0.1:0".parse().unwrap(), null_factory())
        .await
        .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let mut s = TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "POST /exec/{token} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: SPDY/3.1\r\n\
         X-Stream-Protocol-Version: v5.channel.k8s.io, v4.channel.k8s.io\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let head = read_response_head(&mut s).await;
    assert!(head.starts_with("HTTP/1.1 101"), "got: {head}");
    let lower = head.to_lowercase();
    assert!(lower.contains("upgrade: spdy/3.1"), "got: {head}");
    // server max is v4 (never v5); hyper emits header names lowercased
    assert!(
        lower.contains("x-stream-protocol-version: v4.channel.k8s.io"),
        "got: {head}"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn spdy_protocol_mismatch_is_403() {
    let handle = serve("127.0.0.1:0".parse().unwrap(), null_factory())
        .await
        .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    let mut s = TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "POST /exec/{token} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: SPDY/3.1\r\n\
         X-Stream-Protocol-Version: v5.channel.k8s.io\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let head = read_response_head(&mut s).await;
    assert!(head.starts_with("HTTP/1.1 403"), "got: {head}");
    // 403 lists the supported versions (header names come back lowercased)
    assert!(
        head.to_lowercase()
            .contains("x-stream-protocol-version: v4.channel.k8s.io"),
        "got: {head}"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn token_is_single_use_second_request_404() {
    let handle = serve("127.0.0.1:0".parse().unwrap(), null_factory())
        .await
        .unwrap();
    let token = handle
        .registry()
        .mint(StreamVerb::Exec, exec_params())
        .unwrap();
    let addr = handle.base_url().trim_start_matches("http://").to_string();

    // first use: consumes the token (SPDY 101)
    {
        let mut s = TcpStream::connect(&addr).await.unwrap();
        let req = format!(
            "POST /exec/{token} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n\
             Upgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: v4.channel.k8s.io\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let head = read_response_head(&mut s).await;
        assert!(head.starts_with("HTTP/1.1 101"), "first: {head}");
    }
    // second use: token already consumed → 404
    {
        let mut s = TcpStream::connect(&addr).await.unwrap();
        let req =
            format!("POST /exec/{token} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let head = read_response_head(&mut s).await;
        assert!(head.starts_with("HTTP/1.1 404"), "second: {head}");
    }

    handle.shutdown().await;
}
