//! axum HTTP server: ephemeral 127.0.0.1 bind, token-gated streaming routes,
//! and dispatch between the WebSocket and SPDY/3.1 upgrade paths.
//!
//! `/exec/:token`, `/attach/:token`, `/portforward/:token`: consume the
//! single-use token (404 on miss/expiry/reuse), then UPGRADE. The `Upgrade`
//! header selects the protocol — `websocket` → axum WS; `SPDY/3.1` → manual
//! 101 handshake + `hyper::upgrade::on` connection takeover.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use axum::Router;
use hyper_util::rt::TokioIo;

use crate::{portforward, spdy, ws};
use crate::{ServerHandle, SessionFactory, StreamParams, StreamVerb, TokenRegistry};

#[derive(Clone)]
struct AppState {
    registry: Arc<TokenRegistry>,
    factory: Arc<dyn SessionFactory>,
}

pub async fn serve(
    addr: SocketAddr,
    factory: Arc<dyn SessionFactory>,
) -> std::io::Result<ServerHandle> {
    let registry = Arc::new(TokenRegistry::new());
    let state = AppState {
        registry: registry.clone(),
        factory,
    };

    let app = Router::new()
        .route("/exec/:token", get(exec_attach).post(exec_attach))
        .route("/attach/:token", get(exec_attach).post(exec_attach))
        .route("/portforward/:token", any(portforward_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let base_url = format!("http://{local}");

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await;
    });

    Ok(ServerHandle {
        base_url,
        registry,
        shutdown: Some(tx),
        join: Some(join),
    })
}

/// Is this an `Upgrade: SPDY/3.1` request?
fn wants_spdy(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("SPDY/3.1"))
        .unwrap_or(false)
}

/// exec/attach route handler. Picks the verb from the request path.
async fn exec_attach(
    State(state): State<AppState>,
    Path(token): Path<String>,
    ws: Option<WebSocketUpgrade>,
    req: Request<Body>,
) -> Response<Body> {
    let path_verb = if req.uri().path().starts_with("/attach/") {
        StreamVerb::Attach
    } else {
        StreamVerb::Exec
    };

    // Consume the single-use token first (404 on miss/expiry/reuse).
    let Some((verb, params)) = state.registry.consume(&token) else {
        return not_found();
    };
    // The token's verb is authoritative; the path must agree.
    if verb != path_verb {
        return not_found();
    }

    let headers = req.headers().clone();
    if wants_spdy(&headers) {
        return spdy_exec_upgrade(req, verb, params, state.factory.clone());
    }

    // WebSocket path.
    if let Some(ws) = ws {
        let proto = ws::negotiate(
            headers
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok()),
        );
        let proto_owned = proto.to_string();
        let factory = state.factory.clone();
        let upgrade = if proto.is_empty() {
            ws
        } else {
            ws.protocols([proto])
        };
        return upgrade
            .on_upgrade(move |socket| async move {
                ws::run_session(socket, proto_owned, verb, params, factory).await;
            })
            .into_response();
    }

    bad_request("expected websocket or SPDY/3.1 upgrade")
}

/// portforward route handler.
async fn portforward_handler(
    State(state): State<AppState>,
    Path(token): Path<String>,
    ws: Option<WebSocketUpgrade>,
    req: Request<Body>,
) -> Response<Body> {
    let Some((verb, params)) = state.registry.consume(&token) else {
        return not_found();
    };
    if verb != StreamVerb::PortForward {
        return not_found();
    }

    let headers = req.headers().clone();
    let dial_host = params
        .dial_target
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());

    if wants_spdy(&headers) {
        return spdy_portforward_upgrade(req, dial_host);
    }

    if let Some(ws) = ws {
        let query = req.uri().query().unwrap_or("").to_string();
        let ports = portforward::parse_ports(&query);
        // WS portforward subprotocols are negotiated like exec (binary
        // channel); k8s uses "portforward.k8s.io" / "v4.portforward.k8s.io".
        let proto = headers
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .map(pick_pf_protocol)
            .unwrap_or("");
        let upgrade = if proto.is_empty() {
            ws
        } else {
            ws.protocols([proto])
        };
        return upgrade
            .on_upgrade(move |socket| async move {
                portforward::run_ws(socket, ports, dial_host).await;
            })
            .into_response();
    }

    bad_request("expected websocket or SPDY/3.1 upgrade")
}

/// Pick the WS portforward subprotocol the client offered (echo it back).
fn pick_pf_protocol(offered: &str) -> &'static str {
    const ALLOWED: &[&str] = &["v4.portforward.k8s.io", "portforward.k8s.io"];
    for cand in ALLOWED {
        if offered.split(',').any(|p| p.trim() == *cand) {
            return cand;
        }
    }
    ""
}

/// Manual SPDY/3.1 upgrade: negotiate the stream-protocol version, return 101,
/// then take the connection over for the SPDY exec/attach driver.
fn spdy_exec_upgrade(
    mut req: Request<Body>,
    verb: StreamVerb,
    params: StreamParams,
    factory: Arc<dyn SessionFactory>,
) -> Response<Body> {
    let proto_hdr = req
        .headers()
        .get("x-stream-protocol-version")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let negotiated = match proto_hdr.as_deref() {
        Some(h) => match spdy::negotiate_exec_protocol(h) {
            Some(p) => p,
            None => return spdy_protocol_mismatch(spdy::SPDY_EXEC_PROTOCOLS),
        },
        // No version header: default to the server max (v4), matching the
        // reference library's tolerance.
        None => spdy::SPDY_PROTOCOL_V4,
    };

    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        if let Ok(upgraded) = on_upgrade.await {
            let io = TokioIo::new(upgraded);
            spdy::conn::run_exec(io, verb, params, factory).await;
        }
    });

    build_101_spdy(Some(negotiated))
}

fn spdy_portforward_upgrade(mut req: Request<Body>, dial_host: String) -> Response<Body> {
    // portforward negotiates "portforward.k8s.io" via X-Stream-Protocol-Version.
    let ok = req
        .headers()
        .get("x-stream-protocol-version")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.split(',').any(|p| p.trim() == spdy::SPDY_PORTFORWARD))
        .unwrap_or(true); // tolerate absence
    if !ok {
        return spdy_protocol_mismatch(&[spdy::SPDY_PORTFORWARD]);
    }

    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        if let Ok(upgraded) = on_upgrade.await {
            let io = TokioIo::new(upgraded);
            spdy::conn::run_portforward(io, dial_host).await;
        }
    });

    build_101_spdy(Some(spdy::SPDY_PORTFORWARD))
}

/// Build the `101 Switching Protocols` response advertising SPDY/3.1.
fn build_101_spdy(proto: Option<&str>) -> Response<Body> {
    let mut builder = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "SPDY/3.1");
    if let Some(p) = proto {
        builder = builder.header("X-Stream-Protocol-Version", p);
    }
    builder.body(Body::empty()).expect("101 response builds")
}

/// 403 listing the protocol versions the server supports (handshake mismatch).
fn spdy_protocol_mismatch(versions: &[&str]) -> Response<Body> {
    let mut builder = Response::builder().status(StatusCode::FORBIDDEN);
    for v in versions {
        builder = builder.header("X-Stream-Protocol-Version", *v);
    }
    builder
        .body(Body::from("unsupported stream protocol version"))
        .expect("403 response builds")
}

fn not_found() -> Response<Body> {
    (StatusCode::NOT_FOUND, "token not found").into_response()
}

fn bad_request(msg: &'static str) -> Response<Body> {
    (StatusCode::BAD_REQUEST, msg).into_response()
}
