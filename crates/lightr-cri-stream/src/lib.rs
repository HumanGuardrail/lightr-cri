//! lightr-cri-stream — SPDY/3.1 + WebSocket streaming server (WP-C).
//!
//! FROZEN laws (build-spec-r1 §3 WP-C / docs/research/r1-streaming.md):
//! - SPDY exec/attach (negotiate v4 max), SPDY portforward (pairs, port/requestID headers, RST on dial failure)
//! - WS exec/attach ≤v4 (channel framing, initial empty write, resize JSON, close 1000)
//! - WS portforward (query ports, u16 LITTLE-endian prefix per channel)
//! - v4 metav1.Status exit delivery (bare-decimal ExitCode cause)
//! - Token registry: 8-char base64url/6 random bytes, 1-min TTL, single-use, 1000 cap, 404 on miss
//! - Server: axum on 127.0.0.1 ephemeral; handler consumes token → drives a StreamSession
//!   (contract §B) via tokio File bridges; sessions die with the connection (no persisted state)
//! - zlib SPDY dictionary (de)compression for SYN_STREAM/SYN_REPLY header blocks (real dictionary path)

mod b64;
mod bridge;
mod channel;
mod portforward;
mod server;
mod spdy;
mod status;
mod token;
mod ws;

use std::sync::Arc;

pub use lightr_cri_backend::StreamSession;
pub use token::TokenRegistry;

/// Verb for a streaming session token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamVerb {
    Exec,
    Attach,
    PortForward,
}

impl StreamVerb {
    /// URL path segment for this verb (mint URL: `/<verb>/<token>`).
    pub fn path(self) -> &'static str {
        match self {
            StreamVerb::Exec => "exec",
            StreamVerb::Attach => "attach",
            StreamVerb::PortForward => "portforward",
        }
    }
}

/// Parameters encoded in a streaming token (cached server-side under it).
#[derive(Clone, Debug)]
pub struct StreamParams {
    pub container: Option<String>,
    pub sandbox: Option<String>,
    pub cmd: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    /// portforward: requested ports (informational; the wire carries the port).
    pub ports: Vec<i32>,
    /// portforward dial host (pod IP or `127.0.0.1`); the streamer connects to
    /// `<dial_target>:<port>` where `<port>` comes from the SPDY/WS stream.
    pub dial_target: Option<String>,
    /// portforward sandbox netns path (contract §B, AMENDED 2026-06-19). When
    /// `Some`, the streamer `setns(CLONE_NEWNET)` into this path on a dedicated
    /// thread and dials `127.0.0.1:<port>` INSIDE the sandbox netns. `None` =
    /// host_network sandbox → keep the host-netns dial of `dial_target`.
    pub netns_path: Option<String>,
}

/// The shell's factory: opens a live exec/attach I/O session when a token is
/// consumed. PortForward never calls this — the streamer dials
/// `StreamParams.dial_target` itself (contract §B).
///
/// Object-safe so the shell (WP-D) can hand `FakeBackend.open_exec` /
/// `open_attach` behind an `Arc<dyn SessionFactory>` without leaking backend
/// types into this crate. A bare closure also satisfies the trait (blanket
/// impl below) so tests can pass an inline factory.
pub trait SessionFactory: Send + Sync + 'static {
    /// Open the exec/attach session for `verb` (`Exec` or `Attach`). Errors
    /// are surfaced to the client as a stream error message.
    fn open_session(
        &self,
        verb: StreamVerb,
        params: &StreamParams,
    ) -> Result<StreamSession, String>;
}

impl<F> SessionFactory for F
where
    F: Fn(StreamVerb, &StreamParams) -> Result<StreamSession, String> + Send + Sync + 'static,
{
    fn open_session(
        &self,
        verb: StreamVerb,
        params: &StreamParams,
    ) -> Result<StreamSession, String> {
        (self)(verb, params)
    }
}

/// Handle to the running stream server. Holds the bound base URL and a
/// shutdown trigger; dropping it (or calling [`ServerHandle::shutdown`]) stops
/// the server. The token registry is shared so the shell mints against the
/// same instance the server consumes from.
pub struct ServerHandle {
    base_url: String,
    registry: Arc<TokenRegistry>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl ServerHandle {
    /// Bound base URL, e.g. `http://127.0.0.1:54321` (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The shared token registry. Mint here; the server consumes from it.
    pub fn registry(&self) -> &Arc<TokenRegistry> {
        &self.registry
    }

    /// Build the absolute streaming URL for a freshly minted token:
    /// `http://127.0.0.1:<port>/<verb>/<token>`.
    pub fn stream_url(&self, verb: StreamVerb, token: &str) -> String {
        format!("{}/{}/{}", self.base_url, verb.path(), token)
    }

    /// Trigger graceful shutdown and await the server task.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Start the stream server on `addr` (use port 0 for an ephemeral 127.0.0.1
/// port). `factory` opens exec/attach sessions on demand. Returns a
/// [`ServerHandle`] bound to the actual address.
pub async fn serve(
    addr: std::net::SocketAddr,
    factory: Arc<dyn SessionFactory>,
) -> std::io::Result<ServerHandle> {
    server::serve(addr, factory).await
}
