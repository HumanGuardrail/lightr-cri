//! lightr-cri-stream — SPDY/WS streaming server (WP-C skeleton).
//!
//! FROZEN laws (build-spec-r1 §3 WP-C):
//! - SPDY exec/attach (negotiate v4 max), SPDY portforward (pairs, port/requestID headers, RST on dial failure)
//! - WS exec/attach ≤v4 (channel framing, initial empty write, resize JSON, close 1000)
//! - WS portforward (query ports, u16 LITTLE-endian prefix per channel)
//! - v4 metav1.Status exit delivery (bare-decimal ExitCode cause)
//! - Token registry: 8-char base64url/6 random bytes, 1-min TTL, single-use, 1000 cap, 404 on miss
//! - Server: axum on 127.0.0.1 ephemeral; handler consumes token → drives a StreamSession
//!   (contract §B) via spawn_blocking/AsyncFd bridges; sessions die with the connection (no persisted state)
//! - zlib SPDY dictionary inflate for SYN_STREAM headers; emit SYN_REPLY with valid compressed headers

// StreamSession is the §B contract type; WP-C will use it when implemented.
// Imported here to validate the backend dependency is correct.
#[allow(unused_imports)]
use lightr_cri_backend::StreamSession;

/// Opaque handle to the running stream server. Drop to shut down.
pub struct ServerHandle {
    _todo: (),
}

/// Verb for a streaming session token.
pub enum StreamVerb {
    Exec,
    Attach,
    PortForward,
}

/// Parameters encoded in a streaming token.
pub struct StreamParams {
    pub container: Option<String>,
    pub sandbox: Option<String>,
    pub cmd: Vec<String>,
    pub tty: bool,
    pub stdin: bool,
    pub ports: Vec<i32>,
}

/// Token registry: mint single-use tokens, consume them to retrieve params.
pub struct TokenRegistry {
    _todo: (),
}

impl TokenRegistry {
    pub fn new() -> Self {
        todo!("WP-C")
    }

    /// Mint a single-use token for the given verb + params. Returns an 8-char base64url token.
    pub fn mint(&self, _verb: StreamVerb, _params: StreamParams) -> String {
        todo!("WP-C")
    }

    /// Consume a token (single-use). Returns None if missing or expired.
    pub fn consume(&self, _token: &str) -> Option<(StreamVerb, StreamParams)> {
        todo!("WP-C")
    }
}

impl Default for TokenRegistry {
    fn default() -> Self {
        todo!("WP-C")
    }
}

/// Start the stream server on the given address.
/// Returns a ServerHandle that shuts down the server when dropped.
pub async fn serve(_addr: std::net::SocketAddr) -> std::io::Result<ServerHandle> {
    todo!("WP-C")
}
