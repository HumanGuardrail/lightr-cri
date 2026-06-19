//! SPDY/3.1 server: framing (`frame`), the fixed dictionary (`dict`), header
//! block (de)serialization (`headers`), the connection-scoped zlib codecs
//! (`zlib`), and the connection driver (`conn`).
//!
//! The HTTP/1.1 → SPDY upgrade handshake and the subsequent multiplexed
//! exec/attach/portforward sessions are driven over a single upgraded TCP
//! stream (`hyper::upgrade::Upgraded`, bridged through axum).

pub mod conn;
pub mod dict;
pub mod frame;
pub mod headers;
pub mod zlib;

/// The SPDY exec/attach subprotocol the server advertises (max = v4).
pub const SPDY_PROTOCOL_V4: &str = "v4.channel.k8s.io";
/// The SPDY portforward subprotocol.
pub const SPDY_PORTFORWARD: &str = "portforward.k8s.io";

/// SPDY exec/attach protocol versions the server understands, lowest→highest.
/// v5 is wire-compatible with v4 — it only adds per-stream half-close signalling
/// on top of identical v4 framing. Modern crictl/cri-tools (v1.33) offer ONLY
/// v5 even over the SPDY transport (default `--transport=spdy`), so we accept and
/// echo it, served through the v4 connection driver (matches modern containerd/CRI-O).
pub const SPDY_EXEC_PROTOCOLS: &[&str] = &[
    "channel.k8s.io",
    "v2.channel.k8s.io",
    "v3.channel.k8s.io",
    "v4.channel.k8s.io",
    "v5.channel.k8s.io",
];

/// Negotiate the SPDY exec/attach stream-protocol version from the client's
/// `X-Stream-Protocol-Version` header value (comma/space separated). Echo the
/// highest the server supports (v4 max). Returns `None` if no overlap (caller
/// replies 403 listing the supported versions).
pub fn negotiate_exec_protocol(header: &str) -> Option<&'static str> {
    let offered: Vec<&str> = header
        .split(',')
        .flat_map(|s| s.split(' '))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    // pick the highest supported that the client offered
    SPDY_EXEC_PROTOCOLS
        .iter()
        .rev()
        .find(|sv| offered.iter().any(|o| o == *sv))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_picks_v5_max() {
        assert_eq!(
            negotiate_exec_protocol("v5.channel.k8s.io,v4.channel.k8s.io,v3.channel.k8s.io"),
            Some("v5.channel.k8s.io")
        );
    }

    #[test]
    fn negotiate_v5_only() {
        // crictl v1.33 offers only v5 over SPDY → accepted (v4-compatible framing)
        assert_eq!(
            negotiate_exec_protocol("v5.channel.k8s.io"),
            Some("v5.channel.k8s.io")
        );
    }

    #[test]
    fn negotiate_lower_version() {
        assert_eq!(
            negotiate_exec_protocol("v3.channel.k8s.io"),
            Some("v3.channel.k8s.io")
        );
    }

    #[test]
    fn negotiate_space_separated() {
        assert_eq!(
            negotiate_exec_protocol("v4.channel.k8s.io v3.channel.k8s.io"),
            Some("v4.channel.k8s.io")
        );
    }

    #[test]
    fn negotiate_no_overlap() {
        assert_eq!(negotiate_exec_protocol("bogus.k8s.io"), None);
    }
}
