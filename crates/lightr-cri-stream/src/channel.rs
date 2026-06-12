//! Channel constants + framing shared by the WS and SPDY exec/attach paths
//! (r1-streaming.md items 3 & 5).
//!
//! Channel bytes: 0 stdin, 1 stdout, 2 stderr, 3 error, 4 resize.
//! WS binary frame = `[channel_byte | payload]`. The `base64.*` subprotocol
//! variants send the channel as an ASCII DIGIT ('0'..'4') followed by the
//! base64 of the payload, over TEXT frames.

use serde::Deserialize;

use crate::b64;

pub const STDIN: u8 = 0;
pub const STDOUT: u8 = 1;
pub const STDERR: u8 = 2;
pub const ERROR: u8 = 3;
pub const RESIZE: u8 = 4;

/// Resize channel payload: concatenated JSON `{"Width":u16,"Height":u16}`.
/// client-go uses capitalized field names (`remotecommand.TerminalSize`
/// marshals `Width`/`Height`).
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct TerminalSize {
    #[serde(rename = "Width")]
    pub width: u16,
    #[serde(rename = "Height")]
    pub height: u16,
}

/// Parse the resize channel payload. The channel may carry one or more
/// concatenated JSON objects (client-go streams them back-to-back without a
/// delimiter); return the LAST one (the current size).
pub fn parse_resize(payload: &[u8]) -> Option<TerminalSize> {
    let mut de = serde_json::Deserializer::from_slice(payload).into_iter::<TerminalSize>();
    let mut last = None;
    while let Some(Ok(sz)) = de.next() {
        last = Some(sz);
    }
    last
}

/// Encode an outbound frame for the binary subprotocols
/// (`""`, `channel.k8s.io`, `v4.channel.k8s.io`): `[channel | payload]`.
pub fn encode_binary(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(channel);
    buf.extend_from_slice(payload);
    buf
}

/// Decode an inbound binary frame → `(channel, payload)`. `None` if empty.
pub fn decode_binary(frame: &[u8]) -> Option<(u8, &[u8])> {
    frame.split_first().map(|(c, rest)| (*c, rest))
}

/// Encode an outbound frame for the base64 subprotocols
/// (`base64.channel.k8s.io`, `v4.base64.channel.k8s.io`): an ASCII digit for
/// the channel followed by standard-base64 of the payload, as TEXT.
pub fn encode_base64(channel: u8, payload: &[u8]) -> String {
    debug_assert!(channel <= 9);
    let mut s = String::with_capacity(1 + payload.len() * 4 / 3 + 4);
    s.push((b'0' + channel) as char);
    s.push_str(&b64::encode_std(payload));
    s
}

/// Decode an inbound base64 text frame → `(channel, payload)`. The first byte
/// is the ASCII channel digit, the remainder is standard base64.
pub fn decode_base64(frame: &[u8]) -> Option<(u8, Vec<u8>)> {
    let (first, rest) = frame.split_first()?;
    if !first.is_ascii_digit() {
        return None;
    }
    let channel = first - b'0';
    let payload = b64::decode_std(rest)?;
    Some((channel, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_roundtrip() {
        let f = encode_binary(STDOUT, b"hello");
        assert_eq!(f, vec![1, b'h', b'e', b'l', b'l', b'o']);
        let (ch, p) = decode_binary(&f).unwrap();
        assert_eq!(ch, STDOUT);
        assert_eq!(p, b"hello");
    }

    #[test]
    fn binary_empty_initial_write() {
        // initial empty write on a channel: just the channel byte
        let f = encode_binary(STDOUT, b"");
        assert_eq!(f, vec![1]);
        let (ch, p) = decode_binary(&f).unwrap();
        assert_eq!(ch, STDOUT);
        assert!(p.is_empty());
    }

    #[test]
    fn binary_decode_empty_is_none() {
        assert!(decode_binary(&[]).is_none());
    }

    #[test]
    fn base64_roundtrip() {
        let f = encode_base64(STDIN, b"foo");
        assert_eq!(f, "0Zm9v");
        let (ch, p) = decode_base64(f.as_bytes()).unwrap();
        assert_eq!(ch, STDIN);
        assert_eq!(p, b"foo");
    }

    #[test]
    fn base64_decode_rejects_non_digit_channel() {
        assert!(decode_base64(b"xZm9v").is_none());
    }

    #[test]
    fn resize_parse_single() {
        let sz = parse_resize(br#"{"Width":80,"Height":24}"#).unwrap();
        assert_eq!(
            sz,
            TerminalSize {
                width: 80,
                height: 24
            }
        );
    }

    #[test]
    fn resize_parse_concatenated_returns_last() {
        let payload = br#"{"Width":80,"Height":24}{"Width":120,"Height":40}"#;
        let sz = parse_resize(payload).unwrap();
        assert_eq!(
            sz,
            TerminalSize {
                width: 120,
                height: 40
            }
        );
    }

    #[test]
    fn resize_parse_garbage_is_none() {
        assert!(parse_resize(b"not json").is_none());
    }
}
