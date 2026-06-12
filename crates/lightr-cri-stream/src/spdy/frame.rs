//! SPDY/3.1 frame parsing and serialization (the wire framing only — header
//! block compression lives in `zlib`/`headers`).
//!
//! Control frame (8-byte header):
//! ```text
//!   bit31=1 | version(15) | type(16)
//!   flags(8) | length(24)
//!   ...payload (length bytes)...
//! ```
//! Data frame (8-byte header):
//! ```text
//!   bit31=0 | stream_id(31)
//!   flags(8) | length(24)
//!   ...payload (length bytes)...
//! ```

pub const SPDY_VERSION: u16 = 3;

// Control frame types.
pub const TYPE_SYN_STREAM: u16 = 1;
pub const TYPE_SYN_REPLY: u16 = 2;
pub const TYPE_RST_STREAM: u16 = 3;
pub const TYPE_SETTINGS: u16 = 4;
pub const TYPE_PING: u16 = 6;
pub const TYPE_GOAWAY: u16 = 7;
pub const TYPE_HEADERS: u16 = 8;
pub const TYPE_WINDOW_UPDATE: u16 = 9;

// Flags.
pub const FLAG_FIN: u8 = 0x01;
#[allow(dead_code)] // protocol completeness
pub const FLAG_UNIDIRECTIONAL: u8 = 0x02;

// RST_STREAM status codes (the documented set; we emit REFUSED_STREAM on
// portforward dial failure, the rest round out the protocol surface).
#[allow(dead_code)]
pub const RST_PROTOCOL_ERROR: u32 = 1;
pub const RST_REFUSED_STREAM: u32 = 3;
#[allow(dead_code)]
pub const RST_CANCEL: u32 = 5;
#[allow(dead_code)]
pub const RST_INTERNAL_ERROR: u32 = 6;

/// A parsed SPDY frame. Header blocks are kept COMPRESSED here; the codec
/// (de)compresses them with the connection-scoped zlib streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SynStream {
        stream_id: u32,
        assoc_stream_id: u32,
        priority: u8,
        flags: u8,
        /// compressed header block
        header_block: Vec<u8>,
    },
    SynReply {
        stream_id: u32,
        flags: u8,
        header_block: Vec<u8>,
    },
    RstStream {
        stream_id: u32,
        status: u32,
    },
    Settings {
        /// raw payload (we accept and ignore peer settings)
        payload: Vec<u8>,
    },
    Ping {
        id: u32,
    },
    GoAway {
        last_good_stream_id: u32,
        status: u32,
    },
    Headers {
        stream_id: u32,
        flags: u8,
        header_block: Vec<u8>,
    },
    WindowUpdate {
        stream_id: u32,
        delta: u32,
    },
    Data {
        stream_id: u32,
        flags: u8,
        data: Vec<u8>,
    },
}

/// Outcome of a parse attempt against a buffer.
pub enum ParseResult {
    /// A full frame plus the number of bytes it consumed.
    Frame(Frame, usize),
    /// Not enough bytes yet; need at least this many total.
    NeedMore,
    /// Malformed framing.
    Error(String),
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Try to parse one frame from the front of `buf`.
pub fn parse_frame(buf: &[u8]) -> ParseResult {
    if buf.len() < 8 {
        return ParseResult::NeedMore;
    }
    let length = (u32::from(buf[5]) << 16 | u32::from(buf[6]) << 8 | u32::from(buf[7])) as usize;
    let total = 8 + length;
    if buf.len() < total {
        return ParseResult::NeedMore;
    }
    let payload = &buf[8..total];
    let flags = buf[4];

    let is_control = buf[0] & 0x80 != 0;
    if !is_control {
        let stream_id = be_u32(&buf[0..4]) & 0x7fff_ffff;
        return ParseResult::Frame(
            Frame::Data {
                stream_id,
                flags,
                data: payload.to_vec(),
            },
            total,
        );
    }

    let version = (u16::from(buf[0] & 0x7f) << 8) | u16::from(buf[1]);
    if version != SPDY_VERSION {
        return ParseResult::Error(format!("unsupported SPDY version {version}"));
    }
    let ftype = (u16::from(buf[2]) << 8) | u16::from(buf[3]);

    let frame = match ftype {
        TYPE_SYN_STREAM => {
            if payload.len() < 10 {
                return ParseResult::Error("short SYN_STREAM".into());
            }
            let stream_id = be_u32(&payload[0..4]) & 0x7fff_ffff;
            let assoc_stream_id = be_u32(&payload[4..8]) & 0x7fff_ffff;
            let priority = payload[8] >> 5;
            Frame::SynStream {
                stream_id,
                assoc_stream_id,
                priority,
                flags,
                header_block: payload[10..].to_vec(),
            }
        }
        TYPE_SYN_REPLY => {
            if payload.len() < 4 {
                return ParseResult::Error("short SYN_REPLY".into());
            }
            let stream_id = be_u32(&payload[0..4]) & 0x7fff_ffff;
            Frame::SynReply {
                stream_id,
                flags,
                header_block: payload[4..].to_vec(),
            }
        }
        TYPE_RST_STREAM => {
            if payload.len() < 8 {
                return ParseResult::Error("short RST_STREAM".into());
            }
            Frame::RstStream {
                stream_id: be_u32(&payload[0..4]) & 0x7fff_ffff,
                status: be_u32(&payload[4..8]),
            }
        }
        TYPE_SETTINGS => Frame::Settings {
            payload: payload.to_vec(),
        },
        TYPE_PING => {
            if payload.len() < 4 {
                return ParseResult::Error("short PING".into());
            }
            Frame::Ping {
                id: be_u32(&payload[0..4]),
            }
        }
        TYPE_GOAWAY => {
            if payload.len() < 8 {
                return ParseResult::Error("short GOAWAY".into());
            }
            Frame::GoAway {
                last_good_stream_id: be_u32(&payload[0..4]) & 0x7fff_ffff,
                status: be_u32(&payload[4..8]),
            }
        }
        TYPE_HEADERS => {
            if payload.len() < 4 {
                return ParseResult::Error("short HEADERS".into());
            }
            Frame::Headers {
                stream_id: be_u32(&payload[0..4]) & 0x7fff_ffff,
                flags,
                header_block: payload[4..].to_vec(),
            }
        }
        TYPE_WINDOW_UPDATE => {
            if payload.len() < 8 {
                return ParseResult::Error("short WINDOW_UPDATE".into());
            }
            Frame::WindowUpdate {
                stream_id: be_u32(&payload[0..4]) & 0x7fff_ffff,
                delta: be_u32(&payload[4..8]) & 0x7fff_ffff,
            }
        }
        other => return ParseResult::Error(format!("unknown control frame type {other}")),
    };
    ParseResult::Frame(frame, total)
}

fn control_header(out: &mut Vec<u8>, ftype: u16, flags: u8, length: usize) {
    out.push(0x80 | (SPDY_VERSION >> 8) as u8);
    out.push((SPDY_VERSION & 0xff) as u8);
    out.extend_from_slice(&ftype.to_be_bytes());
    out.push(flags);
    out.push((length >> 16) as u8);
    out.push((length >> 8) as u8);
    out.push(length as u8);
}

impl Frame {
    /// Serialize this frame to the wire. Header blocks are already compressed.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Frame::SynReply {
                stream_id,
                flags,
                header_block,
            } => {
                let len = 4 + header_block.len();
                control_header(&mut out, TYPE_SYN_REPLY, *flags, len);
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(header_block);
            }
            Frame::SynStream {
                stream_id,
                assoc_stream_id,
                priority,
                flags,
                header_block,
            } => {
                let len = 10 + header_block.len();
                control_header(&mut out, TYPE_SYN_STREAM, *flags, len);
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(&(assoc_stream_id & 0x7fff_ffff).to_be_bytes());
                out.push(priority << 5);
                out.push(0); // slot
                out.extend_from_slice(header_block);
            }
            Frame::Headers {
                stream_id,
                flags,
                header_block,
            } => {
                let len = 4 + header_block.len();
                control_header(&mut out, TYPE_HEADERS, *flags, len);
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(header_block);
            }
            Frame::RstStream { stream_id, status } => {
                control_header(&mut out, TYPE_RST_STREAM, 0, 8);
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(&status.to_be_bytes());
            }
            Frame::Ping { id } => {
                control_header(&mut out, TYPE_PING, 0, 4);
                out.extend_from_slice(&id.to_be_bytes());
            }
            Frame::GoAway {
                last_good_stream_id,
                status,
            } => {
                control_header(&mut out, TYPE_GOAWAY, 0, 8);
                out.extend_from_slice(&(last_good_stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(&status.to_be_bytes());
            }
            Frame::Settings { payload } => {
                control_header(&mut out, TYPE_SETTINGS, 0, payload.len());
                out.extend_from_slice(payload);
            }
            Frame::WindowUpdate { stream_id, delta } => {
                control_header(&mut out, TYPE_WINDOW_UPDATE, 0, 8);
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.extend_from_slice(&(delta & 0x7fff_ffff).to_be_bytes());
            }
            Frame::Data {
                stream_id,
                flags,
                data,
            } => {
                out.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
                out.push(*flags);
                let len = data.len();
                out.push((len >> 16) as u8);
                out.push((len >> 8) as u8);
                out.push(len as u8);
                out.extend_from_slice(data);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unwrap_frame(buf: &[u8]) -> (Frame, usize) {
        match parse_frame(buf) {
            ParseResult::Frame(f, n) => (f, n),
            ParseResult::NeedMore => panic!("need more"),
            ParseResult::Error(e) => panic!("parse error: {e}"),
        }
    }

    #[test]
    fn data_frame_roundtrip() {
        let f = Frame::Data {
            stream_id: 5,
            flags: FLAG_FIN,
            data: b"payload".to_vec(),
        };
        let bytes = f.serialize();
        // control bit clear
        assert_eq!(bytes[0] & 0x80, 0);
        let (back, n) = unwrap_frame(&bytes);
        assert_eq!(n, bytes.len());
        assert_eq!(back, f);
    }

    #[test]
    fn syn_stream_parse() {
        // build a SYN_STREAM with stream_id=1, streamType header block stub
        let f = Frame::SynStream {
            stream_id: 1,
            assoc_stream_id: 0,
            priority: 0,
            flags: 0,
            header_block: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let bytes = f.serialize();
        assert_eq!(bytes[0] & 0x80, 0x80);
        let (back, n) = unwrap_frame(&bytes);
        assert_eq!(n, bytes.len());
        assert_eq!(back, f);
    }

    #[test]
    fn ping_roundtrip() {
        let f = Frame::Ping { id: 42 };
        let (back, _) = unwrap_frame(&f.serialize());
        assert_eq!(back, f);
    }

    #[test]
    fn rst_stream_roundtrip() {
        let f = Frame::RstStream {
            stream_id: 7,
            status: RST_CANCEL,
        };
        let (back, _) = unwrap_frame(&f.serialize());
        assert_eq!(back, f);
    }

    #[test]
    fn need_more_on_partial_header() {
        assert!(matches!(parse_frame(&[0x80, 0x03]), ParseResult::NeedMore));
    }

    #[test]
    fn need_more_on_partial_payload() {
        let f = Frame::Data {
            stream_id: 1,
            flags: 0,
            data: vec![1, 2, 3, 4],
        };
        let bytes = f.serialize();
        assert!(matches!(
            parse_frame(&bytes[..bytes.len() - 1]),
            ParseResult::NeedMore
        ));
    }

    #[test]
    fn fin_flag_detected() {
        let f = Frame::Data {
            stream_id: 1,
            flags: FLAG_FIN,
            data: vec![],
        };
        let (back, _) = unwrap_frame(&f.serialize());
        match back {
            Frame::Data { flags, .. } => assert_eq!(flags & FLAG_FIN, FLAG_FIN),
            _ => panic!("wrong frame"),
        }
    }
}
