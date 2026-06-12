//! Hand-rolled base64 (std-only — no `base64` crate of record).
//!
//! Two alphabets are needed:
//! - `url_nopad`: token minting — RFC 4648 §5 URL-safe, no padding.
//! - `std`: the `base64.channel.k8s.io` WS subprotocol payload framing —
//!   RFC 4648 §4 standard alphabet, padded (client-go uses `StdEncoding`).

const STD_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_with(alphabet: &[u8; 64], pad: bool, input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut chunks = input.chunks_exact(3);
    for c in &mut chunks {
        let n = (u32::from(c[0]) << 16) | (u32::from(c[1]) << 8) | u32::from(c[2]);
        out.push(alphabet[((n >> 18) & 0x3f) as usize] as char);
        out.push(alphabet[((n >> 12) & 0x3f) as usize] as char);
        out.push(alphabet[((n >> 6) & 0x3f) as usize] as char);
        out.push(alphabet[(n & 0x3f) as usize] as char);
    }
    let rem = chunks.remainder();
    match rem.len() {
        1 => {
            let n = u32::from(rem[0]) << 16;
            out.push(alphabet[((n >> 18) & 0x3f) as usize] as char);
            out.push(alphabet[((n >> 12) & 0x3f) as usize] as char);
            if pad {
                out.push('=');
                out.push('=');
            }
        }
        2 => {
            let n = (u32::from(rem[0]) << 16) | (u32::from(rem[1]) << 8);
            out.push(alphabet[((n >> 18) & 0x3f) as usize] as char);
            out.push(alphabet[((n >> 12) & 0x3f) as usize] as char);
            out.push(alphabet[((n >> 6) & 0x3f) as usize] as char);
            if pad {
                out.push('=');
            }
        }
        _ => {}
    }
    out
}

/// URL-safe base64, no padding (token alphabet).
pub fn encode_url_nopad(input: &[u8]) -> String {
    encode_with(URL_ALPHABET, false, input)
}

/// Standard base64, padded (`base64.channel.k8s.io` outbound frames).
pub fn encode_std(input: &[u8]) -> String {
    encode_with(STD_ALPHABET, true, input)
}

fn decode_value(alphabet: &[u8; 64], b: u8) -> Option<u8> {
    alphabet.iter().position(|&c| c == b).map(|p| p as u8)
}

/// Standard base64 decode (tolerant of padding; rejects invalid chars).
/// Used to decode inbound `base64.channel.k8s.io` text frames.
pub fn decode_std(input: &[u8]) -> Option<Vec<u8>> {
    let mut acc: u32 = 0;
    let mut nbits = 0u32;
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    for &b in input {
        if b == b'=' || b == b'\r' || b == b'\n' {
            continue;
        }
        let v = decode_value(STD_ALPHABET, b)?;
        acc = (acc << 6) | u32::from(v);
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((acc >> nbits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_nopad_known_vectors() {
        assert_eq!(encode_url_nopad(b""), "");
        assert_eq!(encode_url_nopad(b"f"), "Zg");
        assert_eq!(encode_url_nopad(b"fo"), "Zm8");
        assert_eq!(encode_url_nopad(b"foo"), "Zm9v");
        assert_eq!(encode_url_nopad(b"foob"), "Zm9vYg");
        assert_eq!(encode_url_nopad(b"fooba"), "Zm9vYmE");
        assert_eq!(encode_url_nopad(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn url_nopad_is_url_safe() {
        // bytes that map to index 62 (-) and 63 (_) under url alphabet
        let enc = encode_url_nopad(&[0xfb, 0xff]);
        assert!(!enc.contains('+'));
        assert!(!enc.contains('/'));
        assert!(enc.contains('-') || enc.contains('_'));
    }

    #[test]
    fn six_bytes_is_eight_chars() {
        // token law: 6 random bytes → 8 base64 chars, no padding
        let enc = encode_url_nopad(&[0u8; 6]);
        assert_eq!(enc.len(), 8);
    }

    #[test]
    fn std_known_vectors() {
        assert_eq!(encode_std(b""), "");
        assert_eq!(encode_std(b"f"), "Zg==");
        assert_eq!(encode_std(b"fo"), "Zm8=");
        assert_eq!(encode_std(b"foo"), "Zm9v");
        assert_eq!(encode_std(b"foob"), "Zm9vYg==");
    }

    #[test]
    fn std_roundtrip() {
        for s in [
            &b""[..],
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"hello world",
            &[0u8, 255, 1, 254],
        ] {
            let enc = encode_std(s);
            assert_eq!(decode_std(enc.as_bytes()).unwrap(), s);
        }
    }

    #[test]
    fn std_decode_rejects_invalid() {
        assert!(decode_std(b"@@@@").is_none());
    }
}
