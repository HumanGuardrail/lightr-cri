//! SPDY/3.1 name/value header block (de)serialization.
//!
//! Layout (big-endian, all lengths u32 — SPDY/3 widened these from u16):
//! ```text
//!   u32 count
//!   repeat count times:
//!     u32 name_len  | name bytes (lowercase, NUL-separated for multi-value)
//!     u32 value_len | value bytes
//! ```
//! Header names are lowercase per spec; values MAY contain NUL separators for
//! repeated headers, but the CRI streaming headers we handle
//! (`streamtype`, `port`, `requestid`) are single-valued.

/// Serialize a name/value list into a SPDY/3.1 header block (uncompressed).
pub fn serialize_headers(headers: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + headers.len() * 16);
    out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    for (name, value) in headers {
        out.extend_from_slice(&(name.len() as u32).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    let slice = buf.get(*pos..end)?;
    *pos = end;
    Some(u32::from_be_bytes(slice.try_into().ok()?))
}

fn read_bytes<'a>(buf: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = pos.checked_add(len)?;
    let slice = buf.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

/// Deserialize a SPDY/3.1 header block (uncompressed) into a name/value list.
/// Returns `None` on truncation or non-UTF-8 names/values.
pub fn deserialize_headers(buf: &[u8]) -> Option<Vec<(String, String)>> {
    let mut pos = 0usize;
    let count = read_u32(buf, &mut pos)? as usize;
    // Guard against a hostile count claiming millions of pairs.
    if count > 1024 {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let name_len = read_u32(buf, &mut pos)? as usize;
        let name = read_bytes(buf, &mut pos, name_len)?;
        let value_len = read_u32(buf, &mut pos)? as usize;
        let value = read_bytes(buf, &mut pos, value_len)?;
        out.push((
            String::from_utf8(name.to_vec()).ok()?,
            String::from_utf8(value.to_vec()).ok()?,
        ));
    }
    Some(out)
}

/// Case-insensitive lookup of a single-valued header.
pub fn get<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let h = vec![
            ("streamtype".to_string(), "stdout".to_string()),
            ("port".to_string(), "8080".to_string()),
        ];
        let bytes = serialize_headers(&h);
        // count = 2
        assert_eq!(&bytes[0..4], &[0, 0, 0, 2]);
        let back = deserialize_headers(&bytes).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn case_insensitive_get() {
        let h = vec![("streamType".to_string(), "stdin".to_string())];
        assert_eq!(get(&h, "streamtype"), Some("stdin"));
        assert_eq!(get(&h, "STREAMTYPE"), Some("stdin"));
        assert_eq!(get(&h, "absent"), None);
    }

    #[test]
    fn truncated_is_none() {
        assert!(deserialize_headers(&[0, 0, 0, 1, 0, 0, 0, 5, b'a']).is_none());
    }

    #[test]
    fn empty_block() {
        let bytes = serialize_headers(&[]);
        assert_eq!(bytes, vec![0, 0, 0, 0]);
        assert_eq!(deserialize_headers(&bytes).unwrap(), vec![]);
    }
}
