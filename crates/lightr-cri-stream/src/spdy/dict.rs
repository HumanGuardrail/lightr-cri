//! The SPDY/3.1 fixed header-compression dictionary.
//!
//! This is the well-known dictionary from the SPDY draft-3 specification
//! (`C.2 Compression`). It is used as the preset zlib dictionary for the
//! single per-direction zlib stream that (de)compresses every SYN_STREAM /
//! SYN_REPLY / HEADERS header block. The bytes below are verbatim from the
//! spec; the trailing NUL is part of the dictionary.
//!
//! Reference: SPDY draft-3 §2.6.10.1; byte-identical to moby/spdystream's
//! `headerDictionary` (the dictionary docker/client-go SPDY uses on the wire).
//! 1423 bytes, ending `...utf-,*,enq=0.` — NO trailing NUL (the entries are
//! NUL-length-prefixed, not NUL-terminated).

/// The SPDY/3.1 zlib dictionary (1423 bytes, verbatim from the spec).
pub static SPDY_DICTIONARY: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\x0dcache-control\x00\x00\x00\x0aconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\x0dcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\x0dif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\x0dlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\x0auser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\x0aset-cookie\x00\x00\x00\x0akeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_length_is_spec_1423() {
        // SPDY/3 dictionary is 1423 bytes including the trailing NUL.
        assert_eq!(SPDY_DICTIONARY.len(), 1423);
    }

    #[test]
    fn dictionary_ends_with_enq_0_dot() {
        // verbatim tail "enq=0." — the dictionary is NOT NUL-terminated.
        assert_eq!(&SPDY_DICTIONARY[SPDY_DICTIONARY.len() - 6..], b"enq=0.");
    }

    #[test]
    fn dictionary_starts_with_options_entry() {
        // first entry: 4-byte length 7 + "options"
        assert_eq!(&SPDY_DICTIONARY[0..4], &[0, 0, 0, 7]);
        assert_eq!(&SPDY_DICTIONARY[4..11], b"options");
    }
}
