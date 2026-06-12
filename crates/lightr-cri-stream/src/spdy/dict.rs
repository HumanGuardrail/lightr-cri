//! The SPDY/3.1 fixed header-compression dictionary.
//!
//! This is the well-known dictionary from the SPDY draft-3 specification
//! (§2.6.10.1 / Appendix). It is used as the preset zlib dictionary for the
//! single per-direction zlib stream that (de)compresses every SYN_STREAM /
//! SYN_REPLY / HEADERS header block.
//!
//! ## Byte-exactness (the wire contract)
//!
//! zlib's `inflateSetDictionary` requires the *byte-identical* preset
//! dictionary the peer compressed against, or it returns `Z_DATA_ERROR`. The
//! peer here is the kubernetes streaming SPDY client: `k8s.io/apimachinery`'s
//! SPDY round-tripper uses [`github.com/moby/spdystream`], whose
//! `headerDictionary` (`spdy/dictionary.go`) is the authoritative reference.
//!
//! That reference dictionary is **1423 bytes** and **ends with `enq=0.`
//! (`0x2e`), with NO trailing NUL byte.** The interior `0x00` bytes are the
//! big-endian length *prefixes* of each canonical name/value entry — length
//! prefixes, NOT entry terminators — so there is no terminating NUL on the
//! final entry either. Verified byte-for-byte (and pinned by the sha256
//! assertion in the tests below) against moby/spdystream `master`.
//!
//! (golang.org/x/net once carried an identical `spdy/dictionary.go`; that
//! package has since been removed upstream, leaving moby/spdystream as the live
//! reference for the k8s SPDY path. A *wrong* dictionary — one byte
//! short/long, or with a spurious trailing NUL appended — still round-trips
//! against itself but inflates to `Z_DATA_ERROR` on the wire, so the sha256
//! pin below is the real guard, not a self-round-trip.)
//!
//! [`github.com/moby/spdystream`]: https://github.com/moby/spdystream/blob/master/spdy/dictionary.go

/// Byte length of the canonical SPDY/3.1 zlib dictionary.
///
/// 1423 = the moby/spdystream `headerDictionary` length, ending `enq=0.` with
/// no trailing NUL (see the module docs).
pub const SPDY_DICTIONARY_LEN: usize = 1423;

/// sha256 of [`SPDY_DICTIONARY`], hex-encoded. Pins the exact byte sequence the
/// kubernetes SPDY peer compresses against; a drifted dict (extra/missing byte,
/// spurious trailing NUL) changes this digest and fails the test — catching the
/// wire-break that a self-round-trip cannot.
pub const SPDY_DICTIONARY_SHA256: &str =
    "51d27341373f923f3cd88e1eb7162aeaa3723d7585ff2399201dc06498407f02";

/// The SPDY/3.1 zlib preset dictionary — byte-exact to moby/spdystream's
/// `headerDictionary` ([`SPDY_DICTIONARY_LEN`] = 1423 bytes, ends `enq=0.`, no
/// trailing NUL).
pub static SPDY_DICTIONARY: &[u8] = b"\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-charset\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\x0dcache-control\x00\x00\x00\x0aconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10content-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\x0dcontent-range\x00\x00\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\x00\x00\x11if-modified-since\x00\x00\x00\x0dif-none-match\x00\x00\x00\x08if-range\x00\x00\x00\x13if-unmodified-since\x00\x00\x00\x0dlast-modified\x00\x00\x00\x08location\x00\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0bretry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\x0auser-agent\x00\x00\x00\x04vary\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\x0aset-cookie\x00\x00\x00\x0akeep-alive\x00\x00\x00\x06origin100101201202205206300302303304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative Information204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMTchunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,text/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-8859-1,utf-,*,enq=0.";

// Compile-time guards that keep the documented constants honest and in use
// outside the test build (the `spdy` module is crate-private, so without a
// real reference these `pub` items would read as dead code):
//   - the static's length equals the documented canonical length, and
//   - the pinned sha256 is a well-formed 64-char hex digest.
const _: () = assert!(SPDY_DICTIONARY.len() == SPDY_DICTIONARY_LEN);
const _: () = assert!(SPDY_DICTIONARY_SHA256.len() == 64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_length_is_canonical_1423() {
        // moby/spdystream headerDictionary is 1423 bytes (no trailing NUL).
        assert_eq!(SPDY_DICTIONARY.len(), 1423);
        assert_eq!(SPDY_DICTIONARY.len(), SPDY_DICTIONARY_LEN);
    }

    #[test]
    fn dictionary_ends_with_enq_0_dot_and_no_trailing_nul() {
        // Canonical tail is "enq=0." (…0x65 0x6e 0x71 0x3d 0x30 0x2e); the
        // final byte is the '.' (0x2e), NOT a NUL. zlib needs this byte-exact.
        assert_eq!(&SPDY_DICTIONARY[SPDY_DICTIONARY.len() - 6..], b"enq=0.");
        assert_eq!(*SPDY_DICTIONARY.last().unwrap(), b'.');
        assert_ne!(*SPDY_DICTIONARY.last().unwrap(), 0u8);
    }

    #[test]
    fn dictionary_starts_with_options_entry() {
        // first entry: 4-byte big-endian length 7 + "options"
        assert_eq!(&SPDY_DICTIONARY[0..4], &[0, 0, 0, 7]);
        assert_eq!(&SPDY_DICTIONARY[4..11], b"options");
    }

    /// THE wire-contract guard: the dictionary must be byte-identical to the
    /// moby/spdystream `headerDictionary` the kubernetes SPDY peer compresses
    /// against. A self-round-trip cannot catch drift (a wrong dict round-trips
    /// against itself); this sha256 pin does.
    #[test]
    fn dictionary_sha256_matches_moby_spdystream() {
        let hex = sha256_hex(SPDY_DICTIONARY);
        assert_eq!(
            hex, SPDY_DICTIONARY_SHA256,
            "SPDY dictionary drifted from the canonical moby/spdystream bytes"
        );
    }

    /// Minimal dependency-free SHA-256 (FIPS 180-4), used only to pin the
    /// dictionary bytes in the test above. Not a hot path; clarity over speed.
    fn sha256_hex(data: &[u8]) -> String {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut h: [u32; 8] = [
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
            0x5be0cd19,
        ];

        // pad
        let bitlen = (data.len() as u64) * 8;
        let mut msg = data.to_vec();
        msg.push(0x80);
        while msg.len() % 64 != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bitlen.to_be_bytes());

        for chunk in msg.chunks_exact(64) {
            let mut w = [0u32; 64];
            for (i, word) in w.iter_mut().take(16).enumerate() {
                *word = u32::from_be_bytes([
                    chunk[i * 4],
                    chunk[i * 4 + 1],
                    chunk[i * 4 + 2],
                    chunk[i * 4 + 3],
                ]);
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }
            let mut v = h;
            for i in 0..64 {
                let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
                let ch = (v[4] & v[5]) ^ ((!v[4]) & v[6]);
                let t1 = v[7]
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
                let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
                let t2 = s0.wrapping_add(maj);
                v[7] = v[6];
                v[6] = v[5];
                v[5] = v[4];
                v[4] = v[3].wrapping_add(t1);
                v[3] = v[2];
                v[2] = v[1];
                v[1] = v[0];
                v[0] = t1.wrapping_add(t2);
            }
            for (hi, vi) in h.iter_mut().zip(v.iter()) {
                *hi = hi.wrapping_add(*vi);
            }
        }

        let mut out = String::with_capacity(64);
        for word in h {
            out.push_str(&format!("{word:08x}"));
        }
        out
    }

    #[test]
    fn sha256_helper_is_correct() {
        // sanity-check the in-test SHA-256 against the canonical empty-input
        // and "abc" digests (FIPS 180-4 examples) before trusting it above.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
