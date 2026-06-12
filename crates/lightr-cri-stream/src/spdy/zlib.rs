//! SPDY/3.1 header-block zlib (de)compression with the fixed dictionary.
//!
//! SPDY uses ONE continuous zlib stream per direction for the whole
//! connection. Every header block is compressed with `Z_SYNC_FLUSH` so the
//! receiver can decode it incrementally; the SPDY dictionary is installed as
//! the zlib preset dictionary. State (the sliding window) therefore persists
//! across frames — these codecs live for the connection lifetime, NOT per
//! frame.
//!
//! Compression side: install the dictionary immediately after construction
//! (deflateSetDictionary before any input — zlib-rs semantics).
//! Decompression side: zlib reports `NeedDict` on the first block (the stream
//! was compressed against a preset dict); we install the dictionary then and
//! retry, exactly as inflateSetDictionary prescribes.

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};

use super::dict::SPDY_DICTIONARY;

/// Per-direction header compressor (server → client).
pub struct HeaderCompressor {
    z: Compress,
}

impl HeaderCompressor {
    pub fn new() -> Self {
        // zlib header = true; SPDY uses a zlib stream (not raw deflate).
        let mut z = Compress::new(Compression::default(), true);
        z.set_dictionary(SPDY_DICTIONARY)
            .expect("install SPDY dictionary on compressor");
        HeaderCompressor { z }
    }

    /// Compress one header block, flushing with SYNC so the full block is
    /// emitted and decodable by the peer immediately. A SYNC flush ends with
    /// the empty stored block `00 00 FF FF`; we drain until input is consumed
    /// AND a `compress` call produces less than a full buffer (flush complete).
    pub fn compress(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(input.len() / 2 + 64);
        let mut buf = [0u8; 4096];
        let mut consumed = 0usize;
        loop {
            let before_in = self.z.total_in();
            let before_out = self.z.total_out();
            let _status = self
                .z
                .compress(&input[consumed..], &mut buf, FlushCompress::Sync)
                .expect("spdy header compress");
            let read = (self.z.total_in() - before_in) as usize;
            let wrote = (self.z.total_out() - before_out) as usize;
            consumed += read;
            out.extend_from_slice(&buf[..wrote]);
            // Done once all input is consumed and the flush stopped filling the
            // buffer (a short/empty write means no pending flush output).
            if consumed >= input.len() && wrote < buf.len() {
                break;
            }
            // Safety valve: no progress at all → bail (should not happen).
            if read == 0 && wrote == 0 {
                break;
            }
        }
        out
    }
}

impl Default for HeaderCompressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-direction header decompressor (client → server).
pub struct HeaderDecompressor {
    z: Decompress,
    dict_installed: bool,
}

impl HeaderDecompressor {
    pub fn new() -> Self {
        HeaderDecompressor {
            z: Decompress::new(true),
            dict_installed: false,
        }
    }

    /// Decompress one (SYNC-flushed) header block. Installs the SPDY
    /// dictionary lazily: flate2 surfaces zlib's `Z_NEED_DICT` as a
    /// `DecompressError`, so on the first decompress error before the dict is
    /// present we install it and resume (inflateSetDictionary semantics).
    pub fn decompress(&mut self, input: &[u8]) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(input.len() * 4 + 64);
        let mut buf = [0u8; 4096];
        let mut consumed = 0usize;
        loop {
            let before_in = self.z.total_in();
            let before_out = self.z.total_out();
            match self
                .z
                .decompress(&input[consumed..], &mut buf, FlushDecompress::None)
            {
                Ok(_status) => {}
                Err(e) => {
                    // Z_NEED_DICT (or a stall before the dict is set): install
                    // the SPDY dictionary once and retry from the same offset.
                    if !self.dict_installed {
                        self.z
                            .set_dictionary(SPDY_DICTIONARY)
                            .map_err(|e| format!("set spdy dict: {e}"))?;
                        self.dict_installed = true;
                        // account for any header bytes zlib consumed pre-error
                        consumed += (self.z.total_in() - before_in) as usize;
                        continue;
                    }
                    return Err(format!("spdy header decompress: {e}"));
                }
            }
            let read = (self.z.total_in() - before_in) as usize;
            let wrote = (self.z.total_out() - before_out) as usize;
            consumed += read;
            out.extend_from_slice(&buf[..wrote]);

            // A SYNC-flushed block ends when all input is consumed and the last
            // decompress produced a short buffer (no more pending output).
            if consumed >= input.len() && wrote < buf.len() {
                break;
            }
            if read == 0 && wrote == 0 {
                break;
            }
        }
        Ok(out)
    }
}

impl Default for HeaderDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::headers::{deserialize_headers, serialize_headers};
    use super::*;

    #[test]
    fn dictionary_roundtrip_single_block() {
        let mut c = HeaderCompressor::new();
        let mut d = HeaderDecompressor::new();
        let headers = vec![
            ("streamtype".to_string(), "stdout".to_string()),
            (":status".to_string(), "200".to_string()),
        ];
        let block = serialize_headers(&headers);
        let compressed = c.compress(&block);
        // SPDY header blocks are not stored plaintext.
        assert!(compressed != block);
        let decompressed = d.decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, block);
        let parsed = deserialize_headers(&decompressed).expect("parse");
        assert_eq!(parsed, headers);
    }

    #[test]
    fn dictionary_roundtrip_multiple_blocks_shared_stream() {
        // The window persists across blocks; the second block must still
        // decode on a decompressor that already processed the first.
        let mut c = HeaderCompressor::new();
        let mut d = HeaderDecompressor::new();

        let h1 = vec![("streamtype".to_string(), "stdin".to_string())];
        let h2 = vec![("streamtype".to_string(), "stderr".to_string())];

        let b1 = serialize_headers(&h1);
        let b2 = serialize_headers(&h2);

        let c1 = c.compress(&b1);
        let c2 = c.compress(&b2);

        assert_eq!(d.decompress(&c1).unwrap(), b1);
        assert_eq!(d.decompress(&c2).unwrap(), b2);
    }
}
