//! Vocabulary types transcribed from lightr-core (seam-contract-v1 §1).
//! Transcribed, NOT imported — drift is caught by conformance vectors,
//! never by a git/path dependency (house seam pattern).

use std::fmt;

/// BLAKE3, 32 bytes. Content address for objects, manifests, refs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
            s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
        }
        s
    }

    pub fn from_hex(s: &str) -> Option<Digest> {
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        let bytes = s.as_bytes();
        for (i, chunk) in bytes.chunks_exact(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16)?;
            let lo = (chunk[1] as char).to_digit(16)?;
            out[i] = ((hi << 4) | lo) as u8;
        }
        Some(Digest(out))
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({})", self.to_hex())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    File {
        path: String,
        mode: u32,
        size: u64,
        digest: Digest,
    },
    Symlink {
        path: String,
        target: String,
    },
    Dir {
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub total_size: u64,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefRecord {
    pub name: String,
    pub root: Digest,
    pub parent: Option<Digest>,
    pub created_at_unix: u64,
    pub tool_version: String,
}
