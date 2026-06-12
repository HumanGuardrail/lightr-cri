//! Single-use token registry (r1-streaming.md "URL + token law").
//!
//! - mint: 8-char base64url of 6 crypto-random bytes (read from
//!   `/dev/urandom`), 1-minute TTL, max 1000 in flight (over cap → `None`),
//!   full request params cached server-side under the token.
//! - consume: single-use (removes), 1-minute TTL checked AFTER removal so a
//!   stale entry still frees its slot; missing/expired/used → `None`.
//!
//! Crash-only: the registry is ephemeral (process-local). A crash loses all
//! in-flight tokens; clients retry the RPC — listener law (CLAUDE.md §1).

use std::collections::HashMap;
use std::io::Read;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::b64;
use crate::{StreamParams, StreamVerb};

/// Token TTL (r1-streaming.md: 1-minute).
const TTL: Duration = Duration::from_secs(60);
/// Maximum in-flight tokens (r1-streaming.md: "max 1000 in flight").
const MAX_INFLIGHT: usize = 1000;

struct Entry {
    verb: StreamVerb,
    params: StreamParams,
    minted_at: Instant,
}

/// Token registry: mint single-use tokens, consume them to retrieve params.
pub struct TokenRegistry {
    inner: Mutex<HashMap<String, Entry>>,
}

impl TokenRegistry {
    pub fn new() -> Self {
        TokenRegistry {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Read 6 crypto-random bytes from `/dev/urandom` (getrandom semantics
    /// without an extra crate; brief mandate). Returns an 8-char base64url
    /// token string.
    fn random_token() -> std::io::Result<String> {
        let mut buf = [0u8; 6];
        let mut f = std::fs::File::open("/dev/urandom")?;
        f.read_exact(&mut buf)?;
        Ok(b64::encode_url_nopad(&buf))
    }

    /// Mint a single-use token for the given verb + params.
    ///
    /// Returns `None` when at/over the 1000-token cap (the RPC must reject
    /// "too many in flight") or when `/dev/urandom` is unavailable.
    pub fn mint(&self, verb: StreamVerb, params: StreamParams) -> Option<String> {
        let mut map = self.inner.lock().expect("token registry poisoned");
        // Opportunistically sweep expired entries so the cap reflects only
        // genuinely live tokens.
        let now = Instant::now();
        map.retain(|_, e| now.duration_since(e.minted_at) < TTL);
        if map.len() >= MAX_INFLIGHT {
            return None;
        }
        // Mint, retrying on the (astronomically unlikely) 48-bit collision.
        for _ in 0..8 {
            let tok = Self::random_token().ok()?;
            if !map.contains_key(&tok) {
                map.insert(
                    tok.clone(),
                    Entry {
                        verb,
                        params,
                        minted_at: now,
                    },
                );
                return Some(tok);
            }
        }
        None
    }

    /// Consume a token (single-use). Removes it first (single-use law), then
    /// checks the TTL: `None` if missing, expired, or already used.
    pub fn consume(&self, token: &str) -> Option<(StreamVerb, StreamParams)> {
        let mut map = self.inner.lock().expect("token registry poisoned");
        let entry = map.remove(token)?;
        if Instant::now().duration_since(entry.minted_at) >= TTL {
            return None;
        }
        Some((entry.verb, entry.params))
    }

    /// Test-only: count of in-flight tokens (no sweep).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Test seam: insert an entry with a backdated mint time to exercise TTL
    /// without sleeping a real minute.
    #[cfg(test)]
    fn mint_at(&self, verb: StreamVerb, params: StreamParams, minted_at: Instant) -> String {
        let tok = Self::random_token().unwrap();
        self.inner.lock().unwrap().insert(
            tok.clone(),
            Entry {
                verb,
                params,
                minted_at,
            },
        );
        tok
    }
}

impl Default for TokenRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> StreamParams {
        StreamParams {
            container: Some("c0".into()),
            sandbox: None,
            cmd: vec!["echo".into(), "hi".into()],
            tty: false,
            stdin: true,
            ports: vec![],
            dial_target: None,
        }
    }

    #[test]
    fn mint_yields_8_char_token() {
        let r = TokenRegistry::new();
        let t = r.mint(StreamVerb::Exec, params()).unwrap();
        assert_eq!(t.len(), 8);
        assert!(t
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    }

    #[test]
    fn consume_returns_params_then_gone() {
        let r = TokenRegistry::new();
        let t = r.mint(StreamVerb::Exec, params()).unwrap();
        let (verb, p) = r.consume(&t).expect("first consume");
        assert!(matches!(verb, StreamVerb::Exec));
        assert_eq!(p.cmd, vec!["echo".to_string(), "hi".to_string()]);
        // single-use: a second consume misses
        assert!(r.consume(&t).is_none());
    }

    #[test]
    fn consume_unknown_is_none() {
        let r = TokenRegistry::new();
        assert!(r.consume("nonexist").is_none());
    }

    #[test]
    fn expired_token_is_none_and_frees_slot() {
        let r = TokenRegistry::new();
        let old = Instant::now() - Duration::from_secs(120);
        let t = r.mint_at(StreamVerb::Attach, params(), old);
        assert_eq!(r.len(), 1);
        // expired → consume misses, but the slot is freed by removal
        assert!(r.consume(&t).is_none());
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn cap_at_1000_then_mint_fails() {
        let r = TokenRegistry::new();
        for _ in 0..MAX_INFLIGHT {
            assert!(r.mint(StreamVerb::Exec, params()).is_some());
        }
        assert_eq!(r.len(), MAX_INFLIGHT);
        // 1001st mint over cap → None
        assert!(r.mint(StreamVerb::Exec, params()).is_none());
    }

    #[test]
    fn cap_sweeps_expired_before_rejecting() {
        let r = TokenRegistry::new();
        let old = Instant::now() - Duration::from_secs(120);
        for _ in 0..MAX_INFLIGHT {
            r.mint_at(StreamVerb::Exec, params(), old);
        }
        assert_eq!(r.len(), MAX_INFLIGHT);
        // all expired → sweep frees the slots, mint succeeds
        assert!(r.mint(StreamVerb::Exec, params()).is_some());
    }
}
