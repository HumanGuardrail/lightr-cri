//! WP-5: acceptance suite A1–A8 + A10 harness (build-spec-r0 §7).
//!
//! FROZEN laws:
//! - Probe-truthful: every item SKIPs loudly with a reason when its probe
//!   fails (e.g. no crictl on macOS) — never silent, never green-by-skip
//!   without the skip being visible in output.
//! - Tests live in tests/ and drive the release `lightr-cri` bin over a UDS
//!   with `crictl` (pinned version, see ci/).
//! - A8 (crash-only): kill -9 the server mid-suite, restart, state must
//!   re-derive identically; a Running container's process survives.

/// Probe: is `bin` runnable in PATH?
pub fn have(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Loud skip helper — prints the frozen skip format.
pub fn skip(item: &str, reason: &str) {
    eprintln!("SKIP {item}: {reason} (probe-truthful; see build-spec-r0 §7)");
}
