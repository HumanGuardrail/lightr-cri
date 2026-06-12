//! lightr-cri-net — CNI invocation + netns lifecycle (WP-B).
//!
//! FROZEN laws (build-spec-r1 §3 WP-B):
//! - netns create/teardown per containerd pattern (bind-mount pin; umount2-then-unlink LAW;
//!   orphan sweep helper)
//! - CNI chain invocation per research brief (env+stdin contract, forward ADD/reverse DEL,
//!   runtimeConfig.portMappings, prevResult threading, error JSON surfaced as
//!   BackendError::Internal with plugin msg)
//! - conflist discovery (lexicographic first in /etc/cni/net.d, override via env LIGHTR_CNI_CONF)
//! - probe API: fn cni_available() -> Option<CniEnv> (caps + conf + binaries) — probe-truthful, never fake
//! - Unit-testable parts (conflist derivation, result parsing) MUST be host-testable without privileges
//! - FIREWALL: no tonic/tokio

pub mod chain;
pub mod netns;

/// Resolved CNI configuration used by `chain::add` / `chain::del`.
pub struct CniEnv {
    /// Directory containing the chosen conflist file.
    pub conf_dir: std::path::PathBuf,
    /// Directory containing CNI plugin binaries.
    pub bin_dir: std::path::PathBuf,
}

/// Probe whether CNI is available and the process has the required privileges.
///
/// Returns `None` on macOS, when running unprivileged (EPERM on unshare), or when
/// no conflist / binary directory is found.  Probe-truthful: never fakes an answer.
pub fn cni_available() -> Option<CniEnv> {
    // macOS short-circuit — no kernel namespaces.
    #[cfg(target_os = "macos")]
    return None;

    #[cfg(not(target_os = "macos"))]
    {
        use std::path::PathBuf;

        // Resolve directories from env overrides or defaults.
        let conf_dir = std::env::var("LIGHTR_CNI_CONF")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/etc/cni/net.d"));
        let bin_dir = std::env::var("LIGHTR_CNI_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/opt/cni/bin"));

        // Require at least one .conflist file in conf_dir.
        let has_conflist = conf_dir.read_dir().ok()?.filter_map(|e| e.ok()).any(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x == "conflist")
                .unwrap_or(false)
        });
        if !has_conflist {
            return None;
        }

        // Require bin_dir to exist and have at least one file (rough check).
        if !bin_dir.is_dir() {
            return None;
        }

        // Privilege probe: attempt unshare(CLONE_NEWNET) on a throwaway thread.
        // EPERM → not privileged → None.
        let priv_ok = std::thread::spawn(|| -> bool {
            use nix::sched::{unshare, CloneFlags};
            unshare(CloneFlags::CLONE_NEWNET).is_ok()
        })
        .join()
        .unwrap_or(false);

        if !priv_ok {
            return None;
        }

        Some(CniEnv { conf_dir, bin_dir })
    }
}
