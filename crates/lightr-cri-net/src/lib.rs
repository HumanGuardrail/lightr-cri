//! lightr-cri-net — CNI invocation + netns lifecycle (WP-B skeleton).
//!
//! FROZEN laws (build-spec-r1 §3 WP-B):
//! - netns create/teardown per containerd pattern (bind-mount pin; umount2-then-unlink LAW; orphan sweep helper)
//! - CNI chain invocation per research brief (env+stdin contract, forward ADD/reverse DEL,
//!   runtimeConfig.portMappings, prevResult threading, error JSON surfaced as
//!   BackendError::Internal with plugin msg)
//! - conflist discovery (lexicographic first in /etc/cni/net.d, override via env LIGHTR_CNI_CONF)
//! - probe API: fn cni_available() -> Option<CniEnv> (caps + conf + binaries) — probe-truthful, never fake
//! - Unit-testable parts (conflist derivation, result parsing) MUST be host-testable without privileges
//! - FIREWALL: no tonic/tokio

pub struct CniEnv {
    pub conf_dir: std::path::PathBuf,
    pub bin_dir: std::path::PathBuf,
}

pub fn cni_available() -> Option<CniEnv> {
    todo!("WP-B")
}

pub mod netns {
    /// Create a new named network namespace, bind-mount it, and return its path.
    pub fn create(_name: &str) -> std::io::Result<std::path::PathBuf> {
        todo!("WP-B")
    }

    /// Unmount and unlink a pinned netns (umount2-then-unlink LAW).
    pub fn teardown(_path: &std::path::Path) -> std::io::Result<()> {
        todo!("WP-B")
    }

    /// Sweep orphaned netns entries under `dir`, returning the count removed.
    pub fn sweep(_dir: &std::path::Path) -> std::io::Result<usize> {
        todo!("WP-B")
    }
}

pub mod chain {
    use lightr_cri_backend::PortMapping;

    /// Minimal CNI result returned from a successful ADD.
    pub struct CniResult {
        pub ip: Option<String>,
    }

    /// CNI invocation error (plugin stderr / error JSON message).
    pub struct CniError(pub String);

    impl std::fmt::Debug for CniError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CniError({})", self.0)
        }
    }

    impl std::fmt::Display for CniError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for CniError {}

    /// Invoke the CNI ADD command for the given conflist.
    pub fn add(
        _conflist_path: &std::path::Path,
        _container_id: &str,
        _netns_path: &std::path::Path,
        _port_mappings: &[PortMapping],
    ) -> Result<CniResult, CniError> {
        todo!("WP-B")
    }

    /// Invoke the CNI DEL command for the given conflist.
    pub fn del(
        _conflist_path: &std::path::Path,
        _container_id: &str,
        _netns_path: &std::path::Path,
        _port_mappings: &[PortMapping],
    ) -> Result<(), CniError> {
        todo!("WP-B")
    }
}
