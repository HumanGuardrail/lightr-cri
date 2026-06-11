//! WP-2: implement `runtime_service_server::RuntimeService` for
//! `RuntimeShell<B>` (build-spec-r0 §5). Translation only: decode proto →
//! backend call inside `tokio::task::spawn_blocking` → encode proto.
//!
//! FROZEN laws:
//! - ZERO state beyond `backend` (any caching field = REJECT).
//! - Version: runtime_name="lightr", runtime_api_version="v1".
//! - Status: Runtime+Network conditions, network=true in R0 fake mode.
//! - Unimplemented in R0 (explicit message naming R1): Exec, Attach,
//!   PortForward, GetContainerEvents, UpdateRuntimeConfig,
//!   UpdateContainerResources, ReopenContainerLog, checkpoint RPCs.
//! - Errors map via `crate::map_err` only.

use std::sync::Arc;

use lightr_cri_backend::CriBackend;

pub struct RuntimeShell<B: CriBackend> {
    pub backend: Arc<B>,
}

impl<B: CriBackend> RuntimeShell<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

// WP-2: `impl runtime_service_server::RuntimeService for RuntimeShell<B>` here.
