//! WP-4: conformance-vector runner (build-spec-r0 §6).
//!
//! FROZEN laws:
//! - Vector JSON shape per spec §6 (`$N` = result of step N;
//!   `expect_err` = exact BackendError variant name; `reopen_backend` step
//!   for crash-recovery scripts).
//! - Runs against `&dyn CriBackend` ONLY — never imports backend internals.
//!   These vectors are the shared integration artifact with hugr-lightr.
//! - A vector failure names the vector + step index + expected/actual.

use std::path::Path;

use lightr_cri_backend::CriBackend;

#[derive(Debug, Default)]
pub struct VectorReport {
    pub passed: usize,
    pub failed: Vec<String>,
}

/// Factory so crash-recovery vectors can drop + reopen the backend
/// (`reopen_backend` step). The fake reopens from its state root; the real
/// backend will do the same at integration.
pub trait BackendFactory {
    fn open(&self) -> Box<dyn CriBackend>;
}

pub fn run_vectors(factory: &dyn BackendFactory, dir: &Path) -> std::io::Result<VectorReport> {
    let _ = (factory, dir);
    todo!("WP-4")
}
