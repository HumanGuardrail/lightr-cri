//! The CRI shell: gRPC ↔ CriBackend translation. STATELESS by law — the
//! service structs hold `Arc<B>` and NOTHING else (listener owns nothing).
//!
//! File ownership (build-spec-r0 §8): `runtime.rs` = WP-2, `image.rs` = WP-3.
//! This file is W0-frozen — do not edit.

pub mod image;
pub mod runtime;

pub use image::ImageShell;
pub use runtime::RuntimeShell;

use lightr_cri_backend::BackendError;

/// FROZEN error→gRPC mapping (build-spec-r0 §3). Both WPs use this; nobody
/// re-derives it.
pub fn map_err(e: BackendError) -> tonic::Status {
    match e {
        BackendError::NotFound(_) => tonic::Status::not_found(e.to_string()),
        BackendError::AlreadyExists(_) => tonic::Status::already_exists(e.to_string()),
        BackendError::InvalidArgument(_) => tonic::Status::invalid_argument(e.to_string()),
        BackendError::FailedPrecondition(_) | BackendError::InUse(_) => {
            tonic::Status::failed_precondition(e.to_string())
        }
        BackendError::Internal(_) | BackendError::Io(_) => tonic::Status::internal(e.to_string()),
    }
}
