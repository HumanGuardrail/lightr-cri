//! WP-3: implement `image_service_server::ImageService` for `ImageShell<B>`
//! (build-spec-r0 §5). Translation only, same laws as runtime.rs:
//! zero state beyond `backend`; errors via `crate::map_err`;
//! spawn_blocking bridge; PullImage is resolve-only (lazy law).

use std::sync::Arc;

use lightr_cri_backend::CriBackend;

pub struct ImageShell<B: CriBackend> {
    pub backend: Arc<B>,
}

impl<B: CriBackend> ImageShell<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

// WP-3: `impl image_service_server::ImageService for ImageShell<B>` here.
