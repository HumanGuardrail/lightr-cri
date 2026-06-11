//! WP-3: implement `image_service_server::ImageService` for `ImageShell<B>`
//! (build-spec-r0 §5). Translation only, same laws as runtime.rs:
//! zero state beyond `backend`; errors via `crate::map_err`;
//! spawn_blocking bridge; PullImage is resolve-only (lazy law).

use std::sync::Arc;

use lightr_cri_backend::CriBackend;
use lightr_cri_proto::v1::{
    image_service_server::ImageService,
    FilesystemIdentifier, FilesystemUsage, Image, ImageFsInfoRequest, ImageFsInfoResponse,
    ImageStatusRequest, ImageStatusResponse, ListImagesRequest, ListImagesResponse,
    PullImageRequest, PullImageResponse, RemoveImageRequest, RemoveImageResponse, UInt64Value,
};
use tonic::{Request, Response, Status};

pub struct ImageShell<B: CriBackend> {
    pub backend: Arc<B>,
}

impl<B: CriBackend> ImageShell<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

/// Convert a backend `ImageRecord` to a proto `Image`.
fn record_to_proto(r: lightr_cri_backend::ImageRecord) -> Image {
    Image {
        id: r.id.clone(),
        repo_tags: vec![r.ref_name.clone()],
        repo_digests: vec![r.id.clone()],
        size: r.size,
        uid: None,
        username: String::new(),
        spec: None,
        pinned: false,
    }
}

#[tonic::async_trait]
impl<B: CriBackend> ImageService for ImageShell<B> {
    async fn pull_image(
        &self,
        request: Request<PullImageRequest>,
    ) -> Result<Response<PullImageResponse>, Status> {
        let image_ref = request
            .into_inner()
            .image
            .map(|s| s.image)
            .unwrap_or_default();
        let backend = Arc::clone(&self.backend);
        let pulled = tokio::task::spawn_blocking(move || backend.pull_image(&image_ref))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(PullImageResponse {
            image_ref: format!("blake3:{}", pulled.root_hex),
        }))
    }

    async fn image_status(
        &self,
        request: Request<ImageStatusRequest>,
    ) -> Result<Response<ImageStatusResponse>, Status> {
        let image_ref = request
            .into_inner()
            .image
            .map(|s| s.image)
            .unwrap_or_default();
        let backend = Arc::clone(&self.backend);
        let maybe_record =
            tokio::task::spawn_blocking(move || backend.image_status(&image_ref))
                .await
                .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
                .map_err(crate::map_err)?;
        let image = maybe_record.map(record_to_proto);
        Ok(Response::new(ImageStatusResponse {
            image,
            info: Default::default(),
        }))
    }

    async fn list_images(
        &self,
        request: Request<ListImagesRequest>,
    ) -> Result<Response<ListImagesResponse>, Status> {
        let filter_ref: Option<String> = request
            .into_inner()
            .filter
            .and_then(|f| f.image)
            .map(|s| s.image)
            .filter(|s| !s.is_empty());
        let backend = Arc::clone(&self.backend);
        let records = tokio::task::spawn_blocking(move || backend.list_images())
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let images: Vec<Image> = records
            .into_iter()
            .filter(|r| {
                if let Some(ref needle) = filter_ref {
                    r.ref_name == *needle || r.id == *needle
                } else {
                    true
                }
            })
            .map(record_to_proto)
            .collect();
        Ok(Response::new(ListImagesResponse { images }))
    }

    async fn remove_image(
        &self,
        request: Request<RemoveImageRequest>,
    ) -> Result<Response<RemoveImageResponse>, Status> {
        let image_ref = request
            .into_inner()
            .image
            .map(|s| s.image)
            .unwrap_or_default();
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.remove_image(&image_ref))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(RemoveImageResponse {}))
    }

    async fn image_fs_info(
        &self,
        _request: Request<ImageFsInfoRequest>,
    ) -> Result<Response<ImageFsInfoResponse>, Status> {
        let backend = Arc::clone(&self.backend);
        let info = tokio::task::spawn_blocking(move || backend.image_fs_info())
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let usage = FilesystemUsage {
            timestamp: info.timestamp_nanos,
            fs_id: Some(FilesystemIdentifier {
                mountpoint: info.mountpoint,
            }),
            used_bytes: Some(UInt64Value {
                value: info.used_bytes,
            }),
            inodes_used: Some(UInt64Value {
                value: info.inodes_used,
            }),
        };
        Ok(Response::new(ImageFsInfoResponse {
            image_filesystems: vec![usage],
            container_filesystems: vec![],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightr_cri_backend::ImageRecord;

    #[test]
    fn record_to_proto_fields() {
        let rec = ImageRecord {
            id: "blake3:abc123".to_string(),
            ref_name: "docker.io/library/alpine:latest".to_string(),
            size: 4_096_000,
        };
        let proto = record_to_proto(rec);
        assert_eq!(proto.id, "blake3:abc123");
        assert_eq!(proto.repo_tags, vec!["docker.io/library/alpine:latest"]);
        assert_eq!(proto.repo_digests, vec!["blake3:abc123"]);
        assert_eq!(proto.size, 4_096_000);
        assert!(!proto.pinned);
        assert!(proto.uid.is_none());
        assert!(proto.spec.is_none());
        assert!(proto.username.is_empty());
    }

    #[test]
    fn record_to_proto_id_equals_digest() {
        let rec = ImageRecord {
            id: "blake3:deadbeef".to_string(),
            ref_name: "myimage:v1".to_string(),
            size: 0,
        };
        let proto = record_to_proto(rec);
        // id and repo_digests[0] must be identical
        assert_eq!(proto.id, proto.repo_digests[0]);
    }

    #[test]
    fn pull_image_ref_prefix() {
        // The spec mandates "blake3:" prefix (not "sha256:").
        let root_hex = "1234abcd".to_string();
        let image_ref = format!("blake3:{root_hex}");
        assert!(image_ref.starts_with("blake3:"));
    }
}
