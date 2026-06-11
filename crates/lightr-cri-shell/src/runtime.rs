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

use lightr_cri_backend::{
    ContainerConfig, ContainerFilter, ContainerId, ContainerState, CriBackend, Mount,
    SandboxConfig, SandboxFilter, SandboxId, SandboxState,
};
use lightr_cri_proto::v1 as proto;
use lightr_cri_proto::v1::runtime_service_server::RuntimeService;
use tonic::{Request, Response, Status};

pub struct RuntimeShell<B: CriBackend> {
    pub backend: Arc<B>,
}

impl<B: CriBackend> RuntimeShell<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

// ── Pure mapping helpers (also tested below) ─────────────────────────────────

fn map_sandbox_state(s: SandboxState) -> i32 {
    match s {
        SandboxState::Ready => proto::PodSandboxState::SandboxReady as i32,
        SandboxState::NotReady => proto::PodSandboxState::SandboxNotready as i32,
    }
}

fn map_container_state(s: ContainerState) -> i32 {
    match s {
        ContainerState::Created => proto::ContainerState::ContainerCreated as i32,
        ContainerState::Running => proto::ContainerState::ContainerRunning as i32,
        ContainerState::Exited => proto::ContainerState::ContainerExited as i32,
        ContainerState::Unknown => proto::ContainerState::ContainerUnknown as i32,
    }
}

fn proto_sandbox_filter(f: Option<proto::PodSandboxFilter>) -> SandboxFilter {
    match f {
        None => SandboxFilter::default(),
        Some(f) => SandboxFilter {
            id: if f.id.is_empty() {
                None
            } else {
                Some(SandboxId(f.id))
            },
            state: f.state.map(|sv| {
                if sv.state == proto::PodSandboxState::SandboxReady as i32 {
                    SandboxState::Ready
                } else {
                    SandboxState::NotReady
                }
            }),
            label_selector: f.label_selector.into_iter().collect(),
        },
    }
}

fn proto_container_filter(f: Option<proto::ContainerFilter>) -> ContainerFilter {
    match f {
        None => ContainerFilter::default(),
        Some(f) => ContainerFilter {
            id: if f.id.is_empty() {
                None
            } else {
                Some(ContainerId(f.id))
            },
            sandbox: if f.pod_sandbox_id.is_empty() {
                None
            } else {
                Some(SandboxId(f.pod_sandbox_id))
            },
            state: f.state.map(|sv| match sv.state {
                x if x == proto::ContainerState::ContainerCreated as i32 => ContainerState::Created,
                x if x == proto::ContainerState::ContainerRunning as i32 => ContainerState::Running,
                x if x == proto::ContainerState::ContainerExited as i32 => ContainerState::Exited,
                _ => ContainerState::Unknown,
            }),
            label_selector: f.label_selector.into_iter().collect(),
        },
    }
}

fn proto_stats_filter(f: Option<proto::ContainerStatsFilter>) -> ContainerFilter {
    match f {
        None => ContainerFilter::default(),
        Some(f) => ContainerFilter {
            id: if f.id.is_empty() {
                None
            } else {
                Some(ContainerId(f.id))
            },
            sandbox: if f.pod_sandbox_id.is_empty() {
                None
            } else {
                Some(SandboxId(f.pod_sandbox_id))
            },
            state: None,
            label_selector: f.label_selector.into_iter().collect(),
        },
    }
}

fn build_sandbox_status(s: &lightr_cri_backend::SandboxStatus) -> proto::PodSandboxStatus {
    proto::PodSandboxStatus {
        id: s.id.0.clone(),
        metadata: Some(proto::PodSandboxMetadata {
            name: s.config.name.clone(),
            uid: s.config.uid.clone(),
            namespace: s.config.namespace.clone(),
            attempt: s.config.attempt,
        }),
        state: map_sandbox_state(s.state),
        created_at: s.created_at_nanos,
        network: None,
        linux: None,
        labels: s.config.labels.clone().into_iter().collect(),
        annotations: s.config.annotations.clone().into_iter().collect(),
        runtime_handler: String::new(),
    }
}

fn build_pod_sandbox(s: &lightr_cri_backend::SandboxStatus) -> proto::PodSandbox {
    proto::PodSandbox {
        id: s.id.0.clone(),
        metadata: Some(proto::PodSandboxMetadata {
            name: s.config.name.clone(),
            uid: s.config.uid.clone(),
            namespace: s.config.namespace.clone(),
            attempt: s.config.attempt,
        }),
        state: map_sandbox_state(s.state),
        created_at: s.created_at_nanos,
        labels: s.config.labels.clone().into_iter().collect(),
        annotations: s.config.annotations.clone().into_iter().collect(),
        runtime_handler: String::new(),
    }
}

fn build_container_status(cs: &lightr_cri_backend::ContainerStatus) -> proto::ContainerStatus {
    proto::ContainerStatus {
        id: cs.id.0.clone(),
        metadata: Some(proto::ContainerMetadata {
            name: cs.config.name.clone(),
            attempt: cs.config.attempt,
        }),
        state: map_container_state(cs.state),
        created_at: cs.created_at_nanos,
        started_at: cs.started_at_nanos,
        finished_at: cs.finished_at_nanos,
        exit_code: cs.exit_code,
        image: Some(proto::ImageSpec {
            image: cs.config.image_ref.clone(),
            ..Default::default()
        }),
        image_ref: cs.config.image_ref.clone(),
        image_id: cs.config.image_ref.clone(),
        reason: cs.reason.clone(),
        message: cs.message.clone(),
        labels: cs.config.labels.clone().into_iter().collect(),
        annotations: cs.config.annotations.clone().into_iter().collect(),
        mounts: cs
            .config
            .mounts
            .iter()
            .map(|m| proto::Mount {
                container_path: m.container_path.clone(),
                host_path: m.host_path.clone(),
                readonly: m.readonly,
                ..Default::default()
            })
            .collect(),
        log_path: cs.config.log_path.clone(),
        resources: None,
        user: None,
        stop_signal: 0,
    }
}

fn build_container(cs: &lightr_cri_backend::ContainerStatus) -> proto::Container {
    proto::Container {
        id: cs.id.0.clone(),
        pod_sandbox_id: cs.sandbox.0.clone(),
        metadata: Some(proto::ContainerMetadata {
            name: cs.config.name.clone(),
            attempt: cs.config.attempt,
        }),
        image: Some(proto::ImageSpec {
            image: cs.config.image_ref.clone(),
            ..Default::default()
        }),
        image_ref: cs.config.image_ref.clone(),
        image_id: cs.config.image_ref.clone(),
        state: map_container_state(cs.state),
        created_at: cs.created_at_nanos,
        labels: cs.config.labels.clone().into_iter().collect(),
        annotations: cs.config.annotations.clone().into_iter().collect(),
    }
}

fn build_container_stats(
    rec: &lightr_cri_backend::ContainerStatsRec,
    status: Option<&lightr_cri_backend::ContainerStatus>,
) -> proto::ContainerStats {
    let attributes = proto::ContainerAttributes {
        id: rec.id.0.clone(),
        metadata: status.map(|s| proto::ContainerMetadata {
            name: s.config.name.clone(),
            attempt: s.config.attempt,
        }),
        labels: status
            .map(|s| s.config.labels.clone().into_iter().collect())
            .unwrap_or_default(),
        annotations: status
            .map(|s| s.config.annotations.clone().into_iter().collect())
            .unwrap_or_default(),
    };
    proto::ContainerStats {
        attributes: Some(attributes),
        cpu: Some(proto::CpuUsage {
            timestamp: rec.timestamp_nanos,
            usage_core_nano_seconds: Some(proto::UInt64Value {
                value: rec.cpu_usage_core_nanos,
            }),
            usage_nano_cores: None,
            psi: None,
        }),
        memory: Some(proto::MemoryUsage {
            timestamp: rec.timestamp_nanos,
            working_set_bytes: Some(proto::UInt64Value {
                value: rec.memory_working_set_bytes,
            }),
            available_bytes: None,
            usage_bytes: None,
            rss_bytes: None,
            page_faults: None,
            major_page_faults: None,
            psi: None,
        }),
        writable_layer: Some(proto::FilesystemUsage {
            timestamp: rec.timestamp_nanos,
            fs_id: None,
            used_bytes: Some(proto::UInt64Value { value: 0 }),
            inodes_used: Some(proto::UInt64Value { value: 0 }),
        }),
        swap: None,
        io: None,
    }
}

// ── Associated stream type for GetContainerEvents (UNIMPLEMENTED in R0) ──────

type GetContainerEventsStream = std::pin::Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<
                Item = Result<proto::ContainerEventResponse, Status>,
            > + Send
            + 'static,
    >,
>;

// ── RuntimeService impl ───────────────────────────────────────────────────────

#[tonic::async_trait]
impl<B: CriBackend> RuntimeService for RuntimeShell<B> {
    async fn version(
        &self,
        request: Request<proto::VersionRequest>,
    ) -> Result<Response<proto::VersionResponse>, Status> {
        let api_version = request.into_inner().version;
        Ok(Response::new(proto::VersionResponse {
            version: api_version,
            runtime_name: "lightr".to_string(),
            runtime_version: "0.1.0".to_string(),
            runtime_api_version: "v1".to_string(),
        }))
    }

    async fn status(
        &self,
        request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let verbose = request.into_inner().verbose;
        let conditions = vec![
            proto::RuntimeCondition {
                r#type: "RuntimeReady".to_string(),
                status: true,
                reason: String::new(),
                message: String::new(),
            },
            proto::RuntimeCondition {
                r#type: "NetworkReady".to_string(),
                status: true,
                reason: String::new(),
                message: "r0-fake-network".to_string(),
            },
        ];
        // info is always empty in R0 regardless of verbose
        let _ = verbose;
        Ok(Response::new(proto::StatusResponse {
            status: Some(proto::RuntimeStatus { conditions }),
            info: std::collections::HashMap::new(),
            runtime_handlers: vec![],
            features: None,
        }))
    }

    // ── Sandbox plane ─────────────────────────────────────────────────────────

    async fn run_pod_sandbox(
        &self,
        request: Request<proto::RunPodSandboxRequest>,
    ) -> Result<Response<proto::RunPodSandboxResponse>, Status> {
        let req = request.into_inner();
        let pod_cfg = req
            .config
            .ok_or_else(|| Status::invalid_argument("config is required"))?;
        let meta = pod_cfg
            .metadata
            .ok_or_else(|| Status::invalid_argument("config.metadata is required"))?;
        let cfg = SandboxConfig {
            name: meta.name,
            uid: meta.uid,
            namespace: meta.namespace,
            attempt: meta.attempt,
            labels: pod_cfg.labels.into_iter().collect(),
            annotations: pod_cfg.annotations.into_iter().collect(),
            log_directory: pod_cfg.log_directory,
        };
        let backend = Arc::clone(&self.backend);
        let id = tokio::task::spawn_blocking(move || backend.run_sandbox(cfg))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::RunPodSandboxResponse {
            pod_sandbox_id: id.0,
        }))
    }

    async fn stop_pod_sandbox(
        &self,
        request: Request<proto::StopPodSandboxRequest>,
    ) -> Result<Response<proto::StopPodSandboxResponse>, Status> {
        let id = SandboxId(request.into_inner().pod_sandbox_id);
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.stop_sandbox(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::StopPodSandboxResponse {}))
    }

    async fn remove_pod_sandbox(
        &self,
        request: Request<proto::RemovePodSandboxRequest>,
    ) -> Result<Response<proto::RemovePodSandboxResponse>, Status> {
        let id = SandboxId(request.into_inner().pod_sandbox_id);
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.remove_sandbox(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::RemovePodSandboxResponse {}))
    }

    async fn pod_sandbox_status(
        &self,
        request: Request<proto::PodSandboxStatusRequest>,
    ) -> Result<Response<proto::PodSandboxStatusResponse>, Status> {
        let req = request.into_inner();
        let id = SandboxId(req.pod_sandbox_id);
        let backend = Arc::clone(&self.backend);
        let s = tokio::task::spawn_blocking(move || backend.sandbox_status(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let status = build_sandbox_status(&s);
        Ok(Response::new(proto::PodSandboxStatusResponse {
            status: Some(status),
            info: std::collections::HashMap::new(),
            containers_statuses: vec![],
            timestamp: 0,
        }))
    }

    async fn list_pod_sandbox(
        &self,
        request: Request<proto::ListPodSandboxRequest>,
    ) -> Result<Response<proto::ListPodSandboxResponse>, Status> {
        let filter = proto_sandbox_filter(request.into_inner().filter);
        let backend = Arc::clone(&self.backend);
        let sandboxes = tokio::task::spawn_blocking(move || backend.list_sandboxes(&filter))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let items = sandboxes.iter().map(build_pod_sandbox).collect();
        Ok(Response::new(proto::ListPodSandboxResponse { items }))
    }

    // ── Container plane ───────────────────────────────────────────────────────

    async fn create_container(
        &self,
        request: Request<proto::CreateContainerRequest>,
    ) -> Result<Response<proto::CreateContainerResponse>, Status> {
        let req = request.into_inner();
        let sandbox_id = SandboxId(req.pod_sandbox_id);
        let cfg_proto = req
            .config
            .ok_or_else(|| Status::invalid_argument("config is required"))?;
        let meta = cfg_proto
            .metadata
            .ok_or_else(|| Status::invalid_argument("config.metadata is required"))?;
        let image_ref = cfg_proto.image.map(|i| i.image).unwrap_or_default();
        let cfg = ContainerConfig {
            name: meta.name,
            attempt: meta.attempt,
            image_ref,
            command: cfg_proto.command,
            args: cfg_proto.args,
            working_dir: cfg_proto.working_dir,
            envs: cfg_proto
                .envs
                .into_iter()
                .map(|kv| (kv.key, kv.value))
                .collect(),
            mounts: cfg_proto
                .mounts
                .into_iter()
                .map(|m| Mount {
                    container_path: m.container_path,
                    host_path: m.host_path,
                    readonly: m.readonly,
                })
                .collect(),
            labels: cfg_proto.labels.into_iter().collect(),
            annotations: cfg_proto.annotations.into_iter().collect(),
            log_path: cfg_proto.log_path,
        };
        let backend = Arc::clone(&self.backend);
        let id = tokio::task::spawn_blocking(move || backend.create_container(&sandbox_id, cfg))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::CreateContainerResponse {
            container_id: id.0,
        }))
    }

    async fn start_container(
        &self,
        request: Request<proto::StartContainerRequest>,
    ) -> Result<Response<proto::StartContainerResponse>, Status> {
        let id = ContainerId(request.into_inner().container_id);
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.start_container(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::StartContainerResponse {}))
    }

    async fn stop_container(
        &self,
        request: Request<proto::StopContainerRequest>,
    ) -> Result<Response<proto::StopContainerResponse>, Status> {
        let req = request.into_inner();
        let id = ContainerId(req.container_id);
        let grace = req.timeout;
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.stop_container(&id, grace))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::StopContainerResponse {}))
    }

    async fn remove_container(
        &self,
        request: Request<proto::RemoveContainerRequest>,
    ) -> Result<Response<proto::RemoveContainerResponse>, Status> {
        let id = ContainerId(request.into_inner().container_id);
        let backend = Arc::clone(&self.backend);
        tokio::task::spawn_blocking(move || backend.remove_container(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::RemoveContainerResponse {}))
    }

    async fn list_containers(
        &self,
        request: Request<proto::ListContainersRequest>,
    ) -> Result<Response<proto::ListContainersResponse>, Status> {
        let filter = proto_container_filter(request.into_inner().filter);
        let backend = Arc::clone(&self.backend);
        let containers = tokio::task::spawn_blocking(move || backend.list_containers(&filter))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let containers = containers.iter().map(build_container).collect();
        Ok(Response::new(proto::ListContainersResponse { containers }))
    }

    async fn container_status(
        &self,
        request: Request<proto::ContainerStatusRequest>,
    ) -> Result<Response<proto::ContainerStatusResponse>, Status> {
        let req = request.into_inner();
        let id = ContainerId(req.container_id);
        let backend = Arc::clone(&self.backend);
        let cs = tokio::task::spawn_blocking(move || backend.container_status(&id))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        let status = build_container_status(&cs);
        Ok(Response::new(proto::ContainerStatusResponse {
            status: Some(status),
            info: std::collections::HashMap::new(),
        }))
    }

    async fn container_stats(
        &self,
        request: Request<proto::ContainerStatsRequest>,
    ) -> Result<Response<proto::ContainerStatsResponse>, Status> {
        let id = ContainerId(request.into_inner().container_id);
        let backend = Arc::clone(&self.backend);
        let id2 = id.clone();
        let (rec, status_result) = tokio::task::spawn_blocking(move || {
            let rec = backend.container_stats(&id)?;
            let st = backend.container_status(&id2);
            Ok::<_, lightr_cri_backend::BackendError>((rec, st))
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
        .map_err(crate::map_err)?;
        let cs_opt = status_result.ok();
        let stats = build_container_stats(&rec, cs_opt.as_ref());
        Ok(Response::new(proto::ContainerStatsResponse {
            stats: Some(stats),
        }))
    }

    async fn list_container_stats(
        &self,
        request: Request<proto::ListContainerStatsRequest>,
    ) -> Result<Response<proto::ListContainerStatsResponse>, Status> {
        let filter = proto_stats_filter(request.into_inner().filter);
        let backend = Arc::clone(&self.backend);
        // Re-use the same filter for status lookup as well
        let filter2 = filter.clone();
        let (recs, statuses) = tokio::task::spawn_blocking(move || {
            let recs = backend.list_container_stats(&filter)?;
            let statuses = backend.list_containers(&filter2);
            Ok::<_, lightr_cri_backend::BackendError>((recs, statuses))
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
        .map_err(crate::map_err)?;
        let status_map: std::collections::HashMap<String, lightr_cri_backend::ContainerStatus> =
            statuses
                .unwrap_or_default()
                .into_iter()
                .map(|s| (s.id.0.clone(), s))
                .collect();
        let stats = recs
            .iter()
            .map(|rec| {
                let st = status_map.get(&rec.id.0);
                build_container_stats(rec, st)
            })
            .collect();
        Ok(Response::new(proto::ListContainerStatsResponse { stats }))
    }

    // ── Exec plane ────────────────────────────────────────────────────────────

    async fn exec_sync(
        &self,
        request: Request<proto::ExecSyncRequest>,
    ) -> Result<Response<proto::ExecSyncResponse>, Status> {
        let req = request.into_inner();
        let id = ContainerId(req.container_id);
        let cmd = req.cmd;
        let timeout = req.timeout;
        let backend = Arc::clone(&self.backend);
        let result = tokio::task::spawn_blocking(move || backend.exec_sync(&id, &cmd, timeout))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
            .map_err(crate::map_err)?;
        Ok(Response::new(proto::ExecSyncResponse {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
        }))
    }

    // ── R1 unimplemented ──────────────────────────────────────────────────────

    async fn exec(
        &self,
        _request: Request<proto::ExecRequest>,
    ) -> Result<Response<proto::ExecResponse>, Status> {
        Err(Status::unimplemented("Exec: R1 — see whitepaper §11"))
    }

    async fn attach(
        &self,
        _request: Request<proto::AttachRequest>,
    ) -> Result<Response<proto::AttachResponse>, Status> {
        Err(Status::unimplemented("Attach: R1 — see whitepaper §11"))
    }

    async fn port_forward(
        &self,
        _request: Request<proto::PortForwardRequest>,
    ) -> Result<Response<proto::PortForwardResponse>, Status> {
        Err(Status::unimplemented(
            "PortForward: R1 — see whitepaper §11",
        ))
    }

    type GetContainerEventsStream = GetContainerEventsStream;

    async fn get_container_events(
        &self,
        _request: Request<proto::GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        Err(Status::unimplemented(
            "GetContainerEvents: R1 — see whitepaper §11",
        ))
    }

    async fn update_runtime_config(
        &self,
        _request: Request<proto::UpdateRuntimeConfigRequest>,
    ) -> Result<Response<proto::UpdateRuntimeConfigResponse>, Status> {
        Err(Status::unimplemented(
            "UpdateRuntimeConfig: R1 — see whitepaper §11",
        ))
    }

    async fn update_container_resources(
        &self,
        _request: Request<proto::UpdateContainerResourcesRequest>,
    ) -> Result<Response<proto::UpdateContainerResourcesResponse>, Status> {
        Err(Status::unimplemented(
            "UpdateContainerResources: R1 — see whitepaper §11",
        ))
    }

    async fn reopen_container_log(
        &self,
        _request: Request<proto::ReopenContainerLogRequest>,
    ) -> Result<Response<proto::ReopenContainerLogResponse>, Status> {
        Err(Status::unimplemented(
            "ReopenContainerLog: R1 — see whitepaper §11",
        ))
    }

    async fn checkpoint_container(
        &self,
        _request: Request<proto::CheckpointContainerRequest>,
    ) -> Result<Response<proto::CheckpointContainerResponse>, Status> {
        Err(Status::unimplemented(
            "CheckpointContainer: R1 — see whitepaper §11",
        ))
    }

    async fn pod_sandbox_stats(
        &self,
        _request: Request<proto::PodSandboxStatsRequest>,
    ) -> Result<Response<proto::PodSandboxStatsResponse>, Status> {
        Err(Status::unimplemented(
            "PodSandboxStats: R1 — see whitepaper §11",
        ))
    }

    async fn list_pod_sandbox_stats(
        &self,
        _request: Request<proto::ListPodSandboxStatsRequest>,
    ) -> Result<Response<proto::ListPodSandboxStatsResponse>, Status> {
        Err(Status::unimplemented(
            "ListPodSandboxStats: R1 — see whitepaper §11",
        ))
    }

    async fn list_metric_descriptors(
        &self,
        _request: Request<proto::ListMetricDescriptorsRequest>,
    ) -> Result<Response<proto::ListMetricDescriptorsResponse>, Status> {
        Err(Status::unimplemented(
            "ListMetricDescriptors: R1 — see whitepaper §11",
        ))
    }

    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<proto::ListPodSandboxMetricsRequest>,
    ) -> Result<Response<proto::ListPodSandboxMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "ListPodSandboxMetrics: R1 — see whitepaper §11",
        ))
    }

    async fn runtime_config(
        &self,
        _request: Request<proto::RuntimeConfigRequest>,
    ) -> Result<Response<proto::RuntimeConfigResponse>, Status> {
        Err(Status::unimplemented(
            "RuntimeConfig: R1 — see whitepaper §11",
        ))
    }

    async fn update_pod_sandbox_resources(
        &self,
        _request: Request<proto::UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<proto::UpdatePodSandboxResourcesResponse>, Status> {
        Err(Status::unimplemented(
            "UpdatePodSandboxResources: R1 — see whitepaper §11",
        ))
    }
}

// ── Unit tests for pure mapping helpers ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use lightr_cri_backend::{ContainerState, SandboxState};
    use lightr_cri_proto::v1 as proto;

    #[test]
    fn sandbox_state_ready() {
        assert_eq!(
            map_sandbox_state(SandboxState::Ready),
            proto::PodSandboxState::SandboxReady as i32
        );
    }

    #[test]
    fn sandbox_state_not_ready() {
        assert_eq!(
            map_sandbox_state(SandboxState::NotReady),
            proto::PodSandboxState::SandboxNotready as i32
        );
    }

    #[test]
    fn container_state_created() {
        assert_eq!(
            map_container_state(ContainerState::Created),
            proto::ContainerState::ContainerCreated as i32
        );
    }

    #[test]
    fn container_state_running() {
        assert_eq!(
            map_container_state(ContainerState::Running),
            proto::ContainerState::ContainerRunning as i32
        );
    }

    #[test]
    fn container_state_exited() {
        assert_eq!(
            map_container_state(ContainerState::Exited),
            proto::ContainerState::ContainerExited as i32
        );
    }

    #[test]
    fn container_state_unknown() {
        assert_eq!(
            map_container_state(ContainerState::Unknown),
            proto::ContainerState::ContainerUnknown as i32
        );
    }

    #[test]
    fn sandbox_filter_empty() {
        let f = proto_sandbox_filter(None);
        assert_eq!(f, SandboxFilter::default());
    }

    #[test]
    fn sandbox_filter_with_id_and_state() {
        let proto_f = proto::PodSandboxFilter {
            id: "abc".to_string(),
            state: Some(proto::PodSandboxStateValue {
                state: proto::PodSandboxState::SandboxReady as i32,
            }),
            label_selector: std::collections::HashMap::new(),
        };
        let f = proto_sandbox_filter(Some(proto_f));
        assert_eq!(f.id, Some(SandboxId("abc".to_string())));
        assert_eq!(f.state, Some(SandboxState::Ready));
    }

    #[test]
    fn sandbox_filter_not_ready_state() {
        let proto_f = proto::PodSandboxFilter {
            id: String::new(),
            state: Some(proto::PodSandboxStateValue {
                state: proto::PodSandboxState::SandboxNotready as i32,
            }),
            label_selector: std::collections::HashMap::new(),
        };
        let f = proto_sandbox_filter(Some(proto_f));
        assert_eq!(f.id, None);
        assert_eq!(f.state, Some(SandboxState::NotReady));
    }

    #[test]
    fn container_filter_empty() {
        let f = proto_container_filter(None);
        assert_eq!(f, ContainerFilter::default());
    }

    #[test]
    fn container_filter_with_fields() {
        let mut labels = std::collections::HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        let proto_f = proto::ContainerFilter {
            id: "cid".to_string(),
            pod_sandbox_id: "sid".to_string(),
            state: Some(proto::ContainerStateValue {
                state: proto::ContainerState::ContainerRunning as i32,
            }),
            label_selector: labels,
        };
        let f = proto_container_filter(Some(proto_f));
        assert_eq!(f.id, Some(ContainerId("cid".to_string())));
        assert_eq!(f.sandbox, Some(SandboxId("sid".to_string())));
        assert_eq!(f.state, Some(ContainerState::Running));
        assert_eq!(f.label_selector.get("app"), Some(&"nginx".to_string()));
    }

    #[test]
    fn stats_filter_maps_sandbox_id() {
        let proto_f = proto::ContainerStatsFilter {
            id: String::new(),
            pod_sandbox_id: "sandbox-1".to_string(),
            label_selector: std::collections::HashMap::new(),
        };
        let f = proto_stats_filter(Some(proto_f));
        assert_eq!(f.sandbox, Some(SandboxId("sandbox-1".to_string())));
        assert_eq!(f.id, None);
        assert_eq!(f.state, None);
    }
}
