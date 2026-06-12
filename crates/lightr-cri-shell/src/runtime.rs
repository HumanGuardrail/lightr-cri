//! WP-D: implement `runtime_service_server::RuntimeService` for
//! `RuntimeShell<B>` (build-spec-r1 §3 WP-D).
//!
//! FROZEN laws (carry-over from R0 + R1 additions):
//! - State: Arc<backend> + Arc<TokenRegistry> + base_url (stream server).
//!   NOTHING ELSE. Any additional cached field = REJECT.
//!   Token registry is ephemeral by definition (crash-only): tokens lost on
//!   restart = clients retry. Document here so the pattern is explicit.
//! - Exec/Attach/PortForward: validate state via backend, mint a token on the
//!   shared registry, return an absolute URL:
//!   `http://127.0.0.1:<port>/<verb>/<token>`.
//! - v1.1 decode: RunPodSandbox decodes dns, port_mappings, host_network,
//!   hostname. CreateContainer decodes tty/stdin.
//!   PodSandboxStatus.network.ip from SandboxStatus.ip.
//!   Status.NetworkReady: honest from backend (fake → true via host_network).
//! - RuntimeConfig: literal UNIMPLEMENTED (kubelet 1.33 never calls it;
//!   alpha gate off — r1-kubelet-smoke.md law).
//! - Errors map via `crate::map_err` only.

use std::sync::Arc;

use lightr_cri_backend::{
    ContainerConfig, ContainerFilter, ContainerId, ContainerState, CriBackend, DnsConfig, Mount,
    PortMapping, Protocol, SandboxConfig, SandboxFilter, SandboxId, SandboxState,
};
use lightr_cri_proto::v1 as proto;
use lightr_cri_proto::v1::runtime_service_server::RuntimeService;
use lightr_cri_stream::{ServerHandle, StreamParams, StreamVerb, TokenRegistry};
use tonic::{Request, Response, Status};

pub struct RuntimeShell<B: CriBackend> {
    pub backend: Arc<B>,
    /// Shared token registry from the stream server (crash-only: tokens are
    /// ephemeral — lost on restart — clients retry, per spec §3 WP-D).
    pub registry: Arc<TokenRegistry>,
    /// Bound base URL of the stream server, e.g. `http://127.0.0.1:54321`.
    pub base_url: String,
}

impl<B: CriBackend> RuntimeShell<B> {
    pub fn new(backend: Arc<B>) -> Self {
        // Legacy R0 constructor — no stream server. Only used from tests that
        // don't exercise Exec/Attach/PortForward.  Stream URL methods will
        // return an empty base (no tokens minted without a registry).
        Self {
            backend,
            registry: Arc::new(TokenRegistry::new()),
            base_url: String::new(),
        }
    }

    /// R1 constructor: wire the stream server handle into the shell.
    /// `handle` must be the live handle returned by `lightr_cri_stream::serve`.
    pub fn with_stream(backend: Arc<B>, handle: &ServerHandle) -> Self {
        Self {
            backend,
            registry: Arc::clone(handle.registry()),
            base_url: handle.base_url().to_string(),
        }
    }

    /// Mint an absolute streaming URL for `verb` with `params`.
    /// Returns `Status::resource_exhausted` if the registry cap (1000) is hit.
    ///
    /// `Status` is ~176 bytes but it is the correct return type for gRPC
    /// handlers; boxing here would make every call-site awkward. Allow the
    /// large-err lint for this helper only.
    #[allow(clippy::result_large_err)]
    fn mint_url(&self, verb: StreamVerb, params: StreamParams) -> Result<String, Status> {
        let token = self
            .registry
            .mint(verb, params)
            .ok_or_else(|| Status::resource_exhausted("too many streaming tokens in flight"))?;
        Ok(format!("{}/{}/{}", self.base_url, verb.path(), token))
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

/// Decode proto DnsConfig → backend DnsConfig.
fn decode_dns(d: Option<proto::DnsConfig>) -> Option<DnsConfig> {
    d.map(|d| DnsConfig {
        servers: d.servers,
        searches: d.searches,
        options: d.options,
    })
}

/// Decode a single proto PortMapping → backend PortMapping.
fn decode_port_mapping(pm: proto::PortMapping) -> PortMapping {
    let protocol = match pm.protocol {
        // proto Protocol enum: 0=TCP, 1=UDP, 2=SCTP
        x if x == proto::Protocol::Udp as i32 => Protocol::Udp,
        x if x == proto::Protocol::Sctp as i32 => Protocol::Sctp,
        _ => Protocol::Tcp,
    };
    PortMapping {
        protocol,
        container_port: pm.container_port,
        host_port: pm.host_port,
        host_ip: pm.host_ip,
    }
}

/// Decode host_network from LinuxPodSandboxConfig.
/// network == NODE (2) → host_network=true.
fn decode_host_network(linux: &Option<proto::LinuxPodSandboxConfig>) -> bool {
    linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|sc| sc.namespace_options.as_ref())
        .map(|ns| ns.network == proto::NamespaceMode::Node as i32)
        .unwrap_or(false)
}

fn build_sandbox_status(s: &lightr_cri_backend::SandboxStatus) -> proto::PodSandboxStatus {
    // PodSandboxNetworkStatus.ip: from SandboxStatus.ip (v1.1 §A).
    // None → empty string (proto default; kubelet treats empty as "no IP").
    let network = Some(proto::PodSandboxNetworkStatus {
        ip: s.ip.clone().unwrap_or_default(),
        additional_ips: vec![],
    });
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
        network,
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

// ── Validation helpers (free-standing async fns to avoid large-err closures) ──

/// Verify a container exists and is in Running state.
// Status is ~176 bytes; it is the canonical gRPC error type here.
#[allow(clippy::result_large_err)]
async fn validate_container_running<B: CriBackend>(
    backend: Arc<B>,
    id: ContainerId,
) -> Result<(), Status> {
    tokio::task::spawn_blocking(move || {
        let cs = backend.container_status(&id).map_err(crate::map_err)?;
        if cs.state != ContainerState::Running {
            return Err(Status::failed_precondition(format!(
                "container {} is not Running (state={:?})",
                id.0, cs.state
            )));
        }
        Ok(())
    })
    .await
    .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
}

/// Verify a sandbox exists and is Ready; return its dial_target (ip or 127.0.0.1).
// Status is ~176 bytes; it is the canonical gRPC error type here.
#[allow(clippy::result_large_err)]
async fn validate_sandbox_ready_get_ip<B: CriBackend>(
    backend: Arc<B>,
    id: SandboxId,
) -> Result<String, Status> {
    tokio::task::spawn_blocking(move || {
        let ss = backend.sandbox_status(&id).map_err(crate::map_err)?;
        if ss.state != SandboxState::Ready {
            return Err(Status::failed_precondition(format!(
                "sandbox {} is not Ready (state={:?})",
                id.0, ss.state
            )));
        }
        // dial_target: sandbox ip if set, else 127.0.0.1 (host_network path)
        Ok(ss.ip.unwrap_or_else(|| "127.0.0.1".to_string()))
    })
    .await
    .map_err(|e| Status::internal(format!("spawn_blocking join error: {e}")))?
}

// ── Associated stream type for GetContainerEvents (UNIMPLEMENTED in R1) ──────

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
        // Probe-truthful (contract §D): NetworkReady reflects the backend's
        // REAL network readiness. host_network pods run regardless of this;
        // only non-host-network pods are gated on it — so reporting the truth
        // never blocks the hostNetwork smoke pod, and never lies to a
        // non-host-network sync.
        let net_ready = self.backend.network_ready();
        let conditions = vec![
            proto::RuntimeCondition {
                r#type: "RuntimeReady".to_string(),
                status: true,
                reason: String::new(),
                message: String::new(),
            },
            proto::RuntimeCondition {
                r#type: "NetworkReady".to_string(),
                status: net_ready,
                reason: if net_ready {
                    String::new()
                } else {
                    "NetworkPluginNotReady".to_string()
                },
                message: if net_ready {
                    String::new()
                } else {
                    "CNI not configured (host-network sandboxes only)".to_string()
                },
            },
        ];
        // info is empty (verbose detail deferred to R2 structured status)
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

        // v1.1 decode: dns, port_mappings, host_network (network==NODE), hostname
        let dns = decode_dns(pod_cfg.dns_config);
        let port_mappings = pod_cfg
            .port_mappings
            .into_iter()
            .map(decode_port_mapping)
            .collect();
        let host_network = decode_host_network(&pod_cfg.linux);
        let hostname = pod_cfg.hostname;

        let cfg = SandboxConfig {
            name: meta.name,
            uid: meta.uid,
            namespace: meta.namespace,
            attempt: meta.attempt,
            labels: pod_cfg.labels.into_iter().collect(),
            annotations: pod_cfg.annotations.into_iter().collect(),
            log_directory: pod_cfg.log_directory,
            hostname,
            host_network,
            dns,
            port_mappings,
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
        // v1.1: network.ip from SandboxStatus.ip
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

        // v1.1 decode: tty/stdin from ContainerConfig
        let tty = cfg_proto.tty;
        let stdin = cfg_proto.stdin;

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
            tty,
            stdin,
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

    /// Exec: validate container is Running, mint a token, return absolute URL.
    /// The stream server dials `backend.open_exec` when the kubelet connects.
    async fn exec(
        &self,
        request: Request<proto::ExecRequest>,
    ) -> Result<Response<proto::ExecResponse>, Status> {
        let req = request.into_inner();
        let id = ContainerId(req.container_id.clone());

        // Validate: container must exist and be Running.
        let backend = Arc::clone(&self.backend);
        let id2 = id.clone();
        validate_container_running(backend, id2).await?;

        let params = StreamParams {
            container: Some(req.container_id),
            sandbox: None,
            cmd: req.cmd,
            tty: req.tty,
            stdin: req.stdin,
            ports: vec![],
            dial_target: None,
        };
        let url = self.mint_url(StreamVerb::Exec, params)?;
        Ok(Response::new(proto::ExecResponse { url }))
    }

    /// Attach: validate container is Running, mint a token, return absolute URL.
    async fn attach(
        &self,
        request: Request<proto::AttachRequest>,
    ) -> Result<Response<proto::AttachResponse>, Status> {
        let req = request.into_inner();
        let id = ContainerId(req.container_id.clone());

        // Validate: container must exist and be Running.
        let backend = Arc::clone(&self.backend);
        let id2 = id.clone();
        validate_container_running(backend, id2).await?;

        let params = StreamParams {
            container: Some(req.container_id),
            sandbox: None,
            cmd: vec![],
            tty: req.tty,
            stdin: req.stdin,
            ports: vec![],
            dial_target: None,
        };
        let url = self.mint_url(StreamVerb::Attach, params)?;
        Ok(Response::new(proto::AttachResponse { url }))
    }

    /// PortForward: validate sandbox exists and is Ready, resolve dial_target
    /// (sandbox IP if present, else 127.0.0.1 for host_network), mint a token.
    async fn port_forward(
        &self,
        request: Request<proto::PortForwardRequest>,
    ) -> Result<Response<proto::PortForwardResponse>, Status> {
        let req = request.into_inner();
        let id = SandboxId(req.pod_sandbox_id.clone());

        // Validate: sandbox must exist and be Ready. Also read the sandbox ip.
        let backend = Arc::clone(&self.backend);
        let id2 = id.clone();
        let dial_target: String = validate_sandbox_ready_get_ip(backend, id2).await?;

        let params = StreamParams {
            container: None,
            sandbox: Some(req.pod_sandbox_id),
            cmd: vec![],
            tty: false,
            stdin: false,
            ports: req.port,
            dial_target: Some(dial_target),
        };
        let url = self.mint_url(StreamVerb::PortForward, params)?;
        Ok(Response::new(proto::PortForwardResponse { url }))
    }

    type GetContainerEventsStream = GetContainerEventsStream;

    async fn get_container_events(
        &self,
        _request: Request<proto::GetEventsRequest>,
    ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
        Err(Status::unimplemented(
            "GetContainerEvents: R2 — descoped (kubelet 1.33 never calls; alpha gate off)",
        ))
    }

    /// RuntimeConfig: literal UNIMPLEMENTED — kubelet 1.33 never calls this
    /// (alpha gate off; r1-kubelet-smoke.md law). Do NOT implement.
    async fn update_runtime_config(
        &self,
        _request: Request<proto::UpdateRuntimeConfigRequest>,
    ) -> Result<Response<proto::UpdateRuntimeConfigResponse>, Status> {
        Err(Status::unimplemented(
            "UpdateRuntimeConfig: not implemented (kubelet 1.33 never calls)",
        ))
    }

    async fn update_container_resources(
        &self,
        _request: Request<proto::UpdateContainerResourcesRequest>,
    ) -> Result<Response<proto::UpdateContainerResourcesResponse>, Status> {
        Err(Status::unimplemented(
            "UpdateContainerResources: R2 — see whitepaper §11",
        ))
    }

    async fn reopen_container_log(
        &self,
        _request: Request<proto::ReopenContainerLogRequest>,
    ) -> Result<Response<proto::ReopenContainerLogResponse>, Status> {
        Err(Status::unimplemented(
            "ReopenContainerLog: R2 — see whitepaper §11",
        ))
    }

    async fn checkpoint_container(
        &self,
        _request: Request<proto::CheckpointContainerRequest>,
    ) -> Result<Response<proto::CheckpointContainerResponse>, Status> {
        Err(Status::unimplemented(
            "CheckpointContainer: R2 — see whitepaper §11",
        ))
    }

    async fn pod_sandbox_stats(
        &self,
        _request: Request<proto::PodSandboxStatsRequest>,
    ) -> Result<Response<proto::PodSandboxStatsResponse>, Status> {
        Err(Status::unimplemented(
            "PodSandboxStats: R2 — descoped (kubelet 1.33 never calls; alpha gate off)",
        ))
    }

    async fn list_pod_sandbox_stats(
        &self,
        _request: Request<proto::ListPodSandboxStatsRequest>,
    ) -> Result<Response<proto::ListPodSandboxStatsResponse>, Status> {
        Err(Status::unimplemented(
            "ListPodSandboxStats: R2 — descoped (kubelet 1.33 never calls; alpha gate off)",
        ))
    }

    async fn list_metric_descriptors(
        &self,
        _request: Request<proto::ListMetricDescriptorsRequest>,
    ) -> Result<Response<proto::ListMetricDescriptorsResponse>, Status> {
        Err(Status::unimplemented(
            "ListMetricDescriptors: R2 — see whitepaper §11",
        ))
    }

    async fn list_pod_sandbox_metrics(
        &self,
        _request: Request<proto::ListPodSandboxMetricsRequest>,
    ) -> Result<Response<proto::ListPodSandboxMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "ListPodSandboxMetrics: R2 — see whitepaper §11",
        ))
    }

    /// RuntimeConfig: literal UNIMPLEMENTED — kubelet 1.33 never calls this
    /// (alpha gate off; r1-kubelet-smoke.md law). Do NOT implement.
    async fn runtime_config(
        &self,
        _request: Request<proto::RuntimeConfigRequest>,
    ) -> Result<Response<proto::RuntimeConfigResponse>, Status> {
        Err(Status::unimplemented(
            "RuntimeConfig: not implemented (kubelet 1.33 never calls; alpha gate off)",
        ))
    }

    async fn update_pod_sandbox_resources(
        &self,
        _request: Request<proto::UpdatePodSandboxResourcesRequest>,
    ) -> Result<Response<proto::UpdatePodSandboxResourcesResponse>, Status> {
        Err(Status::unimplemented(
            "UpdatePodSandboxResources: R2 — see whitepaper §11",
        ))
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

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

    // ── v1.1 decode helpers ───────────────────────────────────────────────────

    #[test]
    fn decode_dns_none() {
        assert!(decode_dns(None).is_none());
    }

    #[test]
    fn decode_dns_some() {
        let d = proto::DnsConfig {
            servers: vec!["8.8.8.8".to_string()],
            searches: vec!["cluster.local".to_string()],
            options: vec!["ndots:5".to_string()],
        };
        let result = decode_dns(Some(d)).unwrap();
        assert_eq!(result.servers, vec!["8.8.8.8"]);
        assert_eq!(result.searches, vec!["cluster.local"]);
        assert_eq!(result.options, vec!["ndots:5"]);
    }

    #[test]
    fn decode_port_mapping_tcp() {
        let pm = proto::PortMapping {
            protocol: proto::Protocol::Tcp as i32,
            container_port: 80,
            host_port: 8080,
            host_ip: "0.0.0.0".to_string(),
        };
        let result = decode_port_mapping(pm);
        assert_eq!(result.protocol, Protocol::Tcp);
        assert_eq!(result.container_port, 80);
        assert_eq!(result.host_port, 8080);
        assert_eq!(result.host_ip, "0.0.0.0");
    }

    #[test]
    fn decode_port_mapping_udp() {
        let pm = proto::PortMapping {
            protocol: proto::Protocol::Udp as i32,
            container_port: 53,
            host_port: 53,
            host_ip: String::new(),
        };
        let result = decode_port_mapping(pm);
        assert_eq!(result.protocol, Protocol::Udp);
    }

    #[test]
    fn decode_port_mapping_sctp() {
        let pm = proto::PortMapping {
            protocol: proto::Protocol::Sctp as i32,
            container_port: 9999,
            host_port: 9999,
            host_ip: String::new(),
        };
        let result = decode_port_mapping(pm);
        assert_eq!(result.protocol, Protocol::Sctp);
    }

    #[test]
    fn decode_host_network_node_mode() {
        let linux = Some(proto::LinuxPodSandboxConfig {
            cgroup_parent: String::new(),
            security_context: Some(proto::LinuxSandboxSecurityContext {
                namespace_options: Some(proto::NamespaceOption {
                    network: proto::NamespaceMode::Node as i32,
                    pid: proto::NamespaceMode::Pod as i32,
                    ipc: proto::NamespaceMode::Pod as i32,
                    target_id: String::new(),
                    userns_options: None,
                }),
                ..Default::default()
            }),
            sysctls: std::collections::HashMap::new(),
            overhead: None,
            resources: None,
        });
        assert!(decode_host_network(&linux));
    }

    #[test]
    fn decode_host_network_pod_mode() {
        let linux = Some(proto::LinuxPodSandboxConfig {
            cgroup_parent: String::new(),
            security_context: Some(proto::LinuxSandboxSecurityContext {
                namespace_options: Some(proto::NamespaceOption {
                    network: proto::NamespaceMode::Pod as i32,
                    pid: proto::NamespaceMode::Pod as i32,
                    ipc: proto::NamespaceMode::Pod as i32,
                    target_id: String::new(),
                    userns_options: None,
                }),
                ..Default::default()
            }),
            sysctls: std::collections::HashMap::new(),
            overhead: None,
            resources: None,
        });
        assert!(!decode_host_network(&linux));
    }

    #[test]
    fn decode_host_network_none() {
        assert!(!decode_host_network(&None));
    }

    #[test]
    fn sandbox_status_network_ip_populated() {
        let ss = lightr_cri_backend::SandboxStatus {
            id: SandboxId("sb-1".to_string()),
            config: lightr_cri_backend::SandboxConfig {
                name: "test".to_string(),
                uid: "uid".to_string(),
                namespace: "default".to_string(),
                attempt: 0,
                labels: Default::default(),
                annotations: Default::default(),
                log_directory: String::new(),
                hostname: String::new(),
                host_network: false,
                dns: None,
                port_mappings: vec![],
            },
            state: SandboxState::Ready,
            created_at_nanos: 0,
            ip: Some("10.0.0.5".to_string()),
            netns_path: None,
        };
        let proto_status = build_sandbox_status(&ss);
        let net = proto_status.network.unwrap();
        assert_eq!(net.ip, "10.0.0.5");
    }

    #[test]
    fn sandbox_status_network_ip_none_gives_empty() {
        let ss = lightr_cri_backend::SandboxStatus {
            id: SandboxId("sb-2".to_string()),
            config: lightr_cri_backend::SandboxConfig {
                name: "test".to_string(),
                uid: "uid".to_string(),
                namespace: "default".to_string(),
                attempt: 0,
                labels: Default::default(),
                annotations: Default::default(),
                log_directory: String::new(),
                hostname: String::new(),
                host_network: true,
                dns: None,
                port_mappings: vec![],
            },
            state: SandboxState::Ready,
            created_at_nanos: 0,
            ip: None,
            netns_path: None,
        };
        let proto_status = build_sandbox_status(&ss);
        let net = proto_status.network.unwrap();
        assert_eq!(net.ip, "");
    }

    // ── Token/URL minting ─────────────────────────────────────────────────────

    #[test]
    fn mint_url_exec_format() {
        let registry = Arc::new(TokenRegistry::new());
        let shell: RuntimeShell<lightr_cri_fake::FakeBackend> = {
            // We can't call new() without a valid state dir — use the internals directly.
            let state = tempfile::tempdir().unwrap();
            let backend = Arc::new(lightr_cri_fake::FakeBackend::open(state.path()).unwrap());
            RuntimeShell {
                backend,
                registry: Arc::clone(&registry),
                base_url: "http://127.0.0.1:12345".to_string(),
            }
        };
        let params = StreamParams {
            container: Some("ct-1".to_string()),
            sandbox: None,
            cmd: vec!["sh".to_string()],
            tty: true,
            stdin: true,
            ports: vec![],
            dial_target: None,
        };
        let url = shell.mint_url(StreamVerb::Exec, params).unwrap();
        assert!(url.starts_with("http://127.0.0.1:12345/exec/"), "url={url}");
        // token is 8 chars
        let token = url.split('/').next_back().unwrap();
        assert_eq!(token.len(), 8, "token={token}");
    }

    #[test]
    fn mint_url_portforward_format() {
        let state = tempfile::tempdir().unwrap();
        let backend = Arc::new(lightr_cri_fake::FakeBackend::open(state.path()).unwrap());
        let shell: RuntimeShell<lightr_cri_fake::FakeBackend> = RuntimeShell {
            backend,
            registry: Arc::new(TokenRegistry::new()),
            base_url: "http://127.0.0.1:9876".to_string(),
        };
        let params = StreamParams {
            container: None,
            sandbox: Some("sb-1".to_string()),
            cmd: vec![],
            tty: false,
            stdin: false,
            ports: vec![80],
            dial_target: Some("10.0.0.5".to_string()),
        };
        let url = shell.mint_url(StreamVerb::PortForward, params).unwrap();
        assert!(
            url.starts_with("http://127.0.0.1:9876/portforward/"),
            "url={url}"
        );
    }
}
