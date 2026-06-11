//! The seam crate: vocabulary types + the `CriBackend` trait.
//! FROZEN per docs/spec/build-spec-r0.md §3 and docs/contract/seam-contract-v1.md.
//! Agents transcribe; nobody edits this crate without owner sign-off.

pub mod vocab;

use std::collections::BTreeMap;

#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SandboxId(pub String);
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ContainerId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SandboxConfig {
    pub name: String,
    pub uid: String,
    pub namespace: String,
    pub attempt: u32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(default)]
    pub log_directory: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SandboxState {
    Ready,
    NotReady,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SandboxStatus {
    pub id: SandboxId,
    pub config: SandboxConfig,
    pub state: SandboxState,
    pub created_at_nanos: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Mount {
    pub container_path: String,
    pub host_path: String,
    pub readonly: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerConfig {
    pub name: String,
    pub attempt: u32,
    /// CAS vocabulary: a ref name or digest-hex into the image plane.
    pub image_ref: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_dir: String,
    #[serde(default)]
    pub envs: Vec<(String, String)>,
    #[serde(default)]
    pub mounts: Vec<Mount>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(default)]
    pub log_path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ContainerState {
    Created,
    Running,
    Exited,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerStatus {
    pub id: ContainerId,
    pub sandbox: SandboxId,
    pub config: ContainerConfig,
    pub state: ContainerState,
    pub created_at_nanos: i64,
    /// 0 = never started
    pub started_at_nanos: i64,
    /// 0 = still running / never started
    pub finished_at_nanos: i64,
    /// valid only when state == Exited
    pub exit_code: i32,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerStatsRec {
    pub id: ContainerId,
    pub timestamp_nanos: i64,
    pub cpu_usage_core_nanos: u64,
    pub memory_working_set_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulledImage {
    pub ref_name: String,
    pub root_hex: String,
    pub total_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRecord {
    pub id: String,
    pub ref_name: String,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsInfo {
    pub timestamp_nanos: i64,
    pub mountpoint: String,
    pub used_bytes: u64,
    pub inodes_used: u64,
}

#[derive(Debug)]
pub enum BackendError {
    NotFound(String),
    AlreadyExists(String),
    InvalidArgument(String),
    FailedPrecondition(String),
    /// image present & referenced by live container (RemoveImage refusal)
    InUse(String),
    Internal(String),
    Io(std::io::Error),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::NotFound(m) => write!(f, "not found: {m}"),
            BackendError::AlreadyExists(m) => write!(f, "already exists: {m}"),
            BackendError::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            BackendError::FailedPrecondition(m) => write!(f, "failed precondition: {m}"),
            BackendError::InUse(m) => write!(f, "in use: {m}"),
            BackendError::Internal(m) => write!(f, "internal: {m}"),
            BackendError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for BackendError {}

impl From<std::io::Error> for BackendError {
    fn from(e: std::io::Error) -> Self {
        BackendError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, BackendError>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxFilter {
    pub id: Option<SandboxId>,
    pub state: Option<SandboxState>,
    pub label_selector: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerFilter {
    pub id: Option<ContainerId>,
    pub sandbox: Option<SandboxId>,
    pub state: Option<ContainerState>,
    pub label_selector: BTreeMap<String, String>,
}

/// The seam. Synchronous on purpose: the real backend (hugr-lightr crates)
/// is sync; the shell bridges via spawn_blocking. Object-safe.
///
/// State law (vectors encode this): sandbox Ready→NotReady (stop)→gone
/// (remove). `create_container` requires the sandbox Ready (else
/// FailedPrecondition). Container Created→Running→Exited; `start` only from Created;
/// `stop` from Running (→Exited) or no-op from Created/Exited; `remove`
/// refused (FailedPrecondition) while Running; removing a sandbox
/// stops+removes its containers. All transitions persist BEFORE the call
/// returns (crash-only law).
pub trait CriBackend: Send + Sync + 'static {
    // sandbox plane
    fn run_sandbox(&self, cfg: SandboxConfig) -> Result<SandboxId>;
    /// idempotent
    fn stop_sandbox(&self, id: &SandboxId) -> Result<()>;
    /// idempotent; implies stop; removes its containers
    fn remove_sandbox(&self, id: &SandboxId) -> Result<()>;
    fn sandbox_status(&self, id: &SandboxId) -> Result<SandboxStatus>;
    fn list_sandboxes(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>>;

    // container plane
    fn create_container(&self, sandbox: &SandboxId, cfg: ContainerConfig) -> Result<ContainerId>;
    fn start_container(&self, id: &ContainerId) -> Result<()>;
    /// idempotent
    fn stop_container(&self, id: &ContainerId, grace_seconds: i64) -> Result<()>;
    /// idempotent; only when not Running
    fn remove_container(&self, id: &ContainerId) -> Result<()>;
    fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus>;
    fn list_containers(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatus>>;
    fn container_stats(&self, id: &ContainerId) -> Result<ContainerStatsRec>;
    fn list_container_stats(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatsRec>>;

    // exec plane (R0: sync only)
    fn exec_sync(
        &self,
        id: &ContainerId,
        cmd: &[String],
        timeout_seconds: i64,
    ) -> Result<ExecResult>;

    // image plane (lazy law: pull_image MUST NOT move file bytes)
    fn pull_image(&self, image_ref: &str) -> Result<PulledImage>;
    fn image_status(&self, image_ref: &str) -> Result<Option<ImageRecord>>;
    fn list_images(&self) -> Result<Vec<ImageRecord>>;
    /// idempotent: not-found → Ok; refuses InUse while referenced by a live container
    fn remove_image(&self, image_ref: &str) -> Result<()>;
    fn image_fs_info(&self) -> Result<FsInfo>;
}
