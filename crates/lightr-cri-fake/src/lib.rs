//! The honest fake backend (build-spec-r0 §4) — WP-1 fills this in.
//!
//! Laws (FROZEN, transcribe — do not reinterpret):
//! - File-backed: state root (env `LIGHTR_CRI_STATE`, default
//!   `$TMPDIR/lightr-cri-fake`): `sandboxes/<id>.json`, `containers/<id>.json`,
//!   `images/<name-hash>.json`. Atomic write law: tmp + rename, fsync file.
//! - In-memory index is a CACHE rebuilt from disk at `open` — kill -9 at any
//!   point loses nothing (acceptance A8 proves it).
//! - Execution is REAL: `start_container` spawns the configured command as a
//!   plain host process (no isolation); `exec_sync` really runs and captures.
//! - Images are fake CAS records: `pull_image` synthesizes PulledImage with a
//!   BLAKE3 root over the ref string, instantly (lazy law rehearsed); refuse
//!   unparseable refs with InvalidArgument.
//! - Stats: real /proc/<pid>-based on Linux; zeroed-with-timestamp elsewhere
//!   (probe-truthful).

use std::path::Path;

use lightr_cri_backend::*;

pub struct FakeBackend {
    // WP-1: state root + in-memory cache rebuilt from disk.
    _todo: (),
}

impl FakeBackend {
    /// Open (or create) the state root and rebuild the cache from disk.
    pub fn open(_state_root: &Path) -> std::io::Result<Self> {
        todo!("WP-1")
    }
}

impl CriBackend for FakeBackend {
    fn run_sandbox(&self, _cfg: SandboxConfig) -> Result<SandboxId> {
        todo!("WP-1")
    }
    fn stop_sandbox(&self, _id: &SandboxId) -> Result<()> {
        todo!("WP-1")
    }
    fn remove_sandbox(&self, _id: &SandboxId) -> Result<()> {
        todo!("WP-1")
    }
    fn sandbox_status(&self, _id: &SandboxId) -> Result<SandboxStatus> {
        todo!("WP-1")
    }
    fn list_sandboxes(&self, _filter: &SandboxFilter) -> Result<Vec<SandboxStatus>> {
        todo!("WP-1")
    }
    fn create_container(&self, _sandbox: &SandboxId, _cfg: ContainerConfig) -> Result<ContainerId> {
        todo!("WP-1")
    }
    fn start_container(&self, _id: &ContainerId) -> Result<()> {
        todo!("WP-1")
    }
    fn stop_container(&self, _id: &ContainerId, _grace_seconds: i64) -> Result<()> {
        todo!("WP-1")
    }
    fn remove_container(&self, _id: &ContainerId) -> Result<()> {
        todo!("WP-1")
    }
    fn container_status(&self, _id: &ContainerId) -> Result<ContainerStatus> {
        todo!("WP-1")
    }
    fn list_containers(&self, _filter: &ContainerFilter) -> Result<Vec<ContainerStatus>> {
        todo!("WP-1")
    }
    fn container_stats(&self, _id: &ContainerId) -> Result<ContainerStatsRec> {
        todo!("WP-1")
    }
    fn list_container_stats(&self, _filter: &ContainerFilter) -> Result<Vec<ContainerStatsRec>> {
        todo!("WP-1")
    }
    fn exec_sync(&self, _id: &ContainerId, _cmd: &[String], _timeout_seconds: i64) -> Result<ExecResult> {
        todo!("WP-1")
    }
    fn pull_image(&self, _image_ref: &str) -> Result<PulledImage> {
        todo!("WP-1")
    }
    fn image_status(&self, _image_ref: &str) -> Result<Option<ImageRecord>> {
        todo!("WP-1")
    }
    fn list_images(&self) -> Result<Vec<ImageRecord>> {
        todo!("WP-1")
    }
    fn remove_image(&self, _image_ref: &str) -> Result<()> {
        todo!("WP-1")
    }
    fn image_fs_info(&self) -> Result<FsInfo> {
        todo!("WP-1")
    }
}
