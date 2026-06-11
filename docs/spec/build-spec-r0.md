# lightr-cri — Build Spec R0 (the shell)

- **Status:** FROZEN under owner mandate 2026-06-11
- Any deviation = BLOCKED + ask lead. Agents transcribe; do not decide.
- Canon above this file: `docs/whitepaper/lightr-cri-v1.md` ·
  `docs/contract/seam-contract-v1.md` (seam) · `CLAUDE.md` (principles)

## 1. R0 scope

The CRI shell: gRPC RuntimeService + ImageService over the `CriBackend`
seam, a fake backend that is honest (really executes, really persists),
conformance vectors, crictl-driven acceptance, critest harness in Linux CI.

OUT of R0 (→ R1): CNI, streaming Exec/Attach/PortForward (R0 returns
UNIMPLEMENTED; ExecSync IS in R0), socket activation, evented PLEG stream,
kind/kubelet E2E. OUT entirely: any dependency on hugr-lightr (seam law).

## 2. Workspace

```
Cargo.toml                 # workspace; members below; resolver=2; edition 2021
rust-toolchain.toml        # 1.96.0 (mirror hugr-lightr ADR-0006)
crates/
  lightr-cri-proto/        # vendored k8s CRI v1 proto + COMMITTED tonic/prost codegen
  lightr-cri-backend/      # seam crate: vocabulary types, CriBackend trait, errors (W0, lead)
  lightr-cri-fake/         # fake backend: file-backed state, native exec, fake images
  lightr-cri-shell/        # RPC translation: runtime.rs, image.rs (stateless law)
  lightr-cri-server/       # bin `lightr-cri`: UDS + tonic serve
  lightr-cri-vectors/      # vector runner over any CriBackend
  lightr-cri-acceptance/   # crictl-driven acceptance (probe-gated)
vectors/                   # JSON conformance vectors (seam contract §4)
ci/                        # gate scripts (skill-004 lineage)
.github/workflows/         # Linux CI
```

Workspace deps (exact patch pinned at W0 scaffold from lockfile; majors law):
`tonic 0.12`, `prost 0.13`, `tokio 1 (rt-multi-thread, net, signal)`,
`serde 1 (derive)`, `serde_json 1`, `libc 0.2`, `tempfile 3`, `assert_cmd 2`,
`blake3 1`. **Firewall:** tonic/prost/tokio appear ONLY in proto/shell/server/
acceptance crates. `lightr-cri-backend`, `lightr-cri-fake`,
`lightr-cri-vectors` MUST NOT depend on them (this is what keeps the real
backend implementable inside hugr-lightr without tokio).

Proto pin: `kubernetes/cri-api` tag `kubernetes-1.33.1`,
`pkg/apis/runtime/v1/api.proto`, vendored at
`crates/lightr-cri-proto/proto/api.proto`; sha256 of the vendored file
recorded in `crates/lightr-cri-proto/PROVENANCE.md`. Codegen committed under
`src/generated/`; `verify-codegen` CI job regenerates and diffs (drift = red).

## 3. FROZEN — lightr-cri-backend (the seam crate; W0, lead-authored)

Exact Rust. Transcribed vocabulary types per seam-contract §1 (Digest,
Entry, Manifest, RefRecord — verbatim). Then:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SandboxId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ContainerId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SandboxConfig {
    pub name: String,
    pub uid: String,
    pub namespace: String,
    pub attempt: u32,
    pub labels: std::collections::BTreeMap<String, String>,
    pub annotations: std::collections::BTreeMap<String, String>,
    pub log_directory: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SandboxState { Ready, NotReady }

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
    pub args: Vec<String>,
    pub working_dir: String,
    pub envs: Vec<(String, String)>,
    pub mounts: Vec<Mount>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub annotations: std::collections::BTreeMap<String, String>,
    pub log_path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ContainerState { Created, Running, Exited, Unknown }

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerStatus {
    pub id: ContainerId,
    pub sandbox: SandboxId,
    pub config: ContainerConfig,
    pub state: ContainerState,
    pub created_at_nanos: i64,
    pub started_at_nanos: i64,   // 0 = never
    pub finished_at_nanos: i64,  // 0 = still running / never started
    pub exit_code: i32,          // valid only when state == Exited
    pub reason: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecResult { pub exit_code: i32, pub stdout: Vec<u8>, pub stderr: Vec<u8> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerStatsRec {
    pub id: ContainerId,
    pub timestamp_nanos: i64,
    pub cpu_usage_core_nanos: u64,
    pub memory_working_set_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulledImage { pub ref_name: String, pub root_hex: String, pub total_size: u64 }
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRecord { pub id: String, pub ref_name: String, pub size: u64 }
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsInfo { pub timestamp_nanos: i64, pub mountpoint: String, pub used_bytes: u64, pub inodes_used: u64 }

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
pub type Result<T> = std::result::Result<T, BackendError>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxFilter {
    pub id: Option<SandboxId>,
    pub state: Option<SandboxState>,
    pub label_selector: std::collections::BTreeMap<String, String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerFilter {
    pub id: Option<ContainerId>,
    pub sandbox: Option<SandboxId>,
    pub state: Option<ContainerState>,
    pub label_selector: std::collections::BTreeMap<String, String>,
}

/// The seam. Synchronous on purpose: the real backend (hugr-lightr crates)
/// is sync; the shell bridges via spawn_blocking. Object-safe.
pub trait CriBackend: Send + Sync + 'static {
    // sandbox plane
    fn run_sandbox(&self, cfg: SandboxConfig) -> Result<SandboxId>;
    fn stop_sandbox(&self, id: &SandboxId) -> Result<()>;          // idempotent
    fn remove_sandbox(&self, id: &SandboxId) -> Result<()>;        // idempotent; implies stop; removes its containers
    fn sandbox_status(&self, id: &SandboxId) -> Result<SandboxStatus>;
    fn list_sandboxes(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>>;

    // container plane
    fn create_container(&self, sandbox: &SandboxId, cfg: ContainerConfig) -> Result<ContainerId>;
    fn start_container(&self, id: &ContainerId) -> Result<()>;
    fn stop_container(&self, id: &ContainerId, grace_seconds: i64) -> Result<()>; // idempotent
    fn remove_container(&self, id: &ContainerId) -> Result<()>;    // idempotent; only when not Running
    fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus>;
    fn list_containers(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatus>>;
    fn container_stats(&self, id: &ContainerId) -> Result<ContainerStatsRec>;
    fn list_container_stats(&self, filter: &ContainerFilter) -> Result<Vec<ContainerStatsRec>>;

    // exec plane (R0: sync only)
    fn exec_sync(&self, id: &ContainerId, cmd: &[String], timeout_seconds: i64) -> Result<ExecResult>;

    // image plane (lazy law: pull_image MUST NOT move file bytes)
    fn pull_image(&self, image_ref: &str) -> Result<PulledImage>;
    fn image_status(&self, image_ref: &str) -> Result<Option<ImageRecord>>;
    fn list_images(&self) -> Result<Vec<ImageRecord>>;
    fn remove_image(&self, image_ref: &str) -> Result<()>;
    fn image_fs_info(&self) -> Result<FsInfo>;
}
```

Error→gRPC mapping (frozen; lives in `lightr-cri-shell`):
`NotFound→NOT_FOUND`, `AlreadyExists→ALREADY_EXISTS`,
`InvalidArgument→INVALID_ARGUMENT`, `FailedPrecondition|InUse→FAILED_PRECONDITION`,
`Internal|Io→INTERNAL`. Message = Display of the variant payload.

State law (vectors encode this): sandbox `Ready→NotReady` (stop) `→gone`
(remove). Container `Created→Running→Exited`; `start` only from Created;
`stop` from Running (→Exited) or no-op from Created/Exited; `remove` refused
(FailedPrecondition) while Running; removing a sandbox stops+removes its
containers. All `created/started/finished` transitions persist BEFORE the
call returns (crash-only law).

## 4. FROZEN — lightr-cri-fake (honest fake)

File-backed: state root (env `LIGHTR_CRI_STATE`, default
`$TMPDIR/lightr-cri-fake`) with `sandboxes/<id>.json`,
`containers/<id>.json`, `images/<name-hash>.json`, atomic write law
(tmp + rename; fsync on the file). In-memory index is a CACHE rebuilt from
disk at open — kill -9 at any point loses nothing (A8 proves it).
Execution is REAL: `start_container` spawns the configured command as a
plain host process (no isolation — fake's honesty is execution, not
sandboxing); `exec_sync` really runs and captures. Images are fake CAS
records: `pull_image` synthesizes a `PulledImage` with a BLAKE3 root over
the ref string, instantly (lazy law rehearsed); a fixed allowlist refuses
unparseable refs with InvalidArgument. Stats: real
`/proc/<pid>`-based on Linux, zeroed-with-timestamp elsewhere (probe-truthful).

## 5. FROZEN — lightr-cri-shell + server

`runtime.rs`: implements generated `runtime_service_server::RuntimeService`
for `Shell<B: CriBackend>` — translation only: decode proto → backend call
in `spawn_blocking` → encode proto. ZERO state in the service struct beyond
`Arc<B>` (listener law; reviewers reject any field that caches backend
data). `Version` reports `runtime_name="lightr"`,
`runtime_api_version="v1"`. `Status` reports Runtime+Network conditions
(network=true in R0 fake mode, annotated). Unimplemented in R0:
`Exec`, `Attach`, `PortForward`, `GetContainerEvents`, `UpdateRuntimeConfig`,
`UpdateContainerResources`, `ReopenContainerLog`, checkpoint RPCs → tonic
`unimplemented` with explicit message naming R1.
`image.rs`: same pattern for `image_service_server::ImageService`.
`lightr-cri-server` (bin): clap-free minimal args (`--socket PATH`,
`--state PATH`), tokio rt, UnixListener, serve both services, SIGTERM
graceful; exit codes 0/1; logs one line per lifecycle event to stderr.

## 6. FROZEN — lightr-cri-vectors + vectors/

Runner: `run_vectors(backend: &dyn CriBackend, dir: &Path) -> VectorReport`.
Vector JSON shape (frozen):

```json
{ "name": "container-remove-while-running-refused",
  "steps": [
    {"op": "run_sandbox", "cfg": {"name": "s1", "uid": "u1", "namespace": "ns", "attempt": 0}},
    {"op": "create_container", "sandbox": "$0", "cfg": {"name": "c1", "image_ref": "ref/a", "command": ["/bin/sleep", "5"]}},
    {"op": "start_container", "id": "$1"},
    {"op": "remove_container", "id": "$1", "expect_err": "FailedPrecondition"},
    {"op": "stop_container", "id": "$1", "grace_seconds": 0},
    {"op": "remove_container", "id": "$1"}
  ] }
```

`$N` = result of step N. Vector set MUST cover: every legal transition,
every illegal transition with the exact expected error variant, idempotency
(stop/remove twice), sandbox-removal cascade, image pull/list/status/
remove + InUse refusal, exec_sync exit-code and output capture, and
crash-recovery scripts (`"op": "reopen_backend"` step = drop + reopen from
disk; state must be identical). Vectors are THE shared artifact with
hugr-lightr at integration (seam contract §4) — they must never import
backend internals, only the trait.

## 7. FROZEN — Acceptance (A1–A10) and gates

Suite in `lightr-cri-acceptance`, probe-gated (each item SKIPs loudly with
reason when its probe fails — macOS lacks crictl/critest; never silent).

- **A1 version** — server on UDS; `crictl version` returns runtime_name
  lightr. Probe: crictl in PATH.
- **A2 sandbox lifecycle** — `crictl runp` → Ready; `crictl pods` shows it;
  `stopp` + `rmp` clean.
- **A3 instant pull** — `crictl pull <ref>` exits 0; wall-clock <1s (CI
  budget; target law: resolve-only); `crictl images` lists it.
- **A4 container lifecycle** — create/start over a sandbox; `crictl ps`
  Running; stop → Exited with code.
- **A5 exec sync** — `crictl exec --sync <id> /bin/echo hi` → stdout `hi`,
  exit 0.
- **A6 idempotency + cascade** — double stop/rm OK; `rmp` removes pod's
  containers.
- **A7 stats** — `crictl stats` answers for a running container.
- **A8 crash-only** — kill -9 the server mid-suite; restart; `crictl pods`
  + `ps` re-derive identical state; a Running container's process survived
  the listener death.
- **A9 vectors** — `cargo test -p lightr-cri-vectors` green (runs the full
  `vectors/` set against the fake backend). Runs on macOS too (no crictl
  needed).
- **A10 critest scoped** — `critest --runtime-endpoint <sock>` with the
  frozen skip-list (`ci/critest-skips.txt`: streaming, CNI-dependent,
  RuntimeConfig items — each line carries a `# reason → R1` comment);
  conquered items recorded in `tests/GREENLIST`; regression = red
  (skill-004 GREENLIST law).

Budgets (CI, release build): server RSS at idle after A1–A7 **<10 MB** (CI gate, conservative; product target <5 MB per whitepaper §8);
`PullImage` p50 **<50 ms** (fake). Recorded by `ci/budget.sh`; failures are
red, not warnings. Tense law: these are CI gates on the fake backend, NOT
product claims about the lightr backend.

## 8. Wave partition (R0)

| WP | owner-files (disjoint) | model | dep-on | summary |
|----|------------------------|-------|--------|---------|
| W0 | workspace, all skeletons + frozen sigs, proto vendor+codegen, backend crate COMPLETE, vector schema + 2 examples, CI skeleton | lead | — | everything compiles, stubs `todo!()` |
| WP-1 | `crates/lightr-cri-fake/` | sonnet | W0 | §4 |
| WP-2 | `crates/lightr-cri-shell/src/runtime.rs` | sonnet | W0 | §5 runtime side |
| WP-3 | `crates/lightr-cri-shell/src/image.rs` + `crates/lightr-cri-server/` | sonnet | W0 | §5 image side + bin |
| WP-4 | `crates/lightr-cri-vectors/` + `vectors/` | sonnet | W0 | §6 |
| WP-5 | `crates/lightr-cri-acceptance/` | sonnet | W0 | §7 A1–A8, A10 harness |
| WP-6 | `ci/` + `.github/workflows/` | sonnet | W0 | CI: build+clippy+vectors+crictl/critest pinned install+budgets+GREENLIST |
| critic | suite coverage vs §7 + whitepaper R0 exit | opus | post-WP | cold critic |

Conflict map: **CONFLICT-FREE.** Shared files (workspace Cargo.toml, shell
lib.rs/mod wiring, backend crate) are W0-complete and read-only to the
fleet; WP-2/WP-3 own disjoint files inside the shell crate. Merge DAG:
WP-1 → (WP-2, WP-3, WP-4) → WP-5 → WP-6 → critic. Rolling dispatch:
all WPs may run concurrently (they code against W0 stubs); the DAG is the
MERGE order.

Return shape (every agent): `WP-n | files touched | cargo test -p <crate>
result (count) | clippy clean y/n | deviations: NONE or BLOCKED+question`.
Agents BLOCK instead of deciding; deviations without a block = REJECT.

## 9. R0 exit (definition of done)

cargo build/clippy -D warnings green (macOS + Linux CI); A9 green both
platforms; A1–A8 + A10 green on Linux CI; GREENLIST non-empty and gated;
budgets green; `verify-codegen` green; seam contract untouched (any needed
change = renegotiation, AP-3 refusal).
