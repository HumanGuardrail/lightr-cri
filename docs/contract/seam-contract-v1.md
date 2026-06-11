# Seam Contract v1 — lightr-cri ⇄ hugr-lightr

- **Status:** FROZEN 2026-06-11 (owner mandate). Changes require explicit
  owner sign-off + version bump + conformance-vector update.
- Pairs with `docs/handoff/ADR-0017-cri-ready-not-cri-now.md` (the Lightr-side
  promises this contract relies on).

House pattern: wire-level seam with transcribed types proven by shared
conformance vectors. **No git/path dependency on hugr-lightr, ever.**

## §0 Integration law

1. The CRI shell consumes exactly one seam: the **`CriBackend`** semantics
   (§3) plus the **vocabulary types** (§1). Nothing else from Lightr is
   visible to shell code.
2. Two backends implement the seam: `fake` (in-memory, this repo, R0) and
   `lightr` (hugr-lightr crates, written at integration time inside the
   Lightr workspace). Both must pass the same conformance vectors (§4).
3. Integration = move crates into the hugr-lightr workspace, implement
   `CriBackend` over `lightr-{store,run,engine}`, run vectors + critest.
   If that is not a small PR, this contract failed — stop and renegotiate
   the seam explicitly; never patch around it.
4. tonic/prost stay inside this repo's crates (ADR-0017 §5 firewall).

## §1 Vocabulary types (transcribed from lightr-core — verbatim, frozen)

Source of truth: `hugr-lightr/crates/lightr-core/src/lib.rs` (read 2026-06-11).
Transcribed, not imported (house pattern). Drift is detected by vectors, not
by the compiler — that is deliberate.

```rust
/// BLAKE3, 32 bytes. Content address for objects, manifests, refs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    File { path: String, mode: u32, size: u64, digest: Digest },
    Symlink { path: String, target: String },
    Dir { path: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub total_size: u64,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefRecord {
    pub name: String,
    pub root: Digest,
    pub parent: Option<Digest>,
    pub created_at_unix: u64,
    pub tool_version: String,
}
```

Error vocabulary (shell maps these onto gRPC status codes; mapping frozen in
the build spec): `NotFound(Digest)`, `RefNotFound(String)`,
`Integrity { expected, actual }`, `TooLarge { size, cap }`,
`InvalidRef(String)`, `InvalidManifest(String)`,
`Registry { status, msg }`, `Io`.

## §2 Lightr-side surfaces (informative — how the real backend will be built)

These are NOT consumed by shell code. They exist so the fake backend's
semantics stay implementable by the real one, and they are what ADR-0017
freezes on the Lightr side.

**Engine trait** (`lightr-engine/src/lib.rs:131–145`, verbatim):

```rust
pub struct ExecSpec<'a> {
    pub cwd: &'a Path,
    pub command: &'a [String],
    /// ns/vz: CoW-materialized tree to pivot/boot into. Native: must be None.
    pub rootfs: Option<&'a Path>,
}

pub trait Engine {
    /// Spawn + wait; stdout/stderr inherit. Exit law: code or 128+signal.
    fn run(&self, spec: &ExecSpec) -> Result<i32>;
}
```

**Run-dir layout** (versioned public seam per ADR-0017 §1):
`$LIGHTR_HOME/run/<id>/` → `spec.json` (SpecOnDisk; gains optional
`group_id`), `pid` (plaintext i32, supervisor pid), `status` (`running` |
`exited <code>`), `ctl.sock` (UDS control plane: exec/logs/stop), `stdout`,
`stderr` (append streams). Atomic write law: tmp-in-shard + fsync + rename +
fsync parent.

**Supervisor model** (ADR-0017 §2): one supervisor per workload, survives its
parent (`setsid` + `__supervise`), all control through `ctl.sock`.

**Store surface the real backend uses**: `ref_get/ref_put/ref_log/list_refs`,
`put_bytes/get_bytes/exists/materialize_file`, `ac_get/ac_put`,
`write_guard/gc_guard`. (Per-chunk lazy hydration arrives with Lightr views
(ADR-0013); until then PullImage resolves refs + manifests without moving
file bytes — that is already the lazy win.)

## §3 CriBackend — normative semantics (exact Rust frozen at W0 scaffold)

The single trait the shell consumes. Pod-shaped (CRI's sandbox model), maps
1:1 onto the kubelet's expectations. Exact Rust signatures (async form,
object safety, error type) are frozen in the build spec's W0 scaffold; the
SEMANTICS below are frozen here.

**Sandbox plane** — `run_sandbox(SandboxConfig) -> SandboxId` creates the
pod envelope (group holder; namespaces per isolation tier) and MUST persist
its declarative record before returning (crash-only law: a sandbox that
"exists" exists on disk). `stop_sandbox`, `remove_sandbox` (idempotent;
remove implies stop), `sandbox_status`, `list_sandboxes(filter)`.

**Container plane** — `create_container(SandboxId, ContainerConfig) ->
ContainerId` (config carries: image as a **ref/digest into CAS vocabulary
(§1)**, command, env, mounts, labels/annotations, resources);
`start_container`, `stop_container(id, grace_seconds)` (SIGTERM→SIGKILL law
inherited from Lightr stop), `remove_container` (idempotent),
`container_status`, `list_containers(filter)`, `container_stats(id)` /
`list_container_stats(filter)` — stats read from kernel sources (cgroups)
or fake equivalents; the backend never caches them.

**Exec plane** — `exec_sync(id, cmd, timeout) -> ExecResult{exit_code,
stdout, stderr}`; `open_exec/open_attach(id, ...) -> StreamHandle` where
StreamHandle is the shell-side abstraction the per-session streamer drives
(real backend: `ctl.sock`; fake: in-memory duplex).

**Image plane** — `pull_image(image_ref, auth) -> PulledImage{ref_name,
root: Digest, total_size}`: resolves and records; MUST NOT eagerly move file
bytes (lazy law). `image_status`, `list_images`, `remove_image` (refuses if
in use by a live container), `image_fs_info`.

**Listener law (binds the shell, not the backend):** the gRPC listener holds
zero authoritative state — every RPC answer derives from backend calls; the
process is restartable at any instant with no reconciliation step. Polling
PLEG is the supported baseline; evented PLEG (`GetContainerEvents`) is an
additive stream over the same state transitions.

## §4 Conformance vectors

`vectors/` (this repo) carries JSON vectors both backends must pass:
sandbox/container lifecycle state machines (legal/illegal transitions),
idempotency cases, error-code mapping, digest/ref encoding round-trips, and
crash-recovery scripts (kill listener at step N → state re-derived intact).
At integration, the same vectors run against the lightr backend inside the
hugr-lightr workspace — that run, green, is the definition of "the contract
held".

## §5 Versioning

This is **v1**. Any breaking change to §1/§2/§3 semantics → v2 + explicit
renegotiation with the Lightr session. Additive, serde-compatible evolution
(new optional fields) is allowed with a minor note appended here.

v1 note (2026-06-11): R0 trait omits `open_exec/open_attach` (StreamHandle) and pull auth — both are PLANNED additive v1.1 extensions for R1 streaming + private registries; declared here so R1 is an addition, not a renegotiation.
