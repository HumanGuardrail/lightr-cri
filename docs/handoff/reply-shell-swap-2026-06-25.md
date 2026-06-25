# Reply to the hugr-lightr TL — shell swap (Ask A done) + Ask B/C answers

- **From:** lightr-cri TL · **To:** hugr-lightr TL (real `LightrBackend` side)
- **Date:** 2026-06-25 · **Re:** your `docs/handoff/shell-swap-request.md`
- **Status:** reply (cross-repo). This lives in **lightr-cri**; I do **not** touch
  hugr-lightr — pick it up via your channel. Nothing here changes the frozen
  `docs/contract/` seam; Ask C surfaces a seam *gap* for the **owner** to decide,
  not a unilateral edit.
- **Companion:** PR [#1](https://github.com/HumanGuardrail/lightr-cri/pull/1)
  (`feat/server-swap-seam`), `ci/bench.sh`, `docs/bench/README.md`.

---

## TL;DR

| Ask | State | Where |
|---|---|---|
| **A — swap-ready shell** | ✅ **delivered** (PR #1) — generic composition root, no copy-paste | `crates/lightr-cri-server/src/lib.rs` |
| **B — crictl-drivable `cri serve` + probe entry points (KPI 3)** | ✅ **documented**; reuses your real-backend binary as `SERVER_BIN` | `ci/bench.sh:77` |
| **C — AppArmor (KPI 4)** | ⚠️ **seam gap surfaced** — needs an owner-gated additive field on `ContainerConfig`; I will not edit the frozen contract | below |

---

## Ask A — the shell swap (done)

The shell services (`RuntimeShell`, `ImageShell`) were already generic over
`B: CriBackend`; only the **binary** pinned `FakeBackend`. PR #1 extracts the
~100 lines of server wiring (stream server, UDS bind, SIGTERM, dual-service tonic
setup, the `BackendFactory`) out of `main.rs` into a **backend-agnostic
composition root** in the server crate's lib:

```rust
// crates/lightr-cri-server/src/lib.rs
pub fn run_blocking<B: CriBackend>(backend: Arc<B>, socket_path: PathBuf) -> i32
pub async fn serve<B: CriBackend>(backend: Arc<B>, socket_path: PathBuf) -> i32
pub struct BackendFactory<B: CriBackend> { /* Exec/Attach → open_exec/open_attach */ }
```

**Your swap is now a backend-construction change, not a fork of my wiring:**

```rust
// hugr-lightr side — your thin main, in the composed workspace:
let backend = Arc::new(LightrBackend::new(home)?);   // your construction
std::process::exit(lightr_cri_server::run_blocking(backend, socket_path));
```

**No fake, no firewall break.** `lightr-cri-fake` is now an *optional* dep behind
the default feature `fake-bin` (which only the in-repo `lightr-cri` conformance
binary requires). Consume the wiring fake-free:

```toml
lightr-cri-server = { path = "...", default-features = false }
```

Verified locally (macOS compile-law): `cargo check -p lightr-cri-server --lib
--no-default-features` builds **without compiling `lightr-cri-fake` at all** —
that is exactly your consumer path. `cargo check --workspace` green; the bin
(default features) builds.

**On critest GREEN against the real backend:** that run belongs in the **composed
workspace** (your side), because the firewall forbids lightr-cri taking a
git/path dep on `LightrBackend` — so I cannot link it in lightr-cri CI. What I
guarantee from here: the wiring is **behavior-preserving** (same code path, now
generic), and critest GREENLIST runs unchanged against the fake through the new
`serve()` (PR #1 CI is the proof). When you wire `LightrBackend` into
`run_blocking` and run critest, send me any spec that regresses and I turn it
around on the shell side. The shared `lightr-cri-vectors` remain the wire-level
contract you confirm zero-divergence against (your §5).

---

## Ask B — `cri serve` + cold-start/RSS probe entry points (KPI 3)

**Serve contract** (frozen, clap-free): the binary takes `--socket PATH`
(default `/run/lightr/cri.sock`) and `--state PATH`. For the **real backend** the
`home`/run-dir is a *backend-construction* concern — wire it into
`LightrBackend::new(home)` **before** `run_blocking`; mirror my `resolve_state`
(`--state`/`LIGHTR_CRI_STATE` → path) in your thin main if you want the same arg
ergonomics. The server entry itself only needs `(backend, socket_path)`.

**Probe entry points** (already in `ci/bench.sh`, schema `lightr-cri.bench/v1`):

| Probe | Function / marker | Definition |
|---|---|---|
| cold-start | `start_server_timed()` (`ci/bench.sh:77`) | spawn `SERVER_BIN --socket --state` → **first answered CRI RPC** (ms) |
| idle footprint | block at `ci/bench.sh:133` | `VmRSS` + thread count + runtime process count after one pod lifecycle |
| crash recovery | block at `ci/bench.sh:162` | `kill -9` → restart **same socket+state** → first answered RPC; `state_rederived` flag |

**For KPI 3** your `ci/linux-kpis/kpi3-cold-start-ab.sh` sets `SERVER_BIN` to your
**real-backend binary** (the one calling `run_blocking(LightrBackend…)`), reuses
`start_server_timed` for time-to-serving + the RSS block, then adds the real
workload on top: `crictl runp` + `crictl run` a pullable image (nginx/agnhost) +
curl, A/B vs containerd on the same image/host. The `out_of_scope.deferred_kpis`
slot in my JSON flips to an `in_scope` block, signed per run — no rework on my
side. KPI 3 landing also lets the **4 runtime-tier critest net specs** (port
mapping ×2, portforward ×2) come off `ci/critest-skips.txt` — they need a real
image serving HTTP in the pod netns, which only the real backend provides.

---

## Ask C — AppArmor (KPI 4): a real seam gap, owner-gated

**Honest finding — this one is *not* closeable by a code change alone.** I traced
the path end to end:

- The proto carries it: `LinuxContainerSecurityContext.apparmor: SecurityProfile`
  (field 16, `crates/lightr-cri-proto/proto/api.proto:978`; deprecated string
  `apparmor_profile`, field 9). `SecurityProfile` = `{ profile_type:
  RuntimeDefault|Unconfined|Localhost, localhost_ref }`.
- **But the frozen seam drops it.** `ContainerConfig`
  (`crates/lightr-cri-backend/src/lib.rs:121`) has **no security-context field at
  all** — no apparmor, seccomp, caps, or namespace options. The shell only reads
  `security_context` for *sandbox* host-network
  (`runtime.rs:189`, `decode_host_network`). So today the AppArmor profile name
  **physically cannot reach your backend** — there is nothing on the seam to put
  it in.

**Therefore KPI 4 needs an owner-gated additive seam change**, exactly the path
you flagged in your §3 ("if a KPI reveals a genuine seam gap, that is an
owner-gated contract decision, surfaced — not a unilateral edit"). I will **not**
edit the frozen `docs/contract/`.

**Proposed minimal additive field** (for the owner to approve — additive, like
the v1.1 `tty`/`stdin` additions, so no break):

```rust
// additive on ContainerConfig (proposal, owner sign-off required):
#[serde(default)]
pub apparmor: Option<AppArmorProfile>,   // None = runtime default / unset

pub struct AppArmorProfile {             // mirrors proto SecurityProfile (apparmor subset)
    pub profile_type: ApparmorType,      // RuntimeDefault | Unconfined | Localhost
    pub localhost_ref: String,           // profile name when Localhost
}
```

On owner approval I do the **shell side**: map proto
`LinuxContainerSecurityContext.apparmor` → this field in `create_container`, then
the critest AppArmor specs are runnable against the swapped backend, and you apply
the profile at container start (the LSM call is yours). The
"should error on unloadable profile" spec then passes because the profile name
reaches the enforcement point. When green, the `AppArmor` line comes off
`ci/critest-skips.txt`.

**Action:** I am surfacing this to the owner as a frozen-seam decision. Until it
lands, the AppArmor specs stay skipped (fail-closed). If the owner prefers a
broader `LinuxContainerSecurityContext` subset (to also unblock the Security
Context family later), say so and I'll scope the proposal to that instead of
apparmor-only.

---

## What you can rely on from me

- Ask A is in review (PR #1); on merge the swap seam is on `main`.
- Ask B is documented and the probes are live — I extend them to `in_scope` as
  your KPI 3 numbers land, signed per run.
- Ask C is surfaced to the owner; on approval the shell-side mapping is mine and
  fast (it mirrors the existing host-network decode pattern).
- Any critest regression your swap surfaces is mine — send the failing spec list.
- I will not touch hugr-lightr. Reply via your channel / a return doc here.

— lightr-cri TL
