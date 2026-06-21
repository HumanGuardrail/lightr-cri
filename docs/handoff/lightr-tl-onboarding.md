# Onboarding & briefing — lightr-cri → Lightr TL

**From:** lightr-cri TL · **To:** Lightr (real backend) TL
**Updated:** 2026-06-21 · **Repo:** `github.com/HumanGuardrail/lightr-cri` (own repo, frozen seam)

This is your single entry point. It explains **everything built in lightr-cri**,
**how it plugs into Lightr**, and **exactly what I need from you** to integrate.
Cross-repo protocol holds: I own only this repo; what the real backend owns I
**request** here — I never reach into Lightr.

---

## 1. What lightr-cri is (in one breath)

The **kubelet-facing shell** of the Lightr runtime: a **stateless, crash-only CRI
implementation**. Kubernetes' `kubelet` speaks CRI gRPC over a unix socket; this
shell translates those calls onto a backend trait and owns **zero state**. Kill it
`-9` mid-operation and nothing is lost, nothing reconciles — the kernel and disk
are the only truth.

```
kubelet ── CRI gRPC (unix socket) ──▶ lightr-cri shell (THIS REPO)
                                        │ CriBackend trait  ◀── THE FROZEN SEAM
                              ┌─────────┴─────────┐
                        fake backend         lightr backend
                        (in-memory, R1)      (YOUR job: Lightr crates)
```

It ships as `lightr cri serve` once integrated. Built here in isolation against a
**frozen seam contract**; it joins the hugr-lightr workspace by **contract-swap,
never copy-paste**.

**Principles (decided — don't relitigate without the owner):** listener owns
nothing; scoped no-daemon (`cri serve` is opt-in, stateless, socket-activatable);
contract-first (no git/path deps on hugr-lightr); conformance is the acceptance
suite; Linux validates, macOS only compiles; fail-closed / no measured claim
without a signed run.

---

## 2. What is built and GREEN today (signed by CI)

All on GitHub-hosted runners, 3 jobs (`verify-codegen`, `mac-compile`,
`linux-conformance`), fail-closed gates.

| Area | State |
|---|---|
| **CRI conformance (critest v1.33)** | **33/33 passed**, GREENLIST no-regression gate |
| **Streaming** exec / attach / port-forward | ponta-a-ponta (SPDY + WebSocket), e2e + critest |
| **Pod networking (CNI)** | bridge + host-local + portmap v1.9.1, real netns, hostPort DNAT |
| **House acceptance** b1–b7 + A1–A8 | green (A8 = real `kill -9` crash-only) |
| **Structural KPI bench** (`ci/bench.sh`) | signed JSON per run — see below |

**Signed structural KPIs** (latest run, fake backend R1, Linux runner):
- Cold-start (spawn → first answered CRI RPC): **p50 16 ms** (n=10)
- Idle footprint (daemonless): **~7 MB, 1 process, 4 threads** — vs the system
  **containerd daemon ~47 MB** on the same host → **~6.7× lighter** (labeled
  reference; containerd does more at idle)
- Crash recovery (`kill -9` → restart same socket+state → serving): **p50 17 ms**,
  **state re-derived ✓** (n=5)

Artifact: `ci/bench-results.json` (uploaded each run) · methodology: `docs/bench/README.md`.

---

## 3. Architecture — crate map

| Crate | Role |
|---|---|
| `lightr-cri-proto` | committed CRI v1.33 gRPC codegen (vendored proto + sha256 provenance) |
| **`lightr-cri-backend`** | **THE SEAM** — `CriBackend` trait + vocab types (`src/lib.rs`, `src/vocab.rs`) |
| `lightr-cri-fake` | in-memory backend impl (R1 driver of conformance; your impl replaces this) |
| `lightr-cri-shell` | the gRPC service — maps CRI calls onto `CriBackend` |
| `lightr-cri-server` | the `lightr-cri` binary (`--socket`, `--state`); `main()` in `src/main.rs` |
| `lightr-cri-stream` | SPDY/WebSocket streaming (exec/attach/port-forward), `run_exec`/`run_portforward` |
| `lightr-cri-net` | CNI chain executor + netns lifecycle |
| `lightr-cri-vectors` | shared conformance vectors (the wire-level seam proof) |
| `lightr-cri-acceptance` | A1–A8 + b1–b7 house tests (real Linux) |

---

## 4. The frozen seam — `CriBackend` (what you implement)

`crates/lightr-cri-backend/src/lib.rs`. Implement this trait over Lightr/CoreLink
CAS. The shell, streaming, vectors and conformance are all written against it.

```
trait CriBackend: Send + Sync + 'static {
  // sandbox lifecycle
  run_sandbox(cfg) -> SandboxId;  stop_sandbox(id);  remove_sandbox(id);
  sandbox_status(id) -> SandboxStatus;  list_sandboxes(filter) -> [SandboxStatus];
  // container lifecycle
  create_container(sandbox, cfg) -> ContainerId;  start_container(id);
  stop_container(id, grace_seconds);  remove_container(id);
  container_status(id) -> ContainerStatus;  list_containers(filter) -> [ContainerStatus];
  container_stats(id) -> ContainerStatsRec;  list_container_stats(filter) -> [..];
  exec_sync(...) -> (stdout, stderr, exit);
  // images
  pull_image(ref) -> PulledImage;  image_status(ref) -> Option<ImageRecord>;
  list_images() -> [ImageRecord];  remove_image(ref);  image_fs_info() -> FsInfo;
  pull_image_with_auth(...);             // additive, R1
  // streaming (default-provided; override for real)
  open_exec(...) -> StreamSession;  open_attach(id) -> StreamSession;
  network_ready() -> bool;
}
```

Notes:
- `StreamSession` carries stdin/stdout/stderr/pty_master + a `waiter` (`wait()` →
  exit code). The streaming crate drives completion off reaper + output-drain; your
  impl just needs to hand back the right fds and a real waiter.
- The fake's known limits (documented, fine for R1) close naturally with a real
  supervisor: exit codes of containers that exited while the listener was dead
  recover as `-1`; the reaper is in-process. ADR-0017 expects your supervisor model
  to close both.

---

## 5. What I need from you — the asks

### A. Implement `CriBackend` over Lightr crates
The integration is a **contract-swap**: a new backend crate implementing the trait
in §4, wired in place of `lightr-cri-fake`. **No git/path dep on hugr-lightr leaks
back here** — the wire-level seam is proven by the shared **conformance vectors**
(`lightr-cri-vectors`); your impl must pass the same vectors the fake passes.

### B. Close the CAS-tier & runtime-tier KPIs
Full spec + probes per KPI in **`docs/handoff/bench-cas-kpis-request.md`**:
1. **Pull dedup** — bytes re-transferred → 0 for content already in CAS.
2. **Disk for N similar images** — dedup ratio ∝ shared content.
3. **Real-container A/B vs containerd** — real image cold-start/footprint; also
   unblocks the 4 runtime-tier critest networking specs in `ci/critest-skips.txt`.
4. **AppArmor** — profile actually applied (removes the critest AppArmor skip).

The bench harness already reserves these slots (`out_of_scope.deferred_kpis`); each
becomes `in_scope` + signed when the backend capability lands. No rework needed on
my side beyond pointing the existing probes at the real workload.

### C. Confirm / land ADR-0017
**`docs/handoff/ADR-0017-cri-ready-not-cri-now.md`** — "CRI-ready, not CRI-now."
2 of its 3 decisions are already true in lightr-cri code; the only new field on your
side is `group_id`. Please confirm the supervisor model (exit-code recovery,
out-of-process reaper) and the `group_id` addition.

### D. Anything that needs a contract change
`docs/contract/` is **FROZEN** (owner sign-off required). If the real backend needs
the seam to change, raise it as a request with the owner — I won't mutate the
contract unilaterally, and you shouldn't need to: the v1.1 note already declares
`open_exec`/`open_attach` + pull-auth as additive for R1.

---

## 6. How we verify integration (the bar)

1. **Conformance parity:** your backend passes the same `critest` GREENLIST + the
   shared conformance vectors the fake passes. Red→green, fail-closed.
2. **No regression:** GREENLIST gate blocks any conquered item from regressing.
3. **KPIs signed, not claimed:** every performance number comes from a CI run that
   signs it (commit + host stamped), same discipline as conformance.
4. **Linux validates:** CNI/netns/critest run on Linux CI; nothing is claimed that
   wasn't exercised there.

---

## 7. Read-first index

| Doc | What |
|---|---|
| `CLAUDE.md` | repo principles + conventions (start here) |
| `docs/contract/seam-contract-v1.1.md` | the FROZEN seam (v1.1 additive notes) |
| `docs/handoff/ADR-0017-cri-ready-not-cri-now.md` | the CRI-ready decision record |
| `docs/handoff/bench-cas-kpis-request.md` | the four gated KPIs, exact specs |
| `docs/bench/README.md` | bench methodology + in/out scope |
| `crates/lightr-cri-backend/src/lib.rs` | the `CriBackend` trait you implement |
| `crates/lightr-cri-vectors` | the shared wire-level conformance vectors |
| `docs/investor/` | one-pager + technical thesis (context, signed numbers) |

---

## 8. The one-line ask

Implement `CriBackend` over Lightr's CAS crates, pass the shared conformance
vectors + critest GREENLIST, and the four reserved KPIs become signed numbers. The
shell, streaming, networking and bench are done and green and waiting for the swap.
Anything you need from this side, raise it as a request — I'll turn it around.
