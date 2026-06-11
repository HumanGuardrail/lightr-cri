# ADR-0017 — CRI-ready, not CRI-now

- **Status:** Proposed (authored in the lightr-cri session 2026-06-11; to be
  landed in hugr-lightr by its live session after owner review)
- **Date:** 2026-06-11 (proposed)

One line: freeze the three internal surfaces a future CRI shell depends on —
two are already true and get promoted to law; one (sandbox/group) is reserved
now while it costs a field, not a refactor.

## Context

A CRI front (`lightr-cri`, separate repo) is being built against a frozen
seam contract, to be plugged into this workspace later as the `lightr-cri`
crate / `lightr cri serve` opt-in server mode. Market research (2026-06-11)
established: no standalone business exists in the runtime layer; the CRI
front's role is a third door to CoreLink (own Runners fabric first,
self-managed k8s second). Building externally is only safe if the seam
surfaces it consumes do not drift. Three surfaces decide whether the 2027
integration is a contract swap or a rewrite:

1. **Per-container state on disk.** Already law in practice:
   `$LIGHTR_HOME/run/<id>/` with `spec.json`, `pid`, `status`, `ctl.sock`,
   `stdout`, `stderr`, written via the atomic pattern (tmp + fsync + rename +
   fsync parent) — `lightr-run/src/lib.rs:437–484`.
2. **Supervisor survives the parent.** Already law in practice:
   `spawn_detached` → `setsid` → self-respawn `__supervise` → control via
   `ctl.sock` (exec/logs/stop) — `lightr-run/src/lib.rs:490+`.
3. **Sandbox/group concept.** Does not exist. CRI thinks in pods: N
   containers sharing namespaces (net/IPC) held by a group holder. Without a
   reserved seam, retrofitting it later forces a refactor of `lightr-run`
   state and the `Engine` boundary.

The kubelet requires a live endpoint, which collides with principle 1
("no daemon, ever") unless explicitly scoped.

## Decision

1. **Promote the run-dir layout to a versioned public seam.** The directory
   layout and file semantics above are contract surface consumed by external
   views (the CRI listener reconstructs all runtime state from disk +
   kernel). Any change requires a seam version bump and updated conformance
   vectors. The listener never holds authoritative state (crash-only law).
2. **Promote the supervisor model to contract.** Exactly one supervisor owns
   a workload; its lifecycle is independent of whatever process spawned it;
   `ctl.sock` is the only control plane (exec/logs/stop today; exec-stream
   later). External views talk to workloads only through it.
3. **Reserve the group (pod) concept now; implement later.** `SpecOnDisk`
   gains an optional `group_id: Option<String>` field (serde-default, inert
   in R-current behavior). Semantics reserved: runs sharing a `group_id`
   will share a namespace set held by a group holder (pause-equivalent)
   whose run-dir is the group anchor. No engine work now; the field and the
   documented semantics are the entire present cost.
4. **Scope principle 1.** "No daemon, ever" governs local use: nothing runs
   when nothing runs, `ps` proves it. `lightr cri serve` (future) is an
   explicit opt-in server mode that honors the spirit: stateless,
   crash-only, socket-activatable, zero owned state. This scoping is part of
   the principle's text from this ADR on.
5. **Dependency firewall.** tonic/prost (gRPC) enter this workspace only at
   integration time, inside the `lightr-cri` crate; they never become
   workspace-wide deps of the local product.

## Consequences

- lightr-cri (external repo) codes against `docs/contract/seam-contract-v1.md`
  (transcribed types + these surfaces); integration becomes "implement
  `CriBackend` with lightr crates + run shared conformance vectors", not a
  merge of divergent codebases.
- `lightr-run` takes one trivial diff now (the `group_id` field); whoever
  next touches `SpecOnDisk` must preserve serde compatibility.
- Future ADRs own the implementations: group holder/engine sandbox semantics,
  CRI streaming, CNI — none are decided here.
- Nothing in R-current behavior changes; `lightr bench` and acceptance
  A1–A30 are unaffected.
