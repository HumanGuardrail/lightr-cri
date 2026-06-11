# lightr-cri

**The Kubernetes node that materializes from CAS.** A stateless, crash-only
CRI (Container Runtime Interface) implementation: the kubelet talks to a
socket; behind it, pods hydrate lazily from CoreLink's content-addressed
store and job-shaped work that already ran returns from the Action Cache
without running.

This repo builds the CRI *shell* against a **frozen seam contract** with
[hugr-lightr] — the daemonless runtime whose crates (engine, store, run)
become the real backend at integration time. Until then, the shell runs and
passes conformance against an in-memory fake backend.

- `docs/whitepaper/` — working-backwards vision (canon for this front)
- `docs/contract/` — the frozen seam (types transcribed from lightr-core,
  backend semantics, integration law)
- `docs/handoff/` — ADR to be landed in hugr-lightr by its own session
- `docs/spec/` — frozen build spec + acceptance (critest-anchored)

Status: design phase. Nothing below is shipped; every number is a target
until measured (tense law).

[hugr-lightr]: ../hugr-lightr
