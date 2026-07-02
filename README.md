# lightr-cri

**The Kubernetes node that materializes from CAS.** A stateless, crash-only
CRI (Container Runtime Interface) implementation: the kubelet talks to a
socket; behind it, pods hydrate lazily from a content-addressed store and
job-shaped work that already ran returns from the Action Cache without
running.

```
kubelet ── CRI gRPC (unix socket) ──▶ lightr-cri shell (this repo)
                                        │ CriBackend trait (frozen seam)
                              ┌─────────┴─────────┐
                        fake backend         lightr backend
                        (in-memory, R0/R1)   (hugr-lightr crates, in flight)
```

The CRI *shell* is built here against a **frozen seam contract** with
[hugr-lightr] — the daemonless runtime whose crates (engine, store, run)
become the real backend at integration time (contract swap, never
copy-paste). The shell is generic over `CriBackend`; conformance runs
against an in-memory fake backend today.

## What is green today (CI-signed)

- **critest conformance: 33/33 passed, 0 failed** on GitHub-hosted Linux CI
  — including exec/attach/port-forward streaming (SPDY, e2e) and real CNI
  networking (bridge + host-local + portmap) in real network namespaces.
- **Crash-only, proven:** `kill -9` the server mid-operation, restart on the
  same socket — nothing lost, nothing reconciles. There was never a mirror
  of kernel state to rebuild (acceptance A8, exercised in CI).
- **Footprint:** ~**7.1 MB resident** for `cri-serve` vs containerd's
  ~65.9 MB daemon — **9.3× smaller, no per-container shim** ([measured &
  signed][bench] in the hugr-lightr benchmark ledger).

Everything else — the real backend over lightr's CAS crates, the remaining
KPI closures — is in flight and stated as such: **a number is a claim only
when a CI run signs it** (house tense law).

## Design

1. **The listener owns nothing.** Kernel + disk are the source of truth; the
   listener is a stateless view.
2. **Scoped no-daemon.** lightr's "no daemon, ever" holds for local use;
   `cri serve` is an explicit opt-in server mode honoring the spirit:
   stateless, crash-only, socket-activatable, zero owned state.
3. **Contract-first.** No git/path deps on hugr-lightr — the seam is a
   transcribed, frozen contract proven by shared conformance vectors.
4. **Conformance is the acceptance suite.** critest items are the red→green
   anchor, with a no-regression GREENLIST gate.
5. **Linux validates; macOS only compiles.** Anything not exercised on Linux
   CI is gated honest, never silently claimed.

## Repo map

- `crates/` — proto (committed codegen, CRI v1.33.1), backend seam, fake,
  shell, server, CNI, streaming, conformance vectors, acceptance driver
- `docs/whitepaper/` — working-backwards vision (canon for this front)
- `docs/contract/` — the frozen seam (types, backend semantics, integration law)
- `docs/spec/` — frozen build spec + acceptance (critest-anchored)
- `docs/bench/` — KPI plan + containerd A/B reference

## License

Apache-2.0 © HumanGuardrail Ltda.

[hugr-lightr]: https://github.com/HumanGuardrail/hugr-lightr
[bench]: https://github.com/HumanGuardrail/hugr-lightr/blob/main/docs/benchmarks/RESULTS.md
