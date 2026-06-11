# lightr-cri — Whitepaper v1 (working backwards)

> **This is a working-backwards artifact: it describes the FINISHED feature,
> written before it exists.** Nothing below is shipped; every number is a
> target until CI/bench measures it (tense law). Canon for the lightr-cri
> front; subordinate to hugr-lightr whitepaper v2 where they overlap.

## Abstract

lightr-cri is the Kubernetes face of Lightr: a CRI implementation where the
listener owns nothing, images are never pulled, and job-shaped pods that
already ran return from the Action Cache without running. It ships as
`lightr cri serve` — an opt-in server mode of the same binary that is
daemonless everywhere else. Its first cluster is HuGR's own Runners fabric;
its second audience is self-managed Kubernetes, as a third door into
CoreLink. It is not a business; it is a distribution channel and an enabler,
priced accordingly: free, open, and thin.

## 1. The scene

A node joins the cluster. The kubelet finds
`unix:///run/lightr/cri.sock` and asks for a pod backed by a 1.2 GB image.
There is no pull. `PullImage` returns in milliseconds because the image is a
manifest in CAS and the bytes are wherever they already are — on this node's
L1 from yesterday's deploy, on a peer, in CoreLink. The pod is Ready before
the incumbent's progress bar would have drawn its first block. [target;
precedents: AWS Lambda on-demand loading (ATC'23), Modal's content-addressed
FS]

Mid-rollout, an operator `kill -9`s the runtime. Nothing happens. Containers
keep running under their supervisors; the socket reactivates the listener;
the next `ListContainers` answer is re-derived from disk and kernel in
milliseconds. There is no reconciliation pass because there was never a
mirror to reconcile. Upgrading the runtime is the same non-event.

A CI pod runs a build that any pod on any node of this tenant has run
before. It does not run. The Action Cache returns the result; the kubelet
sees a pod that succeeded in 200 ms. [target; no CRI runtime has this —
verified against the 2026 sandbox/runtime market]

## 2. One law: the listener owns nothing

containerd's operational pain is structural: a daemon that mirrors kernel
state in boltdb + heap must reconcile that mirror on every restart, and the
mirror IS the architecture. lightr-cri inverts it. Sources of truth:

- **execution state** → kernel (cgroups v2, pidfds) + one supervisor per
  workload (Lightr's `__supervise` lineage, ADR-0017);
- **declarative state** → run-dir on disk, atomic writes (Lightr law);
- **content** → CAS (BLAKE3 manifests; verifiable by construction).

The gRPC listener is a stateless translator over those. Crash-only is not a
recovery strategy, it is the design: the demo is killing the runtime in a
loop during a deploy and watching nothing happen. Restart with 100 live
containers: **target <100 ms, zero impact, zero reconciliation** (vs seconds
to minutes for the incumbents' reconcile).

## 3. The sandbox plane

CRI thinks in pods. `RunPodSandbox` creates the group envelope — a group
holder (pause-equivalent) anchoring the namespace set; containers join it.
The group concept is reserved in Lightr's state model by ADR-0017 §3
(`group_id`), so the real backend implements pods without refactoring
`lightr-run`. Isolation tier is a property of the context, inherited from
Lightr's engine ladder (ns on trusted Linux; fc for hostile tenancy — the
Runners fabric pairing).

## 4. Images that are not there

`PullImage` resolves a ref to a manifest and records it — no file bytes move
(seam contract §3, lazy law). Materialization happens at container start via
Lightr's CoW/views machinery, faulting only what the workload touches. The
open ecosystem's lazy-pull attempts (eStargz, SOCI v1) stalled on exactly
the constraint we don't have: they bolted lazy onto OCI registries they
don't control. This stack owns publish→store→cache→runtime end to end — the
one configuration in which lazy loading has actually worked at scale
(Lambda, Modal). OCI images remain an import format, never the model.

Node scale-up, 1 GB image, cold node: **target pod Ready <1 s** (vs 30–60 s
layer pull). Fleet of N nodes: the image costs its unique chunks once, not
N pulls. [targets; tense law]

## 5. The memory

The Action Cache crosses the cluster: deterministic, job-shaped work
(Jobs, CronJobs, CI steps) memoizes exactly as `lightr run` does locally.
Scope honesty: this does not apply to long-running Deployments — it applies
to the job-shaped fraction, which for CI-on-k8s and agent workloads is the
expensive fraction. No other CRI runtime has the concept; it requires a
production CAS+AC behind the runtime, which is the moat (CoreLink), not the
binary.

## 6. Streaming without residency

`Exec`/`Attach`/`PortForward` return URLs; sessions are served by ephemeral
per-session streamers that die with the session (shim philosophy applied to
streaming). The listener never holds a session. Real backend wires streams
to the supervisor's `ctl.sock`; the fake backend wires an in-memory duplex —
same shell code, both conformance-tested.

## 7. Evented, not polled — soberly

Polling PLEG is the supported baseline (it is what every production kubelet
runs in 2026; evented PLEG regressed to alpha). `GetContainerEvents` is
implemented as an additive stream over the same state transitions, so when
the ecosystem re-graduates it, we are already serving it. Stats
(`ContainerStats`) are read from cgroups at request time — never cached,
never owned.

## 8. The records (targets, or not claimed)

| Indicator | Incumbent (containerd stack) | lightr-cri target | Basis |
|---|---|---|---|
| Listener RSS | 50–200 MB (Go heap) + ~10 MB/shim | **<5 MB** + <1 MB/supervisor | Rust, stateless |
| PullImage, 1 GB image | 30–60 s | **<50 ms** (resolve only) | CAS manifest |
| Pod Ready, cold node, 1 GB | 30–60 s | **<1 s** | lazy views; Lambda/Modal precedent |
| Runtime restart under 100 containers | seconds–minutes reconcile | **<100 ms, zero impact** | crash-only |
| Memoized job pod | full execution | **<200 ms, no instantiation** | AC; category of one |
| Warm container start | ~30 ms (crun floor) | **parity, on purpose** | kernel sets the floor |

Tense law: every cell in the target column is a claim about the finished
feature, citable only as a target until a signed CI/bench run measures it.
Declared parity at the kernel floor is part of the credibility strategy:
humiliation where structural, parity where physics rules.

## 9. Principles

1. **The listener owns nothing** (§2; crash-only, socket-activatable).
2. **Scoped no-daemon** — Lightr's principle 1 governs local use;
   `cri serve` is an explicit opt-in server mode honoring its spirit
   (ADR-0017 §4).
3. **Contract-first** — all shell code against seam-contract-v1; the lightr
   backend arrives at integration, vectors prove the seam held.
4. **Conformance is the spec** — critest/crictl anchor acceptance;
   GREENLIST no-regression (imported from skill-004).
5. **Linux validates; macOS compiles.** Probe-truthful gating, never silent.
6. **Fail closed; tense discipline.**
7. **Not a crusade** — no managed-k8s conversion fantasy; ICP is the Runners
   fabric, then self-managed k8s as a CoreLink door.

## 10. What we refuse

- Building a daemon with opinions: no boltdb, no state mirror, no config
  labyrinth — zero-config is a feature with a test.
- Chasing GKE/AKS (closed slots) or benchmark theater at the kernel floor.
- Letting tonic/prost leak into the local product's dependency tree.
- Copy-paste integration: if plugging into hugr-lightr is not a small PR,
  the contract failed and we renegotiate the seam — explicitly.
- Claiming any number in §8 as measured before it is.

## 11. The road

- **R0 — the shell.** CRI gRPC surface (RuntimeService + ImageService) over
  `CriBackend`, fake backend, conformance vectors, crictl smoke + critest
  harness in CI (kind/Linux runners). Exit: critest green on fake backend.
- **R1 — the node.** CNI wiring, per-session streamers, stats from cgroups,
  socket activation, crash-recovery vectors green under kill-loop.
- **R2 — the plug.** Inside hugr-lightr (its session): implement CriBackend
  over lightr crates behind the `cri` feature; same vectors + critest green
  on the real backend; ADR for group holder + exec-stream on ctl.sock.
- **GA gate.** Tied to Runners M1 posture: the first production cluster is
  ours. Self-managed k8s door opens only after the fabric eats it daily.

## 12. Why HuGR wins

Three doors, one cache: the dev's Mac (`lightr`), the agent fleet (Runners),
the cluster (`lightr cri serve`). Every door warms the same CAS/AC, and the
moat compounds exactly as the funnel doc promises — per-chunk economics with
a memory. The runtime layer never monetized for anyone in twelve years; we
don't need it to. It only has to carry tenants to CoreLink, and it is the
cheapest carrier we can build: one crate of shell over a contract that two
sessions froze on purpose.
