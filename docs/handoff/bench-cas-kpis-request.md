# Handoff / request to the Lightr TL — close the CAS-tier & runtime-tier KPI gaps

**From:** lightr-cri TL · **To:** Lightr (real backend) TL · **Date:** 2026-06-20
**Status:** REQUEST (cross-repo protocol — lightr-cri does not implement these; it
requests them from the component that owns the real backend, and the bench harness
is built to receive them).

## Why this exists

lightr-cri's structural KPIs for the **shell** are already signed by CI (see
`docs/bench/` + `ci/bench.sh`): cold-start, idle daemon-less footprint, and
crash-only recovery, all measured against the **R1 fake backend**. Four gaps
remain that are **physically un-closeable in lightr-cri** because they are
properties of the **real Lightr CAS backend**, not of the fake. Faking them would
measure the fake — so they are handed off here with exact specs, not invented.

The bench harness (`ci/bench.sh`, schema `lightr-cri.bench/v1`) already reserves
the slots: `out_of_scope.deferred_kpis` lists exactly these. When the real backend
is wired, each becomes an `in_scope` block measured the same way, signed per run.

## The four gaps, with exact closure specs

### 1. Pull dedup — bytes transferred / re-pull
- **Claim to sign:** content already in CAS re-transfers **0 bytes** on a second
  pull; first pull transfers only the novel blobs.
- **Probe:** pull image A (cold CAS) → record bytes in; pull A again → assert ~0;
  pull image B sharing layers with A → assert only B's novel bytes move.
- **Needs from backend:** real `PullImage` over CoreLink CAS + a bytes-in counter
  (or measured via the CAS fetch path). A/B vs containerd on identical OCI images.

### 2. Disk for N similar images — dedup ratio
- **Claim to sign:** on-disk bytes for N images scale with **shared content**, not
  N × per-image layers.
- **Probe:** import N images with overlapping layers; measure CAS on-disk size;
  compare to containerd's snapshotter store for the same set; report dedup ratio.
- **Needs from backend:** real image import into the CAS store.

### 3. Real-container cold-start / footprint — A/B vs containerd
- **Claim to sign:** starting a REAL container (e.g. nginx/agnhost) reaches
  serving in time/footprint at parity-or-better vs containerd, same image + host.
- **Probe:** extend `ci/bench.sh` in-scope block to drive a real `crictl run` of a
  pullable image and curl it; the harness's cold-start/RSS/recovery probes already
  exist — point them at a real workload.
- **Needs from backend:** real image execution (the fake cannot run a pulled
  image's binary). Also unblocks the **runtime-tier critest networking specs**
  currently in `ci/critest-skips.txt` (port-mapping ×2, portforward ×2) — they
  require a real image serving HTTP in the pod netns. House b-tests b5/b6 already
  cover the path with a local server; critest's own specs need the real backend.

### 4. AppArmor conformance (critest, currently skipped)
- **Claim to sign:** critest AppArmor specs pass (profile actually applied to the
  container), removing them from `ci/critest-skips.txt`.
- **Needs from backend:** real LSM/AppArmor profile application at container start.
  GitHub runners have AppArmor in-kernel; the gap is the backend applying it.

## What lightr-cri provides so closure is mechanical

- A signed-JSON bench harness with reserved KPI slots and the fail-closed
  discipline already in place (`ci/bench.sh`, `docs/bench/README.md`).
- A frozen seam contract so the real backend drops in by contract-swap (no
  copy-paste), per [ADR-0017](ADR-0017-cri-ready-not-cri-now.md).
- Reproducible A/B scaffolding (same runner, same images) — only the backend and
  the containerd head-to-head workload need wiring on the Lightr side.

**Ask:** schedule these four into the R2 integration window; lightr-cri extends the
bench to `in_scope` as each backend capability lands, and each number is signed by
a CI run before it is ever claimed.
