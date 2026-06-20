# lightr-cri — structural KPI bench

Turns the daemonless / crash-only thesis into **signed numbers**, under the same
fail-closed discipline as conformance: *no number is a claim until a CI run emits
the signed artifact.*

Harness: [`ci/bench.sh`](../../ci/bench.sh) · runs in the `linux-conformance`
job after the greenlist gate · output: `ci/bench-results.json` (uploaded as the
`bench-results` CI artifact).

## What it measures (IN SCOPE)

These are properties the `lightr-cri` shell itself exercises and that this binary
can truthfully measure **with the R1 fake backend**:

| KPI | Definition | Why it matters |
|---|---|---|
| `cold_start_ms` | spawn → first **answered** CRI RPC | proves "ready to serve", not just "process up" |
| `idle_rss_kb` | `VmRSS` of the serve process after one pod lifecycle (+ thread / runtime-process count) | the daemonless footprint — what a node pays just to host the runtime |
| `crash_recovery_ms` | `kill -9` → restart on the **same** socket+state → first answered RPC, with the pre-crash pod re-listed | the crash-only payoff: recovery without reconciliation, state re-derived from disk |

`cold_start` and `crash_recovery` are reported as min / p50 (nearest-rank) / max
over N samples (default 10 / 5). `crash_recovery` also asserts `state_rederived`
— the pod created before the kill must reappear after restart, rebuilt from disk,
proving nothing lived in the dead process.

## What it does NOT measure (OUT OF SCOPE — and why)

The CAS-tier KPIs — **pull dedup ratio, bytes-transferred on pull, disk for N
similar images** — are properties of the **real Lightr backend's content-
addressed store**, not of the R1 *fake* backend. Measuring them here would
measure the fake → meaningless. They are explicitly deferred to integration and
emitted in the artifact under `out_of_scope.deferred_kpis`.

This boundary is the point: we sign what we can honestly sign, and we refuse to
sign what this binary cannot.

## Reference, not verdict: containerd

If `containerd` is present on the runner, its **idle daemon RSS** is captured as
a labeled reference point (`reference.containerd_idle_rss_kb`). Workloads differ
— containerd does far more at idle — so this is **context** for the daemonless
argument, never a like-for-like benchmark verdict. Absent containerd, the
reference is skipped loudly (it is not a core probe).

## Fail-closed

`LIGHTR_CRI_REQUIRE_PROBES=1` (set in CI): a missing **core** probe (`crictl`)
fails the step rather than silently producing a hollow result. The containerd
reference is optional and never fails the run.

## Reading the artifact

```jsonc
{
  "schema": "lightr-cri.bench/v1",
  "signed":   { "commit": "...", "kernel": "...", "host": "...", "timestamp_utc": "...", "backend": "fake (R1)" },
  "in_scope": { "cold_start_ms": {…}, "idle_rss_kb": {…}, "crash_recovery_ms": {… "state_rederived": true } },
  "reference":{ "containerd_idle_rss_kb": …, "note": "REFERENCE ONLY; workloads differ" },
  "out_of_scope": { "deferred_kpis": ["pull_dedup_ratio", "pull_bytes_transferred", "disk_for_n_similar_images"] }
}
```

Every field is stamped with the commit and host that produced it. That stamp is
what turns a measurement into a *signed* measurement.

## Roadmap

- **Now (R1):** the in-scope KPIs above, signed per CI run.
- **Integration (R2):** the CAS-tier KPIs move in scope once the real backend is
  wired — measured the same way (signed A/B vs containerd on identical images),
  closing the `out_of_scope` block. That work is requested from the Lightr TL,
  not done here.
