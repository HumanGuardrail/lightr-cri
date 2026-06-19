# R1 close-out — compute-window runbook

Status at authoring: all R1 engineering is **staged in git worktrees**; nothing here has
been compiled or run (the host had zero compute budget — CI/cargo/docker/critest deferred).
Execute this when the Mac is quiet (no competing CI/cargo jobs). Every step below is the
work that needs compute; the code itself is already written and cold-reviewed.

## What's staged (branches / worktrees)

| Branch | Worktree | Contents | Verified premise? |
|---|---|---|---|
| `worktree-agent-a57137d23311532f0` | `.claude/worktrees/agent-a571…` | **WP-A + WP-D** in `fake/lib.rs`: (A) setns mnt+uts in `exec_sync`/`open_exec` so critest sees synthesized `/etc/resolv.conf` + hostname; (A) `ContainerStatus.Reason` normalized to `Completed`/`Error` in `rec_to_status` (single source); (D) `remove_container` force-stops a Running container; `stop_container` grace==0 → SIGKILL; removed `removing running container` from `ci/critest-skips.txt` | high (CRI/critest semantics) |
| `worktree-agent-aafb3f67fcfdf2340` | `.claude/worktrees/agent-aafb3f…` | **WP-C/C2/EF/EF2** in `spdy/conn.rs` (+ `portforward.rs`): (C) `run_exec` completion driven by child reap not transport close — drains stdout/stderr, FLAG_FIN, `metav1.Status` to error stream, FLAG_FIN; (C2) bounded `Notify` barrier waits stdout+error ids (fast-exit race); new no-half-close e2e test in `tests/spdy_exec_e2e.rs`; (EF) **attach** completion fires on first of {output-drain \| client detach} so attach (instant waiter) no longer completes prematurely, exec unchanged; (EF) **port-forward** client→backend half-close propagation (FLAG_FIN → `OwnedWriteHalf::shutdown`) in SPDY + WS, `abort()`→drain; (EF2) port-forward teardown drain **bounded to 3s** then abort stragglers (no hang on non-closing backend, no truncation in common case) | **verified vs upstream** client-go (`spdy.go`/`v4.go`/`v2.go`/`errorstream.go`) + moby/spdystream (`connection.go`/`stream.go`); attach/pf bug-class confirmed by audit, `ws.rs run_session` is the correct reference pattern |
| `worktree-agent-af5cd093e8e4c37f4` (commit `d8a8678`) | `.claude/worktrees/agent-af5…` | SPDY interop **oracle + harness**: `ci/spdy-oracle/` (Go, drives real `client-go` `NewSPDYExecutor` → `ORACLE_OK`/`FAIL`), `examples/spdy_harness.rs`, `examples/zlib_inspect.rs`, `LIGHTR_SPDY_TRACE` in `server.rs` | n/a (test tooling) |

`origin/main` @ `377b678` already carries the pushed conformance fixes (empty-cmd default,
resolv.conf/hostname write, portmap hostPort=0). The last Linux CI run (ffdf357) was RED on
`empty command` — that specific failure is fixed by 377b678 (not yet re-run).

## Why each fix exists (the confirmed R1 blockers)

1. **resolv/hostname**: write happened in the container's private mnt/uts ns, but exec joined
   only NEWNET → critest read host values. (WP-A)
2. **ContainerStatus.Reason**: critest asserts `Completed`/`Error`; fake emitted raw strings. (WP-A)
3. **SPDY deadlock**: server emitted Status only after its read-loop saw transport close, but
   client-go keeps its write half open and blocks waiting for the server → mutual hang. (WP-C)
4. **force-remove**: fake refused RemoveContainer on a Running container; CRI requires force. (WP-D)

## Merge DAG (run when compute is available)

All three branches touch **disjoint files** (`fake/lib.rs` + skips ; `spdy/conn.rs` + e2e ;
oracle/examples/`server.rs`) → no merge conflicts expected. Suggested order:

```
# from repo root, on a clean main @ 377b678
git merge --no-ff worktree-agent-a57137d23311532f0   # WP-A + WP-D (fake)
git merge --no-ff worktree-agent-aafb3f67fcfdf2340   # WP-C + WP-C2 (spdy)
git merge --no-ff worktree-agent-af5cd093e8e4c37f4   # oracle + harness (d8a8678)
```

## Verification gates (in order — STOP at first red, fix at root)

```
# 1. macOS compile lane (local, light once machine is quiet)
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings   # WP-A/C added cfg(linux) code — confirm mac lane still clean
cargo test  --workspace                                  # unit lanes

# 2. SPDY interop oracle (Linux — the real client-go gate the e2e can't prove on mac)
bash ci/spdy-oracle/run.sh        # expect: ORACLE_OK   (was the hang repro)
#    Then exercise the OTHER streaming paths the audit fixed (all unproven on Linux):
#      - attach: detach mid-session AND container-exit both terminate cleanly (not premature, not hung)
#      - port-forward: client CloseWrite reaches backend (half-close); no truncation; no hang on non-closing backend
#    Cold-review check: confirm tokio::task::JoinHandle::abort_handle (used in run_portforward drain bound)
#    is available in the pinned tokio (stable since 1.30 — check Cargo.lock).

# 3. Linux conformance (self-hosted linux runner / CI)
#    push the merged main; linux-conformance job runs critest v1.33.0 via ci/critest-gate.sh
#    expect: empty-command PASSES, resolv/hostname PASS, force-remove PASS, exec-streaming PASS
```

## After first GREEN Linux run (B8 — required for honest "R1 conformance")

- **Append R1 conquest items to GREENLIST** in `ci/critest-gate.sh` (currently holds only the
  21 R0 items; until R1-conquered items are appended, gate.sh passes on the R0 baseline and
  "R1 green" is not provable). Add: streaming exec/attach, CNI networking, container-log,
  default-command, resolv/hostname, force-remove — whatever the green run actually passed.
- Confirm exec/attach/portforward (G2/G3/G4) — these were **never green on Linux**, so treat
  them as unproven until this run; the SPDY fix is the main risk retired.

## Deferred / minor (not blockers — handle opportunistically)

- **stop_container grace>0 100ms cap** (`fake/lib.rs`, WP-D): the SIGTERM→SIGKILL escalation
  uses a 100ms cap, not the real grace window. Fine for the fake; **must be revisited before
  porting to the lightr backend**. Cold-review item.
- **Over-broad skip regexes** (`ci/critest-skips.txt`): `[Ee]vent` and `[Rr]eopen` can swallow
  unrelated future-passing items. Tighten to `GetContainerEvents|EventedPLEG|ContainerEvents`
  and `ReopenContainerLog`. Do during the GREENLIST reconcile.
- **Image Manager 8-item skip block vs "image GRADED in R1"**: reconcile against build-spec §1 —
  is the whole block legitimately R2, or should pull/list/status be reachable on the fake? Read
  `docs/spec` build-spec §1 and decide.

## Decision log

- **2026-06-19** — Owner authorized conquering `removing running container` now (not R2 waiver)
  → WP-D. No rigor waiver outstanding for R1.
