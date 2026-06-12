# lightr-cri — Build Spec R1 (the node)

- **Status:** FROZEN under owner mandate 2026-06-12 ("bora pra cima, mantendo rigor")
- Agents transcribe; deviation = BLOCKED + ask lead.
- Canon: whitepaper §11 R1 · seam-contract v1 + **v1.1** ·
  research briefs `docs/research/r1-{streaming,cni,kubelet-smoke}.md`
  (IMPLEMENTATION-GRADE — read the one for your WP before coding).

## 1. R1 scope

Streaming (SPDY mandatory + WS ≤v4), CNI networking (bridge+host-local+
portmap), CRI log files, resolv.conf synthesis, host-network sandboxes,
tty/stdin containers, **kubelet-smoke** (a real kubelet 1.33.13 runs a
static pod on lightr-cri — R1 exit), critest skip-list narrowed (streaming +
networking + log + image-default-command families now GRADED), 3 flagged
conquests, GREENLIST reconciliation (the ~8 swallowed passing items).

DESCOPED to R2 (evidence: kubelet 1.33 never calls them — alpha gates off;
see r1-kubelet-smoke.md matrix): evented PLEG (GetContainerEvents), pod
stats (PodSandboxStats/ListPodSandboxStats, metrics descriptors), socket
activation, image default-command semantics beyond what critest grades.

## 2. New/changed crates

```
crates/
  lightr-cri-backend/    # v1.1 deltas (W0-R1, lead) — contract §A/§B
  lightr-cri-fake/       # WP-A: pty/pipes spawn, log tee, exec/attach sessions
  lightr-cri-net/        # WP-B (NEW crate): CNI invocation + netns lifecycle
                         #   FIREWALL: no tonic/tokio (sync, like backend)
  lightr-cri-stream/     # WP-C (NEW crate): the streaming server
                         #   axum/tokio + SPDY/3.1 server + WS ≤v4 + tokens
  lightr-cri-shell/      # WP-D: Exec/Attach/PortForward RPCs return URLs
                         #   (token registry wiring), DNS/portMappings/tty
                         #   decode, sandbox ip in status
  lightr-cri-server/     # WP-D: mount stream server on ephemeral 127.0.0.1
  lightr-cri-vectors/    # WP-E: v1.1 vectors (sessions, log format, netns law)
  lightr-cri-acceptance/ # WP-F: B-items below
  ci/                    # WP-G: runner caps, kubelet-smoke harness, plugins
```

Workspace dep additions (W0 pins exact from lockfile): `axum 0.7`,
`tokio-tungstenite 0.24` (or axum's ws), `flate2 1` (SPDY zlib dictionary),
`nix 0.29` (sched/mount/pty), `httparse`. `spdystream-rs` is NOT a
dependency of record — WP-C transcribes/forks needed parts per research
brief (2-week-old crate; vendor what's vetted).

## 3. FROZEN laws per WP

**WP-A fake v1.1** — spawn with held pipes (tty=false) or pty pair
(tty=true, `nix::pty::openpty`); tee output to the CRI log file (contract
§C format, append-only, empty file from start); `open_exec` really executes
inside the container's netns (setns) when present; `open_attach` hands the
held container stdio; exit codes via existing reaper. RemoveImage/InUse and
all v1 laws unchanged. Stats: unchanged (/proc).

**WP-B lightr-cri-net** — netns create/teardown per containerd pattern
(bind-mount pin; umount2-then-unlink LAW; orphan sweep helper); CNI chain
invocation per research brief (env+stdin contract, forward ADD/reverse DEL,
runtimeConfig.portMappings, prevResult threading, error JSON surfaced as
BackendError::Internal with plugin msg); conflist discovery
(lexicographic first in /etc/cni/net.d, override via env LIGHTR_CNI_CONF);
probe API: `fn cni_available() -> Option<CniEnv>` (caps + conf + binaries) —
probe-truthful, never fake. Unit-testable parts (conflist derivation, result
parsing) MUST be host-testable without privileges.

**WP-C lightr-cri-stream** — implement research matrix EXACTLY:
SPDY exec/attach (negotiate v4 max), SPDY portforward (pairs,
port/requestID headers, RST on dial failure), WS exec/attach ≤v4 (channel
framing, initial empty write, resize JSON, close 1000), WS portforward
(query ports, u16 LITTLE-endian prefix per channel), v4 `metav1.Status`
exit delivery (bare-decimal ExitCode cause). Token registry: 8-char
base64url/6 random bytes, 1-min TTL, single-use, 1000 cap, 404 on miss.
Server: axum on 127.0.0.1 ephemeral; handler consumes token → drives a
`StreamSession` (contract §B) via spawn_blocking/AsyncFd bridges; sessions
die with the connection (no persisted state — listener law). zlib UNCERTAIN
from the brief: implement real SPDY dictionary inflate for SYN_STREAM
headers; emit SYN_REPLY with valid compressed headers (do NOT ship the Box
shortcut; budget says real zlib).

**WP-D shell/server v1.1** — Exec/Attach/PortForward RPCs: validate
container/sandbox state via backend, cache params, mint token, return
absolute URL (http://127.0.0.1:PORT/VERB/TOKEN). Decode into v1.1 config
fields: dns, port_mappings, host_network (sandbox), tty/stdin (container).
PodSandboxStatus.network.ip from SandboxStatus.ip. Status.NetworkReady
honest from backend/net probe. RuntimeConfig stays UNIMPLEMENTED (literal
code — kubelet law). Zero state beyond Arc + the token registry handle
(registry is ephemeral by definition; crash = tokens lost = clients retry —
crash-only compatible, document in code).

**WP-E vectors v1.1** — new vectors: exec session echo/exit-code via
open_exec handles; attach round-trip; log file exists + CRI format parse;
netns law (mock CNI via a stub plugin script writing canned result JSON —
vectors stay unprivileged: the stub plugin is a shell script in
vectors/bin/); host_network=true → ip None; dns synthesis content;
crash-recovery: reopen after start preserves log file append point.
Privileged REAL-CNI vectors live in acceptance (B-items), not here.

**WP-F acceptance B-items** (probe-gated; REQUIRE_PROBES=1 on linux CI):
- B1 streaming exec: `crictl exec -it`-equivalent via crictl non-sync exec
  (SPDY path) echoes and returns exit code.
- B2 attach: crictl attach to a `cat` container round-trips a line.
- B3 portforward: crictl port-forward → curl through the tunnel hits a
  pod-IP nginx-equivalent (use our own static-file binary or busybox httpd
  from PATH — NO external image pulls; fake images are synthetic).
- B4 CNI: runp (non-host-network) gets an ip; curl ip:port from runner
  netns answers; rmp tears down (netns gone, ip released).
- B5 hostPort: portmap DNAT 127.0.0.1:12000 → pod answers.
- B6 logs: crictl logs shows container output; file format valid.
- B7 kubelet-smoke (R1 EXIT): kubelet v1.33.13 (pinned sha256, research
  brief) standalone + static hostNetwork pod via OUR socket → phase Running
  via readOnlyPort within 120s; kubelet log free of CRI fatal errors.
- B8 critest re-baseline: narrowed skips (see WP-G), GREENLIST grows;
  zero regressions.

**WP-G ci** — re-provision runner script with
`--cap-add SYS_ADMIN --cap-add NET_ADMIN --security-opt apparmor=unconfined`
(+ document `--privileged` fallback); install CNI plugins v1.9.1 (sha256s in
research brief) to /opt/cni/bin + conflist; kubelet binary cached
(dl.k8s.io pinned sha256); critest-skips.txt NARROWED: remove streaming,
networking, log, default-command families (now graded); keep security-
context/OOM/privileged-mount families with reasons (need real engine — R2);
reconcile the ~8 previously-passing-but-unlisted items into GREENLIST.
kubelet-smoke harness script (config YAML from research brief verbatim).

## 4. Conflict map & merge DAG

W0-R1 (lead): backend v1.1 deltas + new crate skeletons + workspace pins +
vector schema extension — everything compiles, fleet reads frozen surface.
| WP | owner-glob | dep |
|---|---|---|
| A | crates/lightr-cri-fake/** | W0-R1 |
| B | crates/lightr-cri-net/** | W0-R1 |
| C | crates/lightr-cri-stream/** | W0-R1 |
| D | crates/lightr-cri-shell/src/** + lightr-cri-server/src/** | W0-R1 |
| E | crates/lightr-cri-vectors/** + vectors/** | W0-R1 |
| F | crates/lightr-cri-acceptance/** | W0-R1 |
| G | ci/** + .github/workflows/** | W0-R1 |
CONFLICT-FREE (crate/dir-disjoint; shell lib.rs + Cargo.tomls are W0-frozen).
Merge DAG: A → (B, C) → D → E → F → G → critic (opus cold).
Integration wiring fixes = lead.

## 5. R1 exit (definition of done)

All R0 gates stay green; B1–B8 green on linux CI (B7 = the headline);
vectors v1.1 green on macOS too; clippy/fmt clean; budgets unchanged
(machine-class); cold critic verdict resolved with zero unwaived debt;
whitepaper §8 table updated ONLY with CI-signed numbers (tense law).
