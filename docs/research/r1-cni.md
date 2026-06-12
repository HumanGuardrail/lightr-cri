# R1 research — CNI + networking conformance (verified 2026-06-12)

Headline: **no "IP-string-only" path exists.** Everywhere critest reads
PodSandboxStatus.network.ip it immediately DIALS it from the host netns
(HTTP GET, retries ≤1min, requires 200). Networking conformance = real
data path.

## critest v1.33 networking specs → real requirements

| Spec | Requires |
|---|---|
| DNS config [Conformance] | resolv.conf SYNTHESIS only (ExecSync `cat /etc/resolv.conf` asserting `nameserver 10.10.10.10`, `search google.com`, `options ndots:8` substrings) — no packets |
| port mapping (container port) [Conformance] | nginx pod, host-routable pod IP answering :80 |
| port mapping (host+container port) [Conformance] | real hostPort DNAT/proxy 127.0.0.1:12000→pod:80 (ports 12000-12003 must be free) |
| portforward [Conformance] | routable pod IP AND working SPDY portforward streaming server |
| portforward in host network (non-Conf, default-run) | host-netns sandbox + streaming |
| HostNetwork true/false | honest netns sharing/isolation + WORKING CRI LOG FILES (test reads netstat output from the log file at log_path!) |
| Multiple Containers should support network | pod-IP dial again (httpd:80) |

## Pinned plugins (verified byte-exact 2026-06-12)

containernetworking/plugins **v1.9.1**:
```
b98f74a0f8522f0a83867178729c1aa70f2158f90c45a2ca8fa791db1c76b303  cni-plugins-linux-amd64-v1.9.1.tgz
56171987d3947707c3563db2f4001bccaf50fd63468611b9f3cbecb1375ee7ec  cni-plugins-linux-arm64-v1.9.1.tgz
```
Smallest honest conflist (`/etc/cni/net.d/10-lightr.conflist`): **bridge
(isGateway, ipMasq) + host-local IPAM (10.88.0.0/16) + portmap**
(`{"type":"portmap","capabilities":{"portMappings":true}}` — cri-o issue
4771 proves hostPort fails without it). `loopback` invoked separately
per-sandbox (k8s-correct; critest never asserts it). Loopback-only is NOT
viable (plugin does LinkSetUp(lo) only; 127.0.0.1 fails the host dial).

## Invocation contract (CNI spec 1.0/1.1; target cniVersion "1.0.0")

Exec plugin binary with env: CNI_COMMAND (ADD/DEL/CHECK/VERSION),
CNI_CONTAINERID (`[a-z0-9][a-z0-9_.-]*`), CNI_NETNS (path; required ADD,
optional DEL), CNI_IFNAME, CNI_ARGS (`k=v;k=v`), CNI_PATH. stdin = derived
PER-PLUGIN config JSON (not the conflist): inject top-level cniVersion+name,
`runtimeConfig` (only declared capabilities, e.g. portMappings for portmap),
`prevResult` from previous plugin. Iterate plugins[] FORWARD for ADD,
REVERSE for DEL. stdout = result JSON (cniVersion, interfaces[], ips[],
routes[], dns) or error JSON (code,msg,details) on non-zero exit.
Conventions: config dir /etc/cni/net.d, lexicographically FIRST conflist
wins; binaries /opt/cni/bin.

**No maintained Rust invocation crate** (rscni/cni-plugin are authoring-side;
`cni` abandoned; rust-cni unvetted). Command + 6 env vars + stdin/stdout
serde, reimplementing ~200 lines of libcni derivation. Precedent: youki does
no networking; kata consumes pre-populated netns; invocation logic
canonically exists only in Go (go-cni/ocicni).

## Netns lifecycle (containerd pattern, pkg/netns)

Create: mkdir `/run/netns` (NOT /var/run); touch `/run/netns/<name>`; on a
DEDICATED thread: `nix::sched::unshare(CLONE_NEWNET)` then bind-mount
`/proc/self/task/<gettid>/ns/net` → the path; thread exits, mount pins the
ns. (unshare(CLONE_NEWNET) is per-thread; never on an async worker.)
Teardown LAW: `umount2(path, MNT_DETACH)` THEN unlink (skip = EBUSY,
containerd#6143). Sweep /run/netns for orphans at startup.
Join at spawn: open netns file in parent, move OwnedFd into pre_exec, raw
`setns(fd, CLONE_NEWNET)` (async-signal-safe: no alloc in pre_exec).
Holder-process (`/proc/<pid>/ns/net`) is spec-valid but abandoned by
production runtimes (teardown impossible if holder dies) — use bind-mount.

## CI runner container requirements (verified empirically, Docker 28.3.2)

Minimal set: `--cap-add SYS_ADMIN --cap-add NET_ADMIN
--security-opt apparmor=unconfined`. Default seccomp allows
unshare/setns/mount IFF the container has CAP_SYS_ADMIN (profile is
cap-templated; verified `unshare -n true` green with just SYS_ADMIN).
veth/bridge/iptables-DNAT need NET_ADMIN. LinuxKit VM already has
bridge/veth/br_netfilter loaded; bridge-nf-call-iptables=1. `--privileged`
is the battle-tested fallback (kind hard-codes it) if cgroup//dev long tail
bites. Mac host CANNOT reach inner IPs (vpnkit) — all critest dials must run
INSIDE the CI container (true today: critest runs in the runner container).

UNCERTAIN: cri-o ≥1.30 hostport internals; rust-cni chaining; ECI status.
