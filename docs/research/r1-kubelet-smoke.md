# R1 research — standalone kubelet smoke (verified 2026-06-12, k8s release-1.33 source)

Goal: smallest honest "a REAL kubelet ran a pod on lightr-cri", in a privileged
Docker container, no apiserver.

## Standalone mode: supported, CI-tested

No `--kubeconfig` ⇒ standalone. Official tutorial exists; in-tree harness
`test/e2e_node/standalone_test.go` (`Feature:StandaloneMode`). Only the CLI
flag `--pod-manifest-path` is deprecated; `staticPodPath` in
KubeletConfiguration is fully supported.

## Minimal KubeletConfiguration (validated against 1.33 validation.go)

```yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
containerRuntimeEndpoint: unix:///run/lightr/lightr.sock
staticPodPath: /etc/kubernetes/manifests
enableServer: false
readOnlyPort: 10255            # http://127.0.0.1:10255/pods = assertion endpoint
address: 127.0.0.1
authentication: { webhook: { enabled: false } }
authorization: { mode: AlwaysAllow }
failSwapOn: false
cgroupDriver: cgroupfs
cgroupsPerQOS: false           # kubelet creates NO cgroup tree (sidesteps nested-docker cgroup v2)
enforceNodeAllocatable: ["none"]   # REQUIRED combo with cgroupsPerQOS=false
localStorageCapacityIsolation: false  # avoids cadvisor capacity flakes in nested containers
makeIPTablesUtilChains: false
fileCheckFrequency: 5s
logging: { format: text }
```

Run: `kubelet --config=... --hostname-override=smoke-node --v=4`.
Smoke pod: static pod, `hostNetwork: true`, `command: ["sleep","infinity"]`,
`imagePullPolicy: IfNotPresent`.
Assert: poll `127.0.0.1:10255/pods` for phase Running.

## Hard runtime requirements (source-verified)

- **Version handshake**: `runtime_api_version` validated as CRI v1 at dial;
  `version` field must be EXACTLY `"0.1.0"` (kuberuntime_manager.go L271) else
  kubelet exits. Non-empty runtime_name/version.
- **RuntimeConfig (KubeletCgroupDriverFromCRI beta ON in 1.33)**: called once
  at startup. Literal gRPC code UNIMPLEMENTED ⇒ clean fallback to config
  cgroupDriver. ANY OTHER ERROR CODE ⇒ 3 retries then kubelet startup FAILS.
- **Status every 5s**: BOTH RuntimeReady and NetworkReady conditions must be
  PRESENT; RuntimeReady=true mandatory or no pod ever syncs.
  NetworkReady=false only blocks non-hostNetwork pods (kubelet.go:1968).
- **PLEG**: ListPodSandbox + ListContainers every 1s; failing relists for
  3min ⇒ "PLEG is not healthy" ⇒ sync blocked.
- **Pod start chain**: RunPodSandbox → PodSandboxStatus → ImageStatus
  (IfNotPresent) → PullImage (returned image_ref must be non-empty) →
  CreateContainer → StartContainer → ContainerStatus.
- **/dev/kmsg fatal if absent** (cadvisor OOM watcher) — privileged container
  has it.
- **Log files decoupled from health**: pod Running derives from
  ContainerStatus only. But if no file exists at log_path, kubelet calls
  ReopenContainerLog every ~10s per container (tolerated, log spam) —
  runtime should touch an empty file at sandbox log_directory + container
  log_path.

## Tolerated/never-called matrix (1.33)

| RPC | Verdict |
|---|---|
| RuntimeConfig | UNIMPLEMENTED tolerated (must be literal code) |
| ImageFsInfo, ListImages, ListContainerStats | tolerated, error logs (~10s eviction cycle) |
| UpdateRuntimeConfig | not called when --pod-cidr unset; errors only logged |
| PodSandboxStats/ListPodSandboxStats | NEVER (PodAndContainerStatsFromCRI alpha off) |
| GetContainerEvents | NEVER (EventedPLEG alpha off) |
| ListMetricDescriptors/ListPodSandboxMetrics | NEVER (same gate) |
| ReopenContainerLog | ~10s/container if log_path missing; tolerated |
| Exec/Attach/PortForward/ExecSync | not used by smoke pod (no probes) |

## Binary (pinned)

`https://dl.k8s.io/release/v1.33.13/bin/linux/amd64/kubelet`
sha256 `0415ef7778646172f7aad830cbe09a7d903461eef9e15cd34193b85d95dfff0d`
(82,022,692 bytes; Apache-2.0; verified live 2026-06-12 via stable-1.33.txt).

## Prior art

k8s e2e_node standalone_test.go is the canonical harness; no public precedent
of standalone-kubelet-vs-handwritten-CRI in CI found (youki/runwasi/kata use
containerd/kind/cluster e2e) — this harness is genuinely uncommon.

UNCERTAIN: cadvisor-in-Docker-Desktop env quirks beyond the mitigations
above; readOnlyPort deprecation warnings (functional in 1.33).
