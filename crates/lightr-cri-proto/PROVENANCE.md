# Proto provenance

- Source: `kubernetes/cri-api`, tag `kubernetes-1.33.1`,
  `pkg/apis/runtime/v1/api.proto`
- Fetched: 2026-06-11 via https://raw.githubusercontent.com/kubernetes/cri-api/kubernetes-1.33.1/pkg/apis/runtime/v1/api.proto
- sha256: `7d60d146fb4cc8c43b1c8b28ca6d79c2f80577eae5a0158aa920b170ae664ebb`
- Companion: `proto/github.com/gogo/protobuf/gogoproto/gogo.proto` (import
  required by api.proto; from gogo/protobuf master, fetched 2026-06-11,
  sha256 `f2f77edf7de807ded7884813d851656f4ccb18262db717a8f31061995f3e7324`)
- Codegen: committed under `src/generated/` via `tools/protogen`
  (tonic-build; no build.rs, no protoc needed by consumers).
  CI job `verify-codegen` re-runs protogen and diffs — drift is red.
