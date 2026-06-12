# R1 research — CRI streaming server (verified 2026-06-12)

Headline: **SPDY/3.1 server-side is NOT avoidable.** critest v1.33.0 is
SPDY-only (no transport flag; `NewSPDYExecutor` hardcoded in
pkg/validate/streaming.go; portforward via `spdy.RoundTripperFor` +
`portforward.New`). kubectl 1.33 talks WebSocket to the apiserver, but the
apiserver TRANSLATES back to SPDY before the kubelet (KEP-4006
TranslateStreamCloseWebsocketRequests / PortForwardWebsockets), and the
kubelet blind-proxies (`NewUpgradeAwareHandler`) — the runtime sees SPDY.
WS-to-kubelet leg deferred to ~1.36 and even then the runtime stays SPDY.

## Conformance matrix to build

1. **SPDY exec/attach**, negotiate `v4.channel.k8s.io` (server max v4 — the
   reference CRI library does NOT serve v5 over SPDY; critest offers v5→v1,
   pick v4). Handshake: POST + `X-Stream-Protocol-Version` (echo pick),
   `101` + `Upgrade: SPDY/3.1`. Streams arrive as SYN_STREAM with header
   `streamType` ∈ stdin|stdout|stderr|error|resize; wait for expected count
   (30s creation timeout). Exec options in query params
   (`input/output/error/tty=1`).
2. **SPDY portforward**, subprotocol `portforward.k8s.io`: stream PAIRS with
   headers `port` (decimal), `streamType` ∈ data|error, `requestID`
   (multiplex; tolerate absence via id/id-2 heuristic). Error stream =
   plain text; RST both streams on failure so client can retry.
3. **WebSocket exec/attach ≤ v4** (`""`, `channel.k8s.io`, `base64.…`,
   `v4.channel.k8s.io`, `v4.base64.…`; NOT v5): binary frames
   `[channel_byte | payload]` — 0 stdin, 1 stdout, 2 stderr, 3 error,
   4 resize. On open: server writes one EMPTY message on lowest writable
   channel. Resize channel: concatenated JSON `{"Width":u16,"Height":u16}`.
   Close with code 1000; answer pings.
4. **WebSocket portforward** (legacy channel-based): ports from query string
   `?port=80&port=443`; 2 channels per port (2i data RW, 2i+1 error W);
   first 2 bytes server writes on EACH channel = port number u16
   LITTLE-endian.
5. **v4 exit/error delivery**: JSON `metav1.Status` on channel 3 / error
   stream. Success: `{"metadata":{},"status":"Success"}`. Non-zero exit:
   `status:"Failure"`, `reason:"NonZeroExitCode"`,
   `details.causes:[{"type":"ExitCode","message":"<bare decimal 0-255>"}]`.

## URL + token law (mirror reference library)

`http://127.0.0.1:<ephemeral-port>/{exec|attach|portforward}/{token}`.
Token: 8-char base64url from 6 crypto-random bytes, 1-minute TTL,
SINGLE-USE (consume before expiry check), max 1000 in flight ("too many in
flight" rejects the RPC), unknown/expired ⇒ HTTP 404. Full request params
cached server-side under the token. Plain HTTP on loopback is the accepted
model (containerd: stream_server 127.0.0.1:0, TLS off; kubelet verifies
https against system roots — so don't use TLS). critest requires non-empty
URL host (absolute URL; kubelet does NOT substitute empty hosts in 1.33).

## Rust ecosystem (verified)

- `spdystream-rs` v0.1.2 (pelagos-containers, Apache-2.0, created
  2026-05-28): tokio SPDY/3.1 mux WITH server side, built for CRI runtimes.
  TWO WEEKS OLD — treat as starting fork, not dependency of record.
- conmon-rs `streaming_server.rs` (containers org): production axum
  WebSocket exec/attach reference (v5-only; portforward is `todo!()`).
- AI45Lab/Box `src/cri/src/spdy.rs` (MIT): minimal hand-rolled SPDY/3.1
  server reference — maps SYN_STREAMs by open order, dodging zlib dictionary
  decompression; documented shortcuts.
- kube-rs: CLIENT-side WS (v4+v5) — useful as integration-test client, plus
  `crictl --transport websocket` (crictl 1.33 has the flag; critest doesn't).
- youki/kata/runwasi/krustlet/aurae: zero server-side streaming code.

## UNCERTAIN (budget for these)

1. **SPDY zlib header compression**: SYN_STREAM headers are zlib-compressed
   with the SPDY dictionary. Box's open-order shortcut may work against
   critest, but client-go may require valid compressed SYN_REPLY headers —
   UNVERIFIED against a live critest; budget real zlib-dictionary support.
2. spdystream-rs maturity (vet/fork).
3. v5-over-SPDY untrodden — advertise v4 max, like containerd/CRI-O.
