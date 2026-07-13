# Design

## Context and constraints (verified on-platform 2026-07-10)

- Serverless workloads are HTTP-only; raw TCP requires standard/stateful workloads.
- `minScale: 0` on standard workloads is rejected unless KEDA is used (platform validation error).
- The platform does not hold/queue inbound TCP during scale-up: at zero replicas, connects are refused.
- **Chosen wake/sleep mechanism: the workload `suspend` flag**, toggled via the Control Plane API. No KEDA anywhere — the proxy is the single owner of both transitions.
- Spike measurements (drakkan/sftpgo 2.7.x-distroless-slim, 20MB, test GVC): initial deploy incl. image pull 17s; suspend→wake to ready: 10–18s across cycles. Readiness-probe `failureThreshold` caps at 20.

## Architecture

Single static Go binary, distroless image, deployed as a standard `minScale: 1` workload.

- **One target workload per proxy instance.** Multiple listener→target port mappings are supported (env-configured), but they all belong to one target — this keeps the wake/sleep state machine trivial. Another service = another proxy instance.
- **Generic by design**: nothing protocol- or service-specific. Config is entirely env-driven so any template can reuse the image.

### State machine

- On inbound accept: hold the connection (no bytes). If target is suspended/waking, trigger wake (singleflight — N connects, one API call).
- Wake: `PATCH` the target workload, `spec.defaultOptions.suspend: false`, authorized by the proxy's workload identity (injected `CPLN_TOKEN`; policy grants edit on exactly the target workload — no service-account keys).
- Readiness gate: TCP-dial the target's internal endpoint port until accept; only then splice held connections (application handshakes must reach the real backend, never the proxy).
- Active-connection tracking: splice count > 0 → target stays awake. Count hits 0 → idle-hold timer starts; any new connection cancels it. Timer expiry → `PATCH suspend: true`.
- Held-connection cap: connections held longer than a configurable max-hold are dropped (dead clients must not pin wakes).

### Failure-mode analysis

- **Proxy restart** loses in-memory state (active counts, timers). Consequence: held/spliced client connections drop with the proxy (clients reconnect → fresh wake); a fresh idle timer starts. Worst case is a late sleep or an extra wake — never a mid-transfer scale-down, never data loss. Accepted trade-off.
- **Wake API failure**: retry with backoff while connections are held; connections exceeding max-hold drop. Surface failures in logs.
- **Client gives up before backend ready** (short application timeout): unavoidable by design — the proxy cannot speak the application protocol to stall for time. Mitigations are client-side (timeout ≥60s or retries) and template-side (slim image, embedded state, no external dependencies on the wake path). Documented honestly in every consuming template.

### Config surface (env)

| Variable | Meaning |
|---|---|
| `TARGET_WORKLOAD` | target workload name |
| `TARGET_GVC` | target GVC (defaults to own GVC) |
| `PORT_MAPPINGS` | listener→target port pairs, e.g. `2022:2022` (comma-separated for multiple) |
| `IDLE_HOLD` | keep-awake window after last active connection closes (e.g. `5m`) |
| `MAX_HOLD` | max time to hold an unspliced connection before dropping (e.g. `90s`) |
| `WAKE_POLL_INTERVAL` / `WAKE_TIMEOUT` | readiness-gate dial cadence and give-up bound |
| `HEALTH_PORT` | HTTP health endpoint for the proxy's own probes |

### Resolved (micro-spike, 2026-07-13)

1. **Auth from inside a workload — VALIDATED, with a critical gotcha.** Calls must go to **`$CPLN_ENDPOINT`** (injected env, value `http://api.cpln.io` — plain HTTP; this path attests the calling workload) with a **raw `Authorization: $CPLN_TOKEN` header** (no `Bearer`). Calling `https://api.cpln.io` directly arrives as anonymous and 403s — the token is only honored on the attested path, and only from the workload it was injected into. Permission verb: `edit`; policy is `targetKind: workload` + `targetLinks` pinned to exactly the target workload. Negative test confirmed: PATCHing any other workload returns 403 naming the identity as the principal.
2. **PATCH shape — VALIDATED**: `PATCH {CPLN_ENDPOINT}/org/{org}/gvc/{gvc}/workload/{name}` with deep-merge body `{"spec":{"defaultOptions":{"suspend":true|false}}}` (cpln's patch syntax deep-merges scalars; `$`-operators only needed for arrays/removals). Returns 200 + updated workload. Suspend drains the deployment; un-suspend to ready measured at **14s** (consistent with the earlier 10–18s window).

### Open items

3. Client-observed connect→banner timing through the full proxy path (e2e test with a real sftp client) vs the ~15s bar.
