# scale-to-zero-proxy

Scale-to-zero for raw-TCP workloads on Control Plane — an always-on proxy that holds incoming connections, wakes the suspended workload, and suspends it again when idle.

## Purpose

Control Plane's serverless workloads scale to zero natively — but only for HTTP. Raw-TCP services (SFTP, databases, brokers, game servers) must run as standard/stateful workloads, and the platform neither holds inbound TCP connections during a scale-up nor scales standard workloads to zero without KEDA. This proxy fills that gap: it is the tiny always-on floor that makes a TCP workload effectively serverless.

## How it works

1. **Hold** — accepts the inbound TCP connection immediately (no more "connection refused") and holds it silently, sending zero bytes. It operates purely at the transport layer (L4): it never parses or produces application-protocol bytes, so TLS/SSH handshakes pass through untouched and the proxy holds no keys or credentials.
2. **Wake** — the first held connection un-suspends the target workload via the Control Plane API, authenticated as the proxy's own workload identity (`CPLN_TOKEN` + a policy scoped to exactly the target workload). Concurrent connects are deduplicated into a single wake.
3. **Readiness probe** — the target counts as ready only when it sends its first protocol bytes (e.g. the SSH banner) on a probe connection, which is then discarded; each client is spliced over a fresh connection. A bare TCP accept is not trusted: the platform mesh accepts connections even for suspended workloads.
4. **Splice** — bidirectional byte copy between client and target, with half-close propagation.
5. **Idle-down** — tracks active spliced connections; when the count reaches zero and a configurable idle-hold window expires, suspends the target again. Scale-down can never happen mid-transfer by construction.

Similar in spirit to Knative's Activator, adapted to Control Plane's workload and suspend model.

## Configuration

All configuration is via environment variables. `CPLN_*` values are injected automatically by the platform.

| Variable | Default | Meaning |
|---|---|---|
| `TARGET_WORKLOAD` | — (required) | Target workload name |
| `TARGET_GVC` | own GVC | Target workload's GVC |
| `TARGET_HOST` | `{workload}.{gvc}.cpln.local` | Override the target's internal endpoint |
| `PORT_MAPPINGS` | — (required) | Listener→target port pairs, e.g. `2022:2022` or `2022:2022,9000:9000`; a bare port maps to itself |
| `IDLE_HOLD` | `5m` | Keep-awake window after the last active connection closes |
| `MAX_HOLD` | `90s` | Max time to hold an unspliced connection before dropping it |
| `WAKE_TIMEOUT` | `120s` | Give-up bound for a single wake (API call + readiness polling) |
| `WAKE_POLL_INTERVAL` | `500ms` | Readiness probe cadence during a wake |
| `PROBE_WINDOW` | `2s` | Max wait for the server's first bytes on a readiness probe |
| `HEALTH_PORT` | `8081` | HTTP `/healthz` endpoint (JSON: active connections, wake-in-progress) for the proxy's own probes |

## Deploying

- Run the proxy as a **standard workload, `minScale: 1`** (it is the always-on floor — typically ~100m CPU / 128Mi).
- Give it an **identity** and a **policy** granting `edit` on exactly the target workload (`targetKind: workload` + `targetLinks`). No service-account keys: the injected `CPLN_TOKEN` is used via `CPLN_ENDPOINT`.
- Expose the listener port(s) publicly with `loadBalancer.direct` on the proxy; the target stays internal.
- Image: `ghcr.io/controlplane-com/scale-to-zero-proxy` — pin a version tag (e.g. `:0.1.0`), published automatically from `main`/`v*` tags.

## Important notes

- **Plan for the cold start.** Measured end to end with a real OpenSSH `sftp` client on Control Plane: **~13.5s** from connect to transfer-complete against a cold target (**~1.4s** warm). The proxy prevents connection-refused during the wake, but it cannot extend a client's application-level timeout — SSH clients commonly default to ~15s banner timeouts (paramiko, WinSCP). Instruct clients to set generous timeouts (≥60s) or retry with backoff; the idle-hold window makes retries land warm. For clients you cannot configure, run the target always-on instead of behind this proxy.
- **The target must speak first.** Readiness is proven by the server's first protocol bytes, so server-speaks-first protocols (SSH/SFTP, FTP control, SMTP, ...) are supported. Client-speaks-first protocols are not, yet — the mesh makes a bare TCP accept meaningless as a readiness signal.
- **One target per proxy instance.** Multiple port mappings are supported, but they all belong to one target workload; deploy another proxy instance for another service.
- **A proxy restart forgets its idle timer.** Held/spliced connections drop with the proxy (clients reconnect, triggering a fresh wake) and a new idle countdown starts. Worst case is a late suspend or an extra wake — never a mid-transfer scale-down.
- **Account for the load balancer in idle cost.** Public raw-TCP exposure requires a dedicated `loadBalancer.direct`; when idle, that LB — not the proxy's compute — is typically the dominant cost.
