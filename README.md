# scale-to-zero-proxy

Scale-to-zero for raw-TCP workloads on Control Plane — an always-on proxy that holds incoming connections, wakes the suspended workload, and suspends it again when idle.

## Why

Control Plane's serverless workloads scale to zero natively — but only for HTTP. Raw-TCP services (SFTP, databases, brokers, game servers) must run as standard/stateful workloads, and the platform neither holds inbound TCP connections during a scale-up nor scales standard workloads to zero without KEDA. This proxy fills that gap: it is the tiny always-on floor that makes a TCP workload effectively serverless.

## How it works

1. **Hold** — accepts the inbound TCP connection immediately (no more "connection refused") and holds it silently, sending zero bytes. It operates purely at the transport layer (L4): it never parses or produces application-protocol bytes, so TLS/SSH handshakes pass through untouched and the proxy holds no keys or credentials.
2. **Wake** — the first held connection un-suspends the target workload via the Control Plane API, authenticated as the proxy's own workload identity (`CPLN_TOKEN` + a policy scoped to exactly the target workload). Concurrent connects are deduplicated into a single wake.
3. **Splice** — polls the target's real port until it accepts, then splices each held connection through (bidirectional byte copy).
4. **Idle-down** — tracks active spliced connections; when the count reaches zero and a configurable idle-hold window expires, suspends the target again. Scale-down can never happen mid-transfer by construction.

Similar in spirit to Knative's Activator, adapted to Control Plane's workload and suspend model.

## Status

Design phase. See [docs/DESIGN.md](docs/DESIGN.md) for the architecture, platform constraints, and validated spike measurements. First consumer: the `sftpgo` marketplace template.

## Honest limits

The proxy prevents connection-refused during wake; it cannot extend a client's application-level timeout (e.g. SSH banner timeout, commonly ~15s). Measured wake times on Control Plane are 10–18s with a slim image — cooperative clients (timeout ≥60s, or retry with backoff) see zero failures; for strict/unmodifiable clients, run the target always-warm instead.
