# Changelog

All notable changes to PulseMQ are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
PulseMQ uses [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

---

## [1.0.1] — 2026-04-09

### Fixed
- **Queue restart bug**: In-flight messages restored after a server restart now correctly preserve `MaxRetries`. Previously, `MaxRetries` defaulted to `0` after restart, causing messages to be retried indefinitely instead of being routed to the DLQ after the configured retry count was exhausted.
- **Consume response**: The `attempt` field in consume (`GET .../messages`) responses was always `0`. It now correctly reflects the current delivery attempt number.

### Added
- Test coverage increased from 86.7% → 89.7% across all packages (broker, config, consumer, dlq, metrics, namespace, node, queue, scheduler, storage/local, HTTP handlers, WebSocket, types, client).

---

## [1.0.0] — 2026-02-28

### Added

**Core server**
- Single-binary Go server with zero runtime dependencies
- Append-only WAL + bbolt index storage layer; crash-safe on restart
- Min-Heap scheduler: O(1) peek, O(log N) insert — no database polling
- Queue state machine: READY → IN_FLIGHT → DELETED / DEAD_LETTER
- Visibility timeout + ACK / NACK with automatic redelivery on timeout expiry
- Per-queue dead-letter queue (DLQ); manual replay via `POST .../dlq/replay`
- Namespace registry with JSON persistence; auto-created on first use
- Configurable queue limits: `max_messages`, `max_retries`, `visibility_timeout_ms`
- WAL format is Raft-ready from day 1 (`term`, `log_index`, `node_id` fields)

**HTTP API**
- `POST /namespaces/{ns}/queues/{name}/messages` — single publish with optional `deliver_at`
- `POST /namespaces/{ns}/queues/{name}/messages/batch` — batch publish (up to 100)
- `GET  /namespaces/{ns}/queues/{name}/messages` — poll consume with visibility timeout
- `DELETE /messages/{receipt_handle}` — ACK
- `POST   /messages/{receipt_handle}/nack` — NACK with retry / DLQ routing
- `GET  /namespaces/{ns}/queues/{name}/dlq` — inspect DLQ
- `POST /namespaces/{ns}/queues/{name}/dlq/replay` — replay DLQ → primary queue
- `POST /namespaces/{ns}/queues/{name}/dlq/purge` — purge DLQ messages
- `POST /namespaces/{ns}/queues/{name}/subscriptions` — register webhook
- `DELETE /subscriptions/{id}` — unregister webhook
- `GET  /namespaces/{ns}/queues/{name}/ws` — WebSocket push consumer
- Full namespace CRUD: `POST/GET/DELETE /namespaces`
- Full queue CRUD: `POST/GET/DELETE /namespaces/{ns}/queues/{name}`
- `POST /namespaces/{ns}/queues/{name}/purge` — purge all messages from a queue

**Observability**
- `GET /health` — JSON status (node ID, queue count, uptime, version)
- `GET /metrics` — Prometheus text format on both main port and dedicated metrics port (9090)
- Metrics: `published_total`, `consumed_total`, `acked_total`, `nacked_total`, `dlq_routed_total`, HTTP request count + duration

**Dashboard**
- Live browser dashboard at `/GET /dashboard`
- Auto-refreshing queue table: depth, ready, in-flight, scheduled, DLQ depth, status
- Summary cards: total depth, namespaces, scheduled, DLQ alerts
- Paginated queue table (50 queues per page) — O(N) summary endpoint + page-local DLQ lookups
- Create queue modal (validates lowercase naming, auto-lowercases input)
- Delete queue button with confirmation
- Send message modal with optional `deliver_at` and metadata
- Purge queue button
- Namespace grouping headers
- API key settings (stored in browser `localStorage`)
- Playground page at `/playground`

**Security & Middleware**
- CORS middleware (permissive defaults, safe for same-origin dashboard)
- `MaxBodyMiddleware` — request body size limit
- `LoggingMiddleware` — structured JSON request logs
- `AuthMiddleware` — static `X-Api-Key` enforcement (opt-in via `auth.enabled`)
- `RateLimitMiddleware` — per-IP token bucket
- Queue/namespace name validation: lowercase alphanumeric + hyphens enforced at HTTP layer

**Performance optimisations**
- `GET /api/stats/summary` — O(N) aggregate (zero storage I/O): total queues, namespaces, total depth, scheduled, DLQ alert count
- `GET /api/stats?page=N&limit=50` — paginated queue stats; DLQ depth lookup only for current page
- `QueueStatsPaged()` in broker: alphabetically sorted, page-local DLQ I/O

**Go SDK** (`pkg/client`)
- `New(baseURL, ...Option)` — create client
- `WithAPIKey(key)`, `WithHTTPClient(c)`, `WithTimeout(d)` options
- `Publish(ctx, ns, queue, body, ...PublishOption)` — single publish
- `WithDelay(d)`, `WithDeliverAt(t)`, `WithMaxRetries(n)`, `WithMetadata(m)` publish options
- `PublishBatch(ctx, ns, queue, [][]byte, ...PublishOption)` — batch publish
- `Consume(ctx, ns, queue, n, ...ConsumeOption)` — poll consume
- `WithVisibilityTimeout(d)` consume option
- `Ack(ctx, receiptHandle)`, `Nack(ctx, receiptHandle)`
- `DrainDLQ(ctx, ns, queue, limit)`, `ReplayDLQ(ctx, ns, queue, limit)`
- `Subscribe(ctx, ns, queue, url, secret)`, `Unsubscribe(ctx, id)`
- `CreateNamespace(ctx, name)`, `ListNamespaces(ctx)`, `DeleteNamespace(ctx, name)`
- `CreateQueue(ctx, ns, name, ...QueueOption)`, `DeleteQueue(ctx, ns, name)`
- `Health(ctx)` — server health
- `Stats(ctx)` — all queue depths (first page, up to 200)
- `StatsPaged(ctx, page, limit)` — paginated queue depths + total

**Docker**
- Multi-stage Dockerfile: builder (Go 1.24) + distroless runtime — ~20 MB image
- `docker-compose.yml` with data volume and config mount

**Documentation**
- `docs/getting-started.md` — 10-step quickstart guide
- `docs/api-reference.md` — complete HTTP API reference with request/response examples
- `docs/architecture.md` — internal design, package map, write path, scheduler algorithm
- `README.md` — project overview, quick start, SDK examples, performance table, roadmap
- `CONTRIBUTING.md` — development setup, coding standards, commit message convention
- `CODE_OF_CONDUCT.md` — Contributor Covenant v2.1
- `SECURITY.md` — vulnerability reporting, security model, known limitations
- `CHANGELOG.md` — this file

**Tests**
- Unit tests for broker, queue, scheduler, DLQ, namespace, metrics, node, config, storage
- HTTP handler integration tests covering all major endpoints
- Go SDK integration tests (spin up real server in-process)
- Test coverage for invalid name validation → 400, paginated stats SDK, DLQ drain/replay

---

[Unreleased]: https://github.com/sneh-joshi/pulsemq/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/sneh-joshi/pulsemq/releases/tag/v1.0.0
