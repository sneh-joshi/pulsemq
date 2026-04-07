# PulseMQ HTTP API Reference

**Base URL:** `http://<host>:<port>` (default port 8080)

**Authentication:** When `auth.enabled = true`, every request must include the header:
```
X-Api-Key: <your-api-key>
```

**Request/Response format:** `application/json`

**Message body encoding:** All message bodies are [base64](https://en.wikipedia.org/wiki/Base64) encoded (standard encoding, no line breaks). Decode the `body` field before processing.

**Time:** All timestamps are UTC milliseconds since Unix epoch (`int64`).

---

## Table of Contents

- [Health & Observability](#health--observability)
- [Namespaces](#namespaces)
- [Queues](#queues)
- [Messages — Publish](#messages--publish)
- [Messages — Consume](#messages--consume)
- [Messages — Ack / Nack](#messages--ack--nack)
- [Dead-Letter Queue (DLQ)](#dead-letter-queue-dlq)
- [Webhook Subscriptions](#webhook-subscriptions)
- [Error responses](#error-responses)

---

## Health & Observability

### `GET /health`

Returns the server's current status.

**Response `200 OK`**

```json
{
  "status": "ok",
  "node_id": "01JP5X4PRJ3G2V9WB8E7KT8M5F",
  "queues": 4,
  "uptime": "2m30s",
  "uptime_ms": 150000,
  "version": "1.0.0"
}
```

---

### `GET /metrics`

Returns Prometheus-format metrics (text/plain; version=0.0.4).
Also available on the dedicated metrics port (default 9090).

**Metrics exposed:**

| Metric | Labels | Description |
|--------|--------|-------------|
| `pulsemq_messages_published_total` | namespace, queue | Messages published |
| `pulsemq_messages_consumed_total` | namespace, queue | Messages delivered to consumers |
| `pulsemq_messages_acked_total` | namespace, queue | Messages ACKed |
| `pulsemq_messages_nacked_total` | namespace, queue | Messages NACKed |
| `pulsemq_messages_dlq_routed_total` | namespace, queue | Messages routed to DLQ |
| `pulsemq_http_requests_total` | method, path, status | HTTP requests |
| `pulsemq_http_request_duration_milliseconds_sum` | method, path | Sum of request durations |
| `pulsemq_http_request_duration_milliseconds_count` | method, path | Request count |

---

### `GET /dashboard`

Returns the built-in HTML dashboard. Open in a browser.

---

### `GET /api/stats`

### `GET /api/stats`

Returns a paginated page of per-queue depth snapshots (used by the dashboard).

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `page` | 1 | Page number (1-based) |
| `limit` | 50 | Page size (1–200) |

**Response `200 OK`**

```json
{
  "queues": [
    {
      "namespace": "payments",
      "name": "invoices",
      "key": "payments/invoices",
      "ready": 42,
      "in_flight": 3,
      "scheduled": 5,
      "depth": 50,
      "dlq_depth": 1
    }
  ],
  "total": 1,
  "page": 1,
  "limit": 50,
  "total_pages": 1
}
```

Queues are sorted alphabetically by namespace then name. DLQ depth is only looked up for queues on the current page — making this endpoint efficient regardless of total queue count.

---

### `GET /api/stats/summary`

Returns aggregate counts in a single O(N) pass — zero per-queue storage I/O. Used by the dashboard summary cards.

**Response `200 OK`**

```json
{
  "total_queues": 12,
  "namespaces": 3,
  "total_depth": 847,
  "total_scheduled": 35,
  "dlq_alerts": 2
}
```

| Field | Description |
|-------|-------------|
| `total_queues` | Total number of primary queues |
| `namespaces` | Number of unique namespaces with at least one queue |
| `total_depth` | Sum of ready + in-flight + scheduled across all queues |
| `total_scheduled` | Total messages awaiting future delivery |
| `dlq_alerts` | Number of queues that have at least one DLQ message |

---

## Namespaces

Namespaces are logical containers for queues. They are created automatically when a message is first published to a queue (auto-create behaviour). Use the namespace API to manage them explicitly.

---

### `POST /namespaces`

Create a namespace.

**Request body**

```json
{ "name": "payments" }
```

Name rules: 1–64 lowercase alphanumeric characters or hyphens, must start with a letter or digit.

**Response `201 Created`**

```json
{ "status": "created", "name": "payments" }
```

**Errors**

| Code | Meaning |
|------|---------|
| 400 | Invalid name |
| 409 | Namespace already exists |

---

### `GET /namespaces`

List all registered namespaces sorted by name.

**Response `200 OK`**

```json
{
  "namespaces": [
    { "name": "analytics", "created_at": 1740744000000 },
    { "name": "payments",   "created_at": 1740700000000 }
  ]
}
```

---

### `DELETE /namespaces/{ns}`

Delete a namespace. The caller is responsible for deleting all queues first.

**Response `204 No Content`**

**Errors**

| Code | Meaning |
|------|---------|
| 404 | Namespace not found |

---

## Queues

### `POST /namespaces/{ns}/queues/{name}`

Create a queue with optional configuration.

**Path parameters**

| Parameter | Description |
|-----------|-------------|
| `ns` | Namespace name |
| `name` | Queue name |

**Name rules:** Both namespace and queue name must match `^[a-z0-9][a-z0-9\-]{0,63}$` — lowercase letters, digits, and hyphens only; 1–64 characters; must begin with a letter or digit. Uppercase, underscores, spaces and other characters are rejected with `400 Bad Request`.

**Request body** (all fields optional — omit to use server defaults)

```json
{
  "visibility_timeout_ms": 30000,
  "max_messages": 100000,
  "max_retries": 5,
  "max_batch_size": 100
}
```

| Field | Type | Description |
|-------|------|-------------|
| `visibility_timeout_ms` | int64 | How long a consumed message stays locked before reverting to READY. Default: 30000 (30s). |
| `max_messages` | int64 | Hard cap on READY + IN_FLIGHT messages. Producers get 429 when exceeded. Default: 100000. |
| `max_retries` | int | Delivery attempts before the message is routed to DLQ. 0 = move to DLQ on first failure. Default: 5 (≈150s recovery window at 30s visibility timeout). |
| `max_batch_size` | int | Max messages returned per `Consume` call. Default: 100. |

**Response `201 Created`**

```json
{ "status": "created" }
```

**Errors**

| Code | Meaning |
|------|---------|
| 400 | Invalid namespace or queue name (must be lowercase alphanumeric + hyphens) |

---

### `GET /namespaces/{ns}/queues`

List all queues in a namespace.

**Response `200 OK`**

```json
{
  "queues": [
    "payments/invoices",
    "payments/refunds"
  ]
}
```

---

### `DELETE /namespaces/{ns}/queues/{name}`

Permanently delete a queue and all its messages.

**Response `204 No Content`**

---

## Messages — Publish

### `POST /namespaces/{ns}/queues/{name}/messages`

Publish a single message.

**Request body**

```json
{
  "body": "<base64-encoded payload>",
  "deliver_at": 1740747600000,
  "max_retries": 5,
  "metadata": {
    "source": "checkout-service",
    "trace-id": "abc123"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `body` | string | yes | Base64-encoded message payload. Max size controlled by `max_message_size_kb` (default 256 KB). |
| `deliver_at` | int64 | no | UTC ms; 0 or omit = deliver immediately |
| `max_retries` | int | no | Override queue default; 0 = use queue default |
| `metadata` | object | no | User-defined string key/value pairs. Max 16 keys, key ≤ 64 bytes, value ≤ 512 bytes. |

**Metadata best practices:** Use `metadata` for small routing signals — trace IDs, source service name, priority tags. Do not embed large blobs; store them in object storage and pass a reference ID in `body` instead.

**Scheduled delivery:** If `deliver_at` is in the future, the message is placed in the scheduler's Min-Heap and will not appear in consume calls until the time is reached.

**Response `201 Created`**

```json
{ "id": "01JP5X4PRJ3G2V9WB8E7KT8M5F" }
```

---

### `POST /namespaces/{ns}/queues/{name}/messages/batch`

Publish multiple messages in a single request (up to `max_batch_size`, default 100).

**Request body** — array of message objects (same schema as single publish)

```json
[
  { "body": "bXNnMQ==" },
  { "body": "bXNnMg==", "deliver_at": 1740747600000 },
  { "body": "bXNnMw==", "metadata": { "priority": "high" } }
]
```

**Response `201 Created`**

```json
{ "ids": ["01JP…", "01JP…", "01JP…"] }
```

---

## Messages — Consume

### `GET /namespaces/{ns}/queues/{name}/messages`

Consume (dequeue) and lock messages for processing.

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `n` | 1 | Max messages to return (1–`max_batch_size`) |
| `visibility_timeout_ms` | 30000 | Lock duration; message reappears if not ACKed in time |

**Response `200 OK`**

```json
{
  "messages": [
    {
      "id": "01JP5X4PRJ3G2V9WB8E7KT8M5F",
      "body": "eyJhbW91bnQiOjQyfQ==",
      "receipt_handle": "01JP5X4…-rh",
      "namespace": "payments",
      "queue": "invoices",
      "attempt": 1,
      "published_at": 1740744000000,
      "metadata": { "source": "checkout-service" }
    }
  ]
}
```

Returns an empty `messages` array when the queue is empty. Never blocks.

**Important:** The message remains locked (`IN_FLIGHT`) until either:
- You call `DELETE /messages/{receipt_handle}` to ACK it (success — message deleted), or
- The `visibility_timeout_ms` elapses — the message becomes READY again **and its `attempt` counter increments**, exactly as if it had been NACKed. If `attempt` exceeds `max_retries`, the message is routed to the DLQ automatically.

> **Warning:** Do not use consume as a read-only inspection tool. Every consumed-but-not-ACKed timeout counts as a failed attempt. If you need to inspect messages without side effects, a future `GET .../messages/peek` endpoint is planned.

---

## Messages — Ack / Nack

### `DELETE /messages/{receipt_handle}`

ACK a message — signal successful processing and permanently remove it from the queue.

**Path parameters:** `receipt_handle` from the consume response.

**Response `204 No Content`**

**Errors**

| Code | Meaning |
|------|---------|
| 410 | Receipt handle unknown (already ACKed, expired, or invalid) |

---

### `POST /messages/{receipt_handle}/nack`

NACK a message — signal failed processing. The message will be:
- Re-queued as READY with `attempt` incremented (if remaining retries > 0), or
- Moved to the dead-letter queue (if `attempt` now exceeds `max_retries`).

> **Note:** A visibility timeout expiry has identical behavior to an explicit NACK — the attempt counter increments and the DLQ condition is checked. Both are treated as a failed delivery attempt.

**Response `204 No Content`**

---

## Dead-Letter Queue (DLQ)

A message is routed to the DLQ automatically when it has been NACKed (or its visibility timeout expired unACKed) more times than its `max_retries` limit. The DLQ is a regular queue named `__dlq__<primary-name>` — it appears in the `/api/stats` depth counts under `dlq_depth`.

**DLQ lifecycle:**

```
publish → READY → consumed (in-flight) → NACKed/timeout
                                              ↓ (attempt >= max_retries)
                                           __dlq__<name>  ← inspect / replay
```

---

### `GET /namespaces/{ns}/queues/{name}/dlq`

Dequeue and lock up to `limit` messages from the DLQ. Behaves identically to the regular consume endpoint — messages are locked for `visibility_timeout_ms` and **must be ACKed** using their `receipt_handle`, otherwise they reappear in the DLQ.

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 10 | Max messages to return (1–100) |

**Response `200 OK`** — same shape as consume response, including `receipt_handle`, `body`, `metadata`, and `attempt`.

```bash
# Inspect up to 5 DLQ messages for the "invoices" queue
curl "http://localhost:8080/namespaces/payments/queues/invoices/dlq?limit=5"
```

> **Important:** After inspecting, you must ACK each message using its `receipt_handle` via `DELETE /messages/{receiptHandle}/ack`. If you don't, the messages will reappear in the DLQ once the visibility timeout expires. To discard messages permanently, drain and ACK them. To reprocess them, use the replay endpoint instead.

---

### `POST /namespaces/{ns}/queues/{name}/dlq/replay`

Move messages from the DLQ back into the primary queue for reprocessing. Each replayed message:
- Is published back to the **primary queue** as READY
- Has its **attempt counter reset** to 0 — it gets a fresh set of retries
- Has its **`deliver_at` cleared** — it becomes immediately deliverable
- **Preserves** the original `body`, `metadata`, and message ID (for idempotency)

**Query parameters**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 100 | Max messages to replay in one call |

**Response `200 OK`**

```json
{ "replayed": 7 }
```

```bash
# Replay all DLQ messages for the "invoices" queue (up to 100)
curl -X POST "http://localhost:8080/namespaces/payments/queues/invoices/dlq/replay"

# Replay only 10 at a time (useful for gradual reprocessing)
curl -X POST "http://localhost:8080/namespaces/payments/queues/invoices/dlq/replay?limit=10"
```

> **Tip:** Check `dlq_depth` in `/api/stats` before replaying to know how many messages are waiting. After replay, monitor the primary queue — if the same messages fail again they will return to the DLQ.

> **Note:** There is no peek-only (non-destructive view) endpoint. To inspect DLQ messages without permanently removing them, use `GET .../dlq` with a short `visibility_timeout_ms` and do not ACK — messages will reappear after the timeout. A non-destructive `GET .../messages/peek` endpoint is planned for a future release.

---

## Webhook Subscriptions

### `POST /namespaces/{ns}/queues/{name}/subscriptions`

Register a webhook. PulseMQ will POST each READY message to the given URL.

**Request body**

```json
{
  "url": "https://myapp.example.com/webhook/invoices",
  "secret": "my-hmac-secret"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | yes | PulseMQ will POST messages here |
| `secret` | string | no | HMAC-SHA256 signing secret; leave empty to disable |

**Webhook delivery**

PulseMQ sends:
```
POST <url>
Content-Type: application/json
X-PulseMQ-Signature: sha256=<hmac-sha256(secret, body)>

{message object}
```

- `200 OK` → auto-ACK
- `4xx` (except 429) → treated as permanent failure; message moves to DLQ immediately
- `5xx` or `429` or timeout → retry with exponential backoff (configured by `webhook.retry_delays_ms`)
- After all retries exhausted → message moves to DLQ

**Response `201 Created`**

```json
{ "id": "01JP5X4…-sub" }
```

---

### `DELETE /subscriptions/{id}`

Unregister a webhook subscription.

**Response `204 No Content`**

---

## WebSocket

### `GET /namespaces/{ns}/queues/{name}/ws`

Upgrade to a WebSocket connection. The server pushes messages as they become READY.

**Protocol:** The server sends JSON message objects and expects ACK frames in return. See [architecture.md](architecture.md) for the frame format.

---

## Error responses

All error responses use the same JSON shape:

```json
{ "error": "human readable description" }
```

| Status | When |
|--------|------|
| 400 Bad Request | Invalid JSON, missing required fields, or failed validation (e.g. metadata limits exceeded) |
| 401 Unauthorized | Missing or invalid `X-Api-Key` |
| 404 Not Found | Namespace, queue, or receipt handle not found |
| 409 Conflict | Resource already exists (namespace, queue) |
| 410 Gone | Receipt handle expired or already ACKed |
| 413 Request Entity Too Large | Request body exceeds 32 MB (max batch: 100 × 256 KB) |
| 429 Too Many Requests | Rate limit exceeded (default 100 req/s per IP, configurable) |
| 500 Internal Server Error | Unexpected server error (check server logs) |

---

## Server limits reference

All limits are enforced server-side and return `400` or `413` if exceeded. Most are configurable in `config.yaml`.

| Limit | Default | Config key |
|-------|---------|------------|
| Max message body size | 256 KB | `queue.max_message_size_kb` |
| Max messages per queue | 100,000 | `queue.max_messages` (per queue, overridable at creation) |
| Max batch size (publish / consume) | 100 | `queue.max_batch_size` |
| Max HTTP request body | 32 MB | hardcoded (100 × 256 KB headroom) |
| Metadata keys per message | 16 | hardcoded |
| Metadata key length | 64 bytes | hardcoded |
| Metadata value length | 512 bytes | hardcoded |
| Rate limit | 100 req/s per IP (burst 200) | `producers.max_rate`, `producers.burst` |
| Namespace / queue name length | 1–128 chars, `[a-z0-9_-]` only | hardcoded |
