# PulseMQ Architecture

This document describes the internal design of PulseMQ Phase 1 (single-node). It explains **what each package does**, **how they are wired together**, and **why specific design decisions were made**.

---

## Guiding principles

1. **Never bypass the storage interface** — all writes go through `StorageEngine.Append()`, never directly to disk.
2. **Everything goes through the Broker** — HTTP handlers talk only to the Broker; the Broker talks to the queue Manager.
3. **Message format is final** — define once, only add optional fields, never rename or remove.
4. **Config structure never shrinks** — add fields freely, never remove existing ones.
5. **UTC milliseconds everywhere** — `time.Now().UnixMilli()` always, no timezone conversions.
6. **ULID for all IDs** — time-sortable, globally unique, cluster-safe, no coordination needed.

---

## Package map

```
cmd/server/main.go          ← wires everything together, starts listeners
    │
    ├── internal/config/    ← YAML config loader and validator
    ├── internal/node/      ← ULID node identity (persisted to data_dir/node_id)
    │
    ├── internal/broker/    ← central orchestrator (everyone talks to this)
    │       │
    │       ├── internal/queue/         ← queue state machine + manager + message
    │       │       │
    │       │       └── internal/storage/   ← StorageEngine interface
    │       │               └── local/      ← WAL, append log, bbolt index, compaction
    │       │
    │       ├── internal/scheduler/     ← min-heap timer goroutine
    │       ├── internal/dlq/           ← dead-letter queue manager
    │       ├── internal/namespace/     ← namespace registry (JSON file)
    │       └── internal/metrics/       ← Prometheus text exporter
    │
    ├── internal/consumer/  ← webhook delivery manager
    │
    └── internal/transport/
            ├── http/       ← HTTP server, handlers, middleware (auth, rate-limit, logging)
            └── websocket/  ← WebSocket push handler
```

---

## Write path

```
Producer POST /namespaces/ns/queues/q/messages
    │
    ▼
Handler.publishMessage
    │  deserialise JSON, decode base64 body
    │
    ▼
Broker.Publish(PublishRequest)
    │  1. auto-register namespace (namespace.Registry.Ensure)
    │  2. generate ULID for message ID
    │  3. metrics.Published.Inc
    │
    ▼
queue.Manager.GetOrCreate(ns, name)  →  creates queue + storage on-demand
    │
    ▼
queue.Queue.Publish(msg)
    │  ┌─ deliverAt == 0 or <= now? ─── write to storage → push to READY list
    │  └─ deliverAt > now?          ─── write to storage → insert into Min-Heap
    │
    ▼
local.StorageEngine.Append(msg)
    │  1. write WAL entry (crash safety)
    │  2. append to log.dat
    │  3. upsert index entry (bbolt)
    │
    ▼
200 OK {"id":"01JP…"}
```

The WAL entry is written **before** the in-memory queue state changes. On restart, PulseMQ replays the WAL to reconstruct in-memory state for all queues.

---

## Scheduler — Min-Heap timer

The scheduler is the key performance differentiator.

### Traditional approach: database polling
```
every 1 second:
    SELECT * FROM messages WHERE deliver_at <= NOW()   ← O(N), gets slower as queue grows
```

### PulseMQ approach: Min-Heap
```
Min-Heap (ordered by deliverAt, root = earliest)
    peek root → O(1)
    pop root  → O(log N)
    insert    → O(log N)

Background goroutine:
    loop:
        root = heap.Peek()
        if root == nil: wait for signal
        if root.deliverAt <= now:
            heap.Pop()
            queue.Manager.pushReady(msg)
        else:
            timer := time.NewTimer(root.deliverAt - now)
            select {
            case <-timer.C:    // time to deliver
            case <-newMsgCh:   // new message inserted, re-evaluate
            case <-ctx.Done(): // shutdown
            }
```

The goroutine sleeps **exactly** until the next message is due, waking only when necessary. CPU usage is effectively zero when no messages are due.

**Delivery accuracy:** ±10 ms (OS timer resolution). In a cluster, ±500 ms is expected due to clock drift. The guarantee is **"at or after"**, not "exactly at".

---

## Queue state machine

Each message progresses through these states:

```
           publish
              │
              ▼
    ┌─── SCHEDULED ───────────────────────────────┐
    │    (deliverAt > now)                         │
    │    stored in Min-Heap                        │
    │    scheduler moves it → READY when due       │
    └──────────────────────────────────────────────┘
              │ deliverAt reached (or 0)
              ▼
           READY
              │ consumer calls Consume()
              │ visibility timeout starts
              ▼
         IN_FLIGHT
              │
    ┌─────────┼──────────────────────────────────────────┐
    │         │              │                            │
    │    ACK  │         NACK │               timeout      │
    │         ▼         (retries > 0)   (retries > 0)     │
    │      DELETED           └──────────────┐             │
    │                       READY (requeue) │             │
    │                                                     │
    │                    NACK (retries == 0)               │
    │                    timeout (retries == 0)    DEAD_LETTER
    └──────────────────────────────────────────────────────┘
```

**DELETED** messages are not immediately removed from disk. The append-only log retains them until the compaction goroutine runs and rewrites the log file, skipping deleted entries.

---

## Storage layer

### Disk layout per queue

```
data/namespaces/{ns}/{name}/
    wal.dat          ← write-ahead log (crash safety)
    log.dat          ← append-only raw message store
    index.dat        ← bbolt B-tree (msgID → offset + status)
```

### WAL entry format

```
[term:8 bytes][logIndex:8][nodeId:26][timestamp:8][msgId:26][payloadSize:4][payload:N][checksum:4]
```

- **term** and **logIndex** are set to 0 in Phase 1 and will be populated by the Raft layer in Phase 2 without changing the on-disk format.
- **checksum** is CRC32 of the payload; used to detect partial writes on crash recovery.

### Compaction

A background goroutine runs every `storage.compaction_interval` (default 1 hour):
1. Read `log.dat` sequentially.
2. For each entry: if status is DELETED → skip. Otherwise write to `new_log.dat`.
3. Atomic rename `new_log.dat` → `log.dat`.
4. Rebuild bbolt index from the new log.

Compaction does not run during heavy write load (configurable grace period).

### Durability guarantees

**What is never lost:**
On a clean shutdown or a restart PulseMQ replays the WAL and rebuilds in-memory state exactly as it was. CRC32 checksums detect and discard partial trailing writes caused by a mid-write crash.

**What can be lost on an unclean shutdown** (hard kill, OOM, power cut) depends on the configured `fsync` mode:

| fsync mode | Data-loss window | Recommended for |
|---|---|---|
| `always` | **Zero** — every write is flushed to disk before the API responds | Payment retries, financial workflows |
| `interval` *(default, 1 000 ms)* | Up to **~1 second** of published messages | General use |
| `batch` | Up to the configured batch count | Throughput-oriented workloads |
| `never` | Any writes since the last OS page flush (seconds–minutes) | Dev / ephemeral environments only |

> **Production recommendation:** For payment retries or any workflow where losing an enqueued message is unacceptable, set `storage.fsync: always` in `config.yaml`. Expect ~5 000 writes/sec at that setting. The default `interval` mode loses at most the messages published in the last 1 second before a hard crash.

### fsync policy (config reference)

| Setting | Behaviour | Tradeoff |
|---------|-----------|----------|
| `always` | fsync after every write | Safest, slowest (~5k writes/sec) |
| `interval` | fsync every N ms (default 1000 ms) | Balanced (~50k writes/sec) |
| `batch` | fsync every N writes | Throughput-oriented |
| `never` | no explicit fsync | Fastest (~500k writes/sec), unsafe |

---

## Broker — the central façade

The Broker is the only layer that HTTP handlers are allowed to call. It:
- Translates between HTTP DTOs and domain types
- Owns the receipt-handle routing table (`map[receiptHandle]→queueKey`)
- Increments metrics counters
- Auto-registers namespaces on first use
- Exposes `QueueStats()` (used by the dashboard API)

**No transport layer** (HTTP, WebSocket, Webhook) is allowed to import the queue or storage packages directly.

---

## Consumer (webhook) delivery

The `consumer.Manager` maintains a set of active `consumer.Delivery` goroutines, one per registered webhook. Each goroutine:
1. Polls its queue at configurable interval.
2. Calls `broker.Consume(…, N=1)` to dequeue a message.
3. Signs the JSON payload with `HMAC-SHA256(secret, body)` and POSTs to the webhook URL.
4. On `200 OK` → calls `broker.Ack`.
5. On `5xx` / timeout → waits for the next retry delay from `webhook.retry_delays_ms`, then re-tries.
6. After all retries → calls `broker.Nack` (which routes to DLQ).

---

## HTTP middleware chain

```
RateLimitMiddleware  ← per-IP token bucket (golang.org/x/time/rate)
        │
AuthMiddleware       ← X-Api-Key header check (when auth.enabled = true)
        │
LoggingMiddleware    ← slog JSON logging, captures status code + duration
        │
http.ServeMux        ← Go 1.22+ method-qualified patterns
        │
Handler methods      ← call Broker, return JSON
```

---

## Dashboard API — performance design

The dashboard polls two endpoints instead of one to keep both the summary cards and the queue table efficient at large scale (e.g. 500+ queues):

### `GET /api/stats/summary`

```
Broker.Summary()
    → qm.AllStats()          one in-memory snapshot pass — O(N), zero storage I/O
    → count namespaces, depth, scheduled, DLQ alerts from snapshot
    → return QueueSummary{total_queues, namespaces, total_depth, total_scheduled, dlq_alerts}
```

Cost: **O(N) over the in-memory snapshot**. No bbolt reads.

### `GET /api/stats?page=P&limit=L`

```
Broker.QueueStatsPaged(page, limit)
    → qm.AllStats()                 one snapshot pass — O(N)
    → filter out __dlq__ keys       O(N)
    → sort by ns/name               O(N log N)
    → slice for current page        O(1)
    → dlqMgr.Len(ns, name) ×L      O(L) bbolt reads — only current page's ~50 queues
    → return QueuePage{queues, total, page, limit, total_pages}
```

Cost: **O(N log N) sort + O(L) storage reads** where L ≤ 200. DLQ depth is never read for off-page queues.

### Dashboard behaviour

```
refresh() every 5 seconds:
    fetch /api/stats/summary      → update 4 summary cards
    fetch /api/stats?page=P       → update queue table rows + pagination controls
    fetch /health                 → update status pill

pgPrev / pgNext click:
    fetch /api/stats?page=P±1     → update table only (skip health + summary)
```

---

## Naming rules

Queue and namespace names must match `^[a-z0-9][a-z0-9\-]{0,63}$`:

- Lowercase letters, digits, hyphens only
- 1–64 characters
- Must begin with a letter or digit

Enforcement is layered:
1. **HTTP handler layer** — `namespace.ValidateName()` called unconditionally in `createQueue` handler, regardless of whether a namespace registry is attached. Returns `400 Bad Request`.
2. **Namespace registry** — `Ensure()` and `Register()` call the same validator internally. Returns `ErrInvalidName`.
3. **Dashboard UI** — both inputs auto-lowercase as the user types, making it impossible to submit an invalid name from the browser.

---

## Namespace registry

Namespaces are stored in `<data_dir>/namespaces.json` as a JSON array. On every mutation (create/delete) the file is written atomically:

```
write namespaces.json.tmp → rename namespaces.json.tmp → namespaces.json
```

This ensures the file is never partially written. On startup, the registry loads from the file and populates its in-memory map.

---

## Metrics

`internal/metrics.Registry` uses `sync.Map` of `*atomic.Int64` values to provide lock-free per-label counters. It deliberately does **not** depend on `prometheus/client_golang` to keep the binary small and dependency-free.

The Prometheus text exposition format is generated manually in `Registry.Handler()`, which writes `# HELP`, `# TYPE`, and metric lines for each non-empty counter family.

---

## Scaling path

### Phase 1 → Phase 2 (Raft cluster) — no rewrite needed

The following design decisions were made upfront to allow Phase 2 without breaking changes:

| Decision | Why it matters |
|----------|----------------|
| WAL format has `term` + `logIndex` + `nodeID` | Raft can populate these without format changes |
| `StorageEngine` is an interface | Swap `local.LocalStorage` for `replicated.ReplicatedStorage` |
| ULID IDs | No coordination needed; IDs are globally unique by construction |
| Scheduler runs in the Broker | Easy to gate "leader-only" with a Raft leadership check |
| `cluster:` section already in config | Just flip `enabled: true` |

### Phase 3 (sharding)

Consistent hash routing: `Hash(namespace+"/"+queue) % numClusters → clusterIndex`. Each cluster is an independent 3-node Raft group. A thin routing layer (or client-side SDK logic) directs requests to the right cluster.

---

## Testing strategy

| Layer | Approach |
|-------|----------|
| `internal/storage/local` | Integration tests with real disk I/O in `t.TempDir()` |
| `internal/queue` | Unit + integration; full state machine lifecycle |
| `internal/scheduler` | Timing tests with fast clock injection |
| `internal/broker` | Integration with real storage in temp dir |
| `internal/transport/http` | `httptest.Server` backed by real broker |
| `pkg/client` | End-to-end: real SDK → `httptest.Server` → real broker |
| `internal/namespace` | Unit; persistence round-trip |
| `internal/metrics` | Unit; Prometheus text format validation |

All tests run with `go test ./... -count=1 -timeout 90s` and use no network ports (all `httptest` or temp dirs).
