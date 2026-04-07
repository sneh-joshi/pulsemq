# Getting Started with PulseMQ

This guide gets you from zero to running a PulseMQ server and publishing your first scheduled message in under 5 minutes.

---

## Prerequisites

- Docker + Docker Compose **or** Go 1.24+
- `curl` (for the quick-start examples)

---

## 1. Start the server

### Option A — Docker Compose (recommended)

```bash
git clone https://github.com/sneh-joshi/pulsemq
cd pulsemq/docker
docker compose up -d
```

### Option B — Build from source

```bash
git clone https://github.com/sneh-joshi/pulsemq
cd pulsemq
go build -o pulsemq ./cmd/server
./pulsemq --config config.yaml
```

### Option C — Run tests first

```bash
go test ./... -count=1 -timeout 90s
```

All packages should pass. Then:

```bash
./pulsemq                        # uses ./config.yaml by default
# or
./pulsemq --config /etc/pulsemq/config.yaml
```

### Verify the server is up

```bash
curl http://localhost:8080/health
```

Expected:
```json
{"status":"ok","node_id":"01JP…","queues":0,"uptime":"2s","uptime_ms":2000,"version":"1.0.0"}
```

Open the dashboard at http://localhost:8080/dashboard.

---

## 2. Publish your first message

PulseMQ uses namespaces to group queues. Namespaces are auto-created on first use.

```bash
BASE=http://localhost:8080

# Publish a message immediately
curl -s -X POST "$BASE/namespaces/payments/queues/invoices/messages" \
  -H "Content-Type: application/json" \
  -d '{"body":"eyJhbW91bnQiOjQyfQ=="}'
# body is base64("{"amount":42}")
```

Response:
```json
{"id":"01JP5X4…"}
```

---

## 3. Consume the message

```bash
curl -s "$BASE/namespaces/payments/queues/invoices/messages?n=1"
```

Response:
```json
{
  "messages": [{
    "id": "01JP5X4…",
    "body": "eyJhbW91bnQiOjQyfQ==",
    "receipt_handle": "01JP5X4…-rh",
    "namespace": "payments",
    "queue": "invoices",
    "attempt": 1,
    "published_at": 1740744000000
  }]
}
```

The `body` is base64. Decode it before processing.

---

## 4. ACK the message

Once your consumer has successfully processed the message, delete it:

```bash
curl -s -X DELETE "$BASE/messages/01JP5X4…-rh"
# 204 No Content
```

If your consumer fails, NACK the message to requeue it:

```bash
curl -s -X POST "$BASE/messages/01JP5X4…-rh/nack"
# 204 No Content
```

---

## 5. Schedule a message for the future

```bash
# Deliver in 5 minutes (deliverAt = now + 300000 ms)
DELIVER_AT=$(( $(date +%s%3N) + 300000 ))

curl -s -X POST "$BASE/namespaces/payments/queues/invoices/messages" \
  -H "Content-Type: application/json" \
  -d "{\"body\":\"eyJhbW91bnQiOjk5fQ==\", \"deliver_at\":$DELIVER_AT}"
```

The message will not appear in consume calls until the `deliver_at` time is reached. The scheduler uses a Min-Heap (O(1) peek) — no polling.

---

## 6. Use the Go SDK

```bash
go get github.com/sneh-joshi/pulsemq/pkg/client
```

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/sneh-joshi/pulsemq/pkg/client"
)

func main() {
    c := client.New("http://localhost:8080")
    ctx := context.Background()

    // Publish immediately
    id, err := c.Publish(ctx, "payments", "invoices", mustJSON(map[string]any{"amount": 42}))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("published:", id)

    // Publish in 1 hour
    id, err = c.Publish(ctx, "payments", "invoices",
        mustJSON(map[string]any{"amount": 99}),
        client.WithDelay(time.Hour),
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("scheduled:", id)

    // Consume
    msgs, err := c.Consume(ctx, "payments", "invoices", 10)
    if err != nil {
        log.Fatal(err)
    }
    for _, m := range msgs {
        fmt.Printf("got: %s\n", m.Body)
        _ = c.Ack(ctx, m.ReceiptHandle)
    }
}

func mustJSON(v any) []byte {
    b, _ := json.Marshal(v)
    return b
}
```

---

## 7. Using from Python or Node.js

PulseMQ exposes a plain HTTP API — no SDK required. Any HTTP client works.

### Python (requests)

```python
import requests, base64, json, time

BASE = "http://localhost:8080"

def encode(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode()).decode()

# Publish immediately
requests.post(f"{BASE}/namespaces/payments/queues/invoices/messages",
              json={"body": encode({"amount": 42})})

# Schedule in 1 hour
deliver_at = int(time.time() * 1000) + 3_600_000
requests.post(f"{BASE}/namespaces/payments/queues/invoices/messages",
              json={"body": encode({"amount": 99}), "deliver_at": deliver_at})

# Consume + ACK
r = requests.get(f"{BASE}/namespaces/payments/queues/invoices/messages?n=10")
for msg in r.json()["messages"]:
    body = json.loads(base64.b64decode(msg["body"]))
    print("got:", body)
    requests.delete(f"{BASE}/messages/{msg['receipt_handle']}")
```

### Node.js (built-in fetch, Node 18+)

```js
const BASE = "http://localhost:8080";
const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64");

// Publish immediately
await fetch(`${BASE}/namespaces/payments/queues/invoices/messages`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ body: encode({ amount: 42 }) }),
});

// Schedule in 1 hour
await fetch(`${BASE}/namespaces/payments/queues/invoices/messages`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ body: encode({ amount: 99 }), deliver_at: Date.now() + 3_600_000 }),
});

// Consume + ACK
const { messages } = await fetch(
  `${BASE}/namespaces/payments/queues/invoices/messages?n=10`
).then((r) => r.json());

for (const msg of messages) {
  const body = JSON.parse(Buffer.from(msg.body, "base64").toString());
  console.log("got:", body);
  await fetch(`${BASE}/messages/${msg.receipt_handle}`, { method: "DELETE" });
}
```

---

## 8. Subscribe a webhook

PulseMQ will POST messages to your URL as they become READY.

```bash
curl -s -X POST "$BASE/namespaces/payments/queues/invoices/subscriptions" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://myapp.example.com/webhooks/invoices","secret":"hmac-secret"}'
```

Response:
```json
{"id":"01JP5X4…-sub"}
```

Your endpoint will receive:
```
POST https://myapp.example.com/webhooks/invoices
Content-Type: application/json
X-PulseMQ-Signature: sha256=<hmac>

{"id":"…","body":"…","namespace":"payments","queue":"invoices",…}
```

Return `200 OK` to ACK. Return `5xx` to trigger retry with exponential backoff. After exhausting retries the message moves to DLQ.

To unsubscribe:
```bash
curl -s -X DELETE "$BASE/subscriptions/01JP5X4…-sub"
```

---

## 9. Inspect the DLQ

After `max_retries` exhausted, failed messages land in the DLQ:

```bash
# Inspect
curl -s "$BASE/namespaces/payments/queues/invoices/dlq?limit=10"

# Replay all DLQ messages back into the primary queue
curl -s -X POST "$BASE/namespaces/payments/queues/invoices/dlq/replay"
```

---

## 10. Enable auth

In `config.yaml`:

```yaml
auth:
  enabled: true
  api_key: "my-secret-key"
```

Then pass the key on every request:

```bash
curl -s -H "X-Api-Key: my-secret-key" http://localhost:8080/health
```

Or with the SDK:

```go
c := client.New("http://localhost:8080", client.WithAPIKey("my-secret-key"))
```

---

## 11. Prometheus metrics

PulseMQ exposes Prometheus metrics on port 9090 (configurable):

```bash
curl http://localhost:9090/metrics
```

Or on the main port:

```bash
curl http://localhost:8080/metrics
```

Add a scrape config to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: pulsemq
    static_configs:
      - targets: ['localhost:9090']
```

---

## Next steps

- [API Reference](api-reference.md) — complete endpoint documentation
- [Architecture](architecture.md) — how PulseMQ works internally
- [config.yaml](../config.yaml) — all configuration options with comments
