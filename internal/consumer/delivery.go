package consumer

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sneh-joshi/pulsemq/internal/queue"
)

// webhookPayload is the JSON body POSTed to the webhook URL.
type webhookPayload struct {
	ID            string            `json:"id"`
	Body          string            `json:"body"` // base64-encoded
	ReceiptHandle string            `json:"receipt_handle"`
	Namespace     string            `json:"namespace"`
	Queue         string            `json:"queue"`
	PublishedAt   int64             `json:"published_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// deliverMessage POSTs res to the subscription URL.
// Returns nil only when the endpoint responds with HTTP 200 OK.
func deliverMessage(ctx context.Context, client *http.Client, sub *Subscription, res *queue.DequeueResult) error {
	p := webhookPayload{
		ReceiptHandle: res.ReceiptHandle,
	}
	if res.Message != nil {
		p.ID = res.Message.ID
		p.Body = base64.StdEncoding.EncodeToString(res.Message.Body)
		p.Namespace = res.Message.Namespace
		p.Queue = res.Message.Queue
		p.PublishedAt = res.Message.PublishedAt
		p.Metadata = res.Message.Metadata
	}

	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("consumer: marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("consumer: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Sign the request body when a secret is provided.
	if sub.secret != "" {
		mac := hmac.New(sha256.New, []byte(sub.secret))
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("X-PulseMQ-Signature", "sha256="+sig)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("consumer: POST to %s: %w", sub.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("consumer: endpoint returned %d", resp.StatusCode)
	}
	return nil
}
