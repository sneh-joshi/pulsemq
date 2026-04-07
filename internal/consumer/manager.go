package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/node"
)

var ErrSubscriptionNotFound = errors.New("consumer: subscription not found")

type Subscription struct {
	ID        string
	Namespace string
	Queue     string
	URL       string
	secret    string
	cancel    context.CancelFunc
}

type Manager struct {
	broker *broker.Broker
	mu     sync.RWMutex
	subs   map[string]*Subscription
}

func NewManager(b *broker.Broker) *Manager {
	return &Manager{broker: b, subs: make(map[string]*Subscription)}
}

func (m *Manager) Register(ns, queueName, url, secret string) (string, error) {
	id, err := node.NewID()
	if err != nil {
		return "", fmt.Errorf("consumer: generate subscription ID: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sub := &Subscription{ID: id, Namespace: ns, Queue: queueName, URL: url, secret: secret, cancel: cancel}
	m.mu.Lock()
	m.subs[id] = sub
	m.mu.Unlock()
	go m.deliveryLoop(ctx, sub)
	slog.Info("subscription registered", "id", id, "ns", ns, "queue", queueName, "url", url)
	return id, nil
}

func (m *Manager) Deregister(id string) error {
	m.mu.Lock()
	sub, ok := m.subs[id]
	if ok {
		delete(m.subs, id)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubscriptionNotFound, id)
	}
	sub.cancel()
	slog.Info("subscription deregistered", "id", id)
	return nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sub := range m.subs {
		sub.cancel()
	}
	m.subs = make(map[string]*Subscription)
}

func (m *Manager) deliveryLoop(ctx context.Context, sub *Subscription) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			results, err := m.broker.Consume(broker.ConsumeRequest{Namespace: sub.Namespace, Queue: sub.Queue, N: 1})
			if err != nil {
				slog.Warn("consumer: consume error", "sub", sub.ID, "err", err)
				continue
			}
			for _, res := range results {
				if deliverErr := deliverMessage(ctx, client, sub, res); deliverErr != nil {
					slog.Warn("consumer: delivery failed, nacking", "sub", sub.ID, "err", deliverErr)
					_ = m.broker.Nack(res.ReceiptHandle)
				} else {
					_ = m.broker.Ack(res.ReceiptHandle)
				}
			}
		}
	}
}
