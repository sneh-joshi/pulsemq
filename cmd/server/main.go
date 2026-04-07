// Command pulsemq-server is the PulseMQ queue server process.
// It loads configuration, initialises node identity, and starts the server.
//
// Usage:
//
//	pulsemq-server [--config path/to/config.yaml]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
	"github.com/sneh-joshi/pulsemq/internal/metrics"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	"github.com/sneh-joshi/pulsemq/internal/node"
	transphttp "github.com/sneh-joshi/pulsemq/internal/transport/http"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "pulsemq: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// ── 1. Load configuration ────────────────────────────────────────────────
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// ── 2. Set up structured logger ──────────────────────────────────────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ── 3. Initialise node identity ──────────────────────────────────────────
	n, err := node.New(cfg.Node.DataDir, cfg.Node.ID)
	if err != nil {
		return fmt.Errorf("init node: %w", err)
	}

	slog.Info("pulsemq starting",
		"node_id", n.ID(),
		"host", cfg.Node.Host,
		"port", cfg.Node.Port,
		"data_dir", n.DataDir(),
		"cluster_enabled", cfg.Cluster.Enabled,
	)

	// ── 4. Initialise namespace registry ────────────────────────────────────
	nsReg, err := namespace.New(cfg.Node.DataDir)
	if err != nil {
		return fmt.Errorf("init namespace registry: %w", err)
	}

	// ── 5. Initialise metrics registry ───────────────────────────────────────
	metricsReg := &metrics.Registry{}

	// ── 6. Initialise broker (storage + queue + scheduler + DLQ) ────────────
	b, err := broker.New(cfg, string(n.ID()),
		broker.WithNamespaceRegistry(nsReg),
		broker.WithMetrics(metricsReg),
	)
	if err != nil {
		return fmt.Errorf("init broker: %w", err)
	}

	// ── 7. Initialise webhook consumer manager ────────────────────────────────
	cm := consumer.NewManager(b)

	// ── 8. Start HTTP / WebSocket transport ──────────────────────────────────
	srv := transphttp.New(b, cm, cfg, nsReg, metricsReg)
	addr := fmt.Sprintf("%s:%d", cfg.Node.Host, cfg.Node.Port)

	// Serve in a background goroutine so we can handle signals.
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("pulsemq ready", "node_id", n.ID(), "addr", addr)
		if err := srv.ListenAndServe(addr); !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		} else {
			serveErr <- nil
		}
	}()

	// ── 9. Start dedicated Prometheus metrics listener ───────────────────────
	if cfg.Metrics.Enabled {
		metricsAddr := fmt.Sprintf(":%d", cfg.Metrics.Port)
		go func() {
			slog.Info("metrics server listening", "addr", metricsAddr)
			if err := http.ListenAndServe(metricsAddr, metricsReg.Handler()); err != nil {
				slog.Warn("metrics server error", "err", err)
			}
		}()
	}

	// ── 10. Graceful shutdown on SIGINT / SIGTERM ─────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		slog.Info("shutting down", "signal", sig)
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	}

	// Give in-flight requests 5 seconds to complete.
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cm.Close()

	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Warn("server shutdown error", "err", err)
	}
	if err := b.Close(); err != nil {
		slog.Warn("broker close error", "err", err)
	}

	slog.Info("pulsemq stopped")
	return nil
}
