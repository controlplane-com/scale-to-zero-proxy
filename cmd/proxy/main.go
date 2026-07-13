// scale-to-zero-proxy: an always-on L4 proxy that holds inbound TCP
// connections, wakes a suspended Control Plane workload, splices traffic
// once it is ready, and suspends it again after idle. See docs/DESIGN.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/controlplane-com/scale-to-zero-proxy/internal/activator"
	"github.com/controlplane-com/scale-to-zero-proxy/internal/config"
	"github.com/controlplane-com/scale-to-zero-proxy/internal/cpln"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("fatal", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	client := &cpln.Client{
		Endpoint: cfg.Endpoint,
		Org:      cfg.Org,
		GVC:      cfg.TargetGVC,
		Workload: cfg.TargetWorkload,
		Token:    cfg.Token,
		Logger:   log,
	}

	act := activator.New(client, activator.Options{
		TargetHost:       cfg.TargetHost,
		IdleHold:         cfg.IdleHold,
		MaxHold:          cfg.MaxHold,
		WakePollInterval: cfg.WakePollInterval,
		WakeTimeout:      cfg.WakeTimeout,
		Logger:           log,
	})
	defer act.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var wg sync.WaitGroup
	listeners := make([]net.Listener, 0, len(cfg.Mappings))
	for _, m := range cfg.Mappings {
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(m.Front))
		if err != nil {
			return fmt.Errorf("listen :%d: %w", m.Front, err)
		}
		listeners = append(listeners, ln)
		log.Info("listening", "front", m.Front, "target", net.JoinHostPort(cfg.TargetHost, strconv.Itoa(m.Target)))

		wg.Add(1)
		go func(ln net.Listener, targetPort int) {
			defer wg.Done()
			for {
				conn, err := ln.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					log.Warn("accept error", "error", err.Error())
					continue
				}
				go act.HandleConn(ctx, conn, targetPort)
			}
		}(ln, m.Target)
	}

	// Health endpoint for the proxy's own probes.
	health := &http.Server{
		Addr: ":" + strconv.Itoa(cfg.HealthPort),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(act.Stats()) //nolint:errcheck
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", "error", err.Error())
		}
	}()

	log.Info("proxy up",
		"targetWorkload", cfg.TargetWorkload, "targetGvc", cfg.TargetGVC,
		"idleHold", cfg.IdleHold.String(), "maxHold", cfg.MaxHold.String())

	<-ctx.Done()
	log.Info("shutting down: closing listeners (in-flight connections continue)")

	for _, ln := range listeners {
		ln.Close() //nolint:errcheck
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	health.Shutdown(shutdownCtx) //nolint:errcheck
	wg.Wait()
	return nil
}
