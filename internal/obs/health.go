package obs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"beacon/internal/store"
)

var schedulerLeader atomic.Int64

type ComponentStatus struct {
	client        atomic.Pointer[store.Client]
	startupDone   atomic.Bool
	ready         atomic.Bool
	failReadiness bool
}

func NewComponentStatus(failReadiness bool) *ComponentStatus {
	return &ComponentStatus{failReadiness: failReadiness}
}

func (s *ComponentStatus) MarkStarted(client *store.Client) {
	s.client.Store(client)
	s.startupDone.Store(true)
}

func (s *ComponentStatus) MarkReady() {
	s.ready.Store(true)
}

func (s *ComponentStatus) Started() bool {
	return s != nil && s.startupDone.Load()
}

func (s *ComponentStatus) ReadyState() bool {
	return s != nil && s.ready.Load()
}

func (s *ComponentStatus) Client() *store.Client {
	if s == nil {
		return nil
	}
	return s.client.Load()
}

func (s *ComponentStatus) Ready() error {
	if !s.Started() {
		return fmt.Errorf("status: starting")
	}
	if !s.ReadyState() {
		return fmt.Errorf("status: initializing")
	}
	if s.failReadiness {
		return fmt.Errorf("readiness fault enabled")
	}
	client := s.Client()
	if client == nil {
		return fmt.Errorf("NATS connection is not ready")
	}
	return client.Ready()
}

func (s *ComponentStatus) StartupHandler(w http.ResponseWriter, _ *http.Request) {
	if !s.Started() {
		writeStatus(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
		return
	}
	writeStatus(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *ComponentStatus) ReadyHandler(w http.ResponseWriter, _ *http.Request) {
	if err := s.Ready(); err != nil {
		writeStatus(w, http.StatusServiceUnavailable, map[string]string{"dependency": err.Error()})
		return
	}
	writeStatus(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *ComponentStatus) HealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *ComponentStatus) VersionHandler(version, gitSHA, pod string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, map[string]string{
			"version": version, "git_sha": gitSHA, "pod": pod,
		})
	}
}

func StartHealthServer(address string, status *ComponentStatus, component, version, gitSHA, pod string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", status.HealthHandler)
	mux.HandleFunc("GET /startupz", status.StartupHandler)
	mux.HandleFunc("GET /readyz", status.ReadyHandler)
	mux.Handle("GET /metrics", MetricsHandler(status, component))
	mux.HandleFunc("GET /version", status.VersionHandler(version, gitSHA, pod))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("health server: %v", err)
		}
	}()
	return nil
}

func WaitForStartupDelay(ctx context.Context, seconds int) bool {
	if seconds <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func writeStatus(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write status response: %v", err)
	}
}

func SetSchedulerLeader(isLeader bool) {
	if isLeader {
		schedulerLeader.Store(1)
	} else {
		schedulerLeader.Store(0)
	}
}
