package obs

import (
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

func SetSchedulerLeader(isLeader bool) {
	if isLeader {
		schedulerLeader.Store(1)
	} else {
		schedulerLeader.Store(0)
	}
}

func StartHealthServer(address string, client *store.Client, failReadiness bool, component string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /startupz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("started\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if failReadiness {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"dependency": "readiness fault enabled"})
			return
		}
		if err := client.Ready(); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"dependency": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		connected := 0
		if client.Connected() {
			connected = 1
		}
		_, _ = fmt.Fprintf(w, "# HELP beacon_nats_connected NATS connection status.\n# TYPE beacon_nats_connected gauge\nbeacon_nats_connected %d\n", connected)
		if component == "scheduler" {
			_, _ = fmt.Fprintf(w, "# HELP beacon_scheduler_is_leader Whether this scheduler holds the leader Lease.\n# TYPE beacon_scheduler_is_leader gauge\nbeacon_scheduler_is_leader %d\n", schedulerLeader.Load())
		}
	})
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
