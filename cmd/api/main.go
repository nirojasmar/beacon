package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
	"beacon/internal/obs"
	"beacon/internal/store"

	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
)

//go:embed index.html
var web embed.FS

type server struct {
	cfg             *config.Config
	client          *store.Client
	checks          nats.KeyValue
	states          nats.KeyValue
	componentStatus *obs.ComponentStatus
}

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if cfg.BEACONToken == "" {
		log.Fatal("BEACON_TOKEN is required by the API")
	}
	log.Printf("component=api config=%s", cfg.Log())
	obs.StartFaults(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	status := obs.NewComponentStatus(cfg.FaultFailReadiness)
	if err := obs.StartHealthServer(cfg.MetricsAddr, status, "api", cfg.AppVersion, cfg.GitSHA, cfg.PodName); err != nil {
		log.Fatalf("start metrics server: %v", err)
	}

	s := &server{cfg: cfg, componentStatus: status}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /startupz", status.StartupHandler)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", obs.MetricsHandler(status, "api"))
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("GET /demo/ok", s.demoOK)
	mux.HandleFunc("GET /demo/status/{code}", s.demoStatus)
	mux.HandleFunc("GET /demo/slow", s.demoSlow)
	mux.HandleFunc("GET /demo/flaky", s.demoFlaky)
	mux.HandleFunc("GET /api/v1/checks", s.listChecks)
	mux.HandleFunc("POST /api/v1/checks", s.createCheck)
	mux.HandleFunc("GET /api/v1/checks/{id}", s.getCheck)
	mux.HandleFunc("PUT /api/v1/checks/{id}", s.updateCheck)
	mux.HandleFunc("DELETE /api/v1/checks/{id}", s.deleteCheck)
	mux.HandleFunc("POST /api/v1/checks/{id}/run", s.runCheck)
	mux.HandleFunc("GET /api/v1/checks/{id}/results", s.results)
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("GET /api/v1/stream", s.stream)

	shutdownContext, stopStreams := context.WithCancel(context.Background())
	httpServer, stopStreams := newHTTPServer(cfg.HTTPAddr, requestLog(startupGate(status, mux)), shutdownContext, stopStreams)
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- httpServer.ListenAndServe()
	}()
	if !obs.WaitForStartupDelay(ctx, cfg.StartupDelaySeconds) {
		shutdownAPI(httpServer, serverErrors, stopStreams)
		return
	}
	client, err := store.NewClient(cfg.NATSURL, cfg.NATSUser, cfg.NATSPassword)
	if err != nil {
		log.Fatalf("connect NATS: %v", err)
	}
	defer client.Close()
	status.MarkStarted(client)
	initCtx, cancelInit := context.WithTimeout(ctx, 30*time.Second)
	err = client.EnsureInfrastructure(initCtx, cfg.CheckConcurrency)
	cancelInit()
	if err != nil {
		log.Fatalf("initialize JetStream: %v", err)
	}
	checks, err := client.KV("checks")
	if err != nil {
		log.Fatal(err)
	}
	states, err := client.KV("state")
	if err != nil {
		log.Fatal(err)
	}
	s.client, s.checks, s.states = client, checks, states
	status.MarkReady()
	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("API server: %v", err)
		}
	case <-ctx.Done():
		shutdownAPI(httpServer, serverErrors, stopStreams)
	}
}

func newHTTPServer(address string, handler http.Handler, shutdownContext context.Context, stopStreams context.CancelFunc) (*http.Server, context.CancelFunc) {
	server := &http.Server{
		Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return shutdownContext },
	}
	server.RegisterOnShutdown(stopStreams)
	return server, stopStreams
}

func shutdownAPI(server *http.Server, serverErrors <-chan error, stopStreams context.CancelFunc) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful API shutdown: %v", err)
	}
	stopStreams()
	if err := <-serverErrors; err != nil && err != http.ErrServerClosed {
		log.Printf("API server shutdown: %v", err)
	}
}

func startupGate(status *obs.ComponentStatus, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/startupz", "/readyz", "/metrics", "/version":
		default:
			if !status.ReadyState() {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	content, err := web.ReadFile("index.html")
	if err != nil {
		http.Error(w, "status page unavailable", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(content)
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	s.componentStatus.HealthHandler(w, nil)
}

func (s *server) ready(w http.ResponseWriter, _ *http.Request) {
	s.componentStatus.ReadyHandler(w, nil)
}

func (s *server) demoOK(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) demoStatus(w http.ResponseWriter, r *http.Request) {
	code, err := strconv.Atoi(r.PathValue("code"))
	if err != nil || code < 200 || code > 599 {
		writeError(w, http.StatusBadRequest, "status code must be between 200 and 599")
		return
	}
	writeJSON(w, code, map[string]int{"status": code})
}

func (s *server) demoSlow(w http.ResponseWriter, r *http.Request) {
	delayMS := 1500
	if value := r.URL.Query().Get("ms"); value != "" {
		var err error
		delayMS, err = strconv.Atoi(value)
		if err != nil || delayMS < 0 || delayMS > 30000 {
			writeError(w, http.StatusBadRequest, "ms must be between 0 and 30000")
			return
		}
	}
	timer := time.NewTimer(time.Duration(delayMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return
	case <-timer.C:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func (s *server) demoFlaky(w http.ResponseWriter, r *http.Request) {
	failPercent := 30
	if value := r.URL.Query().Get("fail_percent"); value != "" {
		var err error
		failPercent, err = strconv.Atoi(value)
		if err != nil || failPercent < 0 || failPercent > 100 {
			writeError(w, http.StatusBadRequest, "fail_percent must be between 0 and 100")
			return
		}
	}
	if rand.Intn(100) < failPercent {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) version(w http.ResponseWriter, r *http.Request) {
	s.componentStatus.VersionHandler(s.cfg.AppVersion, s.cfg.GitSHA, s.cfg.PodName)(w, r)
}

func (s *server) listChecks(w http.ResponseWriter, r *http.Request) {
	items, err := s.checkViews()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checks": items})
}

type createRequest struct {
	Name             string       `json:"name"`
	Type             string       `json:"type"`
	Target           string       `json:"target"`
	IntervalSeconds  int          `json:"interval_seconds"`
	TimeoutSeconds   int          `json:"timeout_seconds"`
	Expect           model.Expect `json:"expect"`
	RecordType       string       `json:"record_type,omitempty"`
	FailureThreshold int          `json:"failure_threshold"`
	SuccessThreshold int          `json:"success_threshold"`
	Enabled          *bool        `json:"enabled"`
	Tags             []string     `json:"tags"`
}

type updateCheckRequest struct {
	Name             string       `json:"name"`
	Type             string       `json:"type"`
	Target           string       `json:"target"`
	IntervalSeconds  int          `json:"interval_seconds"`
	TimeoutSeconds   int          `json:"timeout_seconds"`
	Expect           model.Expect `json:"expect"`
	RecordType       string       `json:"record_type,omitempty"`
	FailureThreshold int          `json:"failure_threshold"`
	SuccessThreshold int          `json:"success_threshold"`
	Enabled          *bool        `json:"enabled"`
	Tags             []string     `json:"tags"`
}

func (s *server) createCheck(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	var input createRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if input.IntervalSeconds == 0 {
		input.IntervalSeconds = 30
	}
	if input.TimeoutSeconds == 0 {
		input.TimeoutSeconds = 5
	}
	if input.FailureThreshold == 0 {
		input.FailureThreshold = 3
	}
	if input.SuccessThreshold == 0 {
		input.SuccessThreshold = 1
	}
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	check := &model.Check{
		ID: ulid.Make().String(), Name: input.Name, Type: input.Type, Target: input.Target,
		IntervalSeconds: input.IntervalSeconds, TimeoutSeconds: input.TimeoutSeconds,
		Expect: input.Expect, RecordType: input.RecordType, FailureThreshold: input.FailureThreshold,
		SuccessThreshold: input.SuccessThreshold, Enabled: enabled, Tags: input.Tags,
		Source: "manual", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if check.Tags == nil {
		check.Tags = []string{}
	}
	if err := check.Validate(); err != nil {
		writeValidationError(w, err)
		return
	}
	data, err := json.Marshal(check)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	revision, err := s.checks.Create(check.ID, data)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("store check: %v", err))
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", revision))
	writeJSON(w, http.StatusCreated, check)
}

func (s *server) getCheck(w http.ResponseWriter, r *http.Request) {
	check, revision, err := s.loadCheck(r.PathValue("id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	state, err := s.getState(check.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", revision))
	writeJSON(w, http.StatusOK, map[string]any{"check": check, "state": state})
}

func (s *server) updateCheck(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	id := r.PathValue("id")
	old, revision, err := s.loadCheck(id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	match := strings.Trim(r.Header.Get("If-Match"), "\"")
	if match == "" {
		writeError(w, http.StatusPreconditionRequired, "If-Match revision is required")
		return
	}
	expected, err := strconv.ParseUint(match, 10, 64)
	if err != nil || expected != revision {
		writeError(w, http.StatusPreconditionFailed, "check revision does not match")
		return
	}
	var input updateCheckRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	check := applyUpdateRequest(old, input, id, time.Now().UTC())
	if check.Tags == nil {
		check.Tags = []string{}
	}
	if err := check.Validate(); err != nil {
		writeValidationError(w, err)
		return
	}
	data, err := json.Marshal(check)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	newRevision, err := s.checks.Update(id, data, revision)
	if err != nil {
		_, currentRevision, currentErr := s.loadCheck(id)
		if errors.Is(currentErr, nats.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, "check not found")
			return
		}
		if currentErr == nil && currentRevision != revision {
			writeError(w, http.StatusPreconditionFailed, "check revision does not match")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", newRevision))
	writeJSON(w, http.StatusOK, check)
}

func applyUpdateRequest(old *model.Check, input updateCheckRequest, id string, updatedAt time.Time) *model.Check {
	enabled := old.Enabled
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	return &model.Check{
		ID: id, Name: input.Name, Type: input.Type, Target: input.Target,
		IntervalSeconds: input.IntervalSeconds, TimeoutSeconds: input.TimeoutSeconds,
		Expect: input.Expect, RecordType: input.RecordType,
		FailureThreshold: input.FailureThreshold, SuccessThreshold: input.SuccessThreshold,
		Enabled: enabled, Tags: input.Tags, Source: old.Source,
		CreatedAt: old.CreatedAt, UpdatedAt: updatedAt,
	}
}

func (s *server) deleteCheck(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	id := r.PathValue("id")
	if _, _, err := s.loadCheck(id); err != nil {
		s.storeError(w, err)
		return
	}
	if err := s.checks.Delete(id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.states.Delete(stateKey(id)); err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) runCheck(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	check, _, err := s.loadCheck(r.PathValue("id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	if !check.Enabled {
		writeError(w, http.StatusConflict, "check is disabled")
		return
	}
	runID := ulid.Make().String()
	job, _ := json.Marshal(map[string]any{"check_id": check.ID, "run_id": runID, "enqueued_at": time.Now().UTC()})
	if err := s.client.Publish(r.Context(), "jobs.run", runID, job); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"run_id": runID})
}

func (s *server) results(w http.ResponseWriter, r *http.Request) {
	if _, _, err := s.loadCheck(r.PathValue("id")); err != nil {
		s.storeError(w, err)
		return
	}
	state, err := s.getState(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		limit = parsed
	}
	if limit < len(state.Recent) {
		state.Recent = state.Recent[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": state.Recent})
}

func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	checks, err := s.checkViews()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	counts := map[string]int{"up": 0, "down": 0, "unknown": 0}
	down := make([]checkView, 0)
	for _, check := range checks {
		counts[check.State.Status]++
		if check.State.Status == "down" {
			down = append(down, check)
		}
	}
	natsStatus := "disconnected"
	if s.client.Connected() {
		natsStatus = "connected"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"counts": counts, "checks": checks, "down": down,
		"metadata": map[string]string{
			"pod_name": s.cfg.PodName, "version": s.cfg.AppVersion,
			"git_sha": s.cfg.GitSHA, "nats": natsStatus,
		},
	})
}

func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	obs.SSEClients.Inc()
	defer obs.SSEClients.Dec()
	eventMessages, subscription, err := s.client.Subscribe("events.state_change")
	if err != nil {
		http.Error(w, "event stream unavailable", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		if err := subscription.Unsubscribe(); err != nil {
			log.Printf("unsubscribe from state events: %v", err)
		}
	}()
	resultMessages, resultSubscription, err := s.client.Subscribe("results.*")
	if err != nil {
		http.Error(w, "result stream unavailable", http.StatusServiceUnavailable)
		return
	}
	defer func() {
		if err := resultSubscription.Unsubscribe(); err != nil {
			log.Printf("unsubscribe from results: %v", err)
		}
	}()
	checks, err := s.checkViews()
	if err != nil {
		http.Error(w, "status snapshot unavailable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := s.writeEvent(w, "snapshot", "", map[string]any{"checks": checks}); err != nil {
		return
	}
	flusher.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case msg := <-eventMessages:
			if msg == nil {
				return
			}
			if err := s.writeEvent(w, "state_change", msg.Header.Get(nats.MsgIdHdr), json.RawMessage(msg.Data)); err != nil {
				return
			}
			flusher.Flush()
		case msg := <-resultMessages:
			if msg == nil {
				return
			}
			if err := s.writeEvent(w, "result", msg.Header.Get(nats.MsgIdHdr), json.RawMessage(msg.Data)); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *server) writeEvent(w io.Writer, name, id string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, raw)
	return err
}

func (s *server) allChecks() ([]*model.Check, error) {
	keys, err := s.checks.Keys()
	if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
		return nil, fmt.Errorf("list checks: %w", err)
	}
	checks := make([]*model.Check, 0, len(keys))
	for _, key := range keys {
		entry, err := s.checks.Get(key)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("read check %s: %w", key, err)
		}
		var check model.Check
		if err := json.Unmarshal(entry.Value(), &check); err != nil {
			return nil, fmt.Errorf("decode check %s: %w", key, err)
		}
		checks = append(checks, &check)
	}
	return checks, nil
}

type checkView struct {
	*model.Check
	State *model.State `json:"state"`
}

func (s *server) checkViews() ([]checkView, error) {
	checks, err := s.allChecks()
	if err != nil {
		return nil, err
	}
	views := make([]checkView, 0, len(checks))
	for _, check := range checks {
		state, err := s.getState(check.ID)
		if err != nil {
			return nil, fmt.Errorf("load state for %s: %w", check.ID, err)
		}
		views = append(views, checkView{Check: check, State: state})
	}
	return views, nil
}

func (s *server) loadCheck(id string) (*model.Check, uint64, error) {
	entry, err := s.checks.Get(id)
	if err != nil {
		return nil, 0, err
	}
	var check model.Check
	if err := json.Unmarshal(entry.Value(), &check); err != nil {
		return nil, 0, fmt.Errorf("decode check: %w", err)
	}
	return &check, entry.Revision(), nil
}

func (s *server) getState(id string) (*model.State, error) {
	entry, err := s.states.Get(stateKey(id))
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return model.NewState(id), nil
		}
		return nil, err
	}
	var state model.State
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, fmt.Errorf("decode check state: %w", err)
	}
	return &state, nil
}

func (s *server) authorized(w http.ResponseWriter, r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		writeError(w, http.StatusUnauthorized, "valid bearer token required")
		return false
	}
	provided := strings.TrimPrefix(header, "Bearer ")
	if len(provided) != len(s.cfg.BEACONToken) ||
		subtle.ConstantTimeCompare([]byte(provided), []byte(s.cfg.BEACONToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "valid bearer token required")
		return false
	}
	return true
}

func (s *server) storeError(w http.ResponseWriter, err error) {
	if errors.Is(err, nats.ErrKeyNotFound) {
		writeError(w, http.StatusNotFound, "check not found")
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func stateKey(id string) string { return "check_" + id }

func decodeJSON(r *http.Request, dst any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return fmt.Errorf("request body exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("request body must contain one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeValidationError(w http.ResponseWriter, err error) {
	var fieldError *model.FieldError
	if errors.As(err, &fieldError) {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"errors": map[string]string{fieldError.Field: fieldError.Error()},
		})
		return
	}
	writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
		"errors": map[string]string{"check": err.Error()},
	})
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(start))
	})
}
