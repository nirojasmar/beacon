package api

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
	"beacon/internal/obs"
)

func TestDemoEndpoints(t *testing.T) {
	s := &server{}
	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{name: "ok", target: "/demo/ok", wantStatus: http.StatusOK},
		{name: "status", target: "/demo/status/500", wantStatus: http.StatusInternalServerError},
		{name: "slow", target: "/demo/slow?ms=0", wantStatus: http.StatusOK},
		{name: "flaky success", target: "/demo/flaky?fail_percent=0", wantStatus: http.StatusOK},
		{name: "flaky failure", target: "/demo/flaky?fail_percent=100", wantStatus: http.StatusInternalServerError},
		{name: "invalid status", target: "/demo/status/700", wantStatus: http.StatusBadRequest},
		{name: "invalid delay", target: "/demo/slow?ms=30001", wantStatus: http.StatusBadRequest},
		{name: "invalid probability", target: "/demo/flaky?fail_percent=-1", wantStatus: http.StatusBadRequest},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /demo/ok", s.demoOK)
	mux.HandleFunc("GET /demo/status/{code}", s.demoStatus)
	mux.HandleFunc("GET /demo/slow", s.demoSlow)
	mux.HandleFunc("GET /demo/flaky", s.demoFlaky)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.target, nil))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", recorder.Code, test.wantStatus, recorder.Body)
			}
		})
	}
}

func TestVersionEndpoint(t *testing.T) {
	s := &server{
		cfg:             &config.Config{AppVersion: "1.2.3", GitSHA: "abc123", PodName: "beacon-api-0"},
		componentStatus: obs.NewComponentStatus(false),
	}
	recorder := httptest.NewRecorder()
	s.version(recorder, httptest.NewRequest(http.MethodGet, "/version", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	for _, value := range []string{`"version":"1.2.3"`, `"git_sha":"abc123"`, `"pod":"beacon-api-0"`} {
		if !strings.Contains(recorder.Body.String(), value) {
			t.Errorf("version response %q does not contain %q", recorder.Body.String(), value)
		}
	}
}

func TestUpdateRequestPreservesEnabledWhenOmitted(t *testing.T) {
	old := &model.Check{
		Enabled: true, Source: "manual", CreatedAt: time.Unix(1, 0),
	}
	request := httptest.NewRequest(http.MethodPut, "/checks/test", strings.NewReader(`{"name":"updated"}`))
	var input updateCheckRequest
	if err := decodeJSON(request, &input); err != nil {
		t.Fatalf("decode update request: %v", err)
	}
	got := applyUpdateRequest(old, input, "test", time.Unix(2, 0))
	if !got.Enabled {
		t.Fatal("omitted enabled field disabled the check")
	}
	if got.Source != old.Source || !got.CreatedAt.Equal(old.CreatedAt) {
		t.Fatalf("immutable fields changed: %+v", got)
	}

	request = httptest.NewRequest(http.MethodPut, "/checks/test", strings.NewReader(`{"enabled":false}`))
	input = updateCheckRequest{}
	if err := decodeJSON(request, &input); err != nil {
		t.Fatalf("decode explicit update request: %v", err)
	}
	if got := applyUpdateRequest(old, input, "test", time.Unix(3, 0)); got.Enabled {
		t.Fatal("explicit enabled=false was not applied")
	}
}

func TestShutdownCancelsLongLivedRequestContexts(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	})
	shutdownContext, stopStreams := context.WithCancel(context.Background())
	server, _ := newHTTPServer("", handler, shutdownContext, stopStreams)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.Serve(listener)
	}()

	response, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("GET long-lived response: %v", err)
	}
	defer response.Body.Close()
	<-started

	shutdownContextWithTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContextWithTimeout); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("long-lived request context was not canceled")
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatalf("finish response: %v", err)
	}
	if err := <-serverErrors; err != http.ErrServerClosed {
		t.Fatalf("Serve() error = %v, want http.ErrServerClosed", err)
	}
}
