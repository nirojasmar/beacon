package obs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStartupHandlerReflectsStartupState(t *testing.T) {
	status := NewComponentStatus(false)
	recorder := httptest.NewRecorder()
	status.StartupHandler(recorder, httptest.NewRequest(http.MethodGet, "/startupz", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"status":"starting"`) {
		t.Fatalf("before startup: status=%d body=%s", recorder.Code, recorder.Body)
	}

	status.MarkStarted(nil)
	recorder = httptest.NewRecorder()
	status.StartupHandler(recorder, httptest.NewRequest(http.MethodGet, "/startupz", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"status":"started"`) {
		t.Fatalf("after startup: status=%d body=%s", recorder.Code, recorder.Body)
	}
}

func TestMetricsExposeRequiredMetricFamilies(t *testing.T) {
	handler := MetricsHandler(nil, "api")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	for _, name := range []string{
		"go_goroutines", "process_cpu_seconds_total", "beacon_nats_connected",
		"beacon_nats_reconnects_total", "beacon_check_runs_total",
		"beacon_check_duration_seconds", "beacon_scheduler_jobs_published_total",
		"beacon_state_cas_retries_total", "beacon_notifications_total",
		"beacon_dlq_total", "beacon_sse_clients",
	} {
		if !strings.Contains(recorder.Body.String(), name) {
			t.Errorf("metrics output is missing %q", name)
		}
	}
}

func TestWaitForStartupDelayCanBeCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if WaitForStartupDelay(ctx, 1) {
		t.Fatal("canceled startup delay returned success")
	}
}
