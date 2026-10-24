package notifier

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSignPayload(t *testing.T) {
	got := SignPayload("secret", []byte(`{"status":"down"}`))
	const want = "sha256=e38ed0b09880e69eec467c19c77f562e1f48d3438aef06b6ab3a669e5c779b06"
	if got != want {
		t.Fatalf("SignPayload() = %q, want %q", got, want)
	}
}

func TestPostWebhookSignsPayload(t *testing.T) {
	body := []byte(`{"status":"down"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read webhook body: %v", err)
		}
		if string(got) != string(body) {
			t.Errorf("webhook body = %q, want %q", got, body)
		}
		if got := r.Header.Get("X-Beacon-Signature"); got != SignPayload("secret", body) {
			t.Errorf("signature = %q, want %q", got, SignPayload("secret", body))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := postWebhook(context.Background(), server.Client(), server.URL, "secret", body); err != nil {
		t.Fatalf("postWebhook() error = %v", err)
	}
}

func TestRetryBackoffSchedule(t *testing.T) {
	want := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute}
	if len(retryBackoff) != len(want) {
		t.Fatalf("backoff entries = %d, want %d", len(retryBackoff), len(want))
	}
	for i := range want {
		if retryBackoff[i] != want[i] {
			t.Errorf("retryBackoff[%d] = %s, want %s", i, retryBackoff[i], want[i])
		}
	}
}
