package probes

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
)

func TestForbiddenIP(t *testing.T) {
	tests := []struct {
		ip           string
		blockPrivate bool
		want         bool
	}{
		{"169.254.1.2", false, true},
		{"fe80::1", false, true},
		{"10.0.0.1", false, false},
		{"10.0.0.1", true, true},
		{"8.8.8.8", true, false},
	}
	for _, test := range tests {
		t.Run(test.ip, func(t *testing.T) {
			if got := forbiddenIP(net.ParseIP(test.ip), test.blockPrivate); got != test.want {
				t.Fatalf("forbiddenIP(%s, %v) = %v, want %v", test.ip, test.blockPrivate, got, test.want)
			}
		})
	}
}

func TestHTTPProbeValidatesStatusAndBody(t *testing.T) {
	noContentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer noContentServer.Close()

	check := model.NewCheck("test", "test", "http", noContentServer.URL, 30, 2)
	check.Expect.BodyContains = "expected"
	result := Run(context.Background(), check, &config.Config{AppVersion: "test"})
	if result.OK || result.HTTPStatus != http.StatusNoContent {
		t.Fatalf("probe without expected body should fail with HTTP status retained: %+v", result)
	}

	bodyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("expected body"))
	}))
	defer bodyServer.Close()
	check.Target = bodyServer.URL
	check.Expect.BodyContains = "expected"
	result = Run(context.Background(), check, &config.Config{AppVersion: "test"})
	if !result.OK || result.HTTPStatus != http.StatusOK || result.LatencyMs < 0 {
		t.Fatalf("expected successful body check: %+v", result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := probeHTTP(ctx, check, &config.Config{AppVersion: "test"}); err != nil {
		t.Fatalf("valid HTTP probe failed: %v", err)
	}
}
