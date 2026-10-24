package digest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"time"

	"beacon/internal/config"
	"beacon/internal/obs"
)

type check struct {
	Name  string `json:"name"`
	State struct {
		Status    string `json:"status"`
		ChangeSeq int64  `json:"change_seq"`
	} `json:"state"`
}

type statusResponse struct {
	Counts map[string]int `json:"counts"`
	Checks []check        `json:"checks"`
}

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("component=digest config=%s", cfg.Log())
	if cfg.StartupDelaySeconds > 0 {
		time.Sleep(time.Duration(cfg.StartupDelaySeconds) * time.Second)
	}
	obs.StartFaults(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.BaseURL+"/api/v1/status", nil)
	if err != nil {
		log.Fatalf("create status request: %v", err)
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		log.Fatalf("fetch status: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		log.Fatalf("status endpoint returned HTTP %d", response.StatusCode)
	}
	var status statusResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&status); err != nil {
		log.Fatalf("decode status response: %v", err)
	}
	down := make([]check, 0)
	for _, item := range status.Checks {
		if item.State.Status == "down" {
			down = append(down, item)
		}
	}
	sort.Slice(status.Checks, func(i, j int) bool {
		return status.Checks[i].State.ChangeSeq > status.Checks[j].State.ChangeSeq
	})
	flapping := status.Checks
	if len(flapping) > 3 {
		flapping = flapping[:3]
	}
	summary, err := json.Marshal(map[string]any{
		"generated_at":  time.Now().UTC(),
		"counts":        status.Counts,
		"down_checks":   down,
		"most_flapping": flapping,
	})
	if err != nil {
		log.Fatalf("encode digest: %v", err)
	}
	if cfg.WebhookURL == "" {
		log.Printf("digest: %s", summary)
		return
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, cfg.WebhookURL, bytes.NewReader(summary))
	if err != nil {
		log.Fatalf("create webhook request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if cfg.WebhookSecret != "" {
		mac := hmac.New(sha256.New, []byte(cfg.WebhookSecret))
		_, _ = mac.Write(summary)
		request.Header.Set("X-Beacon-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	response, err = (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		log.Fatalf("post digest: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		log.Fatalf("webhook returned HTTP %d", response.StatusCode)
	}
	fmt.Println("digest delivered")
}
