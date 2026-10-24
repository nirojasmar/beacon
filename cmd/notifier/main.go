package notifier

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"beacon/internal/config"
	"beacon/internal/obs"
	"beacon/internal/store"

	"github.com/nats-io/nats.go"
)

var retryBackoff = []time.Duration{
	time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute,
}

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("component=notifier config=%s", cfg.Log())
	if cfg.StartupDelaySeconds > 0 {
		time.Sleep(time.Duration(cfg.StartupDelaySeconds) * time.Second)
	}
	obs.StartFaults(cfg)
	client, err := store.NewClient(cfg.NATSURL, cfg.NATSUser, cfg.NATSPassword)
	if err != nil {
		log.Fatalf("connect NATS: %v", err)
	}
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := client.EnsureInfrastructure(ctx, cfg.CheckConcurrency); err != nil {
		log.Fatalf("initialize JetStream: %v", err)
	}
	if err := obs.StartHealthServer(cfg.MetricsAddr, client, cfg.FaultFailReadiness, "notifier"); err != nil {
		log.Fatalf("start health server: %v", err)
	}
	subscription, err := client.Stream().PullSubscribe("events.state_change", "notifier", nats.BindStream("EVENTS"))
	if err != nil {
		log.Fatalf("subscribe to state changes: %v", err)
	}
	defer func() {
		if err := subscription.Unsubscribe(); err != nil {
			log.Printf("unsubscribe from events: %v", err)
		}
	}()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	for {
		if ctx.Err() != nil {
			return
		}
		messages, err := subscription.Fetch(1, nats.MaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			log.Printf("fetch events: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			if err := deliver(ctx, cfg, client, httpClient, message); err != nil {
				log.Printf("deliver event: %v", err)
			}
		}
	}
}

func deliver(ctx context.Context, cfg *config.Config, client *store.Client, httpClient *http.Client, message *nats.Msg) error {
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("read delivery metadata: %w", err)
	}
	if err := postWebhook(ctx, httpClient, cfg.WebhookURL, cfg.WebhookSecret, message.Data); err != nil {
		if metadata.NumDelivered >= 8 {
			if publishErr := client.Publish(ctx, "events.dlq", fmt.Sprintf("dlq:%d", metadata.Sequence.Stream), message.Data); publishErr != nil {
				_ = message.NakWithDelay(time.Minute)
				return fmt.Errorf("post webhook: %v; copy event to DLQ: %w", err, publishErr)
			}
			return message.Term()
		}
		return retry(message, metadata.NumDelivered, err)
	}
	return message.Ack()
}

func postWebhook(ctx context.Context, client *http.Client, webhookURL, secret string, body []byte) error {
	if webhookURL == "" {
		log.Printf("webhook not configured; event=%s", body)
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if secret != "" {
		request.Header.Set("X-Beacon-Signature", SignPayload(secret, body))
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
	}
	return nil
}

func SignPayload(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func retry(message *nats.Msg, attempt uint64, cause error) error {
	delayIndex := int(attempt) - 1
	if delayIndex < 0 {
		delayIndex = 0
	}
	if delayIndex >= len(retryBackoff) {
		delayIndex = len(retryBackoff) - 1
	}
	if err := message.NakWithDelay(retryBackoff[delayIndex]); err != nil {
		return fmt.Errorf("%v; negative-ack failed: %w", cause, err)
	}
	return cause
}
