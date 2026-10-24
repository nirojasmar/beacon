package bootstrap

import (
	"context"
	"log"
	"time"

	"beacon/internal/config"
	"beacon/internal/store"
)

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("component=bootstrap config=%s", cfg.Log())
	deadline := time.Now().Add(2 * time.Minute)
	delay := time.Second
	for {
		client, err := store.NewClient(cfg.NATSURL, cfg.NATSUser, cfg.NATSPassword)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = client.EnsureInfrastructure(ctx, cfg.CheckConcurrency)
			cancel()
			client.Close()
			if err == nil {
				log.Print("JetStream streams, buckets, and consumers are ready")
				return
			}
		}
		if time.Now().After(deadline) {
			log.Fatalf("bootstrap failed after 2 minutes: %v", err)
		}
		log.Printf("bootstrap dependency unavailable: %v; retrying in %s", err, delay)
		time.Sleep(delay)
		delay *= 2
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
	}
}
