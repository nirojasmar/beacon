package checker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
	"beacon/internal/obs"
	"beacon/internal/probes"
	"beacon/internal/store"

	"github.com/nats-io/nats.go"
)

type job struct {
	CheckID    string    `json:"check_id"`
	RunID      string    `json:"run_id"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("component=checker config=%s", cfg.Log())
	if cfg.StartupDelaySeconds > 0 {
		time.Sleep(time.Duration(cfg.StartupDelaySeconds) * time.Second)
	}
	obs.StartFaults(cfg)
	client, err := store.NewClient(cfg.NATSURL, cfg.NATSUser, cfg.NATSPassword)
	if err != nil {
		log.Fatalf("connect NATS: %v", err)
	}
	defer client.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := client.EnsureInfrastructure(ctx, cfg.CheckConcurrency); err != nil {
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
	subscription, err := client.Stream().PullSubscribe("jobs.run", "checkers", nats.BindStream("JOBS"))
	if err != nil {
		log.Fatalf("subscribe to jobs: %v", err)
	}
	defer func() {
		if err := subscription.Unsubscribe(); err != nil {
			log.Printf("unsubscribe from jobs: %v", err)
		}
	}()
	if err := obs.StartHealthServer(cfg.MetricsAddr, client, cfg.FaultFailReadiness, "checker"); err != nil {
		log.Fatalf("start health server: %v", err)
	}

	for {
		messages, err := subscription.Fetch(cfg.CheckConcurrency, nats.MaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			log.Printf("fetch jobs: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var workers sync.WaitGroup
		for _, message := range messages {
			workers.Add(1)
			go func(message *nats.Msg) {
				defer workers.Done()
				if err := processJob(ctx, cfg, client, checks, states, message); err != nil {
					log.Printf("process job: %v", err)
					if nakErr := message.NakWithDelay(time.Second); nakErr != nil {
						log.Printf("negative-ack job: %v", nakErr)
					}
				}
			}(message)
		}
		workers.Wait()
	}
}

func processJob(ctx context.Context, cfg *config.Config, client *store.Client, checks, states nats.KeyValue, message *nats.Msg) error {
	var current job
	if err := json.Unmarshal(message.Data, &current); err != nil {
		_ = message.Term()
		return fmt.Errorf("decode job: %w", err)
	}
	if current.CheckID == "" || current.RunID == "" {
		_ = message.Term()
		return fmt.Errorf("job is missing check_id or run_id")
	}
	if current.EnqueuedAt.IsZero() {
		current.EnqueuedAt = time.Now().UTC()
	}

	checkEntry, err := checks.Get(current.CheckID)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return message.Ack()
		}
		return fmt.Errorf("load check: %w", err)
	}
	var check model.Check
	if err := json.Unmarshal(checkEntry.Value(), &check); err != nil {
		_ = message.Term()
		return fmt.Errorf("decode check: %w", err)
	}
	if !check.Enabled || time.Since(current.EnqueuedAt) > 2*time.Duration(check.IntervalSeconds)*time.Second {
		return message.Ack()
	}

	result := probes.Run(ctx, &check, cfg)
	result.RunID = current.RunID
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	if err := client.Publish(ctx, "results."+check.ID, current.RunID, resultBytes); err != nil {
		return err
	}

	key := "check_" + check.ID
	for attempt := 0; attempt < 5; attempt++ {
		state := model.NewState(check.ID)
		var revision uint64
		entry, err := states.Get(key)
		if err == nil {
			revision = entry.Revision()
			if err := json.Unmarshal(entry.Value(), state); err != nil {
				return fmt.Errorf("decode state: %w", err)
			}
		} else if !errors.Is(err, nats.ErrKeyNotFound) {
			return fmt.Errorf("load state: %w", err)
		}

		event, applied := state.ApplyResult(&check, result)
		if !applied {
			if event != nil {
				if err := publishEvent(ctx, client, event); err != nil {
					return err
				}
			}
			return message.Ack()
		}
		stateBytes, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("encode state: %w", err)
		}
		if revision == 0 {
			_, err = states.Create(key, stateBytes)
		} else {
			_, err = states.Update(key, stateBytes, revision)
		}
		if err != nil {
			latest, getErr := states.Get(key)
			if getErr == nil && (revision == 0 || latest.Revision() != revision) {
				continue
			}
			return fmt.Errorf("update state: %w", err)
		}
		if event != nil {
			if err := publishEvent(ctx, client, event); err != nil {
				return err
			}
		}
		return message.Ack()
	}
	return fmt.Errorf("state update exceeded compare-and-swap retry limit")
}

func publishEvent(ctx context.Context, client *store.Client, event *model.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode state-change event: %w", err)
	}
	return client.Publish(ctx, "events.state_change", fmt.Sprintf("%s:%d", event.CheckID, event.ChangeSeq), data)
}
