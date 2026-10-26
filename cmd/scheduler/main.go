package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"beacon/internal/config"
	"beacon/internal/model"
	"beacon/internal/obs"
	"beacon/internal/store"

	"github.com/nats-io/nats.go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("component=scheduler config=%s", cfg.Log())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	status := obs.NewComponentStatus(cfg.FaultFailReadiness)
	if err := obs.StartHealthServer(cfg.MetricsAddr, status, "scheduler", cfg.AppVersion, cfg.GitSHA, cfg.PodName); err != nil {
		log.Fatalf("start health server: %v", err)
	}
	obs.StartFaults(cfg)
	if !obs.WaitForStartupDelay(ctx, cfg.StartupDelaySeconds) {
		return
	}
	client, err := store.NewClient(cfg.NATSURL, cfg.NATSUser, cfg.NATSPassword)
	if err != nil {
		log.Fatalf("connect NATS: %v", err)
	}
	defer client.Close()
	status.MarkStarted(client)
	if err := client.EnsureInfrastructure(ctx, cfg.CheckConcurrency); err != nil {
		log.Fatalf("initialize JetStream: %v", err)
	}
	checks, err := client.KV("checks")
	if err != nil {
		log.Fatal(err)
	}
	status.MarkReady()
	if cfg.LeaderElection == "disabled" {
		obs.SetSchedulerLeader(true)
		runScheduler(ctx, cfg, client, checks)
		obs.SetSchedulerLeader(false)
		return
	}
	if cfg.LeaderElection != "kubernetes" {
		log.Fatalf("unsupported LEADER_ELECTION value %q", cfg.LeaderElection)
	}
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("load in-cluster Kubernetes config: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("create Kubernetes client: %v", err)
	}
	lock, err := resourcelock.New(
		resourcelock.LeasesResourceLock, cfg.PodNamespace, "beacon-scheduler",
		kubeClient.CoreV1(), kubeClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{Identity: cfg.PodName},
	)
	if err != nil {
		log.Fatalf("create leader election lock: %v", err)
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second,
		RetryPeriod: 2 * time.Second, ReleaseOnCancel: true, Name: "beacon-scheduler",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				obs.SetSchedulerLeader(true)
				log.Printf("leader election acquired by %s", cfg.PodName)
				runScheduler(leaderCtx, cfg, client, checks)
				obs.SetSchedulerLeader(false)
			},
			OnStoppedLeading: func() {
				obs.SetSchedulerLeader(false)
				log.Printf("leader election stopped for %s", cfg.PodName)
			},
			OnNewLeader: func(identity string) {
				if identity != cfg.PodName {
					log.Printf("scheduler leader is %s", identity)
				}
			},
		},
	})
}

func runScheduler(ctx context.Context, cfg *config.Config, client *store.Client, checks nats.KeyValue) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nextDue := make(map[string]time.Time)
	lastSlot := make(map[string]int64)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			keys, err := checks.Keys()
			if err != nil && !errors.Is(err, nats.ErrNoKeysFound) {
				log.Printf("list checks: %v", err)
				continue
			}
			seen := make(map[string]bool, len(keys))
			for _, key := range keys {
				seen[key] = true
				entry, err := checks.Get(key)
				if err != nil {
					log.Printf("read check %s: %v", key, err)
					continue
				}
				var check model.Check
				if err := json.Unmarshal(entry.Value(), &check); err != nil {
					log.Printf("decode check %s: %v", key, err)
					continue
				}
				if !check.Enabled {
					delete(nextDue, check.ID)
					continue
				}
				due, ok := nextDue[check.ID]
				if !ok {
					nextDue[check.ID] = now.Add(time.Duration(rand.Int63n(int64(check.IntervalSeconds)*int64(time.Second) + 1)))
					continue
				}
				if now.Before(due) {
					continue
				}
				interval := int64(check.IntervalSeconds)
				slot := now.Unix() / interval
				if lastSlot[check.ID] == slot {
					nextDue[check.ID] = due.Add(time.Duration(check.IntervalSeconds) * time.Second)
					continue
				}
				job := map[string]any{
					"check_id":    check.ID,
					"run_id":      fmt.Sprintf("%s:%d", check.ID, slot),
					"enqueued_at": now.UTC(),
				}
				payload, err := json.Marshal(job)
				if err != nil {
					log.Printf("encode job for %s: %v", check.ID, err)
					continue
				}
				messageID := fmt.Sprintf("%s:%d", check.ID, slot)
				if err := client.Publish(ctx, "jobs.run", messageID, payload); err != nil {
					log.Printf("enqueue check %s: %v", check.ID, err)
					continue
				}
				obs.SchedulerJobsPublished.Inc()
				lastSlot[check.ID] = slot
				nextDue[check.ID] = now.Add(time.Duration(check.IntervalSeconds) * time.Second)
			}
			for id := range nextDue {
				if !seen[id] {
					delete(nextDue, id)
					delete(lastSlot, id)
				}
			}
		}
	}
}
