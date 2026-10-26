package obs

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	CheckRunsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_check_runs_total",
		Help: "Total number of executed check runs.",
	}, []string{"type", "result"})
	CheckDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_check_duration_seconds",
		Help:    "Duration of check probes in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})
	SchedulerJobsPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "beacon_scheduler_jobs_published_total",
		Help: "Total number of scheduled jobs successfully published.",
	})
	StateCASRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "beacon_state_cas_retries_total",
		Help: "Total number of state compare-and-swap retries.",
	})
	NotificationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_notifications_total",
		Help: "Total number of notification delivery attempts.",
	}, []string{"result"})
	DLQTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "beacon_dlq_total",
		Help: "Total number of events copied to the dead-letter queue.",
	})
	SSEClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "beacon_sse_clients",
		Help: "Number of currently connected server-sent event clients.",
	})
)

func init() {
	for _, checkType := range []string{"http", "tcp", "dns"} {
		for _, result := range []string{"success", "failure"} {
			CheckRunsTotal.WithLabelValues(checkType, result)
		}
		CheckDuration.WithLabelValues(checkType)
	}
	NotificationsTotal.WithLabelValues("success")
	NotificationsTotal.WithLabelValues("failure")
}

func MetricsHandler(status *ComponentStatus, component string) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		CheckRunsTotal,
		CheckDuration,
		SchedulerJobsPublished,
		StateCASRetries,
		NotificationsTotal,
		DLQTotal,
		SSEClients,
	)
	registry.MustRegister(newRuntimeCollector(status, component))
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

type runtimeCollector struct {
	status      *ComponentStatus
	component   string
	connected   *prometheus.Desc
	reconnects  *prometheus.Desc
	leader      *prometheus.Desc
	jobsPending *prometheus.Desc
	jobsAcked   *prometheus.Desc
}

func newRuntimeCollector(status *ComponentStatus, component string) *runtimeCollector {
	return &runtimeCollector{
		status:      status,
		component:   component,
		connected:   prometheus.NewDesc("beacon_nats_connected", "Whether Beacon is connected to NATS.", nil, nil),
		reconnects:  prometheus.NewDesc("beacon_nats_reconnects_total", "Total NATS reconnects for this process.", nil, nil),
		leader:      prometheus.NewDesc("beacon_scheduler_is_leader", "Whether this scheduler holds the leader Lease.", nil, nil),
		jobsPending: prometheus.NewDesc("beacon_jobs_pending", "Number of jobs waiting in the checker consumer.", nil, nil),
		jobsAcked:   prometheus.NewDesc("beacon_jobs_ack_pending", "Number of jobs currently awaiting acknowledgement.", nil, nil),
	}
}

func (c *runtimeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connected
	ch <- c.reconnects
	if c.component == "scheduler" {
		ch <- c.leader
	}
	if c.component == "checker" {
		ch <- c.jobsPending
		ch <- c.jobsAcked
	}
}

func (c *runtimeCollector) Collect(ch chan<- prometheus.Metric) {
	client := c.status.Client()
	connected, reconnects := 0.0, 0.0
	if client != nil {
		if client.Connected() {
			connected = 1
		}
		reconnects = float64(client.Reconnects())
	}
	ch <- prometheus.MustNewConstMetric(c.connected, prometheus.GaugeValue, connected)
	ch <- prometheus.MustNewConstMetric(c.reconnects, prometheus.CounterValue, reconnects)
	if c.component == "scheduler" {
		ch <- prometheus.MustNewConstMetric(c.leader, prometheus.GaugeValue, float64(schedulerLeader.Load()))
	}
	if c.component == "checker" && client != nil {
		info, err := client.Stream().ConsumerInfo("JOBS", "checkers")
		if err != nil {
			log.Printf("collect checker consumer metrics: %v", err)
			return
		}
		ch <- prometheus.MustNewConstMetric(c.jobsPending, prometheus.GaugeValue, float64(info.NumPending))
		ch <- prometheus.MustNewConstMetric(c.jobsAcked, prometheus.GaugeValue, float64(info.NumAckPending))
	}
}

var _ prometheus.Collector = (*runtimeCollector)(nil)

func RegisterCheckRun(checkType string, ok bool, durationSeconds float64) {
	result := "failure"
	if ok {
		result = "success"
	}
	CheckRunsTotal.WithLabelValues(checkType, result).Inc()
	CheckDuration.WithLabelValues(checkType).Observe(durationSeconds)
}
