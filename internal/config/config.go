package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"net/url"
)

type Config struct {
	NATSURL               string
	NATSUser              string
	NATSPassword          string
	BEACONToken           string
	WebhookURL            string
	WebhookSecret         string
	BaseURL               string
	HTTPAddr              string
	MetricsAddr           string
	LeaderElection        string
	PodName               string
	PodNamespace          string
	CheckConcurrency      int
	BlockPrivateNetworks  bool
	InsecureSkipVerify    bool
	DiscoveryEnabled      bool
	DiscoveryNamespaces   []string
	LogLevel              string
	AppVersion            string
	GitSHA                string
	StartupDelaySeconds   int
	FaultCrashAfterSeconds int
	FaultLeakMBPerMinute  float64
	FaultFailReadiness    bool
	DNSServer             string
}

func Load() (*Config, error) {
	cfg := &Config{
		NATSURL:             os.Getenv("NATS_URL"),
		NATSUser:            os.Getenv("NATS_USER"),
		NATSPassword:        os.Getenv("NATS_PASSWORD"),
		BEACONToken:         os.Getenv("BEACON_TOKEN"),
		WebhookURL:          os.Getenv("WEBHOOK_URL"),
		WebhookSecret:       os.Getenv("WEBHOOK_SECRET"),
		BaseURL:             envOr("BASE_URL", "http://localhost:8080"),
		HTTPAddr:            envOr("HTTP_ADDR", ":8080"),
		MetricsAddr:         envOr("METRICS_ADDR", ":9090"),
		LeaderElection:      envOr("LEADER_ELECTION", "kubernetes"),
		PodName:             envOr("POD_NAME", "local"),
		PodNamespace:        envOr("POD_NAMESPACE", "default"),
		CheckConcurrency:    20,
		DiscoveryNamespaces: []string{},
		LogLevel:            envOr("LOG_LEVEL", "INFO"),
		AppVersion:          envOr("APP_VERSION", "dev"),
		GitSHA:              envOr("GIT_SHA", "unknown"),
		DNSServer:           os.Getenv("DNS_SERVER"),
	}
	var err error

	if cfg.NATSURL == "" || cfg.NATSUser == "" || cfg.NATSPassword == "" {
		return nil, fmt.Errorf("NATS_URL, NATS_USER, and NATS_PASSWORD are required")
	}
	if cfg.HTTPAddr == "" || cfg.MetricsAddr == "" {
		return nil, fmt.Errorf("HTTP_ADDR and METRICS_ADDR cannot be empty")
	}
	if cfg.CheckConcurrency, err = intEnv("CHECKER_CONCURRENCY", 20); err != nil || cfg.CheckConcurrency < 1 {
		return nil, fmt.Errorf("CHECKER_CONCURRENCY must be a positive integer")
	}
	if cfg.BlockPrivateNetworks, err = boolEnv("BLOCK_PRIVATE_NETWORKS", false); err != nil {
		return nil, fmt.Errorf("BLOCK_PRIVATE_NETWORKS: %w", err)
	}
	if cfg.InsecureSkipVerify, err = boolEnv("INSECURE_SKIP_VERIFY", false); err != nil {
		return nil, fmt.Errorf("INSECURE_SKIP_VERIFY: %w", err)
	}
	if cfg.DiscoveryEnabled, err = boolEnv("DISCOVERY_ENABLED", false); err != nil {
		return nil, fmt.Errorf("DISCOVERY_ENABLED: %w", err)
	}
	if cfg.FaultFailReadiness, err = boolEnv("FAULT_FAIL_READINESS", false); err != nil {
		return nil, fmt.Errorf("FAULT_FAIL_READINESS: %w", err)
	}
	if cfg.StartupDelaySeconds, err = intEnv("STARTUP_DELAY_SECONDS", 0); err != nil || cfg.StartupDelaySeconds < 0 {
		return nil, fmt.Errorf("STARTUP_DELAY_SECONDS must be a non-negative integer")
	}
	if cfg.FaultCrashAfterSeconds, err = intEnv("FAULT_CRASH_AFTER_SECONDS", 0); err != nil || cfg.FaultCrashAfterSeconds < 0 {
		return nil, fmt.Errorf("FAULT_CRASH_AFTER_SECONDS must be a non-negative integer")
	}
	if cfg.FaultLeakMBPerMinute, err = floatEnv("FAULT_LEAK_MB_PER_MINUTE", 0); err != nil || cfg.FaultLeakMBPerMinute < 0 {
		return nil, fmt.Errorf("FAULT_LEAK_MB_PER_MINUTE must be a non-negative number")
	}

	for _, namespace := range strings.Split(os.Getenv("DISCOVERY_NAMESPACES"), ",") {
		if namespace = strings.TrimSpace(namespace); namespace != "" {
			cfg.DiscoveryNamespaces = append(cfg.DiscoveryNamespaces, namespace)
		}
	}
	return cfg, nil
}

func (c *Config) Log() string {
	return fmt.Sprintf(
		"NATS_URL=%q NATS_USER=%q NATS_PASSWORD=*** BEACON_TOKEN=*** WEBHOOK_URL=%q WEBHOOK_SECRET=*** BASE_URL=%q HTTP_ADDR=%q METRICS_ADDR=%q LEADER_ELECTION=%q POD_NAME=%q POD_NAMESPACE=%q CHECKER_CONCURRENCY=%d BLOCK_PRIVATE_NETWORKS=%t INSECURE_SKIP_VERIFY=%t DISCOVERY_ENABLED=%t DISCOVERY_NAMESPACES=%q LOG_LEVEL=%q APP_VERSION=%q GIT_SHA=%q STARTUP_DELAY_SECONDS=%d FAULT_CRASH_AFTER_SECONDS=%d FAULT_LEAK_MB_PER_MINUTE=%g FAULT_FAIL_READINESS=%t",
		redactURL(c.NATSURL), c.NATSUser, redactURL(c.WebhookURL), redactURL(c.BaseURL),
		c.HTTPAddr, c.MetricsAddr, c.LeaderElection, c.PodName, c.PodNamespace,
		c.CheckConcurrency, c.BlockPrivateNetworks, c.InsecureSkipVerify,
		c.DiscoveryEnabled, c.DiscoveryNamespaces, c.LogLevel, c.AppVersion, c.GitSHA,
		c.StartupDelaySeconds, c.FaultCrashAfterSeconds, c.FaultLeakMBPerMinute, c.FaultFailReadiness,
	)
}

func redactURL(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return "<configured>"
	}
	return parsed.Scheme + "://" + parsed.Hostname()
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func boolEnv(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseBool(value)
}

func floatEnv(key string, fallback float64) (float64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseFloat(value, 64)
}
