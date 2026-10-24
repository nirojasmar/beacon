package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"beacon/cmd/api"
	"beacon/cmd/bootstrap"
	"beacon/cmd/checker"
	"beacon/cmd/digest"
	"beacon/cmd/notifier"
	"beacon/cmd/scheduler"
)

type jsonLogWriter struct {
	logger *slog.Logger
}

func (w jsonLogWriter) Write(message []byte) (int, error) {
	w.logger.Info(strings.TrimSpace(string(message)))
	return len(message), nil
}

func init() {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	log.SetFlags(0)
	log.SetOutput(jsonLogWriter{logger: logger})
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: beacon <api|scheduler|checker|notifier|digest|bootstrap|sleep>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "api":
		api.Run()
	case "scheduler":
		scheduler.Run()
	case "checker":
		checker.Run()
	case "notifier":
		notifier.Run()
	case "digest":
		digest.Run()
	case "bootstrap":
		if len(os.Args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: beacon bootstrap")
			os.Exit(2)
		}
		bootstrap.Run()
	case "sleep":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: beacon sleep <duration>")
			os.Exit(2)
		}
		duration, err := time.ParseDuration(os.Args[2])
		if err != nil || duration < 0 {
			fmt.Fprintf(os.Stderr, "invalid sleep duration %q\n", os.Args[2])
			os.Exit(2)
		}
		time.Sleep(duration)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}
