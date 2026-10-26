# Beacon

Beacon is a Go uptime-monitoring service backed by NATS JetStream. Its single binary exposes `api`, `scheduler`, `checker`, `notifier`, `digest`, and `bootstrap` commands.

## Run locally

Docker Compose starts JetStream, two API instances, a scheduler, three checkers, and a notifier:

```sh
docker compose up --build
```

The status page is available at `http://localhost:8081` (second API instance: `http://localhost:8082`). The local development bearer token defaults to `local-development-token`; override it with `BEACON_TOKEN`. NATS defaults to user `beacon` and password `beacon`; set `NATS_PASSWORD` before starting Compose to change it. These defaults are for local development only.

Create a check:

```sh
curl -X POST http://localhost:8081/api/v1/checks \
  -H 'Authorization: Bearer local-development-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Example","type":"http","target":"https://example.com/","interval_seconds":30,"timeout_seconds":5}'
```

The API also supports listing/reading, replacing with `If-Match` revision checks, deleting, immediate runs, recent results, status summaries, and an SSE event stream at `/api/v1/stream`. `docker compose --profile digest run --rm beacon-digest` runs a one-off summary.

Built-in unauthenticated targets are available at `/demo/ok`, `/demo/status/{code}`, `/demo/slow?ms=1500`, and `/demo/flaky?fail_percent=30`. The delay is limited to 30 seconds; status codes must be 200–599 and failure percentages 0–100. `/version` returns the configured version, Git SHA, and pod name. The API serves health, startup, readiness, version, and Prometheus metrics on both its HTTP and metrics listeners; the other long-running components expose them on the metrics listener. `STARTUP_DELAY_SECONDS` keeps `/startupz` at `503 {"status":"starting"}` until the first NATS connection succeeds.

Prometheus metrics include probe counts/durations, scheduled jobs, checker backlog, state CAS retries, notifications and DLQ events, connected SSE clients, NATS connection state/reconnects, scheduler leadership, and Go/process collectors. API shutdown cancels active SSE request contexts so streams close before the server's graceful-shutdown deadline. The checker stops fetching on SIGTERM and gives already-fetched work up to 20 seconds to finish.

## Build and test

Go 1.23 or newer:

```sh
go test -race ./...
go vet ./...
go build -o beacon .
```

Or use `make test`, `make vet`, and `make build`. The production image is built with a static Go builder and runs as a non-root user in `gcr.io/distroless/static:nonroot`.

## Configuration

`NATS_URL`, `NATS_USER`, and `NATS_PASSWORD` are required. The API also requires `BEACON_TOKEN`. Other supported settings include `WEBHOOK_URL`, `WEBHOOK_SECRET`, `BASE_URL`, `HTTP_ADDR`, `METRICS_ADDR`, `LEADER_ELECTION`, `POD_NAME`, `POD_NAMESPACE`, `CHECKER_CONCURRENCY`, `BLOCK_PRIVATE_NETWORKS`, `INSECURE_SKIP_VERIFY`, `DNS_SERVER`, `APP_VERSION`, `GIT_SHA`, and the `STARTUP_DELAY_SECONDS` / `FAULT_*` demo knobs. Secret values are redacted from startup configuration logs.

Link-local IPs are refused by HTTP and TCP checks. Private addresses remain allowed unless `BLOCK_PRIVATE_NETWORKS=true`; this lets checks target in-cluster services by default.

## Current implementation boundary

The application runtime, local Compose stack, and image promotion workflow are implemented. Optional annotated-Service discovery is not included. Kubernetes resource manifests and GitOps overlays are maintained in the companion `beacon-argo-gitops` repository.
