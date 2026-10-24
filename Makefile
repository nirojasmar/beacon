GO ?= go

.PHONY: fmt test vet build docker

fmt:
	$(GO) fmt ./...

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

build:
	$(GO) build -trimpath -o beacon .

docker:
	docker build -t beacon:dev .
