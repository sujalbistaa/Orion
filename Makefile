SHELL := /bin/bash
.DEFAULT_GOAL := help

VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE     ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  := -X github.com/sujalbistaa/orion/internal/version.Version=$(VERSION) \
            -X github.com/sujalbistaa/orion/internal/version.Commit=$(COMMIT) \
            -X github.com/sujalbistaa/orion/internal/version.BuildDate=$(DATE)

BIN      := bin
GO       ?= go
BINARIES := orion-server orion-agent orionctl

## help: list available targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/## //' | awk -F': ' '{printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2}'

## build: compile all Orion binaries into ./bin
build: $(addprefix $(BIN)/,$(BINARIES))

$(BIN)/%: FORCE
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $@ ./cmd/$*

FORCE:

## test: run unit and integration tests
test:
	$(GO) test ./... -count=1

## test-race: run the full suite under the race detector
test-race:
	$(GO) test -race ./... -count=1

## test-short: skip tests that need Docker or wall-clock convergence
test-short:
	$(GO) test -short ./... -count=1

## cover: run tests with coverage and write coverage.html
cover:
	$(GO) test ./... -coverprofile=coverage.out -covermode=atomic -count=1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@$(GO) tool cover -func=coverage.out | tail -1

## bench: run benchmarks (see docs/BENCHMARKS.md for methodology)
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## fmt: format Go sources
fmt:
	$(GO) run mvdan.cc/gofumpt@v0.7.0 -l -w . 2>/dev/null || gofmt -l -w .

## lint: static analysis (vet + staticcheck + gofmt check)
lint: vet
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "gofmt: files need formatting" && exit 1)
	$(GO) run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

## proto: regenerate protobuf/gRPC code
proto:
	@command -v protoc >/dev/null || (echo "protoc is required: brew install protobuf" && exit 1)
	$(GO) run google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 --version >/dev/null
	PATH="$$($(GO) env GOPATH)/bin:$$PATH" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=github.com/sujalbistaa/orion \
		--go-grpc_out=. --go-grpc_opt=module=github.com/sujalbistaa/orion \
		proto/orion/v1/*.proto

## proto-tools: install protoc plugins into GOPATH/bin
proto-tools:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

## run: start a single-node control plane and a local agent
run: build
	./hack/run-local.sh

## dev: start control plane, agent and the web console with live reload
dev: build
	./hack/dev.sh

## web-install: install web console dependencies
web-install:
	cd web && npm ci

## web-build: build the production web console bundle
web-build:
	cd web && npm run build

## web-test: run web console tests
web-test:
	cd web && npm run test -- --run

## web-lint: typecheck and lint the web console
web-lint:
	cd web && npm run typecheck && npm run lint

## up: start the full stack (control plane + 3 agents + Prometheus) in Docker
up:
	docker compose -f deploy/docker-compose.yml up --build -d

## down: stop the Docker stack and remove volumes
down:
	docker compose -f deploy/docker-compose.yml down -v

## clean: remove build output and local cluster state
clean:
	rm -rf $(BIN) dist coverage.out coverage.html .orion

.PHONY: help build test test-race test-short cover bench vet fmt lint proto proto-tools \
        run dev web-install web-build web-test web-lint up down clean
