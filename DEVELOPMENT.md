# Development

## Prerequisites

- Go 1.25+
- Docker (for `pkg/runtime` conformance tests and for actually running
  workloads — Orion schedules real containers)
- Node 20+ (for the web console, `web/`)
- `protoc` + the Go protobuf plugins, only if you're changing
  `proto/orion/v1/*.proto` (`make proto-tools` installs the plugins)

## Build

```
make build     # ./bin/orion-server, ./bin/orion-agent, ./bin/orionctl
```

## Run a local cluster

```
make run        # single-node control plane + one agent, foreground, Ctrl-C to stop
make dev         # the same, plus the web console dev server with live reload
```

Both are thin wrappers (`hack/run-local.sh`, `hack/dev.sh`) around the
built binaries — read them if you want to run with different flags; they're
under 40 lines each. Cluster state lands in `.orion/` (gitignored) and
survives between runs.

Check on it:

```
./bin/orionctl -server http://127.0.0.1:7070 cluster status
./bin/orionctl -server http://127.0.0.1:7070 node list
```

Or the full stack in Docker (control plane + 3 agents + Prometheus):

```
make up       # docker compose -f deploy/docker-compose.yml up --build -d
make down      # tear it down, including the data volume
```

The compose agents talk to the *host's* Docker daemon through a mounted
socket (Docker-outside-of-Docker) — containers they schedule are siblings
of the agent containers, not nested inside them.

## Project layout

```
cmd/               orion-server, orion-agent, orionctl entrypoints
pkg/api/v1/         domain model: types, state machine, validation, resource math
pkg/raft/           Raft consensus (pure state machine + driver + file-backed WAL)
pkg/raft/rafttest/   deterministic seeded network simulator for Raft
pkg/raft/transport/  gRPC-free HTTP transport between control-plane replicas
pkg/store/          the replicated state machine (Raft's FSM) — commands, reads, watch
pkg/scheduler/       filter/score placement
pkg/controller/      reconciliation: scheduling, deployments, node health, endpoints, GC
pkg/runtime/         container runtime abstraction + real Docker Engine API driver
pkg/agent/           node agent: self-fencing, health probes, crash recovery
pkg/nodeservice/     gRPC service the agent talks to (Register/Sync)
pkg/apiserver/       REST API: auth, handlers, SSE change streaming, fault injection API
pkg/faults/          fault injection framework (gate, invariants, experiments)
pkg/proxy/           L4 service load balancer
pkg/client/          Go client library for the REST API
pkg/telemetry/       structured logging + Prometheus metrics
proto/               protobuf definitions (agent<->control-plane gRPC)
web/                 React + TypeScript + Vite console
deploy/               Dockerfiles + docker-compose.yml + Prometheus config
hack/                 local dev scripts used by `make run` / `make dev`
test/                 end-to-end tests
```

## Regenerating protobuf code

```
make proto-tools   # once, installs protoc-gen-go / protoc-gen-go-grpc
make proto
```

## Before sending a change

```
make fmt
make lint     # vet + staticcheck + gofmt check
make test-race
```

`make lint` and `make test-race` are what CI runs (`.github/workflows/ci.yml`);
matching them locally avoids a red build for something you could have
caught in ten seconds.

## Design documents

- [ARCHITECTURE.md](ARCHITECTURE.md) — components and how data flows between them
- [DESIGN.md](DESIGN.md) — decisions made and why, including where Orion
  deliberately does *not* follow Kubernetes
- [API.md](API.md) — REST API reference
- [TESTING.md](TESTING.md) — how the test suite is organized
- [FAILURES.md](FAILURES.md) — what failure modes are handled and how each is verified
- [BENCHMARKS.md](BENCHMARKS.md) — real numbers from real runs
