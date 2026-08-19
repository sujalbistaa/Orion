# Architecture

## Components

```
                          ┌─────────────────────────────────────────┐
                          │              orion-server                │
                          │                                            │
  orionctl / web  ──HTTP──▶  apiserver  ──Command──▶  store (Raft FSM)  │
  console                  │     │                        ▲             │
                          │     │ SSE watch                │ Apply       │
                          │     ▼                        │             │
                          │  proxy (L4 LB) ◀──Endpoints── controller    │
                          │                                manager      │
                          │                                (5 controllers,│
                          │                                 leader-only) │
                          └───────────────┬───────────────────────────┘
                                          │ gRPC: Register / Sync
                     ┌────────────────────┼────────────────────┐
                     ▼                    ▼                    ▼
              orion-agent          orion-agent          orion-agent
              (node 1)             (node 2)             (node 3)
                     │                    │                    │
                     ▼                    ▼                    ▼
              Docker Engine        Docker Engine        Docker Engine
```

- **`orion-server`** hosts the Raft-replicated store, the REST API, the
  controller manager, and (optionally) the L4 service proxy, all in one
  process. Multiple replicas form the control plane via Raft; only the
  leader runs controllers.
- **`orion-agent`** runs one per node, talks to the control plane over
  gRPC, and drives the local Docker Engine.
- **`orionctl`** and the web console are both just REST clients.

## Data flow: creating a workload

1. A client `POST`s to `/api/v1/workloads` or `/api/v1/deployments`.
   `apiserver` validates the request (`pkg/api/v1/validation.go`) and turns
   it into a `store.Command`.
2. The command is proposed to Raft. Once a majority of control-plane
   replicas have durably logged it, it commits and every replica's `store`
   applies it deterministically — the object now exists with `Pending`
   phase and no node.
3. On the leader, the **scheduling controller** notices the pending
   workload (`store.PendingWorkloads()`, priority-ordered), runs it through
   the scheduler's filter/score pipeline, and proposes a `BindWorkload`
   command with the winning node. The store re-validates capacity at apply
   time — the controller's view could be stale by the time the command
   commits — and rejects a placement that would overcommit rather than
   trusting the proposer.
4. The bound node's agent picks up the workload on its next `Sync` (a
   full-state reconciliation, not a delta stream — see
   [DESIGN.md](DESIGN.md#full-state-sync-not-delta-streaming)), pulls the
   image if needed, creates and starts the container, and reports status
   back. The store's transition table (`pkg/api/v1/state.go`) is the single
   source of truth for which phase changes are legal; an illegal report is
   rejected, not silently coerced.
5. If the workload is behind a `Service`, the **endpoint controller**
   updates the service's endpoint list once the workload is `Running` and
   passing its health check. The proxy reads endpoints from the store and
   routes accordingly.

Every step after (2) is level-triggered: controllers reconcile against
current state on a debounced trigger and a periodic full resync, not
against a queue of events they might have missed. See
[DESIGN.md](DESIGN.md#level-triggered-reconciliation) for why.

## The five controllers

All leader-gated (only the current Raft leader runs them) and independently
triggered/debounced (`pkg/controller/manager.go`):

| Controller | Owns |
|---|---|
| scheduling | `Pending -> Scheduled` binding |
| deployment | replica count, rollout, rollback, ordinal naming (`<deployment>-<n>`) |
| node | heartbeat-timeout detection, `Unreachable` transition, eviction after grace period |
| endpoints | keeping a service's endpoint list in sync with workload readiness |
| gc | retention and cleanup of terminated objects |

## Consensus: `pkg/raft`

A from-scratch Raft implementation (Ongaro & Ousterhout, 2014) split
deliberately into layers:

- **`core.go`** — the state machine itself: leader election (with
  pre-vote and CheckQuorum to avoid disruptive elections from a partitioned
  node rejoining), log replication, the current-term commit restriction
  (Figure 8: a leader only commits an entry from a prior term indirectly,
  by committing a current-term entry after it), snapshotting, single-server
  membership changes, and read-index for linearizable reads. This file has
  no goroutines, no IO, and never reads the clock — it's driven purely by
  `Tick()` / `Step()` / `Ready()` / `Advance()`, which is what makes it
  possible to drive with a deterministic simulator instead of real time
  (see [TESTING.md](TESTING.md#the-raft-simulator-pkgraftrafttest)).
- **`node.go`** — the production driver: owns the goroutine, the ticker,
  and the `Ready()`/`Advance()` loop in the order Raft requires (persist
  snapshot → append entries → persist hard state → send messages → apply →
  advance).
- **`storage_file.go`** — the write-ahead log: `[crc32c][type][len][payload]`
  records, segment rotation, torn-tail truncation on restart after a crash
  mid-write, atomic snapshot install (write to temp, fsync, rename, fsync
  the directory).
- **`transport/`** — an HTTP transport between control-plane replicas
  (`POST /raft/message`, per-peer goroutine with a bounded queue,
  `X-Orion-Cluster-Key` shared-secret auth), plus an in-process `Loopback`
  transport for tests.

## The state machine: `pkg/store`

`Store` is Raft's FSM — the only thing that mutates cluster state, and it
does so identically on every replica by construction (no clock reads, no
randomness in `Apply`). Reads (`pkg/store/read.go`) always return deep
copies, so a caller holding a `*v1.Workload` can never observe it mutate
underneath them from a concurrent apply. `Watch()` gives a bounded channel
of changes for the SSE API; a watcher that falls behind is marked stale and
dropped rather than allowed to grow without bound or block the apply loop
— the consumer's job is then a full resync, which is cheap.

## Scheduling: `pkg/scheduler`

Filter/score, explicitly separated (`framework.go`):

- **Filters** (must pass all): node is `Ready` and schedulable, node
  selector match, taints tolerated, no host-port conflict, resource fit.
- **Scorers** (weighted sum): spread across nodes (weight 2, to actively
  favor even distribution over the other signals), least-allocated,
  balanced-resources.

Every placement decision records *why* — winning score, full candidate
breakdown, and rejection reasons for every infeasible node
(`v1.PlacementDecision`) — so "why did it go there" and "why didn't it
schedule" are both answerable from the object itself, not from grepping
logs.

## The node agent: `pkg/agent`

Runs a **self-fencing** control loop: if it can't reach the control plane,
it must stop its own workloads before the control plane's eviction timer
could plausibly have already rescheduled them elsewhere, or the cluster
would briefly run two copies of the same workload. This ordering is
enforced structurally (`nodeservice.New` refuses to start unless
`SelfFenceTimeout < HeartbeatTimeout + EvictionDelay`), not left to
operator configuration. See [FAILURES.md](FAILURES.md#split-brain-prevention-agent-self-fencing).

Other responsibilities: adopting containers left behind by a previous agent
process on restart (rather than recreating them), per-workload health
probing against the container's loopback host port, and restart backoff
(1s doubling to a 5-minute ceiling, forgiven after 60s of stability).

## Runtime integration: `pkg/runtime`

A small `Runtime` interface (`Create`/`Start`/`Stop`/`Logs`/...) with one
real implementation, `Docker` (Docker Engine API via
`github.com/docker/docker`), verified by conformance tests against a real
daemon. Notable choices: Orion disables the engine's own restart policy
(`RestartPolicyDisabled`) because Orion restarts containers through its own
state machine — an engine-level restart would race with the control
plane's view of the world; capability drops are limited to `NET_RAW`,
`MKNOD`, `AUDIT_WRITE` rather than `ALL`, because dropping `ALL` also
removes `CHOWN`/`SETUID`, which most server images use to drop privilege
at startup.

## Service proxy: `pkg/proxy`

An L4 (TCP) load balancer, not L7 — Orion doesn't parse or route on HTTP
semantics, which keeps the proxy protocol-agnostic and simple, at the cost
of not supporting HTTP-aware routing (path/header-based rules). It reads
service endpoints from the store and health-checks them independently
before including them in rotation, with proper half-close handling on
connection teardown.

## Observability: `pkg/telemetry`

Structured logging (`log/slog`, JSON or text) and Prometheus metrics
(`client_golang`), with metric label cardinality kept bounded deliberately
— e.g. HTTP route labels use the matched pattern (`/api/v1/nodes/{name}`),
never the raw path, so metrics cardinality doesn't grow with the number of
nodes ever created.
