# Testing

Orion is tested at four levels, each catching a different class of bug:

1. **Unit tests** — one package, no external dependencies, deterministic.
2. **Integration tests** — multiple packages wired together (e.g. scheduler
   + store + controller) or a real Docker daemon, still deterministic where
   possible.
3. **A deterministic distributed-systems simulator** (`pkg/raft/rafttest`)
   for Raft, because leader election and log replication bugs are
   overwhelmingly timing- and ordering-dependent — a flaky test here is not
   noise, it's a real bug with a hard-to-hit trigger.
4. **Live fault injection** (`pkg/faults`) against a real cluster with real
   Docker containers — see [FAILURES.md](FAILURES.md). This is the only
   level that exercises the actual failure-detection timers, the actual
   Docker Engine API, and actual network behavior together.

157 test functions across 11 packages as of this writing. Run them all:

```
make test          # go test ./...
make test-race      # the same, under the race detector — required before any merge
make test-short      # skips tests that need a Docker daemon or wall-clock convergence
make cover           # writes coverage.html
```

`test-race` matters more than usual here: this project is almost entirely
concurrent state (Raft's goroutines, the agent's per-workload health
probers, the controller manager's debounced triggers, the watcher fan-out)
and a race is a correctness bug, not a performance footnote.

## Running one package or one test

```
go test ./pkg/store/...
go test ./pkg/raft/... -run TestCommitRequiresCurrentTermEntry -v
```

## What needs Docker

`pkg/runtime` runs real conformance tests against a Docker daemon (pull,
create, start, stop, log streaming, capability drops, restart-policy
behavior — verified against Docker 28.3.2). They check `testing.Short()`
and skip cleanly with `go test -short` or `make test-short` when no daemon
is reachable, so CI environments without Docker still get everything else.

## The Raft simulator (`pkg/raft/rafttest`)

`pkg/raft/core.go` is written as a pure state machine — no goroutines, no
IO, no clock reads — driven by `Tick` / `Step` / `Ready` / `Advance`. That
separation exists specifically so it can be driven by a deterministic,
seeded network simulator instead of real time and real sockets:

```go
net := rafttest.NewNetwork(seed, nodeCount)
net.Partition(...)
net.Isolate(...)
net.BlockOneWay(...)
net.Crash(...)
net.Restart(...)
net.CheckSafety(t)  // log matching, state machine safety, leader completeness
```

`CheckSafety` asserts Raft's core safety properties after every scenario. A
failing seed reproduces exactly — rerun with the same seed and you get the
identical interleaving, which is the entire point of not using real
goroutine scheduling and real timers for this: a live flaky test here would
be nearly unreproducible.

## Mutation-verified guards

A test that never fails when the code it protects is broken is a false
sense of safety. Where a check is easy to accidentally weaken without
noticing (a state-transition guard, an invariant condition), the
corresponding test was verified by temporarily breaking the guard and
confirming the test catches it, then reverting. `pkg/faults/invariants_test.go`
documents one such case directly in a comment; the technique was applied
during development to the Raft commit-index guard
(`maybeCommit`'s current-term restriction) and to
`no-scheduling-onto-failed-nodes` as well.

## Test structure conventions

- A `harness` type (see `pkg/store/store_test.go`,
  `pkg/controller/harness_test.go`) wraps repetitive setup — applying
  commands with sequential Raft indices, building minimal fixtures — so
  individual tests read as scenario descriptions, not boilerplate.
- Fixtures are built through the same public `Apply`/`Command` path real
  callers use wherever possible, not by reaching into private state. The
  fault-invariant tests are a deliberate exception: they use `Store.Restore`
  to construct states the normal `Apply` path is designed to prevent, since
  that's precisely the class of bug those checks exist to catch as a second
  line of defense — see the comment at the top of
  `pkg/faults/invariants_test.go`.
- Comments on tests explain *why* a case matters (what it would let through
  if it broke), not what the assertion does.

## Web console

```
make web-test    # cd web && npm run test -- --run
make web-lint     # typecheck + lint
```

See [web/README](web) (if present) or `web/package.json` for the exact
tooling in use.
