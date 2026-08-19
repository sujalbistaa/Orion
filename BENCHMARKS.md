# Benchmarks

Every number on this page comes from an actual run on the hardware listed
below, produced by `make bench` or the fault-injection CLI. Nothing here is
estimated, extrapolated, or aspirational. Re-run the commands shown to
reproduce a number, or to get a number for your own hardware.

## Environment

- CPU: Apple M3 (8 cores)
- OS: macOS 15.6.1
- Go: go1.25.0 darwin/arm64
- `go test -run '^$' -bench . -benchmem ./...`, single run, default `-benchtime` (1s)

These are microbenchmarks on hot in-memory paths, run in isolation. They
measure algorithmic cost, not what a loaded, disk-bound, contended production
node will do — see [Reading these numbers](#reading-these-numbers) below.

## Scheduler

```
BenchmarkScheduleAcross100Nodes-8       77499     13943 ns/op     36058 B/op     218 allocs/op
BenchmarkScheduleBatch1000Workloads-8     100  10530660 ns/op   94961 placements/s   15555790 B/op   112065 allocs/op
```

- `ScheduleAcross100Nodes`: one placement decision (filter + score) against
  100 nodes with a mix of running workloads already bound — **~14μs per
  decision**.
- `ScheduleBatch1000Workloads`: `ScheduleBatch` placing 1000 pending
  workloads across 100 nodes in one cycle, with in-cycle capacity
  reservation so later workloads in the batch see earlier ones' allocation —
  **~95,000 placements/second**.

## Store (Raft state machine)

```
BenchmarkApplyCreateWorkload-8    19588     82368 ns/op    544825 B/op    64 allocs/op
BenchmarkSummary1000Workloads-8  154990      7761 ns/op         0 B/op     0 allocs/op
```

- `ApplyCreateWorkload`: one `CmdCreateWorkload` command applied to the
  state machine (validation, dedup check, indexing) — **~82μs**, i.e. the
  store can apply over 12,000 commands/second sustained, well above what a
  Raft log append at realistic cluster sizes will ever throughput-limit.
- `Summary1000Workloads`: computing the cluster summary (`GET
  /api/v1/summary`) over 1000 workloads — **~7.8μs, zero allocations** (the
  read path iterates without copying).

## Raft write-ahead log

```
BenchmarkFileStorageAppendSync-8     440   2739210 ns/op       491 B/op     4 allocs/op
BenchmarkFileStorageAppendBatch-8    363   3176219 ns/op   20150 entries/s   127089 B/op   327 allocs/op
```

- `AppendSync`: one entry appended and fsync'd — **~2.7ms**, dominated
  entirely by the fsync syscall (this is expected and correct: Raft's
  durability guarantee requires every append to survive a crash before it
  is acknowledged).
- `AppendBatch`: entries appended in batches with one fsync per batch —
  **~20,000 entries/second**, showing the actual throughput available when
  entries are batched under load instead of fsync'd one at a time.

## End-to-end fault recovery

Not a microbenchmark — a real cluster (one control-plane node, two agents,
real Docker containers) with `orionctl fault inject node-failure
node=worker-02` run against a 3-replica nginx deployment behind a service.
See [FAILURES.md](FAILURES.md) for the full scenario and what is asserted
during it.

```
$ orionctl fault inject node-failure node=worker-02
Result: Succeeded (recovered in 9.43s)
```

Recovery time is dominated by the configured `-heartbeat-timeout` (15s
default) and `-eviction-delay` (15s default) — both are operator-tunable
trade-offs between false-positive tolerance and recovery latency, not fixed
costs of the algorithm. Lowering both shortens detection at the cost of
tolerating shorter real network hiccups as failures.

## Reading these numbers

- These are single-node, single-run microbenchmarks against synthetic data
  built in-memory by the benchmark itself (see the `_test.go` files next to
  each package). They isolate one code path; they do not model gRPC
  serialization, network latency, Docker Engine API latency, or contention
  from concurrent Raft proposals.
- No number here is a claim about maximum cluster size, maximum workload
  count, or production capacity — those depend on hardware, network, and
  workload shape that this repository does not control. Nothing beyond what
  is printed above should be inferred from it.
- Reproduce with `make bench`, or `go test -run '^$' -bench BenchmarkName
  -benchmem ./pkg/...` to target one benchmark.
