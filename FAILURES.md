# Failure modes

This document is Orion's claims about what it does when things break, stated
as testable hypotheses, and how each one is checked. "Checked" means one of
two things: a deterministic unit/integration test, or a real fault-injection
experiment run against a live cluster with real Docker containers. Where a
number appears, it came from an actual run — see the commands under each
section to reproduce it yourself.

## The fault-injection framework

`pkg/faults` turns "the control plane should survive X" from a design
sentence into a real, repeatable experiment. The design constraint that
shapes it: an injected fault must be indistinguishable, from the rest of the
system's point of view, from the real thing.

| Fault              | What actually happens                                                        |
|---------------------|-------------------------------------------------------------------------------|
| node failure         | the control plane stops accepting that agent's syncs — a dead machine looks exactly like this |
| network partition    | the same, except the agent is still alive, which is what exercises self-fencing |
| workload crash        | the container is actually stopped |
| leader failure         | the Raft leader actually steps down |
| controller crash       | the controller manager is actually stopped |
| resource exhaustion    | real workloads are created until scheduling genuinely fails |

Every experiment states a `Hypothesis` up front — an experiment without a
stated expectation is just breakage — and is checked continuously against
six invariants, sampled every 100ms while the fault is held and while the
cluster recovers, plus a final check after a stability window. A violation
is caught the moment it happens, not inferred afterwards from the wreckage.

Run one against a live cluster (see [DEVELOPMENT.md](DEVELOPMENT.md) for
bringing one up):

```
$ orionctl fault inject node-failure node=worker-02
```

or list what's available:

```
$ orionctl fault list
```

## Invariants asserted during every experiment

These are Orion's correctness claims expressed as code, in
`pkg/faults/invariants.go`, not as prose:

- **no-duplicate-placements** — a workload is bound to at most one node.
  Rescheduling creates a replacement rather than rebinding, so seeing the
  same workload active on two nodes means the cluster is running the same
  work twice.
- **no-scheduling-onto-failed-nodes** — no workload is freshly bound to a
  node that is `Unreachable` or being deleted. (A workload already running
  when the node failed is fine — only a *new* binding onto a dead node is a
  violation.)
- **no-resource-overcommit** — the sum of resource requests bound to a node
  never exceeds its allocatable capacity.
- **replicas-never-exceed-desired** — a deployment never runs more active
  replicas than its desired count plus surge budget.
- **endpoints-are-proven-healthy** — every endpoint a service marks `Ready`
  belongs to a workload that is actually `Running` with a passing health
  check. This is the invariant that caught a real bug — see below.
- **workload-phases-are-legal** — nothing is `Running` without a node
  binding, nothing is `Pending` with one.

`pkg/faults/invariants_test.go` exercises each check against hand-built
store snapshots that reach states the store's own `Apply` path would never
produce (deliberately, since the store enforces most of these by
construction) — that's the whole point: the invariant checker is a second
line of defense, and testing it against states it should never actually
see is what proves it would catch a real regression rather than a
tautology that only passes because nothing wrong ever happens.

## A bug this framework found

During development, running the node-failure experiment against a live
3-replica deployment produced:

```
Invariants
  ...
  endpoints-are-proven-healthy     VIOLATED (4 times)
    service web-svc advertises web-1 as ready, but it is Running/Unknown
```

The cause: when a node went `Unreachable`, its workloads' health was
correctly downgraded to `Unknown` in the same atomic Raft command, but the
service's endpoint list kept the stale `Ready` flag until the next endpoint
controller pass — up to one reconciliation interval during which the L4
proxy could still route to a dead backend. Fixed by withdrawing endpoints
for that node in the *same* atomic command that downgrades health
(`Store.withdrawEndpointsOnNode`, `pkg/store/store.go`), closing the window
entirely rather than shrinking it. Re-running the experiment afterward:

```
Result: Succeeded (recovered in 9.43s)
Invariants
  ...
  endpoints-are-proven-healthy     held
```

## Node failure and eviction

**Claim:** a node that stops heartbeating is detected within
`-heartbeat-timeout` (default 15s), marked `Unreachable`, and its workloads
are evicted and rescheduled after an additional `-eviction-delay` (default
15s) grace period — not instantly, because a node that is merely slow to
heartbeat should get a chance to recover before its work is torn down and
duplicated elsewhere.

**Checked by:** `pkg/controller/node_test.go` (unit), and the `node-failure`
fault experiment end-to-end (measured recovery: 9.43s against a 2-node
cluster with a 3-replica deployment — see
[BENCHMARKS.md](BENCHMARKS.md#end-to-end-fault-recovery)).

## Split-brain prevention: agent self-fencing

**Claim:** if a node is merely *partitioned* from the control plane rather
than actually dead, its agent must stop acting before the control plane
could plausibly have already rescheduled its workloads elsewhere —
otherwise the cluster would briefly run two copies of the same workload on
different nodes, which no invariant recovery step can undo after the fact
(you can't un-serve traffic that was already served from the wrong place).

This is enforced structurally, not by a config convention that could be set
wrong: `nodeservice.New` refuses to construct the node service unless

```
SelfFenceTimeout < HeartbeatTimeout + EvictionDelay
```

so a deployment cannot even start with a self-fence window loose enough to
let dual-write happen. `cmd/orion-server` derives
`selfFenceTimeout = evictionDeadline - 2*agentHeartbeat` rather than
requiring an operator to compute a safe value by hand.

**Checked by:** `pkg/agent/agent_test.go` (unit, `handleSyncFailure` /
`fenceAll`) and the `network-partition` fault experiment, which is the one
experiment specifically designed to exercise this path — the node's
containers keep running, but the agent must notice it cannot reach the
control plane and stop them itself before eviction fires on the other end.

## Raft leader failure

**Claim:** a forced leader step-down under load elects a new leader, every
already-committed write survives, and controllers on the new leader resume
reconciliation without duplicating in-flight work.

**Checked by:** `pkg/raft` and `pkg/raft/rafttest` — a deterministic,
seeded network simulator that runs a real Raft cluster with drop /
duplicate / delay / partition injection and asserts the five standard Raft
safety properties (election safety, leader append-only, log matching,
leader completeness, state machine safety) after every scenario. This
gives far more scenario coverage than a live experiment could practically
provide, because the simulator is deterministic and seeded — a failing seed
reproduces exactly, every time.

The `leader-failure` fault-injection experiment exists in the catalogue for
running against a live multi-replica control plane (`orion-server -peers
...`), but requires at least 3 replicas to preserve quorum during the
step-down; single-node development clusters correctly refuse to run it
rather than silently no-op.

## Controller crash and level-triggered reconciliation

**Claim:** stopping the controller manager changes nothing about cluster
state while it's down — reconciliation is level-triggered against current
state, not event-driven against a queue of things that happened — and
restarting it converges from wherever state actually is, without recreating
work that already exists.

**Checked by:** `pkg/controller` unit/integration tests and the
`controller-crash` fault experiment.

## Workload crash and restart policy

**Claim:** a crashed container is observed by the agent, which applies the
workload's `RestartPolicy` (`Always` / `OnFailure` / `Never`) with backoff
(1s, doubling to a 5m ceiling, forgiven after 60s of stability); if the
workload belongs to a deployment, the deployment controller independently
notices the missing replica and restores the desired count — and the
deployment never *exceeds* that count while both mechanisms are reacting to
the same event.

**Checked by:** `pkg/agent/workload_test.go` and the `workload-crash` fault
experiment, which specifically asserts `replicas-never-exceed-desired`
throughout.

## Resource exhaustion

**Claim:** when the cluster runs out of schedulable capacity, excess
workloads stay `Pending` with a per-node explanation of why they couldn't
be placed (the scheduler's `Rejections` list — see
`v1.PlacementDecision`), no node is driven over its allocatable capacity,
and affected deployments report `Degraded` rather than silently claiming to
be available.

**Checked by:** `pkg/scheduler` unit tests (`ErrUnschedulable` with
rejection reasons) and the `resource-exhaustion` fault experiment, which
creates real filler workloads (capped at 200) until placement genuinely
fails, then cleans every one of them up on recovery — even if some
deletions fail along the way, so the cluster is never left permanently
full.

## Known gaps

- `node-restart` (verifying an agent re-adopts its existing containers
  rather than recreating them after its own process restarts, not the
  node) is declared as an `ExperimentKind` in `pkg/apiserver/faults.go` but
  has no implementation in `pkg/faults/experiments.go` yet — the fault
  framework has no out-of-process control over the agent binary itself the
  way it does over in-process controllers. The behavior it would exercise
  (`adoptExistingContainers` in `pkg/agent/agent.go`) is covered by a unit
  test, but not yet by a live experiment.
