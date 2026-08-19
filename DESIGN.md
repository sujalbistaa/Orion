# Design

This document is *why*, not *what* — [ARCHITECTURE.md](ARCHITECTURE.md)
covers what each component does. Every decision here was made because an
alternative was considered and rejected for a stated reason, not because it
was the first thing that worked.

## Where Orion deliberately does not follow Kubernetes

Orion is Kubernetes-inspired, not a Kubernetes clone. Three decisions
diverge on purpose:

- **No namespaces.** Orion targets a single administrative domain.
  Namespaces would add addressing complexity (every API path, every
  selector, every RBAC rule gets a namespace dimension) to solve a
  multi-tenancy problem Orion doesn't have. If that changes, namespaces are
  addable later without breaking the object model — `ObjectMeta` doesn't
  assume global uniqueness beyond what a namespace would scope anyway.
- **A Workload is one container, not a pod.** Multi-container co-scheduling
  (sidecars, init containers) is a real and useful feature, but it's
  orthogonal to everything else in this project — the scheduler, the
  resource accounting, the health model, and the state machine all get
  simpler and more exactly correct with a 1:1 workload:container mapping.
  Adding pod-style grouping later is a new layer on top of workloads, not a
  rewrite of them.
- **No admission webhooks, no CRDs, no extensibility API.** Orion is a
  complete, opinionated system, not a platform for building other
  orchestrators. Every feature it has is because Orion's engineering
  priorities (below) called for it, not because an extension mechanism made
  it free to bolt on.

## Engineering priorities, in order

Correctness, reliability, security, testability, observability,
performance, maintainability, UX. When two of these pull in different
directions, the earlier one wins. Concretely, this is why:

- the store re-validates capacity at apply time even though the scheduler
  already checked it (correctness over performance — the scheduler's view
  can be stale by commit time);
- `pkg/raft/core.go` has no goroutines or IO, purely so it can be driven by
  a deterministic simulator (testability, paid for with an extra
  indirection layer in `node.go`);
- capability drops are an explicit allowlist of three, not `ALL`
  (security, but bounded by correctness — `ALL` breaks images that need
  `CHOWN`/`SETUID` to drop privilege, so the "more secure"-looking option
  was actually wrong);
- self-fencing is enforced by a constructor check, not a config
  recommendation in a comment (reliability over convenience — a
  misconfigured timeout that permits split-brain is a category of bug that
  shouldn't be possible to ship).

## Level-triggered reconciliation

Controllers reconcile against **current state**, on a debounced trigger and
a periodic full resync — not against a queue of events they might have
missed. The alternative (edge-triggered: react to "workload created", "node
failed" as discrete events) is more efficient in the common case and
strictly worse under failure: a missed event under edge-triggering is a
permanently missed action, while a missed *trigger* under level-triggering
just means the next resync catches up, because the controller always looks
at where things actually are, not at what it was told happened. This is why
stopping the controller manager entirely (the `controller-crash` fault
experiment) changes nothing while it's down and needs no special
resume-from-here logic on restart — see
[FAILURES.md](FAILURES.md#controller-crash-and-level-triggered-reconciliation).

## Full-state sync, not delta streaming

The agent↔control-plane protocol (`NodeService.Sync`) is a unary,
full-state exchange every heartbeat interval, not a bidirectional stream of
deltas. A delta stream is more efficient in principle and strictly worse
here: it requires both sides to agree on a starting point, detect gaps, and
resynchronize after any disconnection — which is most of the complexity of
a replication protocol, reimplemented for a link carrying a few kilobytes a
second. With full-state sync, a lost message costs exactly one interval, an
agent restart needs no special-case handshake, and a control-plane failover
is invisible to the protocol. It also keeps the node boundary consistent
with the level-triggered model used everywhere else. See
`proto/orion/v1/agent.proto` for the full rationale in context.

## Self-fencing over external fencing

The alternative to agent self-fencing is control-plane-side fencing (e.g.
STONITH — the control plane forcibly kills the suspect node through some
out-of-band mechanism). That requires infrastructure Orion doesn't assume
exists (IPMI, a cloud API, a PDU) and adds a second failure mode (the
fencing mechanism itself can fail). Self-fencing needs nothing external: an
agent that can't prove it's still authorized simply stops acting, which
works identically on a laptop and in a datacenter. The cost is a dependency
on the agent's own clock being roughly sane, which is a far weaker
assumption than "an external fencing controller exists and is reachable."

## Explainable scheduling over pure optimization

The scorer weights (spread=2, least-allocated, balanced-resources) are not
tuned to any formal optimality criterion. The design goal is that every
placement decision is *explainable* — `v1.PlacementDecision` records the
winning score's breakdown and every rejected node's reason — because an
operator debugging "why did this land here" or "why won't this schedule"
needs an answer, not a re-run of a black-box solver. A more sophisticated
bin-packing algorithm would produce marginally better packing at the cost
of that explainability, which is not a trade this project makes.

## Idempotency and optimistic concurrency

Every replicated `Command` may carry a `RequestID`; the store deduplicates
by it and replays the original result for a retried command instead of
applying twice. This exists because Raft can genuinely redeliver a
proposal (a client retries after a leader change) and because controllers
legitimately re-issue the same intent on their next reconciliation pass —
idempotency has to hold at the state-machine layer, not just be a client
convention, because the state machine is what's replicated.

Writes that read-then-modify (delete, update) accept an `ExpectedRevision`
for compare-and-swap: a client acting on stale data gets a clean rejection
instead of silently clobbering a concurrent change.

## JSON commands on the Raft log, protobuf on the data plane

The Raft log encodes commands as JSON (`store.Command.Encode`), not a
binary format. The control-plane write rate is measured in operations per
second, not per microsecond, and an operator being able to read the log
with `jq` during an incident is worth more than the bytes saved. The
agent↔control-plane data plane (heartbeats, stats, high-frequency traffic)
uses protobuf over gRPC, where the volume actually justifies the added
complexity and reduced legibility.

## What's explicitly out of scope

- **Multi-container pods** — see above; a real feature, deliberately not
  this one.
- **HTTP-aware (L7) routing in the proxy** — L4 only; adding path/header
  routing is a different, much larger proxy.
- **An extensibility API** (CRDs, admission webhooks, operators-of-operators)
  — Orion is a complete system for its stated scope, not a platform.
- **Multi-cluster / federation** — one cluster, one control plane.
