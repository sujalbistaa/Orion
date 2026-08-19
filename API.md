# API

Base path: `/api/v1`. JSON in, JSON out. Route table:
`pkg/apiserver/server.go`; handlers: `pkg/apiserver/handlers.go`; fault
injection handlers: `pkg/apiserver/faults.go`.

## Authentication and authorization

```
Authorization: Bearer <token>
```

Set `ORION_API_TOKEN` (grants `operator`) and optionally
`ORION_API_VIEWER_TOKEN` (grants `viewer`, read-only) on `orion-server`. If
neither is set, the server logs a warning and runs open — every caller has
full access. This is intentional for `make run` on a laptop and wrong
anywhere reachable over a network.

- `viewer` — every `GET`.
- `operator` — everything, including destructive operations.

Write requests are additionally rate-limited per authenticated principal
(token bucket, 50 req/s, burst 100) — a `429` means you're being throttled,
not rejected outright.

Destructive operations (delete, drain, deployment/service create/delete)
are recorded to an audit trail with the authenticated principal as actor.

## Error shape

Every failure returns the same envelope:

```json
{ "error": { "code": "validation_failed", "message": "2 field(s) failed validation", "fields": [ {"field": "spec.replicas", "message": "must be >= 0"} ] } }
```

`fields` is only present for `422 validation_failed` responses — build
forms to walk it and highlight the offending input rather than showing the
message alone. Other `code` values (`not_found`, `conflict`,
`would_lose_capacity`, `owned_workload`, `node_is_ready`, ...) map to the
obvious HTTP status and carry no `fields`.

## Object shapes

Every object embeds `ObjectMeta`:

```json
{
  "name": "web-0", "uid": "web-0-00000002", "revision": 41, "generation": 1,
  "labels": {}, "annotations": {},
  "createdAt": "...", "updatedAt": "...", "deletedAt": null,
  "ownerRef": { "kind": "Deployment", "name": "web", "uid": "web-0000001d" }
}
```

`revision` is the Raft log index of the object's last write — pass it back
as `?revision=N` on delete/update calls for optimistic concurrency
(compare-and-swap: the write is rejected if the object changed since you
read it).

CPU and memory are **plain integers on the wire**, not the `"500m"` /
`"64Mi"` shorthand used in CLI flags: `MilliCPU` is thousandths of a core
(`100` = 0.1 vCPU), `Bytes` is bytes (`67108864` = 64 MiB). See
`pkg/api/v1/resource.go` and `pkg/api/v1/types.go` for the exact field
list on every object — this document doesn't restate every field, only the
routes and the things that aren't obvious from the type.

## Cluster and summary

| Method | Path | |
|---|---|---|
| GET | `/api/v1/cluster` | control-plane members, Raft term/commit/applied index, quorum |
| GET | `/api/v1/summary` | counts by phase across nodes/workloads/deployments/services, aggregate capacity |

## Nodes

| Method | Path | |
|---|---|---|
| GET | `/api/v1/nodes` | list |
| GET | `/api/v1/nodes/{name}` | |
| POST | `/api/v1/nodes/{name}/cordon` | mark unschedulable, doesn't touch running work |
| POST | `/api/v1/nodes/{name}/uncordon` | |
| POST | `/api/v1/nodes/{name}/drain` | evict workloads; `409 would_lose_capacity` if this is the only Ready node with running work — retry with `?force=true` |
| DELETE | `/api/v1/nodes/{name}` | `409 node_is_ready` unless the node is not Ready — retry with `?force=true` to remove a live node and terminate its work |

## Workloads

| Method | Path | |
|---|---|---|
| GET | `/api/v1/workloads` | list (query filters: see handler for supported params — node, phase) |
| POST | `/api/v1/workloads` | create a standalone workload (not owned by a deployment) |
| GET | `/api/v1/workloads/{name}` | |
| DELETE | `/api/v1/workloads/{name}` | `409 owned_workload` if it belongs to a deployment — scale/delete the deployment instead, or retry with `?force=true`. Accepts `?revision=N` for compare-and-swap. |
| GET | `/api/v1/workloads/{name}/logs` | streams the agent-fetched container log; `501 logs_unavailable` if the server wasn't wired with a log fetcher |

## Deployments

| Method | Path | |
|---|---|---|
| GET | `/api/v1/deployments` | list |
| POST | `/api/v1/deployments` | `{name, labels, spec: {replicas, template, strategy}}` |
| GET | `/api/v1/deployments/{name}` | |
| PUT | `/api/v1/deployments/{name}` | replace spec (triggers a rollout if the template hash changes) |
| POST | `/api/v1/deployments/{name}/scale` | `{replicas: N}` |
| POST | `/api/v1/deployments/{name}/rollback` | `{targetRevision: N}` — re-applies a retained `DeploymentRevision` |
| GET | `/api/v1/deployments/{name}/revisions` | rollout history, oldest first |
| DELETE | `/api/v1/deployments/{name}` | cascades to owned workloads |

## Services

| Method | Path | |
|---|---|---|
| GET | `/api/v1/services` | list, includes live `status.endpoints` |
| POST | `/api/v1/services` | `{name, spec: {selector, port, targetPort, strategy}}` — a service with an empty selector matches nothing, deliberately (safer than matching everything) |
| GET | `/api/v1/services/{name}` | |
| DELETE | `/api/v1/services/{name}` | |

## Events

| Method | Path | |
|---|---|---|
| GET | `/api/v1/events` | query params: `since`, `before` (event ID cursors, for incremental/paged polling), `kind`, `name`, `severity`, `limit`. Newest first. |

## Live updates: `GET /api/v1/watch`

Server-Sent Events. On connect you get a `sync` event carrying current
state; after that, one event per mutation:

```
event: Workload
data: {"revision":42,"kind":"Workload","op":"Updated","name":"web-0","object":{...}}
```

`op` is `Created` | `Updated` | `Deleted`. If your connection falls behind
the server's retention window you get a `resync` event instead of a gap —
that means your local cache may be stale and you should refetch the
relevant list, not that anything is broken. A watcher that can't keep up at
all is marked stale and closed server-side rather than allowed to grow
without bound; reconnect (a plain `EventSource` does this automatically)
and you'll get a fresh `sync`.

## Fault injection: `/api/v1/faults/*`

Only mounted when `orion-server` is started with `-enable-fault-injection`
(off by default — this endpoint group can deliberately break the cluster).
See [FAILURES.md](FAILURES.md) for what each experiment actually does.

| Method | Path | |
|---|---|---|
| GET | `/api/v1/faults/experiments` | catalogue: kind, description, hypothesis, invariants asserted, parameter schema |
| GET | `/api/v1/faults/runs` | past and in-progress runs |
| POST | `/api/v1/faults/runs` | `{kind, params: {...}, durationSeconds}` — starts one |
| GET | `/api/v1/faults/runs/{id}` | poll while `state` is `Pending`/`Injecting`/`Observing`/`Recovering`; `timeline` and per-invariant `held`/`violations`/`detail` fill in as the run progresses |
| POST | `/api/v1/faults/runs/{id}/abort` | |

## Operational

| Method | Path | |
|---|---|---|
| GET | `/healthz` | liveness, unauthenticated |
| GET | `/readyz` | readiness, unauthenticated |
| GET | `/metrics` | Prometheus exposition format |

## Client library

`pkg/client` is a Go client covering the routes above, used by `orionctl`
and available for anything else you write against the API from Go.
