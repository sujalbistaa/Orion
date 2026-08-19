package v1

import "fmt"

// NodePhase is the lifecycle state of a node as observed by the control plane.
//
//	                  ┌──────────────────────────────┐
//	                  ↓                              │
//	Registering ──► Ready ◄──► NotReady ──► Unreachable
//	                  │            │             │
//	                  ↓            ↓             ↓
//	              Draining ───────────────► Decommissioned
//
// Ready/NotReady are driven by heartbeat freshness and agent-reported
// conditions. Unreachable is terminal for scheduling purposes: its workloads
// become eligible for eviction and rescheduling.
type NodePhase string

const (
	// NodeRegistering: the node has announced itself but has not completed a
	// capability report, so its capacity is unknown and it cannot be scheduled.
	NodeRegistering NodePhase = "Registering"
	// NodeReady: heartbeats are fresh and the runtime is healthy. Schedulable.
	NodeReady NodePhase = "Ready"
	// NodeNotReady: heartbeats are fresh but the node reports a problem
	// (runtime unhealthy, disk pressure). Not schedulable; workloads are not
	// yet evicted because the node may still be running them correctly.
	NodeNotReady NodePhase = "NotReady"
	// NodeUnreachable: no heartbeat within the failure-detection window.
	// Workloads on this node are eligible for eviction after the eviction grace
	// period. This is the state that drives self-healing.
	NodeUnreachable NodePhase = "Unreachable"
	// NodeDraining: an operator requested that workloads move off this node.
	// Not schedulable; existing workloads are evicted in a controlled fashion.
	NodeDraining NodePhase = "Draining"
	// NodeDecommissioned: terminal. The node is removed from the cluster and
	// its record is retained only for audit.
	NodeDecommissioned NodePhase = "Decommissioned"
)

var nodeTransitions = map[NodePhase]map[NodePhase]bool{
	NodeRegistering: {NodeReady: true, NodeNotReady: true, NodeUnreachable: true, NodeDecommissioned: true},
	NodeReady:       {NodeNotReady: true, NodeUnreachable: true, NodeDraining: true, NodeDecommissioned: true},
	NodeNotReady:    {NodeReady: true, NodeUnreachable: true, NodeDraining: true, NodeDecommissioned: true},
	NodeUnreachable: {NodeReady: true, NodeNotReady: true, NodeDecommissioned: true},
	NodeDraining:    {NodeUnreachable: true, NodeDecommissioned: true, NodeReady: true},
	// Decommissioned is terminal.
	NodeDecommissioned: {},
}

// Schedulable reports whether the scheduler may place new workloads here.
func (p NodePhase) Schedulable() bool { return p == NodeReady }

// Terminal reports whether no further transition is possible.
func (p NodePhase) Terminal() bool { return p == NodeDecommissioned }

// CanTransitionTo reports whether from -> to is a legal node transition.
// Self-transitions are always legal and are treated as no-ops by callers.
func (p NodePhase) CanTransitionTo(to NodePhase) bool {
	if p == to {
		return true
	}
	return nodeTransitions[p][to]
}

// WorkloadPhase is the lifecycle state of a single workload instance.
//
//	Pending ──► Scheduled ──► Starting ──► Running ──┬──► Succeeded ──► Terminated
//	   │            │             │           │      ├──► Failed    ──► Terminated
//	   │            │             │           │      │
//	   └────────────┴─────────────┴───────────┴──────┴──► Terminating ──► Terminated
//
// Pending means "accepted, awaiting placement". The scheduler owns the
// Pending -> Scheduled edge and no other component may write NodeName.
// The node agent owns Starting/Running/Succeeded/Failed. The controller
// manager owns Terminating and eviction back to Pending is expressed by
// creating a *new* workload, never by rewinding an existing one — this is the
// invariant that makes "no duplicate workloads" checkable.
type WorkloadPhase string

const (
	// WorkloadPending: accepted by the API server, not yet placed on a node.
	WorkloadPending WorkloadPhase = "Pending"
	// WorkloadScheduled: the scheduler bound it to a node; the agent has not
	// yet acknowledged it.
	WorkloadScheduled WorkloadPhase = "Scheduled"
	// WorkloadStarting: the agent is pulling the image and creating the container.
	WorkloadStarting WorkloadPhase = "Starting"
	// WorkloadRunning: the container is running. Health probes apply here.
	WorkloadRunning WorkloadPhase = "Running"
	// WorkloadSucceeded: the container exited with code 0 and the restart
	// policy does not call for a restart.
	WorkloadSucceeded WorkloadPhase = "Succeeded"
	// WorkloadFailed: the container exited non-zero, or could not be started,
	// and will not be restarted in place.
	WorkloadFailed WorkloadPhase = "Failed"
	// WorkloadTerminating: deletion requested; the agent is stopping the
	// container within the termination grace period.
	WorkloadTerminating WorkloadPhase = "Terminating"
	// WorkloadTerminated: terminal. The container is gone and its resources
	// are released on the node.
	WorkloadTerminated WorkloadPhase = "Terminated"
)

var workloadTransitions = map[WorkloadPhase]map[WorkloadPhase]bool{
	WorkloadPending: {WorkloadScheduled: true, WorkloadTerminating: true, WorkloadFailed: true},
	// Scheduled -> Running is permitted directly. Starting exists to report a
	// slow image pull, not as mandatory ceremony: a container whose image is
	// already present is Running before the agent's next heartbeat, and
	// rejecting that report would leave the workload stuck in Scheduled
	// forever. The transitions this table forbids are the ones that would
	// corrupt state — rewinding a placement, or resurrecting a terminated
	// workload — not fast startups.
	WorkloadScheduled: {WorkloadStarting: true, WorkloadRunning: true, WorkloadTerminating: true, WorkloadFailed: true},
	WorkloadStarting:  {WorkloadRunning: true, WorkloadFailed: true, WorkloadTerminating: true},
	WorkloadRunning:   {WorkloadSucceeded: true, WorkloadFailed: true, WorkloadTerminating: true},
	// A restart in place keeps the workload Running; only unrecoverable exits
	// move it out. Failed/Succeeded may still be explicitly torn down.
	WorkloadSucceeded:   {WorkloadTerminating: true, WorkloadTerminated: true},
	WorkloadFailed:      {WorkloadTerminating: true, WorkloadTerminated: true},
	WorkloadTerminating: {WorkloadTerminated: true},
	WorkloadTerminated:  {},
}

// Terminal reports whether the workload can never run again.
func (p WorkloadPhase) Terminal() bool { return p == WorkloadTerminated }

// Done reports whether the workload has stopped doing useful work. Done
// workloads no longer count against a node's allocated resources once
// Terminated, but Failed/Succeeded still hold their record for diagnosis.
func (p WorkloadPhase) Done() bool {
	switch p {
	case WorkloadSucceeded, WorkloadFailed, WorkloadTerminated:
		return true
	}
	return false
}

// Active reports whether the workload counts toward a deployment's replica
// count and toward node resource allocation.
func (p WorkloadPhase) Active() bool {
	switch p {
	case WorkloadPending, WorkloadScheduled, WorkloadStarting, WorkloadRunning:
		return true
	}
	return false
}

// Placed reports whether the workload has a node assignment.
func (p WorkloadPhase) Placed() bool {
	return p != WorkloadPending
}

func (p WorkloadPhase) CanTransitionTo(to WorkloadPhase) bool {
	if p == to {
		return true
	}
	return workloadTransitions[p][to]
}

// TransitionError describes a rejected state transition. It is returned by the
// state machine rather than silently coerced, so illegal transitions surface as
// errors in tests and logs instead of corrupting cluster state.
type TransitionError struct {
	Kind string
	Name string
	From string
	To   string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("illegal %s transition for %q: %s -> %s", e.Kind, e.Name, e.From, e.To)
}

// HealthState is the result of the most recent health evaluation. It is
// deliberately separate from the lifecycle phase: a workload can be Running but
// Unhealthy, which is what drives restarts and endpoint removal.
type HealthState string

const (
	HealthUnknown   HealthState = "Unknown"
	HealthHealthy   HealthState = "Healthy"
	HealthUnhealthy HealthState = "Unhealthy"
)

// Serving reports whether a workload in this health state should receive
// traffic through a service. Unknown does not serve: endpoints must be proven
// healthy, never assumed.
func (h HealthState) Serving() bool { return h == HealthHealthy }

// RestartPolicy controls what the node agent does when a container exits.
type RestartPolicy string

const (
	// RestartAlways restarts on any exit, with backoff. Default for services.
	RestartAlways RestartPolicy = "Always"
	// RestartOnFailure restarts only on non-zero exit.
	RestartOnFailure RestartPolicy = "OnFailure"
	// RestartNever leaves the workload in Succeeded/Failed.
	RestartNever RestartPolicy = "Never"
)

func (r RestartPolicy) Valid() bool {
	switch r {
	case RestartAlways, RestartOnFailure, RestartNever:
		return true
	}
	return false
}

// ShouldRestart decides, for a given exit code, whether the agent restarts the
// container in place instead of moving the workload to a done phase.
func (r RestartPolicy) ShouldRestart(exitCode int32) bool {
	switch r {
	case RestartAlways:
		return true
	case RestartOnFailure:
		return exitCode != 0
	default:
		return false
	}
}

// DeploymentPhase summarizes rollout state.
type DeploymentPhase string

const (
	// DeploymentProgressing: actual replicas or template revision have not yet
	// converged to the spec.
	DeploymentProgressing DeploymentPhase = "Progressing"
	// DeploymentAvailable: the desired number of replicas are Running and healthy.
	DeploymentAvailable DeploymentPhase = "Available"
	// DeploymentDegraded: converge is blocked — typically unschedulable
	// replicas or crash-looping workloads past the progress deadline.
	DeploymentDegraded DeploymentPhase = "Degraded"
)

// EventSeverity classifies operational events shown in the console.
type EventSeverity string

const (
	SeverityInfo     EventSeverity = "Info"
	SeverityWarning  EventSeverity = "Warning"
	SeverityCritical EventSeverity = "Critical"
)
