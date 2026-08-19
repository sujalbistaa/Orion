// Package store holds Orion's cluster state: the replicated state machine that
// sits on top of Raft, plus the read APIs the scheduler, controllers and API
// server use.
//
// Two rules govern everything in this package:
//
//  1. Apply is deterministic. It never reads the clock, never generates
//     randomness, and never depends on map iteration order. Every replica
//     applying the same log produces byte-identical state, or the cluster has
//     silently diverged and no amount of monitoring will tell you.
//  2. Reads return deep copies. Callers hold objects across goroutine
//     boundaries; sharing pointers into the state machine would be a race
//     waiting for load.
package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/raft"
)

// Errors returned from Apply. They are deterministic: every replica computes
// the same outcome for the same entry, so these are agreed rejections rather
// than local failures.
var (
	ErrNotFound      = errors.New("store: object not found")
	ErrAlreadyExists = errors.New("store: object already exists")
	ErrConflict      = errors.New("store: revision conflict")
	ErrInvalidState  = errors.New("store: illegal state transition")
	ErrUIDMismatch   = errors.New("store: object was recreated with a different identity")
)

// maxRetainedEvents bounds the in-memory operational event log. Events are a
// diagnostic aid, not durable audit storage; the ring is sized to cover a
// realistic incident window without unbounded growth.
const maxRetainedEvents = 5000

// maxDedupEntries bounds idempotency tracking. A request ID older than this
// many mutations is no longer deduplicated; clients retry within seconds, so
// the window is generous by orders of magnitude.
const maxDedupEntries = 8192

// maxRevisionHistory is how many deployment revisions are retained for rollback.
const maxRevisionHistory = 10

type dedupRecord struct {
	Revision uint64 `json:"revision"`
	ErrText  string `json:"err,omitempty"`
}

// state is the entire replicated state. It is serialized verbatim into Raft
// snapshots, so every field must be exported and JSON-stable.
type state struct {
	Nodes       map[string]*v1.Node       `json:"nodes"`
	Workloads   map[string]*v1.Workload   `json:"workloads"`
	Deployments map[string]*v1.Deployment `json:"deployments"`
	Services    map[string]*v1.Service    `json:"services"`
	// Revisions holds rollout history per deployment, oldest first.
	Revisions map[string][]*v1.DeploymentRevision `json:"revisions"`

	Events      []v1.Event `json:"events"`
	NextEventID uint64     `json:"nextEventId"`

	AppliedIndex uint64 `json:"appliedIndex"`

	Dedup      map[string]dedupRecord `json:"dedup"`
	DedupOrder []string               `json:"dedupOrder"`

	ClusterID        string    `json:"clusterId"`
	ClusterCreatedAt time.Time `json:"clusterCreatedAt"`
}

func newState() *state {
	return &state{
		Nodes:       map[string]*v1.Node{},
		Workloads:   map[string]*v1.Workload{},
		Deployments: map[string]*v1.Deployment{},
		Services:    map[string]*v1.Service{},
		Revisions:   map[string][]*v1.DeploymentRevision{},
		Dedup:       map[string]dedupRecord{},
		NextEventID: 1,
	}
}

// Store is the Raft state machine plus its read APIs.
type Store struct {
	mu sync.RWMutex
	s  *state

	// workloadsByNode is a derived index rebuilt on Restore. It is not
	// serialized: derived data in a snapshot is a chance for the two to
	// disagree.
	workloadsByNode map[string]map[string]struct{}

	watchers *watchRegistry

	// hooks observe applied commands for metrics. They must not mutate state.
	onApply func(cmd *Command, res Result)
}

var _ raft.FSM = (*Store)(nil)

func New() *Store {
	return &Store{
		s:               newState(),
		workloadsByNode: map[string]map[string]struct{}{},
		watchers:        newWatchRegistry(),
	}
}

// SetApplyHook installs an observer called after every applied command.
func (st *Store) SetApplyHook(f func(cmd *Command, res Result)) { st.onApply = f }

// ---------------------------------------------------------------------------
// raft.FSM
// ---------------------------------------------------------------------------

// Apply executes one committed command. It runs on a single goroutine, in log
// order, on every replica.
func (st *Store) Apply(e raft.Entry) any {
	cmd, err := DecodeCommand(e.Data)
	if err != nil {
		// A malformed entry is in the replicated log; every replica sees it and
		// every replica must reject it identically.
		return Result{Revision: e.Index, Err: err}
	}

	st.mu.Lock()
	res := st.applyLocked(cmd, e.Index)
	st.s.AppliedIndex = e.Index
	changes := st.watchers.drainPending()
	st.mu.Unlock()

	st.watchers.broadcast(changes)
	if st.onApply != nil {
		st.onApply(cmd, res)
	}
	return res
}

func (st *Store) applyLocked(cmd *Command, index uint64) Result {
	// Idempotency: a retried proposal replays its original outcome instead of
	// applying twice. This is what makes "duplicate requests are handled
	// safely" true rather than aspirational.
	if cmd.RequestID != "" {
		if rec, ok := st.s.Dedup[cmd.RequestID]; ok {
			res := Result{Revision: rec.Revision, Duplicate: true}
			if rec.ErrText != "" {
				res.Err = errors.New(rec.ErrText)
			}
			st.hydrateResult(cmd, &res)
			return res
		}
	}

	res := st.dispatch(cmd, index)
	res.Revision = index

	if cmd.RequestID != "" {
		rec := dedupRecord{Revision: index}
		if res.Err != nil {
			rec.ErrText = res.Err.Error()
		}
		st.s.Dedup[cmd.RequestID] = rec
		st.s.DedupOrder = append(st.s.DedupOrder, cmd.RequestID)
		if len(st.s.DedupOrder) > maxDedupEntries {
			drop := len(st.s.DedupOrder) - maxDedupEntries
			for _, id := range st.s.DedupOrder[:drop] {
				delete(st.s.Dedup, id)
			}
			st.s.DedupOrder = append([]string(nil), st.s.DedupOrder[drop:]...)
		}
	}
	return res
}

// hydrateResult refills the object on a replayed duplicate, so a retry returns
// the same body the original call would have.
func (st *Store) hydrateResult(cmd *Command, res *Result) {
	name := cmd.Name
	if name == "" {
		switch {
		case cmd.Workload != nil:
			name = cmd.Workload.Name
		case cmd.Deployment != nil:
			name = cmd.Deployment.Name
		case cmd.Service != nil:
			name = cmd.Service.Name
		case cmd.Node != nil:
			name = cmd.Node.Name
		}
	}
	if w, ok := st.s.Workloads[name]; ok {
		res.Workload = w.DeepCopy()
	}
	if d, ok := st.s.Deployments[name]; ok {
		res.Deployment = d.DeepCopy()
	}
	if s, ok := st.s.Services[name]; ok {
		res.Service = s.DeepCopy()
	}
	if n, ok := st.s.Nodes[name]; ok {
		res.Node = n.DeepCopy()
	}
}

func (st *Store) dispatch(cmd *Command, index uint64) Result {
	switch cmd.Kind {
	case CmdRegisterNode:
		return st.registerNode(cmd, index)
	case CmdUpdateNodeStatus:
		return st.updateNodeStatus(cmd, index)
	case CmdSetNodePhase:
		return st.setNodePhase(cmd, index)
	case CmdCordonNode:
		return st.cordonNode(cmd, index)
	case CmdDeleteNode:
		return st.deleteNode(cmd, index)

	case CmdCreateWorkload:
		return st.createWorkload(cmd, index)
	case CmdBindWorkload:
		return st.bindWorkload(cmd, index)
	case CmdUpdateWorkloadStatus:
		return st.updateWorkloadStatus(cmd, index)
	case CmdDeleteWorkload:
		return st.deleteWorkload(cmd, index)
	case CmdPurgeWorkload:
		return st.purgeWorkload(cmd, index)

	case CmdCreateDeployment:
		return st.createDeployment(cmd, index)
	case CmdUpdateDeploymentSpec:
		return st.updateDeploymentSpec(cmd, index)
	case CmdUpdateDeploymentStatus:
		return st.updateDeploymentStatus(cmd, index)
	case CmdRollbackDeployment:
		return st.rollbackDeployment(cmd, index)
	case CmdDeleteDeployment:
		return st.deleteDeployment(cmd, index)
	case CmdPurgeDeployment:
		if st.purgeDeploymentLocked(cmd.Name, index) {
			return Result{}
		}
		return Result{Err: fmt.Errorf("%w: deployment %s still owns workloads", ErrInvalidState, cmd.Name)}

	case CmdCreateService:
		return st.createService(cmd, index)
	case CmdUpdateServiceSpec:
		return st.updateServiceSpec(cmd, index)
	case CmdUpdateServiceEndpoint:
		return st.updateServiceEndpoints(cmd, index)
	case CmdDeleteService:
		return st.deleteService(cmd, index)

	case CmdRecordEvent:
		st.recordEvent(cmd, index)
		return Result{}

	default:
		return Result{Err: fmt.Errorf("store: unknown command kind %q", cmd.Kind)}
	}
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (st *Store) registerNode(cmd *Command, index uint64) Result {
	if cmd.Node == nil {
		return Result{Err: fmt.Errorf("store: RegisterNode requires a node")}
	}
	in := cmd.Node
	existing, ok := st.s.Nodes[in.Name]

	if !ok {
		n := in.DeepCopy()
		n.UID = makeUID(in.Name, index)
		n.Revision = index
		n.Generation = 1
		n.CreatedAt = cmd.Timestamp
		n.UpdatedAt = cmd.Timestamp
		n.Status.Phase = v1.NodeReady
		n.Status.LastHeartbeat = cmd.Timestamp
		n.Status.Allocated = v1.Resources{}
		st.s.Nodes[n.Name] = n
		st.emit(cmd, index, v1.SeverityInfo, "NodeRegistered", "Node", n.Name,
			fmt.Sprintf("node registered with %s", n.Status.Allocatable))
		st.notify(index, "Node", "Created", n.Name, n)
		return Result{Node: n.DeepCopy()}
	}

	// Re-registration. An agent restart is legitimate and common: the node
	// keeps its identity and its workload bindings, but capacity and runtime
	// details are refreshed and the phase returns to Ready.
	n := existing
	n.Spec.Address = in.Spec.Address
	n.Labels = in.Labels
	n.Status.Capacity = in.Status.Capacity
	n.Status.Allocatable = in.Status.Allocatable
	n.Status.Runtime = in.Status.Runtime
	n.Status.AgentStartedAt = in.Status.AgentStartedAt
	n.Status.LastHeartbeat = cmd.Timestamp
	n.UpdatedAt = cmd.Timestamp
	n.Revision = index
	if n.Status.Phase != v1.NodeReady && n.Status.Phase.CanTransitionTo(v1.NodeReady) {
		n.Status.Phase = v1.NodeReady
		st.emit(cmd, index, v1.SeverityInfo, "NodeReady", "Node", n.Name, "node re-registered and is ready")
	}
	st.recomputeNodeAllocation(n.Name)
	st.notify(index, "Node", "Updated", n.Name, n)
	return Result{Node: n.DeepCopy()}
}

func (st *Store) updateNodeStatus(cmd *Command, index uint64) Result {
	n, ok := st.s.Nodes[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.UID != "" && n.UID != cmd.UID {
		return Result{Err: ErrUIDMismatch}
	}
	if cmd.NodeStatus == nil {
		return Result{Err: fmt.Errorf("store: UpdateNodeStatus requires a status")}
	}
	in := cmd.NodeStatus

	// The agent owns measurement; the control plane owns phase and accounting.
	// Accepting a phase from the agent would let a node declare itself Ready
	// while the control plane has already evicted its workloads.
	n.Status.Usage = in.Usage
	n.Status.Capacity = in.Capacity
	n.Status.Allocatable = in.Allocatable
	n.Status.Runtime = in.Runtime
	n.Status.LastHeartbeat = cmd.Timestamp
	n.Status.Conditions = in.Conditions
	n.UpdatedAt = cmd.Timestamp
	n.Revision = index

	if n.Status.Phase == v1.NodeUnreachable || n.Status.Phase == v1.NodeRegistering {
		if n.Status.Phase.CanTransitionTo(v1.NodeReady) {
			prev := n.Status.Phase
			n.Status.Phase = v1.NodeReady
			st.emit(cmd, index, v1.SeverityInfo, "NodeReady", "Node", n.Name,
				fmt.Sprintf("node recovered from %s", prev))
		}
	}
	st.recomputeNodeAllocation(n.Name)
	st.notify(index, "Node", "Updated", n.Name, n)
	return Result{Node: n.DeepCopy()}
}

func (st *Store) setNodePhase(cmd *Command, index uint64) Result {
	n, ok := st.s.Nodes[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if !n.Status.Phase.CanTransitionTo(cmd.Phase) {
		return Result{Err: fmt.Errorf("%w: node %s: %s -> %s",
			ErrInvalidState, n.Name, n.Status.Phase, cmd.Phase)}
	}
	if n.Status.Phase == cmd.Phase {
		return Result{Node: n.DeepCopy()}
	}
	prev := n.Status.Phase
	n.Status.Phase = cmd.Phase
	n.UpdatedAt = cmd.Timestamp
	n.Revision = index

	// Losing contact with a node means losing the ability to verify its
	// workloads, so their health becomes Unknown immediately. Continuing to
	// report them Healthy would keep them in service endpoints and would let a
	// deployment claim replicas it can no longer see. Health is downgraded
	// rather than the phase changed: the containers may well still be running,
	// and only the eviction path is allowed to declare them gone.
	if cmd.Phase == v1.NodeUnreachable {
		for name := range st.workloadsByNode[n.Name] {
			w, ok := st.s.Workloads[name]
			if !ok || !w.Status.Phase.Active() || w.Status.Health == v1.HealthUnknown {
				continue
			}
			w.Status.Health = v1.HealthUnknown
			w.Status.Message = "health cannot be verified: node " + n.Name + " is unreachable"
			w.Revision = index
			w.UpdatedAt = cmd.Timestamp
			st.notify(index, "Workload", "Updated", w.Name, w)
		}
		// Endpoints carry a snapshot of a workload's readiness, so downgrading
		// health alone would leave services advertising backends on a dead node
		// until the endpoint controller next ran — a window in which the proxy
		// would keep sending traffic there. Withdrawing them in the same
		// command closes it.
		st.withdrawEndpointsOnNode(n.Name, index, cmd.Timestamp)
	}

	severity := v1.SeverityInfo
	if cmd.Phase == v1.NodeUnreachable || cmd.Phase == v1.NodeNotReady {
		severity = v1.SeverityWarning
	}
	st.emit(cmd, index, severity, "NodePhaseChanged", "Node", n.Name,
		fmt.Sprintf("%s -> %s: %s", prev, cmd.Phase, cmd.Reason))
	st.notify(index, "Node", "Updated", n.Name, n)
	return Result{Node: n.DeepCopy()}
}

func (st *Store) cordonNode(cmd *Command, index uint64) Result {
	n, ok := st.s.Nodes[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.Unschedulable == nil {
		return Result{Err: fmt.Errorf("store: CordonNode requires unschedulable")}
	}
	n.Spec.Unschedulable = *cmd.Unschedulable
	n.UpdatedAt = cmd.Timestamp
	n.Revision = index
	verb := "uncordoned"
	if *cmd.Unschedulable {
		verb = "cordoned"
	}
	st.emit(cmd, index, v1.SeverityInfo, "NodeCordonChanged", "Node", n.Name, "node "+verb)
	st.notify(index, "Node", "Updated", n.Name, n)
	return Result{Node: n.DeepCopy()}
}

func (st *Store) deleteNode(cmd *Command, index uint64) Result {
	n, ok := st.s.Nodes[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	// Workloads bound to a removed node must not linger as phantoms: mark them
	// terminated so the deployment controller replaces them.
	for name := range st.workloadsByNode[n.Name] {
		if w, ok := st.s.Workloads[name]; ok && !w.Status.Phase.Terminal() {
			w.Status.Phase = v1.WorkloadTerminated
			w.Status.Reason = "NodeRemoved"
			w.Status.LastTransition = cmd.Timestamp
			w.Revision = index
			st.notify(index, "Workload", "Updated", w.Name, w)
		}
	}
	delete(st.s.Nodes, n.Name)
	delete(st.workloadsByNode, n.Name)
	st.emit(cmd, index, v1.SeverityWarning, "NodeDeleted", "Node", n.Name, "node removed from the cluster")
	st.notify(index, "Node", "Deleted", n.Name, n)
	return Result{}
}

// recomputeNodeAllocation derives allocated resources from the workloads bound
// to a node. It is recomputed rather than incremented so accounting cannot
// drift after a missed or duplicated update.
func (st *Store) recomputeNodeAllocation(nodeName string) {
	n, ok := st.s.Nodes[nodeName]
	if !ok {
		return
	}
	var total v1.Resources
	count := 0
	for name := range st.workloadsByNode[nodeName] {
		w, ok := st.s.Workloads[name]
		if !ok || !w.Status.Phase.Active() {
			continue
		}
		total = total.Add(w.Spec.Resources.Request)
		count++
	}
	n.Status.Allocated = total
	n.Status.WorkloadCount = count
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

func (st *Store) createWorkload(cmd *Command, index uint64) Result {
	if cmd.Workload == nil {
		return Result{Err: fmt.Errorf("store: CreateWorkload requires a workload")}
	}
	if _, exists := st.s.Workloads[cmd.Workload.Name]; exists {
		return Result{Err: ErrAlreadyExists}
	}
	w := cmd.Workload.DeepCopy()
	w.UID = makeUID(w.Name, index)
	w.Revision = index
	w.Generation = 1
	w.CreatedAt = cmd.Timestamp
	w.UpdatedAt = cmd.Timestamp
	w.Status = v1.WorkloadStatus{
		Phase:          v1.WorkloadPending,
		Health:         v1.HealthUnknown,
		LastTransition: cmd.Timestamp,
	}
	st.s.Workloads[w.Name] = w
	st.emit(cmd, index, v1.SeverityInfo, "WorkloadCreated", "Workload", w.Name,
		fmt.Sprintf("workload accepted (%s, %s)", w.Spec.Image, w.Spec.Resources.Request))
	st.notify(index, "Workload", "Created", w.Name, w)
	return Result{Workload: w.DeepCopy()}
}

// bindWorkload is the scheduler's exclusive write. It is the only path that may
// set NodeName, and it refuses to rebind an already-placed workload: moving a
// workload between nodes is expressed as "terminate and create a replacement",
// which is what makes double-scheduling detectable.
func (st *Store) bindWorkload(cmd *Command, index uint64) Result {
	w, ok := st.s.Workloads[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.UID != "" && w.UID != cmd.UID {
		return Result{Err: ErrUIDMismatch}
	}
	if w.Status.Phase != v1.WorkloadPending {
		return Result{Err: fmt.Errorf("%w: workload %s is %s, only Pending workloads may be bound",
			ErrInvalidState, w.Name, w.Status.Phase)}
	}
	if w.Status.NodeName != "" {
		return Result{Err: fmt.Errorf("%w: workload %s is already bound to %s",
			ErrInvalidState, w.Name, w.Status.NodeName)}
	}
	node, ok := st.s.Nodes[cmd.Placement.NodeName]
	if !ok {
		return Result{Err: fmt.Errorf("%w: target node %s", ErrNotFound, cmd.Placement.NodeName)}
	}
	// Re-validate capacity at apply time. The scheduler decided against a
	// snapshot; between deciding and committing, another binding may have
	// consumed the space. Checking here is what prevents overcommit.
	if !w.Spec.Resources.Request.Fits(node.Available()) {
		return Result{Err: fmt.Errorf("%w: node %s no longer has capacity (available %s, need %s)",
			ErrConflict, node.Name, node.Available(), w.Spec.Resources.Request)}
	}

	w.Status.NodeName = node.Name
	w.Status.Phase = v1.WorkloadScheduled
	w.Status.LastTransition = cmd.Timestamp
	w.Status.Placement = cmd.Placement
	w.UpdatedAt = cmd.Timestamp
	w.Revision = index

	st.indexWorkloadOnNode(node.Name, w.Name)
	st.recomputeNodeAllocation(node.Name)

	st.emit(cmd, index, v1.SeverityInfo, "WorkloadScheduled", "Workload", w.Name, cmd.Placement.Reason)
	st.notify(index, "Workload", "Updated", w.Name, w)
	st.notify(index, "Node", "Updated", node.Name, node)
	return Result{Workload: w.DeepCopy()}
}

func (st *Store) updateWorkloadStatus(cmd *Command, index uint64) Result {
	w, ok := st.s.Workloads[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.UID != "" && w.UID != cmd.UID {
		return Result{Err: ErrUIDMismatch}
	}
	if cmd.WorkloadStatus == nil {
		return Result{Err: fmt.Errorf("store: UpdateWorkloadStatus requires a status")}
	}
	in := cmd.WorkloadStatus

	if !w.Status.Phase.CanTransitionTo(in.Phase) {
		// A late report from an agent that has not seen a newer decision. It is
		// rejected rather than applied, so a stale message can never resurrect
		// a terminated workload.
		return Result{Err: fmt.Errorf("%w: workload %s: %s -> %s",
			ErrInvalidState, w.Name, w.Status.Phase, in.Phase)}
	}

	prevPhase := w.Status.Phase
	prevHealth := w.Status.Health

	if in.Phase != w.Status.Phase {
		w.Status.Phase = in.Phase
		w.Status.LastTransition = cmd.Timestamp
	}
	w.Status.Health = in.Health
	w.Status.ContainerID = in.ContainerID
	w.Status.Message = in.Message
	w.Status.Reason = in.Reason
	w.Status.Usage = in.Usage
	w.Status.HostPorts = in.HostPorts
	// Restart count only ever increases; a lower value means a stale report.
	if in.RestartCount > w.Status.RestartCount {
		w.Status.RestartCount = in.RestartCount
	}
	if in.ExitCode != nil {
		w.Status.ExitCode = in.ExitCode
	}
	if in.StartedAt != nil {
		w.Status.StartedAt = in.StartedAt
	}
	if in.FinishedAt != nil {
		w.Status.FinishedAt = in.FinishedAt
	}
	// The scheduler records why an unplaced workload could not be placed. It is
	// only accepted while the workload is unbound; once bound, the placement
	// decision is immutable history and no status report may rewrite it.
	if in.Placement != nil && w.Status.NodeName == "" {
		w.Status.Placement = in.Placement
	}
	w.UpdatedAt = cmd.Timestamp
	w.Revision = index

	if prevPhase != w.Status.Phase {
		severity := v1.SeverityInfo
		if w.Status.Phase == v1.WorkloadFailed {
			severity = v1.SeverityWarning
		}
		st.emit(cmd, index, severity, "WorkloadPhaseChanged", "Workload", w.Name,
			fmt.Sprintf("%s -> %s%s", prevPhase, w.Status.Phase, suffixIf(": ", w.Status.Reason)))
	}
	if prevHealth != w.Status.Health && w.Status.Health == v1.HealthUnhealthy {
		st.emit(cmd, index, v1.SeverityWarning, "WorkloadUnhealthy", "Workload", w.Name,
			suffixIf("", w.Status.Message))
	}

	if w.Status.NodeName != "" {
		st.recomputeNodeAllocation(w.Status.NodeName)
	}
	st.notify(index, "Workload", "Updated", w.Name, w)
	return Result{Workload: w.DeepCopy()}
}

func (st *Store) deleteWorkload(cmd *Command, index uint64) Result {
	w, ok := st.s.Workloads[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.ExpectedRevision != 0 && w.Revision != cmd.ExpectedRevision {
		return Result{Err: ErrConflict}
	}
	if w.DeletedAt != nil {
		// Deletion is idempotent by nature; a repeat is a no-op, not an error.
		return Result{Workload: w.DeepCopy()}
	}
	ts := cmd.Timestamp
	w.DeletedAt = &ts
	w.UpdatedAt = ts
	w.Revision = index

	// An unplaced workload has no container to stop, so it terminates at once.
	if w.Status.Phase == v1.WorkloadPending {
		w.Status.Phase = v1.WorkloadTerminated
	} else if w.Status.Phase.CanTransitionTo(v1.WorkloadTerminating) {
		w.Status.Phase = v1.WorkloadTerminating
	}
	w.Status.Reason = orDefault(cmd.Reason, "Deleted")
	w.Status.LastTransition = ts

	if w.Status.NodeName != "" {
		st.recomputeNodeAllocation(w.Status.NodeName)
	}
	st.emit(cmd, index, v1.SeverityInfo, "WorkloadDeleting", "Workload", w.Name, w.Status.Reason)
	st.notify(index, "Workload", "Updated", w.Name, w)
	return Result{Workload: w.DeepCopy()}
}

// purgeWorkload removes a terminated workload's record once the agent has
// confirmed the container is gone.
func (st *Store) purgeWorkload(cmd *Command, index uint64) Result {
	w, ok := st.s.Workloads[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.UID != "" && w.UID != cmd.UID {
		return Result{Err: ErrUIDMismatch}
	}
	if w.Status.Phase.Active() {
		return Result{Err: fmt.Errorf("%w: workload %s is still %s", ErrInvalidState, w.Name, w.Status.Phase)}
	}
	delete(st.s.Workloads, w.Name)
	if node := w.Status.NodeName; node != "" {
		if set, ok := st.workloadsByNode[node]; ok {
			delete(set, w.Name)
		}
		st.recomputeNodeAllocation(node)
	}
	st.notify(index, "Workload", "Deleted", w.Name, w)
	return Result{}
}

func (st *Store) indexWorkloadOnNode(node, workload string) {
	set, ok := st.workloadsByNode[node]
	if !ok {
		set = map[string]struct{}{}
		st.workloadsByNode[node] = set
	}
	set[workload] = struct{}{}
}

// ---------------------------------------------------------------------------
// Deployments
// ---------------------------------------------------------------------------

func (st *Store) createDeployment(cmd *Command, index uint64) Result {
	if cmd.Deployment == nil {
		return Result{Err: fmt.Errorf("store: CreateDeployment requires a deployment")}
	}
	if _, exists := st.s.Deployments[cmd.Deployment.Name]; exists {
		return Result{Err: ErrAlreadyExists}
	}
	d := cmd.Deployment.DeepCopy()
	d.UID = makeUID(d.Name, index)
	d.Revision = index
	d.Generation = 1
	d.CreatedAt = cmd.Timestamp
	d.UpdatedAt = cmd.Timestamp
	d.Status = v1.DeploymentStatus{
		Phase:           v1.DeploymentProgressing,
		Revision:        1,
		DesiredReplicas: d.Spec.Replicas,
		LastTransition:  cmd.Timestamp,
	}
	st.s.Deployments[d.Name] = d
	st.appendRevision(d, cmd.Timestamp)
	st.emit(cmd, index, v1.SeverityInfo, "DeploymentCreated", "Deployment", d.Name,
		fmt.Sprintf("%d replicas of %s", d.Spec.Replicas, d.Spec.Template.Image))
	st.notify(index, "Deployment", "Created", d.Name, d)
	return Result{Deployment: d.DeepCopy()}
}

func (st *Store) updateDeploymentSpec(cmd *Command, index uint64) Result {
	d, ok := st.s.Deployments[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.ExpectedRevision != 0 && d.Revision != cmd.ExpectedRevision {
		return Result{Err: ErrConflict}
	}
	if cmd.Deployment == nil {
		return Result{Err: fmt.Errorf("store: UpdateDeploymentSpec requires a deployment")}
	}
	oldHash := v1.HashWorkloadSpec(&d.Spec.Template)
	newHash := v1.HashWorkloadSpec(&cmd.Deployment.Spec.Template)

	d.Spec = cmd.Deployment.Spec
	d.Spec.Template = cmd.Deployment.Spec.Template.DeepCopy()
	if cmd.Deployment.Labels != nil {
		d.Labels = cmd.Deployment.Labels
	}
	d.Generation++
	d.UpdatedAt = cmd.Timestamp
	d.Revision = index
	d.Status.DesiredReplicas = d.Spec.Replicas

	if oldHash != newHash {
		// A template change starts a new rollout and is recorded so it can be
		// rolled back to a spec that actually existed, not a reconstruction.
		d.Status.Revision++
		d.Status.Phase = v1.DeploymentProgressing
		d.Status.LastTransition = cmd.Timestamp
		st.appendRevision(d, cmd.Timestamp)
		st.emit(cmd, index, v1.SeverityInfo, "RolloutStarted", "Deployment", d.Name,
			fmt.Sprintf("revision %d: %s", d.Status.Revision, d.Spec.Template.Image))
	} else {
		st.emit(cmd, index, v1.SeverityInfo, "DeploymentScaled", "Deployment", d.Name,
			fmt.Sprintf("desired replicas set to %d", d.Spec.Replicas))
	}
	st.notify(index, "Deployment", "Updated", d.Name, d)
	return Result{Deployment: d.DeepCopy()}
}

func (st *Store) updateDeploymentStatus(cmd *Command, index uint64) Result {
	d, ok := st.s.Deployments[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.DeployStatus == nil {
		return Result{Err: fmt.Errorf("store: UpdateDeploymentStatus requires a status")}
	}
	prev := d.Status.Phase
	in := *cmd.DeployStatus
	// The controller reports counts and phase; revision history stays owned by
	// the spec path.
	in.Revision = d.Status.Revision
	if in.Phase != prev {
		in.LastTransition = cmd.Timestamp
	} else {
		in.LastTransition = d.Status.LastTransition
	}
	d.Status = in
	d.Revision = index
	d.UpdatedAt = cmd.Timestamp

	if prev != d.Status.Phase {
		severity := v1.SeverityInfo
		if d.Status.Phase == v1.DeploymentDegraded {
			severity = v1.SeverityWarning
		}
		st.emit(cmd, index, severity, "DeploymentPhaseChanged", "Deployment", d.Name,
			fmt.Sprintf("%s -> %s (%d/%d available)", prev, d.Status.Phase,
				d.Status.AvailableReplicas, d.Status.DesiredReplicas))
	}
	st.notify(index, "Deployment", "Updated", d.Name, d)
	return Result{Deployment: d.DeepCopy()}
}

func (st *Store) rollbackDeployment(cmd *Command, index uint64) Result {
	d, ok := st.s.Deployments[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	history := st.s.Revisions[d.Name]
	if len(history) == 0 {
		return Result{Err: fmt.Errorf("%w: no revision history for %s", ErrNotFound, d.Name)}
	}
	target := cmd.TargetRevision
	if target == 0 {
		// Default to the previous revision.
		if len(history) < 2 {
			return Result{Err: fmt.Errorf("%w: %s has no previous revision", ErrNotFound, d.Name)}
		}
		target = history[len(history)-2].Revision
	}
	var rev *v1.DeploymentRevision
	for _, r := range history {
		if r.Revision == target {
			rev = r
			break
		}
	}
	if rev == nil {
		return Result{Err: fmt.Errorf("%w: revision %d of %s", ErrNotFound, target, d.Name)}
	}
	if v1.HashWorkloadSpec(&d.Spec.Template) == rev.TemplateHash {
		return Result{Err: fmt.Errorf("%w: deployment %s already runs revision %d",
			ErrInvalidState, d.Name, target)}
	}

	d.Spec.Template = rev.Template.DeepCopy()
	d.Generation++
	d.UpdatedAt = cmd.Timestamp
	d.Revision = index
	d.Status.Revision++
	d.Status.Phase = v1.DeploymentProgressing
	d.Status.LastTransition = cmd.Timestamp
	st.appendRevision(d, cmd.Timestamp)

	st.emit(cmd, index, v1.SeverityWarning, "RollbackStarted", "Deployment", d.Name,
		fmt.Sprintf("rolling back to revision %d (%s)", target, rev.Template.Image))
	st.notify(index, "Deployment", "Updated", d.Name, d)
	return Result{Deployment: d.DeepCopy()}
}

func (st *Store) appendRevision(d *v1.Deployment, ts time.Time) {
	rev := &v1.DeploymentRevision{
		Deployment:   d.Name,
		Revision:     d.Status.Revision,
		Template:     d.Spec.Template.DeepCopy(),
		Replicas:     d.Spec.Replicas,
		CreatedAt:    ts,
		TemplateHash: v1.HashWorkloadSpec(&d.Spec.Template),
	}
	history := append(st.s.Revisions[d.Name], rev)
	if len(history) > maxRevisionHistory {
		history = append([]*v1.DeploymentRevision(nil), history[len(history)-maxRevisionHistory:]...)
	}
	st.s.Revisions[d.Name] = history
}

func (st *Store) deleteDeployment(cmd *Command, index uint64) Result {
	d, ok := st.s.Deployments[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	// Optimistic concurrency: a caller that read the deployment, showed it to a
	// human and then acted must not delete a version it never saw.
	if cmd.ExpectedRevision != 0 && d.Revision != cmd.ExpectedRevision {
		return Result{Err: ErrConflict}
	}
	if d.DeletedAt != nil {
		return Result{Deployment: d.DeepCopy()}
	}
	ts := cmd.Timestamp
	d.DeletedAt = &ts
	d.UpdatedAt = ts
	d.Revision = index

	// Cascade: owned workloads enter graceful termination. They are not removed
	// here — the agent must confirm the containers are gone first.
	for _, w := range st.s.Workloads {
		if w.OwnerRef == nil || w.OwnerRef.Kind != "Deployment" || w.OwnerRef.UID != d.UID {
			continue
		}
		if w.DeletedAt != nil {
			continue
		}
		wts := ts
		w.DeletedAt = &wts
		if w.Status.Phase == v1.WorkloadPending {
			w.Status.Phase = v1.WorkloadTerminated
		} else if w.Status.Phase.CanTransitionTo(v1.WorkloadTerminating) {
			w.Status.Phase = v1.WorkloadTerminating
		}
		w.Status.Reason = "OwnerDeleted"
		w.Status.LastTransition = ts
		w.Revision = index
		if w.Status.NodeName != "" {
			st.recomputeNodeAllocation(w.Status.NodeName)
		}
		st.notify(index, "Workload", "Updated", w.Name, w)
	}
	st.emit(cmd, index, v1.SeverityWarning, "DeploymentDeleting", "Deployment", d.Name,
		"deployment deleted, terminating owned workloads")
	st.notify(index, "Deployment", "Updated", d.Name, d)
	return Result{Deployment: d.DeepCopy()}
}

// purgeDeploymentLocked removes a deployment record once no owned workloads
// remain. It is deliberately conservative: a deployment whose workloads are
// still terminating stays visible so operators can watch the teardown.
func (st *Store) purgeDeploymentLocked(name string, index uint64) bool {
	d, ok := st.s.Deployments[name]
	if !ok || d.DeletedAt == nil {
		return false
	}
	for _, w := range st.s.Workloads {
		if w.OwnerRef != nil && w.OwnerRef.UID == d.UID {
			return false
		}
	}
	delete(st.s.Deployments, name)
	delete(st.s.Revisions, name)
	st.notify(index, "Deployment", "Deleted", name, d)
	return true
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

func (st *Store) createService(cmd *Command, index uint64) Result {
	if cmd.Service == nil {
		return Result{Err: fmt.Errorf("store: CreateService requires a service")}
	}
	if _, exists := st.s.Services[cmd.Service.Name]; exists {
		return Result{Err: ErrAlreadyExists}
	}
	s := cmd.Service.DeepCopy()
	s.UID = makeUID(s.Name, index)
	s.Revision = index
	s.Generation = 1
	s.CreatedAt = cmd.Timestamp
	s.UpdatedAt = cmd.Timestamp
	s.Status = v1.ServiceStatus{LastTransition: cmd.Timestamp}
	st.s.Services[s.Name] = s
	st.emit(cmd, index, v1.SeverityInfo, "ServiceCreated", "Service", s.Name,
		fmt.Sprintf("port %d -> %d, selector %v", s.Spec.Port, s.Spec.TargetPort, s.Spec.Selector))
	st.notify(index, "Service", "Created", s.Name, s)
	return Result{Service: s.DeepCopy()}
}

func (st *Store) updateServiceSpec(cmd *Command, index uint64) Result {
	s, ok := st.s.Services[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.ExpectedRevision != 0 && s.Revision != cmd.ExpectedRevision {
		return Result{Err: ErrConflict}
	}
	if cmd.Service == nil {
		return Result{Err: fmt.Errorf("store: UpdateServiceSpec requires a service")}
	}
	s.Spec = cmd.Service.Spec
	s.Spec.Selector = cmd.Service.DeepCopy().Spec.Selector
	s.Generation++
	s.UpdatedAt = cmd.Timestamp
	s.Revision = index
	st.notify(index, "Service", "Updated", s.Name, s)
	return Result{Service: s.DeepCopy()}
}

func (st *Store) updateServiceEndpoints(cmd *Command, index uint64) Result {
	s, ok := st.s.Services[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	// Endpoints are ordered deterministically so every replica stores the same
	// bytes and the console does not see phantom churn.
	eps := append([]v1.Endpoint(nil), cmd.Endpoints...)
	sort.Slice(eps, func(i, j int) bool { return eps[i].WorkloadName < eps[j].WorkloadName })

	prevHealthy := s.Status.HealthyEndpoints
	healthy := 0
	for _, e := range eps {
		if e.Ready {
			healthy++
		}
	}
	if endpointsEqual(s.Status.Endpoints, eps) {
		return Result{Service: s.DeepCopy()}
	}
	s.Status.Endpoints = eps
	s.Status.TotalEndpoints = len(eps)
	s.Status.HealthyEndpoints = healthy
	s.Status.ObservedRevision = index
	s.Status.LastTransition = cmd.Timestamp
	s.Revision = index
	s.UpdatedAt = cmd.Timestamp

	if healthy == 0 && prevHealthy > 0 {
		st.emit(cmd, index, v1.SeverityCritical, "ServiceHasNoEndpoints", "Service", s.Name,
			"every endpoint is unhealthy; the service is not serving traffic")
	} else if prevHealthy == 0 && healthy > 0 {
		st.emit(cmd, index, v1.SeverityInfo, "ServiceEndpointsRestored", "Service", s.Name,
			fmt.Sprintf("%d healthy endpoints", healthy))
	}
	st.notify(index, "Service", "Updated", s.Name, s)
	return Result{Service: s.DeepCopy()}
}

func (st *Store) deleteService(cmd *Command, index uint64) Result {
	s, ok := st.s.Services[cmd.Name]
	if !ok {
		return Result{Err: ErrNotFound}
	}
	if cmd.ExpectedRevision != 0 && s.Revision != cmd.ExpectedRevision {
		return Result{Err: ErrConflict}
	}
	delete(st.s.Services, cmd.Name)
	st.emit(cmd, index, v1.SeverityWarning, "ServiceDeleted", "Service", s.Name, "service removed")
	st.notify(index, "Service", "Deleted", s.Name, s)
	return Result{}
}

// withdrawEndpointsOnNode marks every endpoint backed by a workload on the
// given node as not ready. The endpoint controller recomputes the list on its
// next pass; this is the immediate, atomic withdrawal.
func (st *Store) withdrawEndpointsOnNode(node string, index uint64, ts time.Time) {
	for _, svc := range st.s.Services {
		changed := false
		for i := range svc.Status.Endpoints {
			e := &svc.Status.Endpoints[i]
			if e.NodeName != node || (!e.Ready && e.Health == v1.HealthUnknown) {
				continue
			}
			e.Ready = false
			e.Health = v1.HealthUnknown
			changed = true
		}
		if !changed {
			continue
		}
		healthy := 0
		for _, e := range svc.Status.Endpoints {
			if e.Ready {
				healthy++
			}
		}
		svc.Status.HealthyEndpoints = healthy
		svc.Status.LastTransition = ts
		svc.Revision = index
		svc.UpdatedAt = ts
		st.notify(index, "Service", "Updated", svc.Name, svc)
	}
}

func endpointsEqual(a, b []v1.Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

func (st *Store) recordEvent(cmd *Command, index uint64) {
	if cmd.Event == nil {
		return
	}
	e := *cmd.Event
	e.Timestamp = cmd.Timestamp
	e.Actor = cmd.Actor
	st.appendEvent(e, index)
}

func (st *Store) emit(cmd *Command, index uint64, sev v1.EventSeverity, reason, kind, name, msg string) {
	st.appendEvent(v1.Event{
		Timestamp: cmd.Timestamp,
		Severity:  sev,
		Source:    orDefault(cmd.Actor, "control-plane"),
		Reason:    reason,
		Kind:      kind,
		Name:      name,
		Message:   msg,
		Actor:     cmd.Actor,
	}, index)
}

func (st *Store) appendEvent(e v1.Event, index uint64) {
	e.ID = st.s.NextEventID
	st.s.NextEventID++
	st.s.Events = append(st.s.Events, e)
	if len(st.s.Events) > maxRetainedEvents {
		drop := len(st.s.Events) - maxRetainedEvents
		st.s.Events = append([]v1.Event(nil), st.s.Events[drop:]...)
	}
	st.watchers.queue(Change{Revision: index, Kind: "Event", Op: "Created", Name: e.Reason, Object: e})
}

func (st *Store) notify(rev uint64, kind, op, name string, obj any) {
	st.watchers.queue(Change{Revision: rev, Kind: kind, Op: op, Name: name, Object: deepCopyAny(obj)})
}

func deepCopyAny(obj any) any {
	switch o := obj.(type) {
	case *v1.Node:
		return o.DeepCopy()
	case *v1.Workload:
		return o.DeepCopy()
	case *v1.Deployment:
		return o.DeepCopy()
	case *v1.Service:
		return o.DeepCopy()
	default:
		return obj
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeUID derives a unique identifier from the object name and the Raft index
// it was created at. Deterministic across replicas, and never reused because
// the index is monotonic.
func makeUID(name string, index uint64) string {
	return fmt.Sprintf("%s-%08x", name, index)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func suffixIf(sep, v string) string {
	if v == "" {
		return ""
	}
	return sep + v
}
