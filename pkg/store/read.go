package store

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
)

// Read APIs. Every one of them returns deep copies and sorts by name, so
// callers get a stable ordering they can render or diff without re-sorting.
// Linearizability is the caller's concern: the API server calls
// raft.Node.LinearizableRead before serving a read that must not be stale.

func (st *Store) AppliedIndex() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.s.AppliedIndex
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (st *Store) Node(name string) (*v1.Node, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	n, ok := st.s.Nodes[name]
	if !ok {
		return nil, false
	}
	return n.DeepCopy(), true
}

func (st *Store) Nodes() []*v1.Node {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*v1.Node, 0, len(st.s.Nodes))
	for _, n := range st.s.Nodes {
		out = append(out, n.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SchedulableNodes returns the candidate set for the scheduler, already
// filtered on phase, cordon and deletion.
func (st *Store) SchedulableNodes() []*v1.Node {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*v1.Node, 0, len(st.s.Nodes))
	for _, n := range st.s.Nodes {
		if n.Schedulable() {
			out = append(out, n.DeepCopy())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// StaleNodes returns nodes whose last heartbeat is older than the timeout and
// which are not already Unreachable. This is the input to failure detection.
func (st *Store) StaleNodes(now time.Time, timeout time.Duration) []*v1.Node {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var out []*v1.Node
	for _, n := range st.s.Nodes {
		if n.Status.Phase == v1.NodeUnreachable || n.Status.Phase.Terminal() {
			continue
		}
		if now.Sub(n.Status.LastHeartbeat) > timeout {
			out = append(out, n.DeepCopy())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

func (st *Store) Workload(name string) (*v1.Workload, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	w, ok := st.s.Workloads[name]
	if !ok {
		return nil, false
	}
	return w.DeepCopy(), true
}

func (st *Store) Workloads() []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.workloadsMatching(func(*v1.Workload) bool { return true })
}

// PendingWorkloads returns unplaced workloads in priority order (highest
// first), then by creation time. This ordering is the scheduler's queue: it is
// deterministic, so two schedulers replaying the same state make the same
// choices.
func (st *Store) PendingWorkloads() []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := st.workloadsMatching(func(w *v1.Workload) bool {
		return w.Status.Phase == v1.WorkloadPending && w.DeletedAt == nil
	})
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Spec.Priority != out[j].Spec.Priority {
			return out[i].Spec.Priority > out[j].Spec.Priority
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (st *Store) WorkloadsOnNode(node string) []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	names := st.workloadsByNode[node]
	out := make([]*v1.Workload, 0, len(names))
	for name := range names {
		if w, ok := st.s.Workloads[name]; ok {
			out = append(out, w.DeepCopy())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// AssignedWorkloads returns the workloads a node's agent is responsible for:
// everything bound to it that has not finished terminating. It is the payload
// of the agent's sync response.
func (st *Store) AssignedWorkloads(node string) []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	names := st.workloadsByNode[node]
	out := make([]*v1.Workload, 0, len(names))
	for name := range names {
		w, ok := st.s.Workloads[name]
		if !ok || w.Status.Phase == v1.WorkloadTerminated {
			continue
		}
		out = append(out, w.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (st *Store) WorkloadsOwnedBy(deploymentUID string) []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.workloadsMatching(func(w *v1.Workload) bool {
		return w.OwnerRef != nil && w.OwnerRef.UID == deploymentUID
	})
}

// WorkloadsMatching returns workloads whose labels satisfy a selector.
func (st *Store) WorkloadsMatching(selector map[string]string) []*v1.Workload {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.workloadsMatching(func(w *v1.Workload) bool {
		return v1.SelectorMatches(selector, w.Labels)
	})
}

func (st *Store) workloadsMatching(pred func(*v1.Workload) bool) []*v1.Workload {
	out := make([]*v1.Workload, 0, len(st.s.Workloads))
	for _, w := range st.s.Workloads {
		if pred(w) {
			out = append(out, w.DeepCopy())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ---------------------------------------------------------------------------
// Deployments and services
// ---------------------------------------------------------------------------

func (st *Store) Deployment(name string) (*v1.Deployment, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	d, ok := st.s.Deployments[name]
	if !ok {
		return nil, false
	}
	return d.DeepCopy(), true
}

func (st *Store) Deployments() []*v1.Deployment {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*v1.Deployment, 0, len(st.s.Deployments))
	for _, d := range st.s.Deployments {
		out = append(out, d.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (st *Store) DeploymentRevisions(name string) []*v1.DeploymentRevision {
	st.mu.RLock()
	defer st.mu.RUnlock()
	history := st.s.Revisions[name]
	out := make([]*v1.DeploymentRevision, 0, len(history))
	for _, r := range history {
		out = append(out, r.DeepCopy())
	}
	// Newest first: the console and CLI both show recent rollouts at the top.
	sort.Slice(out, func(i, j int) bool { return out[i].Revision > out[j].Revision })
	return out
}

func (st *Store) Service(name string) (*v1.Service, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	s, ok := st.s.Services[name]
	if !ok {
		return nil, false
	}
	return s.DeepCopy(), true
}

func (st *Store) Services() []*v1.Service {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]*v1.Service, 0, len(st.s.Services))
	for _, s := range st.s.Services {
		out = append(out, s.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// EventQuery filters the operational event log.
type EventQuery struct {
	// Since returns only events with ID greater than this, for incremental
	// polling.
	Since uint64
	// Before returns events with ID less than this, for paging backwards.
	Before   uint64
	Kind     string
	Name     string
	Severity v1.EventSeverity
	Limit    int
}

// Events returns matching events, newest first.
func (st *Store) Events(q EventQuery) []v1.Event {
	st.mu.RLock()
	defer st.mu.RUnlock()

	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out := make([]v1.Event, 0, limit)
	for i := len(st.s.Events) - 1; i >= 0 && len(out) < limit; i-- {
		e := st.s.Events[i]
		if q.Since != 0 && e.ID <= q.Since {
			break // events are ID-ordered; nothing older can match
		}
		if q.Before != 0 && e.ID >= q.Before {
			continue
		}
		if q.Kind != "" && e.Kind != q.Kind {
			continue
		}
		if q.Name != "" && e.Name != q.Name {
			continue
		}
		if q.Severity != "" && e.Severity != q.Severity {
			continue
		}
		out = append(out, e)
	}
	return out
}

// ---------------------------------------------------------------------------
// Aggregates
// ---------------------------------------------------------------------------

// ClusterSummary is the Overview page's data in one read, computed under a
// single lock so the numbers are mutually consistent rather than stitched
// together from several racing requests.
type ClusterSummary struct {
	Nodes struct {
		Total       int `json:"total"`
		Ready       int `json:"ready"`
		NotReady    int `json:"notReady"`
		Unreachable int `json:"unreachable"`
		Cordoned    int `json:"cordoned"`
	} `json:"nodes"`

	Workloads struct {
		Total     int `json:"total"`
		Running   int `json:"running"`
		Pending   int `json:"pending"`
		Starting  int `json:"starting"`
		Failed    int `json:"failed"`
		Succeeded int `json:"succeeded"`
		Unhealthy int `json:"unhealthy"`
		// Unschedulable counts pending workloads the scheduler has already
		// tried and failed to place: the number that actually indicates a
		// capacity problem.
		Unschedulable int `json:"unschedulable"`
		Restarts      int `json:"restarts"`
	} `json:"workloads"`

	Deployments struct {
		Total       int `json:"total"`
		Available   int `json:"available"`
		Progressing int `json:"progressing"`
		Degraded    int `json:"degraded"`
	} `json:"deployments"`

	Services struct {
		Total          int `json:"total"`
		WithoutHealthy int `json:"withoutHealthyEndpoints"`
	} `json:"services"`

	Capacity struct {
		CPUAllocatable v1.MilliCPU `json:"cpuAllocatable"`
		CPUAllocated   v1.MilliCPU `json:"cpuAllocated"`
		CPUUsed        v1.MilliCPU `json:"cpuUsed"`
		MemAllocatable v1.Bytes    `json:"memAllocatable"`
		MemAllocated   v1.Bytes    `json:"memAllocated"`
		MemUsed        v1.Bytes    `json:"memUsed"`
	} `json:"capacity"`

	AppliedIndex uint64 `json:"appliedIndex"`
}

func (st *Store) Summary() ClusterSummary {
	st.mu.RLock()
	defer st.mu.RUnlock()

	var s ClusterSummary
	s.AppliedIndex = st.s.AppliedIndex

	for _, n := range st.s.Nodes {
		s.Nodes.Total++
		switch n.Status.Phase {
		case v1.NodeReady:
			s.Nodes.Ready++
		case v1.NodeNotReady:
			s.Nodes.NotReady++
		case v1.NodeUnreachable:
			s.Nodes.Unreachable++
		}
		if n.Spec.Unschedulable {
			s.Nodes.Cordoned++
		}
		// Only Ready nodes contribute capacity: counting an unreachable node's
		// CPU would make the cluster look healthier than it is.
		if n.Status.Phase == v1.NodeReady {
			s.Capacity.CPUAllocatable += n.Status.Allocatable.CPU
			s.Capacity.MemAllocatable += n.Status.Allocatable.Memory
		}
		s.Capacity.CPUAllocated += n.Status.Allocated.CPU
		s.Capacity.MemAllocated += n.Status.Allocated.Memory
		s.Capacity.CPUUsed += n.Status.Usage.CPU
		s.Capacity.MemUsed += n.Status.Usage.Memory
	}

	for _, w := range st.s.Workloads {
		s.Workloads.Total++
		s.Workloads.Restarts += int(w.Status.RestartCount)
		switch w.Status.Phase {
		case v1.WorkloadRunning:
			s.Workloads.Running++
		case v1.WorkloadPending:
			s.Workloads.Pending++
			if w.Status.Placement != nil && w.Status.Placement.NodeName == "" {
				s.Workloads.Unschedulable++
			}
		case v1.WorkloadStarting, v1.WorkloadScheduled:
			s.Workloads.Starting++
		case v1.WorkloadFailed:
			s.Workloads.Failed++
		case v1.WorkloadSucceeded:
			s.Workloads.Succeeded++
		}
		if w.Status.Phase == v1.WorkloadRunning && w.Status.Health == v1.HealthUnhealthy {
			s.Workloads.Unhealthy++
		}
	}

	for _, d := range st.s.Deployments {
		s.Deployments.Total++
		switch d.Status.Phase {
		case v1.DeploymentAvailable:
			s.Deployments.Available++
		case v1.DeploymentProgressing:
			s.Deployments.Progressing++
		case v1.DeploymentDegraded:
			s.Deployments.Degraded++
		}
	}

	for _, svc := range st.s.Services {
		s.Services.Total++
		if svc.Status.HealthyEndpoints == 0 {
			s.Services.WithoutHealthy++
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

// Snapshot serializes the entire state machine. Called from the Raft goroutine.
func (st *Store) Snapshot() ([]byte, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	b, err := json.Marshal(st.s)
	if err != nil {
		return nil, fmt.Errorf("store: serializing snapshot: %w", err)
	}
	return b, nil
}

// Restore replaces all state from a snapshot and rebuilds derived indexes.
func (st *Store) Restore(data []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if len(data) == 0 {
		st.s = newState()
		st.rebuildIndexes()
		return nil
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("store: restoring snapshot: %w", err)
	}
	// Tolerate snapshots written before a map existed rather than panicking on
	// a nil map at the next write.
	if s.Nodes == nil {
		s.Nodes = map[string]*v1.Node{}
	}
	if s.Workloads == nil {
		s.Workloads = map[string]*v1.Workload{}
	}
	if s.Deployments == nil {
		s.Deployments = map[string]*v1.Deployment{}
	}
	if s.Services == nil {
		s.Services = map[string]*v1.Service{}
	}
	if s.Revisions == nil {
		s.Revisions = map[string][]*v1.DeploymentRevision{}
	}
	if s.Dedup == nil {
		s.Dedup = map[string]dedupRecord{}
	}
	if s.NextEventID == 0 {
		s.NextEventID = 1
	}
	st.s = &s
	st.rebuildIndexes()
	return nil
}

// rebuildIndexes derives every index from primary state. Derived data is never
// serialized, so a snapshot cannot disagree with the objects it contains.
func (st *Store) rebuildIndexes() {
	st.workloadsByNode = map[string]map[string]struct{}{}
	for name, w := range st.s.Workloads {
		if w.Status.NodeName == "" {
			continue
		}
		set, ok := st.workloadsByNode[w.Status.NodeName]
		if !ok {
			set = map[string]struct{}{}
			st.workloadsByNode[w.Status.NodeName] = set
		}
		set[name] = struct{}{}
	}
	for name := range st.s.Nodes {
		st.recomputeNodeAllocation(name)
	}
}
