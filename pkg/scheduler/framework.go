// Package scheduler places workloads on nodes.
//
// Scheduling is split into two stages that never blur together:
//
//	Filter  a hard predicate. A node either can run this workload or it cannot.
//	        Failing a filter is always explainable in one sentence.
//	Score   a soft preference over the nodes that survived filtering, in
//	        [0, 100], combined by weight.
//
// Keeping them separate is what makes "why is this workload here?" answerable.
// A single fused cost function can tell you which node won; it cannot tell you
// that the other four were rejected because of a taint, a label and two
// capacity shortfalls.
//
// The scheduler is deterministic: given the same snapshot and the same pending
// workload it always produces the same decision, ties included. That is a
// deliberate constraint — it makes placement reproducible in tests and means a
// scheduler restart does not reshuffle the cluster.
package scheduler

import (
	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
)

// Filter rejects nodes that cannot run a workload.
type Filter interface {
	Name() string
	// Check returns an empty string when the node passes, or a human-readable
	// reason when it does not. The reason is surfaced verbatim to operators.
	Check(w *v1.Workload, n *v1.Node, snap *Snapshot) string
}

// Scorer ranks feasible nodes. Higher is better.
type Scorer interface {
	Name() string
	// Score returns a value in [0, 100].
	Score(w *v1.Workload, n *v1.Node, snap *Snapshot) int32
	// Weight multiplies this scorer's contribution.
	Weight() int32
}

// Snapshot is the cluster view a scheduling cycle runs against. It is taken
// once per cycle so every decision in that cycle sees consistent state, and so
// scorers can look at cluster-wide facts (like how many replicas of a
// deployment already run on a node) without re-querying the store.
type Snapshot struct {
	Nodes []*v1.Node
	// WorkloadsByNode maps node name -> active workloads bound to it.
	WorkloadsByNode map[string][]*v1.Workload
	// reserved tracks resources committed by earlier decisions within this same
	// cycle. Without it, scheduling ten replicas against one snapshot would
	// place all ten on the emptiest node and nine would be rejected at apply
	// time.
	reserved map[string]v1.Resources
}

func NewSnapshot(nodes []*v1.Node, workloadsByNode map[string][]*v1.Workload) *Snapshot {
	return &Snapshot{
		Nodes:           nodes,
		WorkloadsByNode: workloadsByNode,
		reserved:        make(map[string]v1.Resources, len(nodes)),
	}
}

// Available returns a node's free resources, accounting for reservations made
// earlier in this cycle.
func (s *Snapshot) Available(n *v1.Node) v1.Resources {
	return n.Available().Sub(s.reserved[n.Name])
}

// Reserve records that a workload was placed on a node during this cycle.
func (s *Snapshot) Reserve(nodeName string, r v1.Resources) {
	s.reserved[nodeName] = s.reserved[nodeName].Add(r)
}

// WorkloadsOn returns the workloads bound to a node in this snapshot.
func (s *Snapshot) WorkloadsOn(node string) []*v1.Workload { return s.WorkloadsByNode[node] }

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

// ResourceFilter rejects nodes without enough unreserved capacity. This is the
// filter that actually prevents overcommit.
type ResourceFilter struct{}

func (ResourceFilter) Name() string { return "ResourceFit" }

func (ResourceFilter) Check(w *v1.Workload, n *v1.Node, snap *Snapshot) string {
	need := w.Spec.Resources.Request
	free := snap.Available(n)
	switch {
	case need.CPU > free.CPU && need.Memory > free.Memory:
		return "insufficient cpu and memory: needs " + need.String() + ", " + free.String() + " free"
	case need.CPU > free.CPU:
		return "insufficient cpu: needs " + need.CPU.String() + ", " + free.CPU.String() + " free"
	case need.Memory > free.Memory:
		return "insufficient memory: needs " + need.Memory.String() + ", " + free.Memory.String() + " free"
	}
	return ""
}

// NodeReadyFilter rejects nodes that are not accepting work. It duplicates the
// store's SchedulableNodes query on purpose: the snapshot may have aged between
// being taken and being used, and a stale Ready flag must not place work on a
// node the control plane has already given up on.
type NodeReadyFilter struct{}

func (NodeReadyFilter) Name() string { return "NodeReady" }

func (NodeReadyFilter) Check(_ *v1.Workload, n *v1.Node, _ *Snapshot) string {
	if n.DeletedAt != nil {
		return "node is being removed from the cluster"
	}
	if n.Spec.Unschedulable {
		return "node is cordoned"
	}
	if !n.Status.Phase.Schedulable() {
		return "node is " + string(n.Status.Phase)
	}
	return ""
}

// NodeSelectorFilter enforces label requirements.
type NodeSelectorFilter struct{}

func (NodeSelectorFilter) Name() string { return "NodeSelector" }

func (NodeSelectorFilter) Check(w *v1.Workload, n *v1.Node, _ *Snapshot) string {
	if len(w.Spec.NodeSelector) == 0 {
		return ""
	}
	for k, v := range w.Spec.NodeSelector {
		if got, ok := n.Labels[k]; !ok {
			return "node has no label " + k
		} else if got != v {
			return "label " + k + "=" + got + " does not match required " + v
		}
	}
	return ""
}

// TaintFilter enforces NoSchedule taints.
type TaintFilter struct{}

func (TaintFilter) Name() string { return "Taints" }

func (TaintFilter) Check(w *v1.Workload, n *v1.Node, _ *Snapshot) string {
	if v1.ToleratesTaints(w.Spec.Tolerations, n.Spec.Taints) {
		return ""
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == "NoSchedule" {
			return "node is tainted " + t.Key + "=" + t.Value + " (NoSchedule) and the workload does not tolerate it"
		}
	}
	return "node has an untolerated taint"
}

// HostPortFilter rejects nodes where a requested fixed host port is already
// taken. Two containers cannot bind the same host port, and discovering that at
// container-start time would mean a crash loop instead of a clear rejection.
type HostPortFilter struct{}

func (HostPortFilter) Name() string { return "HostPorts" }

func (HostPortFilter) Check(w *v1.Workload, n *v1.Node, snap *Snapshot) string {
	wanted := make(map[int32]bool)
	for _, p := range w.Spec.Ports {
		if p.Host != 0 {
			wanted[p.Host] = true
		}
	}
	if len(wanted) == 0 {
		return ""
	}
	for _, other := range snap.WorkloadsOn(n.Name) {
		if other.Name == w.Name || !other.Status.Phase.Active() {
			continue
		}
		for _, p := range other.Spec.Ports {
			if p.Host != 0 && wanted[p.Host] {
				return "host port " + itoa32(p.Host) + " is already used by workload " + other.Name
			}
		}
		for container, host := range other.Status.HostPorts {
			_ = container
			if wanted[host] {
				return "host port " + itoa32(host) + " is already used by workload " + other.Name
			}
		}
	}
	return ""
}

// DefaultFilters is the filter chain, ordered cheapest-and-most-selective
// first so the common rejection is found without evaluating the rest.
func DefaultFilters() []Filter {
	return []Filter{
		NodeReadyFilter{},
		NodeSelectorFilter{},
		TaintFilter{},
		HostPortFilter{},
		ResourceFilter{},
	}
}

// ---------------------------------------------------------------------------
// Scorers
// ---------------------------------------------------------------------------

// LeastAllocatedScorer prefers nodes with more free capacity, spreading load
// across the cluster. This is the default because an evenly loaded cluster
// degrades gracefully: losing one node costs a predictable fraction of
// capacity rather than an unlucky one.
type LeastAllocatedScorer struct{ W int32 }

func (LeastAllocatedScorer) Name() string { return "LeastAllocated" }
func (s LeastAllocatedScorer) Weight() int32 {
	if s.W == 0 {
		return 1
	}
	return s.W
}

func (LeastAllocatedScorer) Score(w *v1.Workload, n *v1.Node, snap *Snapshot) int32 {
	alloc := n.Status.Allocatable
	if alloc.CPU <= 0 || alloc.Memory <= 0 {
		return 0
	}
	free := snap.Available(n).Sub(w.Spec.Resources.Request)
	cpuScore := ratio(int64(free.CPU), int64(alloc.CPU))
	memScore := ratio(int64(free.Memory), int64(alloc.Memory))
	// CPU and memory weigh equally; a node that is short on either is a poor
	// choice regardless of how much of the other it has.
	return (cpuScore + memScore) / 2
}

// BalancedResourceScorer prefers nodes where CPU and memory utilization end up
// close together. It exists to avoid the classic failure where a node has
// plenty of memory but no schedulable CPU (or vice versa) and becomes dead
// capacity.
type BalancedResourceScorer struct{ W int32 }

func (BalancedResourceScorer) Name() string { return "BalancedResources" }
func (s BalancedResourceScorer) Weight() int32 {
	if s.W == 0 {
		return 1
	}
	return s.W
}

func (BalancedResourceScorer) Score(w *v1.Workload, n *v1.Node, snap *Snapshot) int32 {
	alloc := n.Status.Allocatable
	if alloc.CPU <= 0 || alloc.Memory <= 0 {
		return 0
	}
	used := alloc.Sub(snap.Available(n)).Add(w.Spec.Resources.Request)
	cpuFrac := float64(used.CPU) / float64(alloc.CPU)
	memFrac := float64(used.Memory) / float64(alloc.Memory)
	diff := cpuFrac - memFrac
	if diff < 0 {
		diff = -diff
	}
	return int32((1 - diff) * 100)
}

// SpreadScorer penalizes nodes already running replicas of the same deployment.
// Concentrating a deployment's replicas on one node means a single node failure
// takes the whole service down, which defeats the point of replication.
type SpreadScorer struct{ W int32 }

func (SpreadScorer) Name() string { return "Spread" }
func (s SpreadScorer) Weight() int32 {
	if s.W == 0 {
		return 2 // availability matters more than packing efficiency
	}
	return s.W
}

func (SpreadScorer) Score(w *v1.Workload, n *v1.Node, snap *Snapshot) int32 {
	if w.OwnerRef == nil {
		return 100
	}
	siblings := 0
	for _, other := range snap.WorkloadsOn(n.Name) {
		if other.Name == w.Name || !other.Status.Phase.Active() {
			continue
		}
		if other.OwnerRef != nil && other.OwnerRef.UID == w.OwnerRef.UID {
			siblings++
		}
	}
	// Each sibling costs 25 points; four on one node zeroes the score.
	score := int32(100 - 25*siblings)
	if score < 0 {
		return 0
	}
	return score
}

// DefaultScorers is the scoring chain used by orion-server.
func DefaultScorers() []Scorer {
	return []Scorer{
		SpreadScorer{},
		LeastAllocatedScorer{},
		BalancedResourceScorer{},
	}
}

func ratio(num, den int64) int32 {
	if den <= 0 {
		return 0
	}
	if num < 0 {
		return 0
	}
	v := num * 100 / den
	if v > 100 {
		return 100
	}
	return int32(v)
}

func itoa32(v int32) string {
	if v == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
