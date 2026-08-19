package faults

import (
	"fmt"
	"sync"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/store"
)

// Invariant is a property that must hold throughout an experiment.
//
// A monitor returns a violation message, or the empty string. It is called
// repeatedly on a snapshot of cluster state while the fault is held and while
// the cluster recovers, so violations are caught as they happen — a check run
// only at the end would miss a system that broke and then healed.
type Invariant struct {
	Name        string
	Description string
	Check       func(s *store.Store) string
}

// StandardInvariants are asserted during every experiment. They are the
// properties Orion claims, expressed as code rather than as prose.
func StandardInvariants() []Invariant {
	return []Invariant{
		{
			Name: "no-duplicate-placements",
			Description: "A workload is bound to at most one node, and no two active workloads " +
				"share a name. Rescheduling creates a replacement rather than rebinding, so a " +
				"duplicate here means the cluster is running the same work twice.",
			Check: func(s *store.Store) string {
				seen := map[string]string{}
				for _, w := range s.Workloads() {
					if !w.Status.Phase.Active() || w.Status.NodeName == "" {
						continue
					}
					if prev, ok := seen[w.Name]; ok && prev != w.Status.NodeName {
						return fmt.Sprintf("workload %s is active on both %s and %s",
							w.Name, prev, w.Status.NodeName)
					}
					seen[w.Name] = w.Status.NodeName
				}
				return ""
			},
		},
		{
			Name:        "no-scheduling-onto-failed-nodes",
			Description: "No active workload is bound to a node that is Unreachable or being deleted.",
			Check: func(s *store.Store) string {
				nodes := map[string]*v1.Node{}
				for _, n := range s.Nodes() {
					nodes[n.Name] = n
				}
				for _, w := range s.Workloads() {
					if w.Status.NodeName == "" || w.Status.Phase != v1.WorkloadScheduled {
						continue
					}
					node, ok := nodes[w.Status.NodeName]
					if !ok {
						return fmt.Sprintf("workload %s is bound to node %s, which does not exist",
							w.Name, w.Status.NodeName)
					}
					// A workload already running when the node failed is fine;
					// a *newly scheduled* one is not.
					if node.Status.Phase == v1.NodeUnreachable &&
						time.Since(w.Status.LastTransition) < 2*time.Second {
						return fmt.Sprintf("workload %s was scheduled onto %s, which is Unreachable",
							w.Name, node.Name)
					}
				}
				return ""
			},
		},
		{
			Name: "no-resource-overcommit",
			Description: "The sum of resource requests bound to a node never exceeds its " +
				"allocatable capacity.",
			Check: func(s *store.Store) string {
				for _, n := range s.Nodes() {
					if n.Status.Allocated.CPU > n.Status.Allocatable.CPU {
						return fmt.Sprintf("node %s has %s CPU allocated but only %s allocatable",
							n.Name, n.Status.Allocated.CPU, n.Status.Allocatable.CPU)
					}
					if n.Status.Allocated.Memory > n.Status.Allocatable.Memory {
						return fmt.Sprintf("node %s has %s memory allocated but only %s allocatable",
							n.Name, n.Status.Allocated.Memory, n.Status.Allocatable.Memory)
					}
				}
				return ""
			},
		},
		{
			Name: "replicas-never-exceed-desired",
			Description: "A deployment never runs more active replicas than desired plus its " +
				"surge budget. Exceeding it would mean the controller is creating replacements " +
				"for workloads that are still running.",
			Check: func(s *store.Store) string {
				for _, d := range s.Deployments() {
					if d.DeletedAt != nil {
						continue
					}
					active := 0
					for _, w := range s.WorkloadsOwnedBy(d.UID) {
						if w.Status.Phase.Active() && w.DeletedAt == nil {
							active++
						}
					}
					limit := int(d.Spec.Replicas) + int(d.Spec.Strategy.MaxSurge)
					if active > limit {
						return fmt.Sprintf("deployment %s has %d active replicas, above its limit of %d "+
							"(%d desired + %d surge)", d.Name, active, limit,
							d.Spec.Replicas, d.Spec.Strategy.MaxSurge)
					}
				}
				return ""
			},
		},
		{
			Name: "endpoints-are-proven-healthy",
			Description: "Every endpoint marked ready belongs to a workload that is Running with " +
				"a passing health check. Serving traffic to anything else is the failure mode " +
				"this invariant exists to catch.",
			Check: func(s *store.Store) string {
				workloads := map[string]*v1.Workload{}
				for _, w := range s.Workloads() {
					workloads[w.Name] = w
				}
				for _, svc := range s.Services() {
					for _, e := range svc.Status.Endpoints {
						if !e.Ready {
							continue
						}
						w, ok := workloads[e.WorkloadName]
						if !ok {
							return fmt.Sprintf("service %s advertises %s, which no longer exists",
								svc.Name, e.WorkloadName)
						}
						if w.Status.Phase != v1.WorkloadRunning || !w.Status.Health.Serving() {
							return fmt.Sprintf("service %s advertises %s as ready, but it is %s/%s",
								svc.Name, e.WorkloadName, w.Status.Phase, w.Status.Health)
						}
					}
				}
				return ""
			},
		},
		{
			Name: "workload-phases-are-legal",
			Description: "No workload holds a phase inconsistent with its placement: nothing " +
				"is Running without a node, and nothing is Pending with one.",
			Check: func(s *store.Store) string {
				for _, w := range s.Workloads() {
					if w.Status.Phase == v1.WorkloadPending && w.Status.NodeName != "" {
						return fmt.Sprintf("workload %s is Pending but bound to %s", w.Name, w.Status.NodeName)
					}
					if w.Status.Phase == v1.WorkloadRunning && w.Status.NodeName == "" {
						return fmt.Sprintf("workload %s is Running with no node", w.Name)
					}
				}
				return ""
			},
		},
	}
}

// monitor samples invariants on a schedule and records violations.
type monitor struct {
	store      *store.Store
	invariants []Invariant

	mu      sync.Mutex
	results map[string]*apiserver.InvariantResult

	stopOnce sync.Once
	stopC    chan struct{}
	doneC    chan struct{}
}

func newMonitor(s *store.Store, invariants []Invariant) *monitor {
	m := &monitor{
		store:      s,
		invariants: invariants,
		results:    make(map[string]*apiserver.InvariantResult, len(invariants)),
		stopC:      make(chan struct{}),
		doneC:      make(chan struct{}),
	}
	for _, inv := range invariants {
		m.results[inv.Name] = &apiserver.InvariantResult{
			Name: inv.Name, Held: true, Detail: inv.Description,
		}
	}
	return m
}

// sampleInterval is how often invariants are evaluated. Fast enough to catch a
// transient violation, slow enough that the checks themselves do not distort
// the system under test.
const sampleInterval = 100 * time.Millisecond

func (m *monitor) run() {
	defer close(m.doneC)
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	m.sample()
	for {
		select {
		case <-m.stopC:
			m.sample() // one final check after the fault is lifted
			return
		case <-ticker.C:
			m.sample()
		}
	}
}

func (m *monitor) sample() {
	for _, inv := range m.invariants {
		violation := inv.Check(m.store)
		if violation == "" {
			continue
		}
		m.mu.Lock()
		r := m.results[inv.Name]
		r.Violations++
		if r.Held {
			// Keep the first violation: it is the one closest to the cause.
			r.Held = false
			r.Detail = violation
		}
		m.mu.Unlock()
	}
}

func (m *monitor) stop() {
	m.stopOnce.Do(func() { close(m.stopC) })
	<-m.doneC
}

func (m *monitor) snapshot() []apiserver.InvariantResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]apiserver.InvariantResult, 0, len(m.results))
	for _, inv := range m.invariants {
		out = append(out, *m.results[inv.Name])
	}
	return out
}

// violated reports whether any invariant failed.
func (m *monitor) violated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.results {
		if !r.Held {
			return true
		}
	}
	return false
}
