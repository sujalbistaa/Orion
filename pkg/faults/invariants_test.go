package faults

import (
	"encoding/json"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/store"
)

// snapshot builds a store.Store directly from a state fragment, bypassing the
// Apply path so an invariant violation can be constructed even though the
// store's own command handlers would never produce one. That is the point:
// these checks are a second line of defense, and a test that can only reach
// states the store already forbids would never exercise them.
func snapshot(t *testing.T, fragment map[string]any) *store.Store {
	t.Helper()
	data, err := json.Marshal(fragment)
	if err != nil {
		t.Fatalf("marshal snapshot fragment: %v", err)
	}
	s := store.New()
	if err := s.Restore(data); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	return s
}

func invariant(t *testing.T, name string) Invariant {
	t.Helper()
	for _, inv := range StandardInvariants() {
		if inv.Name == name {
			return inv
		}
	}
	t.Fatalf("no invariant named %q", name)
	return Invariant{}
}

func node(name string, phase v1.NodePhase) *v1.Node {
	return &v1.Node{ObjectMeta: v1.ObjectMeta{Name: name}, Status: v1.NodeStatus{Phase: phase}}
}

func TestNoDuplicatePlacementsCatchesConflict(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, NodeName: "worker-01"}},
			"b": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, NodeName: "worker-02"}},
		},
	})
	if got := invariant(t, "no-duplicate-placements").Check(s); got == "" {
		t.Fatal("expected a violation for a workload active on two nodes")
	}
}

func TestNoDuplicatePlacementsHoldsForDistinctWorkloads(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, NodeName: "worker-01"}},
			"b": {ObjectMeta: v1.ObjectMeta{Name: "web-2"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, NodeName: "worker-02"}},
		},
	})
	if got := invariant(t, "no-duplicate-placements").Check(s); got != "" {
		t.Fatalf("unexpected violation: %s", got)
	}
}

func TestNoSchedulingOntoFailedNodesCatchesFreshBinding(t *testing.T) {
	s := snapshot(t, map[string]any{
		"nodes": map[string]*v1.Node{"worker-02": node("worker-02", v1.NodeUnreachable)},
		"workloads": map[string]*v1.Workload{
			"a": {
				ObjectMeta: v1.ObjectMeta{Name: "web-1"},
				Status: v1.WorkloadStatus{
					Phase: v1.WorkloadScheduled, NodeName: "worker-02", LastTransition: time.Now(),
				},
			},
		},
	})
	if got := invariant(t, "no-scheduling-onto-failed-nodes").Check(s); got == "" {
		t.Fatal("expected a violation for a fresh binding onto an unreachable node")
	}
}

func TestNoSchedulingOntoFailedNodesToleratesPreexistingBinding(t *testing.T) {
	s := snapshot(t, map[string]any{
		"nodes": map[string]*v1.Node{"worker-02": node("worker-02", v1.NodeUnreachable)},
		"workloads": map[string]*v1.Workload{
			"a": {
				ObjectMeta: v1.ObjectMeta{Name: "web-1"},
				Status: v1.WorkloadStatus{
					Phase: v1.WorkloadScheduled, NodeName: "worker-02",
					LastTransition: time.Now().Add(-time.Hour),
				},
			},
		},
	})
	if got := invariant(t, "no-scheduling-onto-failed-nodes").Check(s); got != "" {
		t.Fatalf("unexpected violation for a binding that predates the failure: %s", got)
	}
}

func TestNoResourceOvercommitCatchesExcess(t *testing.T) {
	// Allocated is derived, not stored: rebuildIndexes recomputes it from
	// active workloads bound to the node on every load. So the only way to
	// reach an overcommitted node is to bind more requests than it has
	// allocatable capacity for.
	n := node("worker-01", v1.NodeReady)
	n.Status.Allocatable = v1.Resources{CPU: 1000, Memory: 1 << 30}
	s := snapshot(t, map[string]any{
		"nodes": map[string]*v1.Node{"worker-01": n},
		"workloads": map[string]*v1.Workload{
			"a": {
				ObjectMeta: v1.ObjectMeta{Name: "web-1"},
				Spec:       v1.WorkloadSpec{Resources: v1.ResourceSpec{Request: v1.Resources{CPU: 1500}}},
				Status:     v1.WorkloadStatus{Phase: v1.WorkloadRunning, NodeName: "worker-01"},
			},
		},
	})
	if got := invariant(t, "no-resource-overcommit").Check(s); got == "" {
		t.Fatal("expected a violation for allocated CPU exceeding allocatable")
	}
}

func TestReplicasNeverExceedDesiredCatchesExcess(t *testing.T) {
	s := snapshot(t, map[string]any{
		"deployments": map[string]*v1.Deployment{
			"web": {
				ObjectMeta: v1.ObjectMeta{Name: "web", UID: "dep-1"},
				Spec:       v1.DeploymentSpec{Replicas: 1, Strategy: v1.Strategy{MaxSurge: 0}},
			},
		},
		"workloads": map[string]*v1.Workload{
			"a": {
				ObjectMeta: v1.ObjectMeta{Name: "web-0", OwnerRef: &v1.OwnerReference{UID: "dep-1"}},
				Status:     v1.WorkloadStatus{Phase: v1.WorkloadRunning},
			},
			"b": {
				ObjectMeta: v1.ObjectMeta{Name: "web-1", OwnerRef: &v1.OwnerReference{UID: "dep-1"}},
				Status:     v1.WorkloadStatus{Phase: v1.WorkloadRunning},
			},
		},
	})
	if got := invariant(t, "replicas-never-exceed-desired").Check(s); got == "" {
		t.Fatal("expected a violation for two active replicas against a desired count of one")
	}
}

func TestEndpointsAreProvenHealthyCatchesStaleReady(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthUnknown}},
		},
		"services": map[string]*v1.Service{
			"web-svc": {
				ObjectMeta: v1.ObjectMeta{Name: "web-svc"},
				Status: v1.ServiceStatus{Endpoints: []v1.Endpoint{
					{WorkloadName: "web-1", Ready: true},
				}},
			},
		},
	})
	if got := invariant(t, "endpoints-are-proven-healthy").Check(s); got == "" {
		t.Fatal("expected a violation for an endpoint marked ready against an unknown-health workload")
	}
}

func TestEndpointsAreProvenHealthyHoldsForHealthyBackend(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthHealthy}},
		},
		"services": map[string]*v1.Service{
			"web-svc": {
				ObjectMeta: v1.ObjectMeta{Name: "web-svc"},
				Status: v1.ServiceStatus{Endpoints: []v1.Endpoint{
					{WorkloadName: "web-1", Ready: true},
				}},
			},
		},
	})
	if got := invariant(t, "endpoints-are-proven-healthy").Check(s); got != "" {
		t.Fatalf("unexpected violation: %s", got)
	}
}

func TestWorkloadPhasesAreLegalCatchesRunningWithoutNode(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning}},
		},
	})
	if got := invariant(t, "workload-phases-are-legal").Check(s); got == "" {
		t.Fatal("expected a violation for Running with no node")
	}
}

func TestWorkloadPhasesAreLegalCatchesPendingWithNode(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadPending, NodeName: "worker-01"}},
		},
	})
	if got := invariant(t, "workload-phases-are-legal").Check(s); got == "" {
		t.Fatal("expected a violation for Pending with a node assigned")
	}
}

func TestMonitorRecordsFirstViolationAndSurvivesRecovery(t *testing.T) {
	s := snapshot(t, map[string]any{
		"workloads": map[string]*v1.Workload{
			"a": {ObjectMeta: v1.ObjectMeta{Name: "web-1"}, Status: v1.WorkloadStatus{Phase: v1.WorkloadRunning}},
		},
	})
	m := newMonitor(s, []Invariant{invariant(t, "workload-phases-are-legal")})
	m.sample()
	if !m.violated() {
		t.Fatal("expected the monitor to record the violation")
	}

	// The state heals, but the monitor must remember the violation happened —
	// a later clean sample must not erase evidence of the earlier breach.
	s2 := snapshot(t, map[string]any{})
	m.store = s2
	m.sample()
	if !m.violated() {
		t.Fatal("a later clean sample must not erase a recorded violation")
	}
	results := m.snapshot()
	if len(results) != 1 || results[0].Violations != 1 {
		t.Fatalf("expected exactly one recorded violation, got %+v", results)
	}
}
