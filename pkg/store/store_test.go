package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/raft"
)

var baseTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness applies commands the way the Raft driver would: sequential indices,
// one goroutine, timestamps supplied by the proposer.
type harness struct {
	t     *testing.T
	store *Store
	index uint64
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return &harness{t: t, store: New()}
}

func (h *harness) apply(cmd Command) Result {
	h.t.Helper()
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = baseTime.Add(time.Duration(h.index) * time.Second)
	}
	data, err := cmd.Encode()
	if err != nil {
		h.t.Fatalf("encoding %s: %v", cmd.Kind, err)
	}
	h.index++
	res, ok := h.store.Apply(raft.Entry{Index: h.index, Term: 1, Type: raft.EntryNormal, Data: data}).(Result)
	if !ok {
		h.t.Fatalf("Apply returned %T, want Result", res)
	}
	return res
}

func (h *harness) mustApply(cmd Command) Result {
	h.t.Helper()
	res := h.apply(cmd)
	if res.Err != nil {
		h.t.Fatalf("%s failed: %v", cmd.Kind, res.Err)
	}
	return res
}

func testNode(name string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Node {
	return &v1.Node{
		ObjectMeta: v1.ObjectMeta{Name: name, Labels: map[string]string{"zone": "a"}},
		Spec:       v1.NodeSpec{Address: name + ":9100"},
		Status: v1.NodeStatus{
			Capacity:    v1.Resources{CPU: cpu, Memory: mem},
			Allocatable: v1.Resources{CPU: cpu, Memory: mem},
		},
	}
}

func testWorkload(name string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Workload {
	return &v1.Workload{
		ObjectMeta: v1.ObjectMeta{Name: name, Labels: map[string]string{"app": "web"}},
		Spec: v1.WorkloadSpec{
			Image:         "nginx:1.27-alpine",
			RestartPolicy: v1.RestartAlways,
			Resources:     v1.ResourceSpec{Request: v1.Resources{CPU: cpu, Memory: mem}},
		},
	}
}

func (h *harness) registerNode(name string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Node {
	return h.mustApply(Command{Kind: CmdRegisterNode, Node: testNode(name, cpu, mem)}).Node
}

func (h *harness) createAndBind(name, node string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Workload {
	h.t.Helper()
	w := h.mustApply(Command{Kind: CmdCreateWorkload, Workload: testWorkload(name, cpu, mem)}).Workload
	return h.mustApply(Command{
		Kind:      CmdBindWorkload,
		Name:      name,
		UID:       w.UID,
		Placement: &v1.PlacementDecision{WorkloadName: name, NodeName: node, Reason: "test"},
	}).Workload
}

// ---------------------------------------------------------------------------

func TestRegisterNodeAndReRegister(t *testing.T) {
	h := newHarness(t)
	n := h.registerNode("worker-01", 4000, 8<<30)
	if n.Status.Phase != v1.NodeReady {
		t.Fatalf("new node phase = %s, want Ready", n.Status.Phase)
	}
	if n.UID == "" {
		t.Fatal("node was not assigned a UID")
	}

	// An agent restart re-registers. Identity and bindings must survive.
	h.createAndBind("web-1", "worker-01", 1000, 1<<30)
	again := h.mustApply(Command{Kind: CmdRegisterNode, Node: testNode("worker-01", 8000, 16<<30)}).Node
	if again.UID != n.UID {
		t.Errorf("re-registration changed the node UID: %s -> %s", n.UID, again.UID)
	}
	if again.Status.Allocatable.CPU != 8000 {
		t.Errorf("re-registration did not refresh capacity: %v", again.Status.Allocatable)
	}
	if again.Status.Allocated.CPU != 1000 {
		t.Errorf("re-registration lost workload accounting: allocated %v", again.Status.Allocated)
	}
}

// The scheduler is the only writer of NodeName, and a placed workload may never
// be rebound. This is the invariant behind "no duplicate workloads".
func TestWorkloadCannotBeReboundOnceScheduled(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	h.registerNode("worker-02", 4000, 8<<30)
	w := h.createAndBind("web-1", "worker-01", 1000, 1<<30)

	res := h.apply(Command{
		Kind:      CmdBindWorkload,
		Name:      "web-1",
		UID:       w.UID,
		Placement: &v1.PlacementDecision{WorkloadName: "web-1", NodeName: "worker-02"},
	})
	if res.Err == nil {
		t.Fatal("expected rebinding a scheduled workload to be rejected")
	}
	if !errors.Is(res.Err, ErrInvalidState) {
		t.Errorf("expected ErrInvalidState, got %v", res.Err)
	}
	got, _ := h.store.Workload("web-1")
	if got.Status.NodeName != "worker-01" {
		t.Errorf("workload moved to %s despite the rejection", got.Status.NodeName)
	}
}

// Binding revalidates capacity at apply time. The scheduler decides against a
// snapshot; only the state machine can see the committed truth.
func TestBindRejectedWhenCapacityWasConsumedConcurrently(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 2000, 4<<30)
	h.createAndBind("a", "worker-01", 1500, 3<<30)

	w := h.mustApply(Command{Kind: CmdCreateWorkload, Workload: testWorkload("b", 1000, 2<<30)}).Workload
	res := h.apply(Command{
		Kind:      CmdBindWorkload,
		Name:      "b",
		UID:       w.UID,
		Placement: &v1.PlacementDecision{WorkloadName: "b", NodeName: "worker-01"},
	})
	if res.Err == nil {
		t.Fatal("expected the binding to be rejected for insufficient capacity")
	}
	if !errors.Is(res.Err, ErrConflict) {
		t.Errorf("expected ErrConflict, got %v", res.Err)
	}
	n, _ := h.store.Node("worker-01")
	if n.Status.Allocated.CPU != 1500 {
		t.Errorf("rejected binding still consumed capacity: %v", n.Status.Allocated)
	}
}

// A stale agent report must never resurrect a terminated workload.
func TestStaleStatusReportCannotResurrectTerminatedWorkload(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	w := h.createAndBind("web-1", "worker-01", 1000, 1<<30)

	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadStarting}})
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthHealthy}})
	h.mustApply(Command{Kind: CmdDeleteWorkload, Name: "web-1"})
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadTerminated}})

	// A message from the agent that was in flight during deletion.
	res := h.apply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthHealthy}})
	if res.Err == nil {
		t.Fatal("expected a stale Running report on a terminated workload to be rejected")
	}
	got, _ := h.store.Workload("web-1")
	if got.Status.Phase != v1.WorkloadTerminated {
		t.Errorf("workload was resurrected to %s", got.Status.Phase)
	}
}

// A workload recreated with the same name is a different object. Commands
// carrying the old UID must not touch it.
func TestUIDMismatchProtectsRecreatedObjects(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	old := h.createAndBind("web-1", "worker-01", 1000, 1<<30)
	h.mustApply(Command{Kind: CmdDeleteWorkload, Name: "web-1"})
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: old.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadTerminated}})
	h.mustApply(Command{Kind: CmdPurgeWorkload, Name: "web-1", UID: old.UID})

	fresh := h.createAndBind("web-1", "worker-01", 1000, 1<<30)
	if fresh.UID == old.UID {
		t.Fatal("recreated workload reused the previous UID")
	}
	res := h.apply(Command{Kind: CmdUpdateWorkloadStatus, Name: "web-1", UID: old.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadFailed}})
	if !errors.Is(res.Err, ErrUIDMismatch) {
		t.Fatalf("expected ErrUIDMismatch, got %v", res.Err)
	}
}

// Retrying a proposal after a leader change must not apply it twice.
func TestRequestIDMakesCommandsIdempotent(t *testing.T) {
	h := newHarness(t)
	cmd := Command{Kind: CmdCreateWorkload, RequestID: "req-abc", Workload: testWorkload("web-1", 1000, 1<<30)}

	first := h.mustApply(cmd)
	second := h.apply(cmd)

	if second.Err != nil {
		t.Fatalf("retry returned an error instead of replaying the result: %v", second.Err)
	}
	if !second.Duplicate {
		t.Error("retry was not reported as a duplicate")
	}
	if second.Revision != first.Revision {
		t.Errorf("retry reported revision %d, original was %d", second.Revision, first.Revision)
	}
	if second.Workload == nil || second.Workload.UID != first.Workload.UID {
		t.Error("retry did not replay the original object")
	}
	if len(h.store.Workloads()) != 1 {
		t.Fatalf("retry created a second workload: %d exist", len(h.store.Workloads()))
	}

	// Without a request ID the same command is a genuine second attempt and
	// must be rejected as a duplicate name.
	res := h.apply(Command{Kind: CmdCreateWorkload, Workload: testWorkload("web-1", 1000, 1<<30)})
	if !errors.Is(res.Err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", res.Err)
	}
}

// Node resource accounting is recomputed from workloads, so it cannot drift
// however many status updates arrive.
func TestNodeAllocationIsDerivedNotAccumulated(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	w1 := h.createAndBind("a", "worker-01", 1000, 1<<30)
	h.createAndBind("b", "worker-01", 500, 512<<20)

	n, _ := h.store.Node("worker-01")
	if n.Status.Allocated.CPU != 1500 || n.Status.Allocated.Memory != (1<<30)+(512<<20) {
		t.Fatalf("allocation after two bindings = %v", n.Status.Allocated)
	}
	if n.Status.WorkloadCount != 2 {
		t.Errorf("workload count = %d, want 2", n.Status.WorkloadCount)
	}

	// Repeated status updates must not double-count.
	for i := 0; i < 5; i++ {
		h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "a", UID: w1.UID,
			WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadScheduled, Usage: v1.Resources{CPU: 200}}})
	}
	n, _ = h.store.Node("worker-01")
	if n.Status.Allocated.CPU != 1500 {
		t.Fatalf("allocation drifted to %v after repeated status updates", n.Status.Allocated)
	}

	// A terminated workload releases its reservation.
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "a", UID: w1.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadStarting}})
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "a", UID: w1.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadFailed}})
	n, _ = h.store.Node("worker-01")
	if n.Status.Allocated.CPU != 500 {
		t.Fatalf("failed workload did not release its reservation: %v", n.Status.Allocated)
	}
}

func TestDeleteNodeTerminatesItsWorkloads(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	h.createAndBind("a", "worker-01", 1000, 1<<30)
	h.mustApply(Command{Kind: CmdDeleteNode, Name: "worker-01"})

	w, ok := h.store.Workload("a")
	if !ok {
		t.Fatal("workload record disappeared with the node")
	}
	if w.Status.Phase != v1.WorkloadTerminated {
		t.Errorf("workload on a deleted node is %s, want Terminated", w.Status.Phase)
	}
	if _, ok := h.store.Node("worker-01"); ok {
		t.Error("node was not removed")
	}
}

func TestDeploymentRolloutAndRollback(t *testing.T) {
	h := newHarness(t)
	d := &v1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: "web"},
		Spec: v1.DeploymentSpec{
			Replicas: 3,
			Template: v1.WorkloadSpec{Image: "nginx:1.27", Resources: v1.ResourceSpec{Request: v1.Resources{CPU: 500, Memory: 128 << 20}}},
			Strategy: v1.Strategy{Kind: v1.StrategyRolling, MaxSurge: 1},
		},
	}
	created := h.mustApply(Command{Kind: CmdCreateDeployment, Deployment: d}).Deployment
	if created.Status.Revision != 1 {
		t.Fatalf("initial revision = %d, want 1", created.Status.Revision)
	}

	// Scaling is not a rollout.
	scaled := *d
	scaled.Spec.Replicas = 5
	res := h.mustApply(Command{Kind: CmdUpdateDeploymentSpec, Name: "web", Deployment: &scaled}).Deployment
	if res.Status.Revision != 1 {
		t.Errorf("scaling started a rollout: revision %d", res.Status.Revision)
	}
	if res.Spec.Replicas != 5 {
		t.Errorf("scale not applied: %d replicas", res.Spec.Replicas)
	}

	// Changing the image is a rollout.
	updated := scaled
	updated.Spec.Template.Image = "nginx:1.28"
	res = h.mustApply(Command{Kind: CmdUpdateDeploymentSpec, Name: "web", Deployment: &updated}).Deployment
	if res.Status.Revision != 2 {
		t.Fatalf("image change did not start a rollout: revision %d", res.Status.Revision)
	}

	history := h.store.DeploymentRevisions("web")
	if len(history) != 2 {
		t.Fatalf("expected 2 retained revisions, got %d", len(history))
	}

	// Rollback restores a spec that actually existed.
	rolled := h.mustApply(Command{Kind: CmdRollbackDeployment, Name: "web"}).Deployment
	if rolled.Spec.Template.Image != "nginx:1.27" {
		t.Errorf("rollback produced image %s, want nginx:1.27", rolled.Spec.Template.Image)
	}
	if rolled.Status.Revision != 3 {
		t.Errorf("rollback revision = %d, want 3", rolled.Status.Revision)
	}

	// Rolling back to what is already running is rejected rather than churning
	// every replica for no reason.
	again := h.apply(Command{Kind: CmdRollbackDeployment, Name: "web", TargetRevision: 1})
	if again.Err == nil {
		t.Error("expected a no-op rollback to be rejected")
	}
}

func TestDeleteDeploymentCascadesToOwnedWorkloads(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 8000, 16<<30)
	d := h.mustApply(Command{Kind: CmdCreateDeployment, Deployment: &v1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: "web"},
		Spec: v1.DeploymentSpec{Replicas: 2, Template: v1.WorkloadSpec{
			Image: "nginx", Resources: v1.ResourceSpec{Request: v1.Resources{CPU: 500, Memory: 128 << 20}}}},
	}}).Deployment

	for i := 0; i < 2; i++ {
		w := testWorkload(fmt.Sprintf("web-%d", i), 500, 128<<20)
		w.OwnerRef = &v1.OwnerReference{Kind: "Deployment", Name: d.Name, UID: d.UID}
		created := h.mustApply(Command{Kind: CmdCreateWorkload, Workload: w}).Workload
		h.mustApply(Command{Kind: CmdBindWorkload, Name: created.Name, UID: created.UID,
			Placement: &v1.PlacementDecision{WorkloadName: created.Name, NodeName: "worker-01"}})
	}
	// An unrelated workload must be left alone.
	h.createAndBind("other", "worker-01", 500, 128<<20)

	h.mustApply(Command{Kind: CmdDeleteDeployment, Name: "web", Actor: "operator@example.com"})

	for i := 0; i < 2; i++ {
		w, _ := h.store.Workload(fmt.Sprintf("web-%d", i))
		if w.DeletedAt == nil {
			t.Errorf("owned workload web-%d was not marked for deletion", i)
		}
		if w.Status.Phase != v1.WorkloadTerminating {
			t.Errorf("owned workload web-%d is %s, want Terminating", i, w.Status.Phase)
		}
	}
	other, _ := h.store.Workload("other")
	if other.DeletedAt != nil {
		t.Error("cascade deleted an unowned workload")
	}

	// Destructive operations are attributed in the audit trail.
	events := h.store.Events(EventQuery{Kind: "Deployment", Limit: 10})
	found := false
	for _, e := range events {
		if e.Reason == "DeploymentDeleting" && e.Actor == "operator@example.com" {
			found = true
		}
	}
	if !found {
		t.Error("deletion was not recorded with the acting principal")
	}
}

func TestServiceEndpointsAreOrderedAndDeduplicated(t *testing.T) {
	h := newHarness(t)
	h.mustApply(Command{Kind: CmdCreateService, Service: &v1.Service{
		ObjectMeta: v1.ObjectMeta{Name: "web"},
		Spec:       v1.ServiceSpec{Selector: map[string]string{"app": "web"}, Port: 8080, TargetPort: 80},
	}})

	eps := []v1.Endpoint{
		{WorkloadName: "web-3", Address: "10.0.0.3", Port: 80, Ready: true, Health: v1.HealthHealthy},
		{WorkloadName: "web-1", Address: "10.0.0.1", Port: 80, Ready: true, Health: v1.HealthHealthy},
		{WorkloadName: "web-2", Address: "10.0.0.2", Port: 80, Ready: false, Health: v1.HealthUnhealthy},
	}
	s := h.mustApply(Command{Kind: CmdUpdateServiceEndpoint, Name: "web", Endpoints: eps}).Service
	if len(s.Status.Endpoints) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(s.Status.Endpoints))
	}
	for i := 1; i < len(s.Status.Endpoints); i++ {
		if s.Status.Endpoints[i-1].WorkloadName > s.Status.Endpoints[i].WorkloadName {
			t.Fatalf("endpoints are not deterministically ordered: %v", s.Status.Endpoints)
		}
	}
	if s.Status.HealthyEndpoints != 2 {
		t.Errorf("healthy endpoint count = %d, want 2", s.Status.HealthyEndpoints)
	}

	// Reapplying the same set (in a different order) must not bump the revision:
	// that churn would show up as phantom activity in the console.
	rev := s.Revision
	shuffled := []v1.Endpoint{eps[1], eps[2], eps[0]}
	again := h.mustApply(Command{Kind: CmdUpdateServiceEndpoint, Name: "web", Endpoints: shuffled}).Service
	if again.Revision != rev {
		t.Errorf("identical endpoint set bumped the revision from %d to %d", rev, again.Revision)
	}

	// Losing every endpoint is a critical event.
	h.mustApply(Command{Kind: CmdUpdateServiceEndpoint, Name: "web", Endpoints: nil})
	events := h.store.Events(EventQuery{Kind: "Service", Limit: 10})
	if len(events) == 0 || events[0].Reason != "ServiceHasNoEndpoints" || events[0].Severity != v1.SeverityCritical {
		t.Errorf("losing all endpoints did not produce a critical event: %+v", events)
	}
}

// A snapshot must round-trip to byte-identical state, including derived
// indexes, or a restarted replica silently disagrees with its peers.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 8000, 16<<30)
	h.registerNode("worker-02", 8000, 16<<30)
	for i := 0; i < 10; i++ {
		node := "worker-01"
		if i%2 == 1 {
			node = "worker-02"
		}
		h.createAndBind(fmt.Sprintf("web-%d", i), node, 500, 256<<20)
	}
	h.mustApply(Command{Kind: CmdCreateService, Service: &v1.Service{
		ObjectMeta: v1.ObjectMeta{Name: "web"},
		Spec:       v1.ServiceSpec{Selector: map[string]string{"app": "web"}, Port: 8080, TargetPort: 80},
	}})

	data, err := h.store.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got, want := len(restored.Workloads()), len(h.store.Workloads()); got != want {
		t.Fatalf("restored %d workloads, want %d", got, want)
	}
	// Derived state must be rebuilt, not carried.
	for _, name := range []string{"worker-01", "worker-02"} {
		a, _ := h.store.Node(name)
		b, _ := restored.Node(name)
		if a.Status.Allocated != b.Status.Allocated {
			t.Errorf("node %s allocation after restore = %v, want %v", name, b.Status.Allocated, a.Status.Allocated)
		}
		if len(restored.WorkloadsOnNode(name)) != len(h.store.WorkloadsOnNode(name)) {
			t.Errorf("node %s workload index was not rebuilt", name)
		}
	}

	// Snapshotting the restored store must produce identical bytes.
	again, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Error("snapshot is not stable across a restore cycle")
	}
}

// Applying the same log to two independent stores must yield identical state.
// This is the property that keeps replicas from silently diverging.
func TestApplyIsDeterministicAcrossReplicas(t *testing.T) {
	commands := []Command{
		{Kind: CmdRegisterNode, Node: testNode("worker-01", 4000, 8<<30)},
		{Kind: CmdRegisterNode, Node: testNode("worker-02", 4000, 8<<30)},
		{Kind: CmdCreateWorkload, Workload: testWorkload("a", 1000, 1<<30)},
		{Kind: CmdBindWorkload, Name: "a", Placement: &v1.PlacementDecision{WorkloadName: "a", NodeName: "worker-01", Reason: "r"}},
		{Kind: CmdCreateWorkload, Workload: testWorkload("b", 1000, 1<<30)},
		{Kind: CmdBindWorkload, Name: "b", Placement: &v1.PlacementDecision{WorkloadName: "b", NodeName: "worker-02", Reason: "r"}},
		{Kind: CmdUpdateWorkloadStatus, Name: "a", WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadStarting}},
		{Kind: CmdUpdateWorkloadStatus, Name: "a", WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthHealthy}},
		{Kind: CmdCreateService, Service: &v1.Service{ObjectMeta: v1.ObjectMeta{Name: "svc"},
			Spec: v1.ServiceSpec{Selector: map[string]string{"app": "web"}, Port: 8080, TargetPort: 80}}},
		{Kind: CmdSetNodePhase, Name: "worker-02", Phase: v1.NodeUnreachable, Reason: "heartbeat timeout"},
	}

	snapshotOf := func() string {
		h := newHarness(t)
		for _, c := range commands {
			h.apply(c)
		}
		data, err := h.store.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	a, b := snapshotOf(), snapshotOf()
	if a != b {
		t.Fatal("two replicas applying the same log produced different state")
	}
}

func TestWatchDeliversChangesAndDropsSlowConsumers(t *testing.T) {
	h := newHarness(t)
	w := h.store.Watch()
	defer w.Stop()

	h.registerNode("worker-01", 4000, 8<<30)

	var got []Change
	deadline := time.After(2 * time.Second)
collect:
	for {
		select {
		case c, ok := <-w.Changes():
			if !ok {
				break collect
			}
			got = append(got, c)
			if len(got) >= 2 {
				break collect
			}
		case <-deadline:
			t.Fatalf("timed out waiting for change events, got %d", len(got))
		}
	}
	sawNode := false
	for _, c := range got {
		if c.Kind == "Node" && c.Op == "Created" && c.Name == "worker-01" {
			sawNode = true
		}
	}
	if !sawNode {
		t.Errorf("node creation was not delivered to the watcher: %+v", got)
	}

	// A consumer that never reads must be dropped rather than blocking the
	// apply loop or growing without bound.
	slow := h.store.Watch()
	for i := 0; i < watchBuffer*3; i++ {
		h.registerNode(fmt.Sprintf("filler-%03d", i), 1000, 1<<30)
	}
	if !slow.Stale() {
		t.Error("a watcher that never reads was not marked stale")
	}
}

func TestEventQueryFiltersAndPages(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 5; i++ {
		h.registerNode(fmt.Sprintf("worker-%02d", i), 1000, 1<<30)
	}
	all := h.store.Events(EventQuery{Limit: 100})
	if len(all) != 5 {
		t.Fatalf("expected 5 events, got %d", len(all))
	}
	// Newest first.
	for i := 1; i < len(all); i++ {
		if all[i-1].ID < all[i].ID {
			t.Fatalf("events are not newest-first: %v", all)
		}
	}
	filtered := h.store.Events(EventQuery{Kind: "Node", Name: "worker-03", Limit: 100})
	if len(filtered) != 1 || filtered[0].Name != "worker-03" {
		t.Errorf("name filter returned %+v", filtered)
	}
	if got := h.store.Events(EventQuery{Limit: 2}); len(got) != 2 {
		t.Errorf("limit not honoured: got %d", len(got))
	}
}

func TestSummaryReflectsRealState(t *testing.T) {
	h := newHarness(t)
	h.registerNode("worker-01", 4000, 8<<30)
	h.registerNode("worker-02", 4000, 8<<30)
	h.mustApply(Command{Kind: CmdSetNodePhase, Name: "worker-02", Phase: v1.NodeUnreachable, Reason: "timeout"})

	w := h.createAndBind("a", "worker-01", 1000, 1<<30)
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "a", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadStarting}})
	h.mustApply(Command{Kind: CmdUpdateWorkloadStatus, Name: "a", UID: w.UID,
		WorkloadStatus: &v1.WorkloadStatus{Phase: v1.WorkloadRunning, Health: v1.HealthHealthy}})
	h.mustApply(Command{Kind: CmdCreateWorkload, Workload: testWorkload("b", 1000, 1<<30)})

	s := h.store.Summary()
	if s.Nodes.Total != 2 || s.Nodes.Ready != 1 || s.Nodes.Unreachable != 1 {
		t.Errorf("node counts wrong: %+v", s.Nodes)
	}
	if s.Workloads.Running != 1 || s.Workloads.Pending != 1 {
		t.Errorf("workload counts wrong: %+v", s.Workloads)
	}
	// An unreachable node's capacity must not be counted as available.
	if s.Capacity.CPUAllocatable != 4000 {
		t.Errorf("allocatable CPU = %v, want 4000 (only the Ready node)", s.Capacity.CPUAllocatable)
	}
	if s.Capacity.CPUAllocated != 1000 {
		t.Errorf("allocated CPU = %v, want 1000", s.Capacity.CPUAllocated)
	}
}

func BenchmarkApplyCreateWorkload(b *testing.B) {
	st := New()
	node := Command{Kind: CmdRegisterNode, Timestamp: baseTime, Node: testNode("worker-01", 1<<20, 1<<40)}
	data, _ := node.Encode()
	st.Apply(raft.Entry{Index: 1, Term: 1, Data: data})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cmd := Command{Kind: CmdCreateWorkload, Timestamp: baseTime,
			Workload: testWorkload(fmt.Sprintf("w-%d", i), 100, 64<<20)}
		payload, _ := cmd.Encode()
		st.Apply(raft.Entry{Index: uint64(i + 2), Term: 1, Data: payload})
	}
}

func BenchmarkSummary1000Workloads(b *testing.B) {
	st := New()
	idx := uint64(0)
	apply := func(c Command) {
		idx++
		c.Timestamp = baseTime
		data, _ := c.Encode()
		st.Apply(raft.Entry{Index: idx, Term: 1, Data: data})
	}
	for i := 0; i < 20; i++ {
		apply(Command{Kind: CmdRegisterNode, Node: testNode(fmt.Sprintf("worker-%02d", i), 64000, 256<<30)})
	}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("w-%04d", i)
		apply(Command{Kind: CmdCreateWorkload, Workload: testWorkload(name, 100, 64<<20)})
		apply(Command{Kind: CmdBindWorkload, Name: name,
			Placement: &v1.PlacementDecision{WorkloadName: name, NodeName: fmt.Sprintf("worker-%02d", i%20)}})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = st.Summary()
	}
}
