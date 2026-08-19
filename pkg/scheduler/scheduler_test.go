package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
)

func node(name string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Node {
	return &v1.Node{
		ObjectMeta: v1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Phase:       v1.NodeReady,
			Capacity:    v1.Resources{CPU: cpu, Memory: mem},
			Allocatable: v1.Resources{CPU: cpu, Memory: mem},
		},
	}
}

func allocated(n *v1.Node, cpu v1.MilliCPU, mem v1.Bytes) *v1.Node {
	n.Status.Allocated = v1.Resources{CPU: cpu, Memory: mem}
	return n
}

func workload(name string, cpu v1.MilliCPU, mem v1.Bytes) *v1.Workload {
	return &v1.Workload{
		ObjectMeta: v1.ObjectMeta{Name: name},
		Spec: v1.WorkloadSpec{
			Image:     "nginx",
			Resources: v1.ResourceSpec{Request: v1.Resources{CPU: cpu, Memory: mem}},
		},
		Status: v1.WorkloadStatus{Phase: v1.WorkloadPending},
	}
}

func snapshot(nodes ...*v1.Node) *Snapshot {
	return NewSnapshot(nodes, map[string][]*v1.Workload{})
}

func testScheduler() *Scheduler {
	return New(WithClock(func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }))
}

func TestSchedulerPicksTheEmptiestNode(t *testing.T) {
	s := testScheduler()
	snap := snapshot(
		allocated(node("worker-01", 4000, 8<<30), 3000, 6<<30),
		allocated(node("worker-02", 4000, 8<<30), 1000, 2<<30),
		allocated(node("worker-03", 4000, 8<<30), 2000, 4<<30),
	)

	d, err := s.Schedule(workload("web-1", 500, 512<<20), snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.NodeName != "worker-02" {
		t.Fatalf("placed on %s, want worker-02 (the emptiest node)", d.NodeName)
	}
	if len(d.Candidates) != 3 {
		t.Errorf("expected 3 scored candidates, got %d", len(d.Candidates))
	}
	if d.Candidates[0].NodeName != d.NodeName {
		t.Error("winner is not first in the candidate list")
	}
	if d.Reason == "" || !strings.Contains(d.Reason, "worker-02") {
		t.Errorf("decision reason does not name the winner: %q", d.Reason)
	}
	if d.LatencyMicros < 0 {
		t.Errorf("negative scheduling latency: %d", d.LatencyMicros)
	}
}

// Every rejection must be explainable. This is the difference between "pending"
// and "pending because these four nodes said no, for these reasons".
func TestUnschedulableReportsPerNodeReasons(t *testing.T) {
	s := testScheduler()
	tainted := node("worker-02", 8000, 16<<30)
	tainted.Spec.Taints = []v1.Taint{{Key: "dedicated", Value: "db", Effect: "NoSchedule"}}
	cordoned := node("worker-03", 8000, 16<<30)
	cordoned.Spec.Unschedulable = true
	labeled := node("worker-04", 8000, 16<<30)

	w := workload("big", 6000, 8<<30)
	w.Spec.NodeSelector = map[string]string{"zone": "b"}

	snap := snapshot(
		allocated(node("worker-01", 8000, 16<<30), 7000, 1<<30), // no cpu
		tainted,
		cordoned,
		labeled, // missing label
	)

	_, err := s.Schedule(w, snap)
	if err == nil {
		t.Fatal("expected the workload to be unschedulable")
	}
	var ue *ErrUnschedulable
	if !errors.As(err, &ue) {
		t.Fatalf("expected *ErrUnschedulable, got %T", err)
	}
	if len(ue.Rejections) != 4 {
		t.Fatalf("expected 4 rejections, got %d: %+v", len(ue.Rejections), ue.Rejections)
	}

	byNode := map[string]v1.NodeRejection{}
	for _, r := range ue.Rejections {
		byNode[r.NodeName] = r
	}
	// The label filter runs before the resource filter, so worker-01 is
	// rejected for the selector, not for capacity.
	if got := byNode["worker-01"].Filter; got != "NodeSelector" {
		t.Errorf("worker-01 rejected by %s, want NodeSelector", got)
	}
	if got := byNode["worker-03"].Filter; got != "NodeReady" {
		t.Errorf("cordoned node rejected by %s, want NodeReady", got)
	}
	if !strings.Contains(byNode["worker-03"].Reason, "cordoned") {
		t.Errorf("cordon reason is unhelpful: %q", byNode["worker-03"].Reason)
	}
	if !strings.Contains(err.Error(), "4 nodes rejected") {
		t.Errorf("summary does not report the rejection count: %q", err.Error())
	}
}

func TestResourceFilterReportsWhichResourceIsShort(t *testing.T) {
	s := testScheduler()
	snap := snapshot(allocated(node("worker-01", 4000, 8<<30), 3800, 0))
	_, err := s.Schedule(workload("w", 1000, 1<<30), snap)

	var ue *ErrUnschedulable
	if !errors.As(err, &ue) {
		t.Fatalf("expected unschedulable, got %v", err)
	}
	reason := ue.Rejections[0].Reason
	if !strings.Contains(reason, "insufficient cpu") {
		t.Errorf("expected the reason to identify CPU as the shortfall, got %q", reason)
	}
	if strings.Contains(reason, "memory") {
		t.Errorf("reason wrongly blames memory: %q", reason)
	}
}

// Scheduling a batch against one snapshot must spread the batch, not pile it
// all onto whichever node was emptiest when the snapshot was taken.
func TestBatchSchedulingReservesCapacityAsItGoes(t *testing.T) {
	s := testScheduler()
	snap := snapshot(
		node("worker-01", 4000, 8<<30),
		node("worker-02", 4000, 8<<30),
		node("worker-03", 4000, 8<<30),
	)

	var batch []*v1.Workload
	for i := 0; i < 6; i++ {
		batch = append(batch, workload(fmt.Sprintf("w-%d", i), 1000, 2<<30))
	}
	results := s.ScheduleBatch(batch, snap)

	placements := map[string]int{}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("workload %s failed: %v", r.Workload.Name, r.Err)
		}
		placements[r.Decision.NodeName]++
	}
	if len(placements) != 3 {
		t.Fatalf("batch was not spread across nodes: %v", placements)
	}
	for n, c := range placements {
		if c != 2 {
			t.Errorf("node %s received %d workloads, want an even 2: %v", n, c, placements)
		}
	}
}

// A batch that exceeds cluster capacity must place what fits and reject the
// rest with a real reason, rather than overcommitting.
func TestBatchStopsAtRealCapacity(t *testing.T) {
	s := testScheduler()
	snap := snapshot(node("worker-01", 2000, 4<<30))

	var batch []*v1.Workload
	for i := 0; i < 4; i++ {
		batch = append(batch, workload(fmt.Sprintf("w-%d", i), 1000, 2<<30))
	}
	results := s.ScheduleBatch(batch, snap)

	placed, failed := 0, 0
	for _, r := range results {
		if r.Err == nil {
			placed++
		} else {
			failed++
		}
	}
	if placed != 2 {
		t.Errorf("placed %d workloads on a node with room for 2", placed)
	}
	if failed != 2 {
		t.Errorf("expected 2 rejections, got %d", failed)
	}
}

// Replicas of one deployment must be spread so a single node failure does not
// take the whole service down.
func TestSpreadScorerSeparatesDeploymentReplicas(t *testing.T) {
	s := testScheduler()
	owner := &v1.OwnerReference{Kind: "Deployment", Name: "web", UID: "web-0001"}

	existing := workload("web-a", 500, 512<<20)
	existing.OwnerRef = owner
	existing.Status.Phase = v1.WorkloadRunning
	existing.Status.NodeName = "worker-01"

	// worker-01 is emptier, but already runs a sibling.
	nodes := []*v1.Node{
		allocated(node("worker-01", 8000, 16<<30), 500, 512<<20),
		allocated(node("worker-02", 8000, 16<<30), 2000, 2<<30),
	}
	snap := NewSnapshot(nodes, map[string][]*v1.Workload{"worker-01": {existing}})

	w := workload("web-b", 500, 512<<20)
	w.OwnerRef = owner

	d, err := s.Schedule(w, snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.NodeName != "worker-02" {
		t.Fatalf("replica placed on %s alongside its sibling; want worker-02", d.NodeName)
	}

	// A workload with no owner has nothing to spread from and should follow
	// the least-allocated preference instead.
	solo := workload("solo", 500, 512<<20)
	d2, err := s.Schedule(solo, snap)
	if err != nil {
		t.Fatal(err)
	}
	if d2.NodeName != "worker-01" {
		t.Errorf("unowned workload placed on %s, want the emptiest node worker-01", d2.NodeName)
	}
}

func TestHostPortConflictIsDetectedBeforePlacement(t *testing.T) {
	s := testScheduler()
	existing := workload("web-a", 500, 512<<20)
	existing.Status.Phase = v1.WorkloadRunning
	existing.Spec.Ports = []v1.Port{{Container: 80, Host: 8080}}

	snap := NewSnapshot(
		[]*v1.Node{node("worker-01", 8000, 16<<30), node("worker-02", 8000, 16<<30)},
		map[string][]*v1.Workload{"worker-01": {existing}},
	)

	w := workload("web-b", 500, 512<<20)
	w.Spec.Ports = []v1.Port{{Container: 80, Host: 8080}}

	d, err := s.Schedule(w, snap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.NodeName != "worker-02" {
		t.Fatalf("placed on %s despite a host port conflict; want worker-02", d.NodeName)
	}
	if len(d.Rejections) != 1 || d.Rejections[0].Filter != "HostPorts" {
		t.Errorf("host port rejection not recorded: %+v", d.Rejections)
	}
}

// Identical inputs must produce identical output, ties included, so placement
// is reproducible and a scheduler restart does not reshuffle the cluster.
func TestSchedulingIsDeterministicIncludingTies(t *testing.T) {
	s := testScheduler()
	build := func() *Snapshot {
		return snapshot(
			node("worker-03", 4000, 8<<30),
			node("worker-01", 4000, 8<<30),
			node("worker-02", 4000, 8<<30),
		)
	}
	first, err := s.Schedule(workload("w", 500, 512<<20), build())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		d, err := s.Schedule(workload("w", 500, 512<<20), build())
		if err != nil {
			t.Fatal(err)
		}
		if d.NodeName != first.NodeName || d.Score != first.Score {
			t.Fatalf("run %d chose %s (score %d), first run chose %s (score %d)",
				i, d.NodeName, d.Score, first.NodeName, first.Score)
		}
	}
	// An exact tie must break on node name, not on map or slice order.
	if first.NodeName != "worker-01" {
		t.Errorf("tie broke to %s, want the lowest name worker-01", first.NodeName)
	}
}

func TestBalancedResourceScorerAvoidsLopsidedNodes(t *testing.T) {
	s := New(WithScorers(BalancedResourceScorer{}),
		WithClock(func() time.Time { return time.Time{} }))

	// worker-01 has plenty of memory but almost no CPU left; worker-02 is
	// evenly used. A CPU-heavy workload should go to worker-02.
	snap := snapshot(
		allocated(node("worker-01", 4000, 16<<30), 3000, 1<<30),
		allocated(node("worker-02", 4000, 16<<30), 1000, 4<<30),
	)
	d, err := s.Schedule(workload("cpu-heavy", 500, 256<<20), snap)
	if err != nil {
		t.Fatal(err)
	}
	if d.NodeName != "worker-02" {
		t.Errorf("balanced scorer chose %s, want worker-02", d.NodeName)
	}
}

func TestEmptyClusterProducesAClearError(t *testing.T) {
	s := testScheduler()
	_, err := s.Schedule(workload("w", 100, 64<<20), snapshot())
	if err == nil {
		t.Fatal("expected an error scheduling against an empty cluster")
	}
	if !strings.Contains(err.Error(), "no nodes accepting work") {
		t.Errorf("unhelpful error for an empty cluster: %q", err.Error())
	}
}

// Recorded decision detail must stay bounded: it is written into the Raft log
// on every placement.
func TestDecisionDetailIsBounded(t *testing.T) {
	s := testScheduler()
	nodes := make([]*v1.Node, 0, 50)
	for i := 0; i < 50; i++ {
		nodes = append(nodes, node(fmt.Sprintf("worker-%02d", i), 4000, 8<<30))
	}
	d, err := s.Schedule(workload("w", 100, 64<<20), snapshot(nodes...))
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Candidates) > 5 {
		t.Errorf("recorded %d candidates; detail must stay bounded", len(d.Candidates))
	}
}

func BenchmarkScheduleAcross100Nodes(b *testing.B) {
	s := New()
	nodes := make([]*v1.Node, 0, 100)
	byNode := map[string][]*v1.Workload{}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("worker-%03d", i)
		n := allocated(node(name, 32000, 128<<30), v1.MilliCPU(i*100), v1.Bytes(i)<<28)
		nodes = append(nodes, n)
		for j := 0; j < 10; j++ {
			w := workload(fmt.Sprintf("%s-w%d", name, j), 500, 512<<20)
			w.Status.Phase = v1.WorkloadRunning
			byNode[name] = append(byNode[name], w)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := NewSnapshot(nodes, byNode)
		if _, err := s.Schedule(workload("bench", 1000, 1<<30), snap); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScheduleBatch1000Workloads(b *testing.B) {
	s := New()
	nodes := make([]*v1.Node, 0, 50)
	for i := 0; i < 50; i++ {
		nodes = append(nodes, node(fmt.Sprintf("worker-%03d", i), 64000, 256<<30))
	}
	batch := make([]*v1.Workload, 0, 1000)
	for i := 0; i < 1000; i++ {
		batch = append(batch, workload(fmt.Sprintf("w-%04d", i), 100, 128<<20))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := NewSnapshot(nodes, map[string][]*v1.Workload{})
		s.ScheduleBatch(batch, snap)
	}
	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "placements/s")
}
