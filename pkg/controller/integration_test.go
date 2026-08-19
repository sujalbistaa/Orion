package controller

import (
	"context"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/store"
)

// These tests run a real control plane — real Raft, real controllers, real
// goroutines — and assert the properties Orion claims: desired state is
// reached, failures are repaired, and the system does not oscillate once it
// has converged.

func TestDeploymentConvergesToDesiredReplicas(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 4000, 8<<30)
	c.addNode("worker-02", 4000, 8<<30)

	c.createDeployment("web", 5, 500, 512<<20)

	waitForValue(t, 10*time.Second, "web to reach 5 ready replicas", 5,
		func() int { return c.readyReplicas("web") })

	// Converged means converged: no churn afterwards.
	stayAt(t, 500*time.Millisecond, "ready replica count", 5,
		func() int { return c.readyReplicas("web") })

	d, _ := c.store().Deployment("web")
	if d.Status.Phase != v1.DeploymentAvailable {
		t.Errorf("deployment phase = %s, want Available", d.Status.Phase)
	}
	if d.Status.AvailableReplicas != 5 {
		t.Errorf("status reports %d available replicas, want 5", d.Status.AvailableReplicas)
	}

	// Replicas must be spread across both nodes, not piled onto one.
	perNode := map[string]int{}
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Status.Phase.Active() {
			perNode[w.Status.NodeName]++
		}
	}
	if len(perNode) != 2 {
		t.Errorf("replicas were not spread across nodes: %v", perNode)
	}
}

// The core self-healing property: a workload that dies is replaced without
// anyone asking.
func TestCrashedWorkloadIsReplaced(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 4000, 8<<30)
	c.createDeployment("web", 3, 500, 512<<20)

	waitForValue(t, 10*time.Second, "initial replicas", 3, func() int { return c.readyReplicas("web") })

	d, _ := c.store().Deployment("web")
	victim := c.store().WorkloadsOwnedBy(d.UID)[0]

	// Simulate the container exiting non-recoverably.
	status := victim.Status.DeepCopy()
	status.Phase = v1.WorkloadFailed
	code := int32(137)
	status.ExitCode = &code
	status.Reason = "OOMKilled"
	status.Health = v1.HealthUnhealthy
	if _, err := c.cp.Apply(context.Background(), store.Command{
		Kind: store.CmdUpdateWorkloadStatus, Name: victim.Name, UID: victim.UID, WorkloadStatus: &status,
	}); err != nil {
		t.Fatalf("simulating crash: %v", err)
	}

	waitForValue(t, 10*time.Second, "the crashed replica to be replaced", 3,
		func() int { return c.readyReplicas("web") })

	// The replacement must be a genuinely new workload, not a resurrection.
	found := false
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Ready() && w.UID != victim.UID {
			found = true
		}
		if w.UID == victim.UID && w.Ready() {
			t.Error("the failed workload was resurrected instead of replaced")
		}
	}
	if !found {
		t.Error("no replacement replica was created")
	}
}

// Node failure recovery: the scenario from the project brief, start to finish.
func TestNodeFailureReschedulesWorkloads(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 4000, 8<<30)
	c.addNode("worker-02", 4000, 8<<30)
	c.addNode("worker-03", 4000, 8<<30)

	c.createDeployment("web", 6, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 6, func() int { return c.readyReplicas("web") })

	d, _ := c.store().Deployment("web")
	before := map[string]bool{}
	victimCount := 0
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		before[w.UID] = true
		if w.Status.NodeName == "worker-02" {
			victimCount++
		}
	}
	if victimCount == 0 {
		t.Fatal("test setup: no replicas landed on the node we are about to kill")
	}

	start := time.Now()
	c.killNode("worker-02")

	// 1. the control plane notices
	waitFor(t, 10*time.Second, "worker-02 to be marked unreachable", func() bool {
		n, ok := c.store().Node("worker-02")
		return ok && n.Status.Phase == v1.NodeUnreachable
	})

	// 2. its workloads are evicted and 3. replacements are scheduled and run
	waitForValue(t, 20*time.Second, "the deployment to recover to 6 replicas", 6,
		func() int { return c.readyReplicas("web") })
	recovery := time.Since(start)
	t.Logf("recovered %d replicas after node failure in %s", victimCount, recovery.Round(time.Millisecond))

	// 4. nothing is left running on the dead node
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Status.NodeName == "worker-02" && w.Status.Phase.Active() {
			t.Errorf("workload %s is still active on the failed node", w.Name)
		}
	}
	// 5. and the cluster does not exceed desired replicas
	stayAt(t, 500*time.Millisecond, "active replica count after recovery", 6,
		func() int { return c.activeReplicas("web") })
}

// A node that goes quiet briefly must not trigger a cluster-wide reschedule.
func TestBriefNodeSilenceDoesNotEvict(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 4000, 8<<30)
	c.addNode("worker-02", 4000, 8<<30)
	c.createDeployment("web", 4, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 4, func() int { return c.readyReplicas("web") })

	uidsBefore := map[string]bool{}
	d, _ := c.store().Deployment("web")
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		uidsBefore[w.UID] = true
	}

	// Pause for less than the heartbeat timeout.
	agent := c.agent("worker-02")
	agent.Pause()
	time.Sleep(100 * time.Millisecond)

	// Resume by replacing the paused agent with a fresh one.
	c.killNode("worker-02")
	c.mu.Lock()
	fresh := newFakeAgent(t, c.cp, "worker-02")
	c.agents["worker-02"] = fresh
	c.mu.Unlock()
	fresh.start()

	time.Sleep(300 * time.Millisecond)

	stable := 0
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if uidsBefore[w.UID] && w.Status.Phase.Active() {
			stable++
		}
	}
	if stable != 4 {
		t.Errorf("a brief pause rescheduled workloads: %d of 4 original replicas survived", stable)
	}
}

// A rolling update must never drop below the availability floor.
func TestRollingUpdateMaintainsAvailability(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 8000, 16<<30)
	c.addNode("worker-02", 8000, 16<<30)

	d := c.createDeployment("web", 4, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 4, func() int { return c.readyReplicas("web") })

	// maxSurge 1, maxUnavailable 0: availability must never dip below 4.
	updated := d.DeepCopy()
	updated.Spec.Strategy = v1.Strategy{Kind: v1.StrategyRolling, MaxSurge: 1, MaxUnavailable: 0}
	updated.Spec.Template.Image = "nginx:1.28-alpine"

	stop := make(chan struct{})
	violations := make(chan int, 64)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Count everything serving, old template or new: during a rollout
			// both are valid backends.
			if n := c.readyReplicas("web"); n < 4 {
				select {
				case violations <- n:
				default:
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	if _, err := c.cp.Apply(context.Background(), store.Command{
		Kind: store.CmdUpdateDeploymentSpec, Name: "web", Deployment: updated,
	}); err != nil {
		t.Fatalf("starting rollout: %v", err)
	}

	waitFor(t, 20*time.Second, "the rollout to complete", func() bool {
		cur, ok := c.store().Deployment("web")
		if !ok || cur.Status.Phase != v1.DeploymentAvailable {
			return false
		}
		hash := v1.HashWorkloadSpec(&cur.Spec.Template)
		n := 0
		for _, w := range c.store().WorkloadsOwnedBy(cur.UID) {
			if w.Ready() && w.Labels[labelTemplateHash] == hash {
				n++
			}
		}
		return n == 4
	})
	close(stop)

	if len(violations) > 0 {
		t.Errorf("availability dropped below the floor during the rollout (lowest observed: %d of 4)", <-violations)
	}

	// Every remaining replica must be on the new template.
	cur, _ := c.store().Deployment("web")
	hash := v1.HashWorkloadSpec(&cur.Spec.Template)
	for _, w := range c.store().WorkloadsOwnedBy(cur.UID) {
		if w.Status.Phase.Active() && w.Labels[labelTemplateHash] != hash {
			t.Errorf("workload %s is still on the old template after the rollout", w.Name)
		}
	}
}

func TestScaleUpAndDown(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 8000, 16<<30)

	d := c.createDeployment("web", 2, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 2, func() int { return c.readyReplicas("web") })

	scale := func(n int32) {
		t.Helper()
		cur, _ := c.store().Deployment("web")
		up := cur.DeepCopy()
		up.Spec.Replicas = n
		if _, err := c.cp.Apply(context.Background(), store.Command{
			Kind: store.CmdUpdateDeploymentSpec, Name: "web", Deployment: up,
		}); err != nil {
			t.Fatalf("scaling to %d: %v", n, err)
		}
	}

	scale(6)
	waitForValue(t, 10*time.Second, "scale up to 6", 6, func() int { return c.readyReplicas("web") })

	scale(1)
	waitForValue(t, 10*time.Second, "scale down to 1", 1, func() int { return c.activeReplicas("web") })
	stayAt(t, 300*time.Millisecond, "replica count after scale down", 1,
		func() int { return c.activeReplicas("web") })

	scale(0)
	waitForValue(t, 10*time.Second, "scale to zero", 0, func() int { return c.activeReplicas("web") })
	_ = d
}

// Capacity is finite. What does not fit must stay Pending with a real reason,
// and must be placed as soon as room appears.
func TestUnschedulableWorkloadsReportWhyAndRecover(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 1000, 2<<30)

	c.createDeployment("web", 3, 800, 1<<30)

	waitFor(t, 10*time.Second, "an unschedulable replica to be reported", func() bool {
		d, ok := c.store().Deployment("web")
		if !ok {
			return false
		}
		return d.Status.UnschedulableReplicas > 0
	})

	d, _ := c.store().Deployment("web")
	var pending *v1.Workload
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Status.Phase == v1.WorkloadPending {
			pending = w
		}
	}
	if pending == nil {
		t.Fatal("expected at least one pending replica")
	}
	if pending.Status.Placement == nil || len(pending.Status.Placement.Rejections) == 0 {
		t.Fatal("pending workload carries no explanation of why it could not be placed")
	}
	if pending.Status.Placement.Rejections[0].Filter != "ResourceFit" {
		t.Errorf("rejection blamed %s, expected ResourceFit",
			pending.Status.Placement.Rejections[0].Filter)
	}
	if d.Status.Phase != v1.DeploymentDegraded {
		t.Errorf("deployment with unschedulable replicas is %s, want Degraded", d.Status.Phase)
	}

	// Add capacity: the stuck replicas must be placed without intervention.
	c.addNode("worker-02", 8000, 16<<30)
	waitForValue(t, 15*time.Second, "the deployment to recover once capacity exists", 3,
		func() int { return c.readyReplicas("web") })
}

func TestServiceEndpointsFollowWorkloadHealth(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 8000, 16<<30)
	c.createDeployment("web", 3, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 3, func() int { return c.readyReplicas("web") })

	if _, err := c.cp.Apply(context.Background(), store.Command{
		Kind: store.CmdCreateService,
		Service: &v1.Service{
			ObjectMeta: v1.ObjectMeta{Name: "web-svc"},
			Spec: v1.ServiceSpec{
				Selector:   map[string]string{labelDeployment: "web"},
				Port:       8080,
				TargetPort: 80,
				Strategy:   v1.LBRoundRobin,
			},
		},
	}); err != nil {
		t.Fatalf("creating service: %v", err)
	}

	healthy := func() int {
		s, ok := c.store().Service("web-svc")
		if !ok {
			return -1
		}
		return s.Status.HealthyEndpoints
	}
	waitForValue(t, 10*time.Second, "all replicas to become endpoints", 3, healthy)

	// An unhealthy workload must leave the endpoint list.
	d, _ := c.store().Deployment("web")
	var target *v1.Workload
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Ready() {
			target = w
			break
		}
	}
	c.agent(target.Status.NodeName).MakeUnhealthy(target.Name)

	waitForValue(t, 10*time.Second, "the unhealthy replica to be removed from endpoints", 2, healthy)

	s, _ := c.store().Service("web-svc")
	for _, e := range s.Status.Endpoints {
		if e.WorkloadName == target.Name && e.Ready {
			t.Error("an unhealthy workload is still marked ready as an endpoint")
		}
		if e.Ready && e.Port == 0 {
			t.Errorf("endpoint %s has no port", e.WorkloadName)
		}
	}
}

func TestDeletingADeploymentRemovesEverything(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 8000, 16<<30)
	d := c.createDeployment("web", 3, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 3, func() int { return c.readyReplicas("web") })

	if _, err := c.cp.Apply(context.Background(), store.Command{
		Kind: store.CmdDeleteDeployment, Name: "web", Actor: "operator@example.com",
	}); err != nil {
		t.Fatalf("deleting deployment: %v", err)
	}

	waitFor(t, 15*time.Second, "the deployment and its workloads to be removed", func() bool {
		if _, ok := c.store().Deployment("web"); ok {
			return false
		}
		return len(c.store().WorkloadsOwnedBy(d.UID)) == 0
	})

	// The node's resource accounting must be released too.
	n, _ := c.store().Node("worker-01")
	if !n.Status.Allocated.IsZero() {
		t.Errorf("node still shows %s allocated after everything was deleted", n.Status.Allocated)
	}

	// And nothing recreates it.
	stayAt(t, 300*time.Millisecond, "workload count after deletion", 0,
		func() int { return len(c.store().WorkloadsOwnedBy(d.UID)) })
}

// A replica that cannot start must not consume the whole cluster in a retry
// storm, and the deployment must report the problem rather than hiding it.
func TestCrashLoopingReplicaIsReportedNotHidden(t *testing.T) {
	c := newTestCluster(t)
	agent := c.addNode("worker-01", 8000, 16<<30)
	c.createDeployment("web", 2, 500, 512<<20)

	// Every workload this agent is given fails to start.
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, w := range c.store().WorkloadsOnNode("worker-01") {
				agent.CrashOnStart(w.Name)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	waitFor(t, 10*time.Second, "failures to be recorded", func() bool {
		events := c.store().Events(store.EventQuery{Kind: "Workload", Limit: 50})
		for _, e := range events {
			if e.Reason == "WorkloadPhaseChanged" && e.Severity == v1.SeverityWarning {
				return true
			}
		}
		return false
	})

	// The deployment must not claim to be available.
	d, _ := c.store().Deployment("web")
	if d.Status.Phase == v1.DeploymentAvailable {
		t.Error("a deployment whose replicas cannot start reported itself Available")
	}
	if d.Status.AvailableReplicas != 0 {
		t.Errorf("available replicas = %d, want 0", d.Status.AvailableReplicas)
	}
}

// Losing leadership must stop the controllers, so two replicas never
// reconcile the same cluster at once.
func TestControllersStopWhenLeadershipIsLost(t *testing.T) {
	c := newTestCluster(t)
	c.addNode("worker-01", 4000, 8<<30)
	c.createDeployment("web", 2, 500, 512<<20)
	waitForValue(t, 10*time.Second, "initial replicas", 2, func() int { return c.readyReplicas("web") })

	if !c.manager.Running() {
		t.Fatal("controllers should be running while this replica leads")
	}
	c.cancel()
	<-c.done
	if c.manager.Running() {
		t.Error("controllers still running after the manager stopped")
	}
}
