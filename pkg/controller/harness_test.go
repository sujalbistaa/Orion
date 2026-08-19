package controller

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/raft"
	"github.com/sujalbistaa/orion/pkg/raft/transport"
	"github.com/sujalbistaa/orion/pkg/scheduler"
	"github.com/sujalbistaa/orion/pkg/store"
)

// testCluster runs a real single-replica control plane — real Raft, real
// storage, real controllers on real goroutines — with simulated node agents.
// The agents are simulated rather than the control plane because these tests
// are about reconciliation, not about Docker; the agent's own behaviour is
// tested against a real runtime in pkg/agent.
type testCluster struct {
	t       *testing.T
	cp      *controlplane.ControlPlane
	manager *Manager
	cancel  context.CancelFunc
	done    chan struct{}

	Scheduling *SchedulingController
	Deployment *DeploymentController
	NodeLife   *NodeLifecycleController
	Endpoints  *EndpointController
	GC         *GarbageCollector

	mu     sync.Mutex
	agents map[string]*fakeAgent
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	log := slog.New(slog.NewTextHandler(testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))
	st := store.New()
	lb := transport.NewLoopback()

	cp, err := controlplane.New(controlplane.Options{
		NodeID: 1,
		Peers:  map[uint64]string{1: "local"},
		Raft: raft.Config{
			ID:            1,
			Peers:         []uint64{1},
			ElectionTick:  3,
			HeartbeatTick: 1,
			Storage:       raft.NewMemoryStorage(),
			Logger:        log,
		},
		Transport:      lb.For(1),
		Store:          st,
		TickInterval:   2 * time.Millisecond,
		ProposeTimeout: 5 * time.Second,
		Logger:         log,
	})
	if err != nil {
		t.Fatalf("creating control plane: %v", err)
	}
	lb.Register(1, cp.Raft().Step)
	cp.Start()

	// A single voter still has to win an election before it accepts writes.
	waitFor(t, 5*time.Second, "control plane to become leader", cp.IsLeader)

	c := &testCluster{t: t, cp: cp, agents: map[string]*fakeAgent{}, done: make(chan struct{})}

	c.Scheduling = NewSchedulingController(cp, scheduler.New(), log, nil)
	c.Deployment = NewDeploymentController(cp, log, nil)
	c.NodeLife = NewNodeLifecycleController(cp, log, nil)
	c.Endpoints = NewEndpointController(cp, log)
	c.GC = NewGarbageCollector(cp, log)

	// Tight intervals: these tests assert convergence, and waiting three
	// seconds per pass would make the suite unusable.
	c.Scheduling.Interval = 20 * time.Millisecond
	c.Deployment.Interval = 20 * time.Millisecond
	c.NodeLife.Interval = 20 * time.Millisecond
	c.NodeLife.HeartbeatTimeout = 200 * time.Millisecond
	c.NodeLife.EvictionDelay = 100 * time.Millisecond
	c.Endpoints.Interval = 20 * time.Millisecond
	c.GC.Interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.manager = NewManager(ManagerOptions{
		Logger:      log,
		Leadership:  cp.LeadershipChanges(),
		IsLeader:    cp.IsLeader,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	})
	c.manager.Register(c.Scheduling, c.Deployment, c.NodeLife, c.Endpoints, c.GC)

	go func() {
		defer close(c.done)
		c.manager.Run(ctx)
	}()
	waitFor(t, 5*time.Second, "controllers to start", c.manager.Running)

	t.Cleanup(c.stop)
	return c
}

func (c *testCluster) stop() {
	c.mu.Lock()
	for _, a := range c.agents {
		a.stop()
	}
	c.agents = map[string]*fakeAgent{}
	c.mu.Unlock()

	c.cancel()
	<-c.done
	c.cp.Stop()
}

func (c *testCluster) store() *store.Store { return c.cp.Store() }

// addNode registers a node and starts a simulated agent for it.
func (c *testCluster) addNode(name string, cpu v1.MilliCPU, mem v1.Bytes) *fakeAgent {
	c.t.Helper()
	ctx := context.Background()
	_, err := c.cp.Apply(ctx, store.Command{
		Kind: store.CmdRegisterNode,
		Node: &v1.Node{
			ObjectMeta: v1.ObjectMeta{Name: name, Labels: map[string]string{"zone": "a"}},
			Spec:       v1.NodeSpec{Address: name + ".test:9100"},
			Status: v1.NodeStatus{
				Capacity:    v1.Resources{CPU: cpu, Memory: mem},
				Allocatable: v1.Resources{CPU: cpu, Memory: mem},
			},
		},
	})
	if err != nil {
		c.t.Fatalf("registering node %s: %v", name, err)
	}
	a := newFakeAgent(c.t, c.cp, name)
	c.mu.Lock()
	c.agents[name] = a
	c.mu.Unlock()
	a.start()
	return a
}

func (c *testCluster) agent(name string) *fakeAgent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agents[name]
}

// killNode stops a node's agent without deregistering it: exactly what a
// machine losing power looks like to the control plane.
func (c *testCluster) killNode(name string) {
	c.mu.Lock()
	a := c.agents[name]
	delete(c.agents, name)
	c.mu.Unlock()
	if a != nil {
		a.stop()
	}
}

func (c *testCluster) createDeployment(name string, replicas int32, cpu v1.MilliCPU, mem v1.Bytes) *v1.Deployment {
	c.t.Helper()
	d := &v1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: name, Labels: map[string]string{"app": name}},
		Spec: v1.DeploymentSpec{
			Replicas: replicas,
			Template: v1.WorkloadSpec{
				Image:         "nginx:1.27-alpine",
				RestartPolicy: v1.RestartAlways,
				Ports:         []v1.Port{{Container: 80, Protocol: "tcp"}},
				Resources:     v1.ResourceSpec{Request: v1.Resources{CPU: cpu, Memory: mem}},
			},
			Strategy: v1.Strategy{Kind: v1.StrategyRolling, MaxSurge: 1},
		},
	}
	v1.SetDeploymentDefaults(d)
	res, err := c.cp.Apply(context.Background(), store.Command{Kind: store.CmdCreateDeployment, Deployment: d})
	if err != nil {
		c.t.Fatalf("creating deployment %s: %v", name, err)
	}
	return res.Deployment
}

// readyReplicas counts running, healthy replicas of a deployment.
func (c *testCluster) readyReplicas(deployment string) int {
	d, ok := c.store().Deployment(deployment)
	if !ok {
		return 0
	}
	n := 0
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Ready() {
			n++
		}
	}
	return n
}

// activeReplicas counts replicas that exist and have not finished.
func (c *testCluster) activeReplicas(deployment string) int {
	d, ok := c.store().Deployment(deployment)
	if !ok {
		return 0
	}
	n := 0
	for _, w := range c.store().WorkloadsOwnedBy(d.UID) {
		if w.Status.Phase.Active() && w.DeletedAt == nil {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Simulated node agent
// ---------------------------------------------------------------------------

// fakeAgent drives workloads through their lifecycle the way a real agent
// would: it polls its assignment, starts what it has been given, reports
// health, and stops what has been marked for deletion. It never invents state
// the control plane did not ask for.
type fakeAgent struct {
	t    *testing.T
	cp   *controlplane.ControlPlane
	node string

	stopC chan struct{}
	done  chan struct{}
	once  sync.Once

	mu sync.Mutex
	// crashOnStart names workloads whose containers fail immediately.
	crashOnStart map[string]bool
	// unhealthy names workloads that run but never pass their health check.
	unhealthy map[string]bool
	// paused stops the agent reporting anything, simulating a hung agent.
	paused bool
	// restarts counts in-place restarts, mirroring a real restart policy.
	restarts map[string]int32
}

func newFakeAgent(t *testing.T, cp *controlplane.ControlPlane, node string) *fakeAgent {
	return &fakeAgent{
		t: t, cp: cp, node: node,
		stopC:        make(chan struct{}),
		done:         make(chan struct{}),
		crashOnStart: map[string]bool{},
		unhealthy:    map[string]bool{},
		restarts:     map[string]int32{},
	}
}

func (a *fakeAgent) start() { go a.run() }

func (a *fakeAgent) stop() {
	a.once.Do(func() { close(a.stopC) })
	<-a.done
}

func (a *fakeAgent) CrashOnStart(workload string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.crashOnStart[workload] = true
}

func (a *fakeAgent) MakeUnhealthy(workload string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.unhealthy[workload] = true
}

func (a *fakeAgent) Pause() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.paused = true
}

func (a *fakeAgent) run() {
	defer close(a.done)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopC:
			return
		case <-ticker.C:
			a.mu.Lock()
			paused := a.paused
			a.mu.Unlock()
			if paused {
				continue
			}
			a.heartbeat()
			a.syncWorkloads()
		}
	}
}

func (a *fakeAgent) heartbeat() {
	node, ok := a.cp.Store().Node(a.node)
	if !ok {
		return
	}
	status := node.Status
	_, _ = a.cp.Apply(context.Background(), store.Command{
		Kind:       store.CmdUpdateNodeStatus,
		Name:       a.node,
		UID:        node.UID,
		NodeStatus: &status,
	})
}

func (a *fakeAgent) syncWorkloads() {
	ctx := context.Background()
	for _, w := range a.cp.Store().AssignedWorkloads(a.node) {
		a.mu.Lock()
		crash := a.crashOnStart[w.Name]
		unhealthy := a.unhealthy[w.Name]
		a.mu.Unlock()

		status := w.Status.DeepCopy()
		switch {
		case w.DeletedAt != nil && w.Status.Phase != v1.WorkloadTerminated:
			status.Phase = v1.WorkloadTerminated
			status.Health = v1.HealthUnknown
			status.Reason = "Stopped"

		case w.Status.Phase == v1.WorkloadScheduled:
			status.Phase = v1.WorkloadStarting
			status.ContainerID = "sim-" + w.UID

		case w.Status.Phase == v1.WorkloadStarting:
			if crash {
				status.Phase = v1.WorkloadFailed
				code := int32(1)
				status.ExitCode = &code
				status.Reason = "ContainerCannotRun"
				break
			}
			status.Phase = v1.WorkloadRunning
			status.Health = v1.HealthHealthy
			if unhealthy {
				status.Health = v1.HealthUnhealthy
			}
			// Publish the port mapping the endpoint controller needs.
			status.HostPorts = map[int32]int32{80: 30000 + int32(len(w.UID)%1000)}
			now := time.Now().UTC()
			status.StartedAt = &now

		case w.Status.Phase == v1.WorkloadRunning:
			want := v1.HealthHealthy
			if unhealthy {
				want = v1.HealthUnhealthy
			}
			if status.Health == want {
				continue
			}
			status.Health = want

		default:
			continue
		}

		_, _ = a.cp.Apply(ctx, store.Command{
			Kind:           store.CmdUpdateWorkloadStatus,
			Name:           w.Name,
			UID:            w.UID,
			WorkloadStatus: &status,
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// waitFor polls until cond holds, failing the test with a description if it
// never does. Polling is used rather than sleeping so a fast machine does not
// wait and a slow one does not flake.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// waitForValue is waitFor with the observed value reported on failure.
func waitForValue(t *testing.T, timeout time.Duration, what string, want int, got func() int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		last = got()
		if last == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s: got %d, want %d", timeout, what, last, want)
}

// stayAt asserts a value holds steady for a window, catching controllers that
// converge and then oscillate.
func stayAt(t *testing.T, d time.Duration, what string, want int, got func() int) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if v := got(); v != want {
			t.Fatalf("%s did not hold steady: got %d, want %d", what, v, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
