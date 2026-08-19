package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/runtime"
	"google.golang.org/grpc"
)

// fakeControlPlane stands in for the control plane so the agent's behaviour can
// be tested against conditions that are hard to produce for real: a partition,
// a control plane that rejects the node, a node record that was recreated.
type fakeControlPlane struct {
	mu sync.Mutex

	assigned []*orionv1.AssignedWorkload
	accepted bool

	// unreachable makes every RPC fail, simulating a network partition.
	unreachable bool
	// syncErr fails only Sync.
	syncErr error

	heartbeatMs int64
	fenceMs     int64

	registers int
	syncs     int
	// reported is the last set of workload statuses the agent sent.
	reported []*orionv1.WorkloadStatus
}

func newFakeControlPlane() *fakeControlPlane {
	return &fakeControlPlane{accepted: true, heartbeatMs: 20, fenceMs: 200}
}

func (f *fakeControlPlane) Register(_ context.Context, _ *orionv1.RegisterRequest, _ ...grpc.CallOption) (*orionv1.RegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreachable {
		return nil, errors.New("connection refused")
	}
	f.registers++
	return &orionv1.RegisterResponse{
		NodeUid:             "node-uid-1",
		HeartbeatIntervalMs: f.heartbeatMs,
		SelfFenceTimeoutMs:  f.fenceMs,
		ClusterId:           "test",
	}, nil
}

func (f *fakeControlPlane) Sync(_ context.Context, req *orionv1.SyncRequest, _ ...grpc.CallOption) (*orionv1.SyncResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unreachable {
		return nil, errors.New("connection refused")
	}
	if f.syncErr != nil {
		return nil, f.syncErr
	}
	f.syncs++
	f.reported = req.GetWorkloads()
	return &orionv1.SyncResponse{
		Workloads:           f.assigned,
		Accepted:            f.accepted,
		HeartbeatIntervalMs: f.heartbeatMs,
		SelfFenceTimeoutMs:  f.fenceMs,
	}, nil
}

func (f *fakeControlPlane) Deregister(context.Context, *orionv1.DeregisterRequest, ...grpc.CallOption) (*orionv1.DeregisterResponse, error) {
	return &orionv1.DeregisterResponse{Accepted: true}, nil
}

func (f *fakeControlPlane) assign(specs ...*orionv1.AssignedWorkload) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assigned = specs
}

func (f *fakeControlPlane) partition() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable = true
}

func (f *fakeControlPlane) heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable = false
}

func (f *fakeControlPlane) statusOf(name string) *orionv1.WorkloadStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.reported {
		if s.GetName() == name {
			return s
		}
	}
	return nil
}

func (f *fakeControlPlane) registerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers
}

func spec(name string) *orionv1.AssignedWorkload {
	return &orionv1.AssignedWorkload{
		Name:                     name,
		Uid:                      name + "-uid",
		Image:                    "nginx:test",
		Request:                  &orionv1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20},
		RestartPolicy:            string(v1.RestartAlways),
		TerminationGracePeriodMs: 1000,
		DesiredState:             "Running",
		Ports:                    []*orionv1.Port{{Container: 80, Protocol: "tcp"}},
	}
}

type testAgent struct {
	*Agent
	rt     *runtime.Fake
	cp     *fakeControlPlane
	cancel context.CancelFunc
	done   chan struct{}
}

func startAgent(t *testing.T) *testAgent {
	t.Helper()
	rt := runtime.NewFake("worker-01")
	cp := newFakeControlPlane()
	log := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))

	a, err := New(Config{
		NodeName:          "worker-01",
		Address:           "worker-01:9100",
		HeartbeatInterval: 20 * time.Millisecond,
		SelfFenceTimeout:  200 * time.Millisecond,
		SyncTimeout:       50 * time.Millisecond,
		Logger:            log,
	}, rt, cp)
	if err != nil {
		t.Fatalf("creating agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ta := &testAgent{Agent: a, rt: rt, cp: cp, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(ta.done)
		if err := a.Run(ctx); err != nil {
			t.Errorf("agent exited with error: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-ta.done
	})
	waitFor(t, 3*time.Second, "the agent to register", func() bool { return cp.registerCount() > 0 })
	return ta
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Logf("%s", p); return len(p), nil }

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

// ---------------------------------------------------------------------------

func TestAgentStartsAssignedWorkloads(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"), spec("web-2"))

	waitFor(t, 3*time.Second, "both workloads to be running", func() bool { return a.rt.Running() == 2 })

	st := a.cp.statusOf("web-1")
	if st == nil {
		waitFor(t, 2*time.Second, "web-1 to be reported", func() bool { return a.cp.statusOf("web-1") != nil })
		st = a.cp.statusOf("web-1")
	}
	if st.GetPhase() != string(v1.WorkloadRunning) {
		t.Errorf("reported phase = %s, want Running", st.GetPhase())
	}
	if st.GetContainerId() == "" {
		t.Error("running workload reported no container ID")
	}
	// The published host port must be reported, or services cannot route to it.
	waitFor(t, 2*time.Second, "the host port to be reported", func() bool {
		s := a.cp.statusOf("web-1")
		return s != nil && s.GetHostPorts()[80] != 0
	})
}

// Anything the control plane stops listing must be removed. This is how a
// container orphaned by a crashed agent gets cleaned up.
func TestAgentRemovesWorkloadsNoLongerAssigned(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"), spec("web-2"))
	waitFor(t, 3*time.Second, "both workloads to start", func() bool { return a.rt.Running() == 2 })

	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "web-2 to be removed", func() bool { return a.rt.Total() == 1 })
	if a.rt.Running() != 1 {
		t.Errorf("%d containers running after unassignment, want 1", a.rt.Running())
	}
}

func TestAgentTerminatesOnDesiredStateTerminated(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	terminating := spec("web-1")
	terminating.DesiredState = "Terminated"
	a.cp.assign(terminating)

	waitFor(t, 3*time.Second, "the container to be removed", func() bool { return a.rt.Total() == 0 })
	waitFor(t, 2*time.Second, "Terminated to be reported", func() bool {
		s := a.cp.statusOf("web-1")
		return s != nil && s.GetPhase() == string(v1.WorkloadTerminated)
	})
}

// The self-fencing property: an agent that loses contact stops its workloads
// before the control plane's eviction deadline, so a partitioned node cannot
// keep running containers whose replacements have already started.
func TestAgentSelfFencesWhenIsolated(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"), spec("web-2"))
	waitFor(t, 3*time.Second, "workloads to start", func() bool { return a.rt.Running() == 2 })

	a.cp.partition()

	waitFor(t, 5*time.Second, "the agent to fence itself", a.Fenced)
	waitFor(t, 5*time.Second, "all containers to be stopped", func() bool { return a.rt.Running() == 0 })

	// Containers are stopped, not removed: a fence is reversible, and the
	// stopped containers are evidence for whoever investigates afterwards.
	if a.rt.Total() != 2 {
		t.Errorf("fencing removed containers (%d remain of 2); it should only stop them", a.rt.Total())
	}
}

// Fencing must not trigger on a single missed heartbeat.
func TestAgentDoesNotFenceOnBriefLoss(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	a.cp.partition()
	time.Sleep(60 * time.Millisecond) // well under the 200ms fence timeout
	a.cp.heal()

	time.Sleep(100 * time.Millisecond)
	if a.Fenced() {
		t.Error("the agent fenced itself over a brief connectivity loss")
	}
	if a.rt.Running() != 1 {
		t.Errorf("%d containers running after a brief loss, want 1", a.rt.Running())
	}
}

// Once contact returns, the fence lifts and workloads come back without
// operator intervention.
func TestAgentRecoversAfterFencing(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	a.cp.partition()
	waitFor(t, 5*time.Second, "the agent to fence itself", a.Fenced)
	waitFor(t, 5*time.Second, "the container to stop", func() bool { return a.rt.Running() == 0 })

	a.cp.heal()
	waitFor(t, 5*time.Second, "the fence to lift", func() bool { return !a.Fenced() })
	waitFor(t, 5*time.Second, "the workload to run again", func() bool { return a.rt.Running() == 1 })
}

// A container that dies must be restarted in place under RestartAlways, with
// the restart counted.
func TestAgentRestartsCrashedContainer(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	a.rt.SetState(runtime.ContainerName("web-1", "web-1-uid"), runtime.StateExited, 1)

	waitFor(t, 5*time.Second, "the container to be restarted", func() bool {
		return a.rt.CallCount("Restart") > 0 && a.rt.Running() == 1
	})
	waitFor(t, 3*time.Second, "the restart to be reported", func() bool {
		s := a.cp.statusOf("web-1")
		return s != nil && s.GetRestartCount() > 0
	})
}

// RestartNever means a clean exit is Succeeded, not an endless restart loop.
func TestAgentHonoursRestartNever(t *testing.T) {
	a := startAgent(t)
	s := spec("web-1")
	s.RestartPolicy = string(v1.RestartNever)
	a.cp.assign(s)
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	a.rt.SetState(runtime.ContainerName("web-1", "web-1-uid"), runtime.StateExited, 0)

	waitFor(t, 3*time.Second, "Succeeded to be reported", func() bool {
		st := a.cp.statusOf("web-1")
		return st != nil && st.GetPhase() == string(v1.WorkloadSucceeded)
	})
	time.Sleep(100 * time.Millisecond)
	if n := a.rt.CallCount("Restart"); n != 0 {
		t.Errorf("a RestartNever workload was restarted %d times", n)
	}
}

// An operator running `docker rm` on an Orion container must not leave the
// workload reported as Running forever.
func TestAgentRecreatesVanishedContainer(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	a.rt.VanishContainer(runtime.ContainerName("web-1", "web-1-uid"))
	waitFor(t, 5*time.Second, "the container to be recreated", func() bool { return a.rt.Running() == 1 })
	if a.rt.CallCount("Create") < 2 {
		t.Errorf("expected a second Create after the container vanished, got %d", a.rt.CallCount("Create"))
	}
}

// A workload deleted and recreated with the same name is a different object;
// the agent must not adopt the old container for it.
func TestAgentDoesNotAdoptContainerOfARecreatedWorkload(t *testing.T) {
	a := startAgent(t)
	a.cp.assign(spec("web-1"))
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return a.rt.Running() == 1 })

	recreated := spec("web-1")
	recreated.Uid = "web-1-uid-v2"
	a.cp.assign(recreated)

	waitFor(t, 5*time.Second, "the new container to exist", func() bool {
		return a.rt.CallCount("Create") >= 2
	})
	waitFor(t, 3*time.Second, "the old container to be removed", func() bool { return a.rt.Total() == 1 })
}

// A failed image pull must back off rather than hammering the registry.
func TestAgentBacksOffOnImagePullFailure(t *testing.T) {
	a := startAgent(t)
	a.rt.FailPull("nginx:test", errors.New("manifest unknown"))
	a.cp.assign(spec("web-1"))

	waitFor(t, 3*time.Second, "the pull failure to be reported", func() bool {
		st := a.cp.statusOf("web-1")
		return st != nil && st.GetReason() == "ImagePullFailed"
	})

	pullsAfterFirstFailure := a.rt.CallCount("PullImage")
	time.Sleep(200 * time.Millisecond)
	pulls := a.rt.CallCount("PullImage")
	// With a 20ms heartbeat, an un-throttled agent would try ~10 more times.
	if pulls-pullsAfterFirstFailure > 2 {
		t.Errorf("image pull retried %d times in 200ms; backoff is not working", pulls-pullsAfterFirstFailure)
	}
	if a.rt.Total() != 0 {
		t.Error("a container was created despite the image pull failing")
	}
}

// The control plane telling the agent it is unknown must trigger
// re-registration, not a silent stall.
func TestAgentReRegistersWhenRejected(t *testing.T) {
	a := startAgent(t)
	before := a.cp.registerCount()

	a.cp.mu.Lock()
	a.cp.accepted = false
	a.cp.mu.Unlock()

	waitFor(t, 3*time.Second, "the agent to re-register", func() bool { return a.cp.registerCount() > before })

	a.cp.mu.Lock()
	a.cp.accepted = true
	a.cp.mu.Unlock()
}

// A restarted agent must find the containers it left behind rather than
// creating duplicates.
func TestAgentAdoptsContainersAfterRestart(t *testing.T) {
	rt := runtime.NewFake("worker-01")
	cp := newFakeControlPlane()
	cp.assign(spec("web-1"))
	log := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		NodeName: "worker-01", Address: "worker-01:9100",
		HeartbeatInterval: 20 * time.Millisecond,
		SelfFenceTimeout:  200 * time.Millisecond,
		SyncTimeout:       50 * time.Millisecond,
		Logger:            log,
	}

	first, err := New(cfg, rt, cp)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); first.Run(ctx) }()
	waitFor(t, 3*time.Second, "the workload to start", func() bool { return rt.Running() == 1 })
	createsBefore := rt.CallCount("Create")
	cancel()
	<-done

	// Same engine, new agent process: the container is still there.
	second, err := New(cfg, rt, cp)
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan struct{})
	go func() { defer close(done2); second.Run(ctx2) }()
	t.Cleanup(func() { cancel2(); <-done2 })

	waitFor(t, 3*time.Second, "the new agent to sync", func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		return cp.syncs > 0
	})
	time.Sleep(100 * time.Millisecond)

	if rt.Total() != 1 {
		t.Errorf("%d containers exist after an agent restart, want 1", rt.Total())
	}
	if got := rt.CallCount("Create"); got != createsBefore {
		t.Errorf("the restarted agent created %d new containers; it should have adopted the existing one",
			got-createsBefore)
	}
	if rt.Running() != 1 {
		t.Errorf("%d containers running after restart, want 1", rt.Running())
	}
}

func TestAgentRejectsUnsafeConfiguration(t *testing.T) {
	rt := runtime.NewFake("worker-01")
	// A fence timeout at or below the heartbeat interval would fence on a
	// single missed beat.
	_, err := New(Config{
		NodeName:          "worker-01",
		HeartbeatInterval: time.Second,
		SelfFenceTimeout:  time.Second,
	}, rt, newFakeControlPlane())
	if err == nil {
		t.Fatal("expected a self-fence timeout equal to the heartbeat interval to be rejected")
	}

	_, err = New(Config{HeartbeatInterval: time.Second, SelfFenceTimeout: 10 * time.Second}, rt, newFakeControlPlane())
	if err == nil {
		t.Fatal("expected a missing node name to be rejected")
	}
}

func TestAgentReportsRuntimeUnavailableAsANodeCondition(t *testing.T) {
	a := startAgent(t)
	waitFor(t, 2*time.Second, "the first sync", func() bool {
		a.cp.mu.Lock()
		defer a.cp.mu.Unlock()
		return a.cp.syncs > 0
	})

	a.rt.SetUnavailable(true)

	// The agent must keep heartbeating even with a dead engine: a missed
	// heartbeat would get the node evicted for the wrong reason.
	a.cp.mu.Lock()
	before := a.cp.syncs
	a.cp.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	a.cp.mu.Lock()
	after := a.cp.syncs
	a.cp.mu.Unlock()
	if after <= before {
		t.Error("the agent stopped heartbeating when the container engine became unavailable")
	}
}
