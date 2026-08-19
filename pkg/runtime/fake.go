package runtime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Fake is an in-memory container engine for tests.
//
// It exists so the agent's reconciliation logic can be tested exhaustively —
// including failure modes that are hard to provoke in Docker, like an image
// pull that hangs or a container that vanishes between two inspections —
// without needing a daemon. It is never used by orion-agent; the Docker
// implementation is exercised by the conformance tests in docker_test.go.
type Fake struct {
	mu         sync.Mutex
	containers map[string]*ContainerStatus
	byName     map[string]string
	nextID     int
	images     map[string]bool
	nodeName   string

	// Injected faults. They are set through methods rather than exported
	// fields because tests change them while the agent is running, and an
	// unsynchronized field would be a data race in the test harness itself.
	failPull    map[string]error
	failCreate  error
	failStart   error
	unavailable bool
	pullDelay   time.Duration
	autoExit    *int32

	info NodeInfo

	// calls records the operations performed, so tests can assert the agent
	// did not, for example, recreate a container it should have left alone.
	calls []string
}

// FailPull makes pulling a specific image fail.
func (f *Fake) FailPull(image string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPull[image] = err
}

// FailCreate makes every container creation fail.
func (f *Fake) FailCreate(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failCreate = err
}

// FailStart makes every container start fail.
func (f *Fake) FailStart(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failStart = err
}

// SetUnavailable simulates the container engine going away entirely.
func (f *Fake) SetUnavailable(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailable = v
}

// SetPullDelay simulates a slow registry.
func (f *Fake) SetPullDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pullDelay = d
}

// SetAutoExit makes started containers exit immediately with the given code.
func (f *Fake) SetAutoExit(code *int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.autoExit = code
}

// SetInfo overrides the reported machine capacity.
func (f *Fake) SetInfo(i NodeInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.info = i
}

var _ Runtime = (*Fake)(nil)

func NewFake(nodeName string) *Fake {
	return &Fake{
		containers: map[string]*ContainerStatus{},
		byName:     map[string]string{},
		images:     map[string]bool{},
		nodeName:   nodeName,
		failPull:   map[string]error{},
		info: NodeInfo{
			RuntimeName: "fake", RuntimeVersion: "test", OS: "linux", Arch: "arm64",
			Hostname: nodeName, CPUs: 4, MemoryBytes: 8 << 30,
		},
	}
}

func (f *Fake) record(op string, args ...string) {
	f.calls = append(f.calls, op+"("+strings.Join(args, ",")+")")
}

// CallCount returns how many times an operation was invoked.
func (f *Fake) CallCount(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, op+"(") {
			n++
		}
	}
	return n
}

func (f *Fake) Name() string { return "fake" }

func (f *Fake) Close() error { return nil }

func (f *Fake) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ErrUnavailable
	}
	return nil
}

func (f *Fake) Info(context.Context) (NodeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return NodeInfo{}, ErrUnavailable
	}
	return f.info, nil
}

func (f *Fake) PullImage(ctx context.Context, ref string) error {
	f.mu.Lock()
	if f.unavailable {
		f.mu.Unlock()
		return ErrUnavailable
	}
	f.record("PullImage", ref)
	err := f.failPull[ref]
	delay := f.pullDelay
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.images[ref] = true
	f.mu.Unlock()
	return nil
}

func (f *Fake) Create(_ context.Context, spec ContainerSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return "", ErrUnavailable
	}
	f.record("Create", spec.Name)
	if f.failCreate != nil {
		return "", f.failCreate
	}
	if _, exists := f.byName[spec.Name]; exists {
		return "", fmt.Errorf("%w: %s", ErrAlreadyExists, spec.Name)
	}

	f.nextID++
	id := fmt.Sprintf("fake%08d", f.nextID)
	labels := map[string]string{LabelManagedBy: ManagedByValue, LabelNode: f.nodeName}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	ports := map[int32]int32{}
	for _, p := range spec.Ports {
		host := p.HostPort
		if host == 0 {
			// Mirror the engine's ephemeral allocation.
			host = int32(32768 + f.nextID)
		}
		ports[p.ContainerPort] = host
	}
	f.containers[id] = &ContainerStatus{
		ID: id, Name: spec.Name, Image: spec.Image,
		State: StateCreated, Labels: labels, Ports: ports,
	}
	f.byName[spec.Name] = id
	return id, nil
}

func (f *Fake) Start(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ErrUnavailable
	}
	f.record("Start", id)
	if f.failStart != nil {
		return f.failStart
	}
	c, ok := f.containers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if f.autoExit != nil {
		c.State = StateExited
		c.ExitCode = *f.autoExit
		c.StartedAt = time.Now()
		c.FinishedAt = time.Now()
		return nil
	}
	c.State = StateRunning
	c.StartedAt = time.Now()
	return nil
}

func (f *Fake) Stop(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ErrUnavailable
	}
	f.record("Stop", id)
	c, ok := f.containers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	c.State = StateExited
	c.FinishedAt = time.Now()
	return nil
}

func (f *Fake) Restart(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ErrUnavailable
	}
	f.record("Restart", id)
	c, ok := f.containers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	c.State = StateRunning
	c.RestartCount++
	c.ExitCode = 0
	c.StartedAt = time.Now()
	return nil
}

func (f *Fake) Remove(_ context.Context, id string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ErrUnavailable
	}
	f.record("Remove", id)
	c, ok := f.containers[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	delete(f.containers, id)
	delete(f.byName, c.Name)
	return nil
}

func (f *Fake) Inspect(_ context.Context, id string) (ContainerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return ContainerStatus{}, ErrUnavailable
	}
	c, ok := f.containers[id]
	if !ok {
		return ContainerStatus{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return *c, nil
}

func (f *Fake) List(_ context.Context, labels map[string]string) ([]ContainerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return nil, ErrUnavailable
	}
	var out []ContainerStatus
	for _, c := range f.containers {
		match := true
		for k, v := range labels {
			if c.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, *c)
		}
	}
	return out, nil
}

func (f *Fake) Stats(_ context.Context, id string) (Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return Stats{}, ErrUnavailable
	}
	if _, ok := f.containers[id]; !ok {
		return Stats{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return Stats{CPUMillis: 100, MemoryBytes: 64 << 20, SampledAt: time.Now()}, nil
}

func (f *Fake) Logs(_ context.Context, id string, _ LogOptions) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[id]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return io.NopCloser(strings.NewReader("fake log output for " + id + "\n")), nil
}

// SetState forces a container into a state, so tests can simulate a container
// crashing without going through Start/Stop.
func (f *Fake) SetState(name string, state ContainerState, exitCode int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byName[name]
	if !ok {
		return
	}
	c := f.containers[id]
	c.State = state
	c.ExitCode = exitCode
	if state.Terminal() {
		c.FinishedAt = time.Now()
	}
}

// VanishContainer removes a container behind the agent's back, simulating an
// operator running `docker rm` on a container Orion owns.
func (f *Fake) VanishContainer(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.byName[name]; ok {
		delete(f.containers, id)
		delete(f.byName, name)
	}
}

// Running returns how many containers are in the running state.
func (f *Fake) Running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.containers {
		if c.State == StateRunning {
			n++
		}
	}
	return n
}

// Total returns how many containers exist in any state.
func (f *Fake) Total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.containers)
}
