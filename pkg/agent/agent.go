// Package agent is the Orion node agent: the process that runs on every
// machine, reports its capacity, and makes the containers on that machine match
// what the control plane says should be there.
//
// The agent holds no durable local state. After a restart it asks the container
// engine what it owns — every Orion container carries ownership labels — and
// reconciles from there. A local database would be one more thing that can
// disagree with reality; the engine is already the authority on what is
// running, so it is used as one.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/runtime"
)

// Config configures the agent.
type Config struct {
	// NodeName is this node's cluster identity. It must be stable across
	// restarts: it is how the control plane recognizes a returning node.
	NodeName string
	// Address is host:port where this agent serves its local API, used by the
	// control plane to fetch logs and by services to reach published ports.
	Address string
	Labels  map[string]string

	// ReservedCPU and ReservedMemory are held back from Allocatable for the
	// agent, the container engine and the operating system. Scheduling against
	// full machine capacity is how nodes get pushed into swap and die.
	ReservedCPU    v1.MilliCPU
	ReservedMemory v1.Bytes

	// HeartbeatInterval is the initial value; the control plane may override it
	// at registration so the whole cluster agrees on the failure-detection
	// window.
	HeartbeatInterval time.Duration

	// SelfFenceTimeout is how long the agent may run without reaching the
	// control plane before stopping its own workloads. See the Fence method.
	SelfFenceTimeout time.Duration

	// SyncTimeout bounds one Sync RPC. It must be well under the heartbeat
	// interval so a hung call does not silently skip heartbeats.
	SyncTimeout time.Duration

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 3 * time.Second
	}
	if c.SelfFenceTimeout <= 0 {
		c.SelfFenceTimeout = 20 * time.Second
	}
	if c.SyncTimeout <= 0 {
		c.SyncTimeout = 2 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.ReservedCPU == 0 {
		c.ReservedCPU = 500 // half a core
	}
	if c.ReservedMemory == 0 {
		c.ReservedMemory = 512 << 20
	}
}

// Agent runs on a node.
type Agent struct {
	cfg Config
	rt  runtime.Runtime
	log *slog.Logger

	// client is the control-plane connection. It is an interface so tests can
	// substitute a control plane that partitions, stalls or rejects.
	client orionv1.NodeServiceClient

	startedAt time.Time
	nodeUID   atomic.Value // string

	mu        sync.Mutex
	workloads map[string]*managedWorkload

	// lastContact is when the control plane was last reached. Self-fencing is
	// driven entirely by this value.
	lastContact atomic.Int64 // unix nanos
	fenced      atomic.Bool

	// capacity is discovered once at startup from the engine.
	capacity    v1.Resources
	allocatable v1.Resources
	runtimeInfo v1.RuntimeInfo

	stopOnce sync.Once
	stopC    chan struct{}
	doneC    chan struct{}

	// metrics is optional.
	metrics Metrics
}

// Metrics is implemented by the telemetry package; nil is allowed.
type Metrics interface {
	SyncCompleted(err error, d time.Duration)
	ContainerOperation(op string, err error, d time.Duration)
	WorkloadRestarted(workload string)
	Fenced(active bool)
}

func New(cfg Config, rt runtime.Runtime, client orionv1.NodeServiceClient) (*Agent, error) {
	cfg.setDefaults()
	if cfg.NodeName == "" {
		return nil, errors.New("agent: NodeName is required")
	}
	if cfg.SelfFenceTimeout <= cfg.HeartbeatInterval {
		return nil, fmt.Errorf("agent: SelfFenceTimeout (%s) must exceed HeartbeatInterval (%s)",
			cfg.SelfFenceTimeout, cfg.HeartbeatInterval)
	}
	a := &Agent{
		cfg:       cfg,
		rt:        rt,
		client:    client,
		log:       cfg.Logger.With("component", "agent", "node", cfg.NodeName),
		startedAt: time.Now().UTC(),
		workloads: map[string]*managedWorkload{},
		stopC:     make(chan struct{}),
		doneC:     make(chan struct{}),
	}
	a.nodeUID.Store("")
	a.lastContact.Store(time.Now().UnixNano())
	return a, nil
}

// SetMetrics installs a metrics sink.
func (a *Agent) SetMetrics(m Metrics) { a.metrics = m }

// Run blocks until ctx is cancelled or Stop is called.
func (a *Agent) Run(ctx context.Context) error {
	defer close(a.doneC)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-a.stopC:
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := a.discoverCapacity(ctx); err != nil {
		return err
	}
	a.log.Info("node capacity discovered",
		"capacity", a.capacity.String(), "allocatable", a.allocatable.String(),
		"runtime", a.runtimeInfo.Name+" "+a.runtimeInfo.Version)

	// Adopt containers left behind by a previous agent process before the first
	// sync, so the control plane's first view of this node is accurate rather
	// than showing everything as missing and triggering a needless rebuild.
	if err := a.adoptExistingContainers(ctx); err != nil {
		a.log.Warn("could not enumerate existing containers", "err", err)
	}

	if err := a.registerWithRetry(ctx); err != nil {
		return err
	}

	// The control plane owns the heartbeat interval and may change it, so the
	// ticker is rebuilt whenever the configured value moves rather than being
	// fixed at startup.
	interval := a.cfg.HeartbeatInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return nil
		case <-ticker.C:
			a.tick(ctx)
			if a.cfg.HeartbeatInterval != interval {
				interval = a.cfg.HeartbeatInterval
				ticker.Reset(interval)
				a.log.Info("heartbeat interval updated by the control plane", "interval", interval)
			}
		}
	}
}

// Stop asks the agent to shut down and waits for it.
func (a *Agent) Stop() {
	a.stopOnce.Do(func() { close(a.stopC) })
	<-a.doneC
}

func (a *Agent) tick(ctx context.Context) {
	// A panic in reconciliation must not take down the agent: the containers it
	// manages keep running, and a crashed agent means the control plane evicts
	// them for no reason.
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("recovered from panic in agent tick", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	start := time.Now()
	observed := a.observe(ctx)

	syncCtx, cancel := context.WithTimeout(ctx, a.cfg.SyncTimeout)
	resp, err := a.sync(syncCtx, observed)
	cancel()

	if a.metrics != nil {
		a.metrics.SyncCompleted(err, time.Since(start))
	}

	if err != nil {
		a.handleSyncFailure(ctx, err)
		return
	}

	a.lastContact.Store(time.Now().UnixNano())
	if a.fenced.Swap(false) {
		a.log.Warn("control plane reachable again; lifting fence")
		if a.metrics != nil {
			a.metrics.Fenced(false)
		}
	}

	if !resp.GetAccepted() {
		// The control plane does not know this node — its record was deleted,
		// or it is talking to a cluster that has been rebuilt. Re-register
		// rather than continuing to run workloads nobody has asked for.
		a.log.Warn("control plane rejected this node; re-registering",
			"reason", resp.GetRejectReason())
		if err := a.registerWithRetry(ctx); err != nil {
			a.log.Error("re-registration failed", "err", err)
		}
		return
	}

	a.applyAssignment(ctx, resp.GetWorkloads())
}

// handleSyncFailure decides whether a failed sync means the agent should fence
// itself.
//
// This is the safety mechanism that makes the control plane's eviction sound.
// When a node stops heartbeating, the control plane eventually declares its
// workloads gone and schedules replacements. It cannot tell a dead machine from
// a partitioned one. So the agent takes responsibility for the case it *can*
// observe: if it has not reached the control plane for SelfFenceTimeout, it
// stops everything it is running. Because SelfFenceTimeout is shorter than the
// control plane's eviction deadline, a partitioned node has stopped its
// containers before their replacements start.
func (a *Agent) handleSyncFailure(ctx context.Context, cause error) {
	silence := time.Since(time.Unix(0, a.lastContact.Load()))

	if silence < a.cfg.SelfFenceTimeout {
		a.log.Warn("sync failed", "err", cause, "silence", silence.Round(time.Millisecond),
			"fenceIn", (a.cfg.SelfFenceTimeout - silence).Round(time.Millisecond))
		return
	}
	if a.fenced.Load() {
		return // already fenced; nothing further to do until contact returns
	}

	a.fenced.Store(true)
	if a.metrics != nil {
		a.metrics.Fenced(true)
	}
	a.log.Error("self-fencing: no contact with the control plane",
		"silence", silence.Round(time.Second), "timeout", a.cfg.SelfFenceTimeout,
		"reason", "the control plane may already have replaced these workloads elsewhere")

	a.fenceAll(ctx)
}

// fenceAll stops every managed container. It does not remove them: if contact
// returns and the control plane still wants them, restarting a stopped
// container is cheaper than a full recreate, and keeping the container makes
// the incident visible on the node afterwards.
func (a *Agent) fenceAll(ctx context.Context) {
	a.mu.Lock()
	targets := make([]*managedWorkload, 0, len(a.workloads))
	for _, w := range a.workloads {
		targets = append(targets, w)
	}
	a.mu.Unlock()

	for _, w := range targets {
		if w.containerID == "" {
			continue
		}
		stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := a.rt.Stop(stopCtx, w.containerID, w.terminationGrace())
		cancel()
		if err != nil && !errors.Is(err, runtime.ErrNotFound) {
			a.log.Error("could not stop workload while fencing", "workload", w.name, "err", err)
			continue
		}
		w.setPhase(v1.WorkloadFailed, "Fenced",
			"stopped by the node agent after losing contact with the control plane")
		a.log.Warn("fenced workload", "workload", w.name)
	}
}

// Fenced reports whether the agent has stopped its workloads due to isolation.
func (a *Agent) Fenced() bool { return a.fenced.Load() }

// LastContact returns when the control plane was last reached.
func (a *Agent) LastContact() time.Time { return time.Unix(0, a.lastContact.Load()) }

func (a *Agent) discoverCapacity(ctx context.Context) error {
	info, err := a.rt.Info(ctx)
	if err != nil {
		return fmt.Errorf("agent: cannot reach the container engine: %w", err)
	}
	a.runtimeInfo = v1.RuntimeInfo{
		Name: info.RuntimeName, Version: info.RuntimeVersion,
		OS: info.OS, Arch: info.Arch, KernelVersion: info.KernelVersion,
	}
	a.capacity = v1.Resources{
		CPU:    v1.MilliCPU(info.CPUs) * 1000,
		Memory: v1.Bytes(info.MemoryBytes),
	}
	a.allocatable = a.capacity.Sub(v1.Resources{CPU: a.cfg.ReservedCPU, Memory: a.cfg.ReservedMemory})
	if a.allocatable.CPU < 0 {
		a.allocatable.CPU = 0
	}
	if a.allocatable.Memory < 0 {
		a.allocatable.Memory = 0
	}
	if a.allocatable.CPU == 0 || a.allocatable.Memory == 0 {
		return fmt.Errorf("agent: reservations leave no allocatable capacity (machine has %s, reserved cpu=%s memory=%s)",
			a.capacity, a.cfg.ReservedCPU, a.cfg.ReservedMemory)
	}
	return nil
}

// adoptExistingContainers rebuilds the agent's view from the engine after a
// restart. Containers Orion created carry their workload name and UID, so the
// agent can pick up exactly where the previous process left off — including
// workloads that failed while it was down.
func (a *Agent) adoptExistingContainers(ctx context.Context) error {
	containers, err := a.rt.List(ctx, map[string]string{runtime.LabelNode: a.cfg.NodeName})
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, c := range containers {
		name := c.Labels[runtime.LabelWorkload]
		uid := c.Labels[runtime.LabelWorkloadUID]
		if name == "" || uid == "" {
			continue
		}
		w := &managedWorkload{
			name:        name,
			uid:         uid,
			containerID: c.ID,
			log:         a.log.With("workload", name),
		}
		w.observeContainer(c)
		a.workloads[name] = w
	}
	if len(a.workloads) > 0 {
		a.log.Info("adopted containers from a previous agent process", "count", len(a.workloads))
	}
	return nil
}

func (a *Agent) registerWithRetry(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	const maxBackoff = 15 * time.Second

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := a.client.Register(callCtx, a.registerRequest())
		cancel()

		if err == nil {
			a.nodeUID.Store(resp.GetNodeUid())
			a.lastContact.Store(time.Now().UnixNano())
			a.applyTimings(resp.GetHeartbeatIntervalMs(), resp.GetSelfFenceTimeoutMs())
			a.log.Info("registered with the control plane",
				"uid", resp.GetNodeUid(), "cluster", resp.GetClusterId(),
				"heartbeat", a.cfg.HeartbeatInterval, "selfFence", a.cfg.SelfFenceTimeout)
			return nil
		}

		a.log.Warn("registration failed", "attempt", attempt, "retryIn", backoff, "err", err)
		// Jitter so a whole rack of agents restarting together does not
		// synchronize its retries onto the control plane.
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// applyTimings adopts the control plane's failure-detection settings. The
// self-fence timeout must stay below the cluster's eviction deadline, so the
// control plane owns both numbers and the agent does not get a say.
func (a *Agent) applyTimings(heartbeatMs, fenceMs int64) {
	if heartbeatMs > 0 {
		a.cfg.HeartbeatInterval = time.Duration(heartbeatMs) * time.Millisecond
	}
	if fenceMs > 0 {
		a.cfg.SelfFenceTimeout = time.Duration(fenceMs) * time.Millisecond
	}
	if a.cfg.SelfFenceTimeout <= a.cfg.HeartbeatInterval {
		// Refuse a configuration that would fence on a single missed
		// heartbeat, which would make the cluster unstable rather than safe.
		a.cfg.SelfFenceTimeout = 4 * a.cfg.HeartbeatInterval
		a.log.Warn("control plane sent a self-fence timeout at or below the heartbeat interval; using 4x heartbeat instead",
			"selfFence", a.cfg.SelfFenceTimeout)
	}
}

func (a *Agent) registerRequest() *orionv1.RegisterRequest {
	return &orionv1.RegisterRequest{
		NodeName:       a.cfg.NodeName,
		Address:        a.cfg.Address,
		Labels:         a.cfg.Labels,
		Capacity:       toProtoResources(a.capacity),
		Allocatable:    toProtoResources(a.allocatable),
		Runtime:        toProtoRuntime(a.runtimeInfo),
		AgentStartedAt: toProtoTime(a.startedAt),
		AgentVersion:   agentVersion(),
	}
}

// shutdown stops the agent cleanly. Workloads are left running: a graceful
// agent restart (a deploy of the agent itself) must not restart every container
// on the node. Deregistering tells the control plane to move workloads
// immediately rather than waiting out the heartbeat timeout.
func (a *Agent) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a.mu.Lock()
	for _, w := range a.workloads {
		w.stopProbe()
	}
	a.mu.Unlock()

	uid, _ := a.nodeUID.Load().(string)
	if _, err := a.client.Deregister(ctx, &orionv1.DeregisterRequest{
		NodeName: a.cfg.NodeName,
		NodeUid:  uid,
		Reason:   "agent shutting down",
	}); err != nil {
		a.log.Warn("could not deregister on shutdown", "err", err)
	}
	a.log.Info("agent stopped; containers left running for the next agent process to adopt")
}

func agentVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "dev"
}

// Hostname returns the machine's hostname, used as the default node name.
func Hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "orion-node"
	}
	return h
}
