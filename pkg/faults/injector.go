package faults

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
)

// Options configures the injector.
type Options struct {
	ControlPlane *controlplane.ControlPlane
	Gate         *Gate
	Logger       *slog.Logger

	// ControllerControl lets the controller-crash experiment actually stop and
	// restart reconciliation. Nil disables that experiment rather than faking
	// it.
	ControllerControl ControllerControl

	// MaxConcurrentRuns bounds simultaneous experiments. Running two faults at
	// once makes the result uninterpretable: you cannot attribute a violation
	// to either one.
	MaxConcurrentRuns int
	// MaxRetainedRuns bounds run history.
	MaxRetainedRuns int
	// DefaultDuration is how long a fault is held when unspecified.
	DefaultDuration time.Duration
	// MaxDuration caps how long a fault may be held, so an experiment cannot
	// be left running indefinitely by mistake.
	MaxDuration time.Duration
	// RecoveryTimeout is how long to wait for convergence after the fault is
	// lifted before declaring the run failed.
	RecoveryTimeout time.Duration
}

// ControllerControl stops and starts reconciliation.
type ControllerControl interface {
	StopControllers()
	StartControllers()
	ControllersRunning() bool
}

// Injector runs fault injection experiments.
type Injector struct {
	opts Options
	cp   *controlplane.ControlPlane
	log  *slog.Logger

	mu      sync.Mutex
	runs    map[string]*apiserver.ExperimentRun
	order   []string
	active  int
	cancels map[string]context.CancelFunc

	seq int
	wg  sync.WaitGroup
}

var _ apiserver.FaultInjector = (*Injector)(nil)

func New(opts Options) (*Injector, error) {
	if opts.ControlPlane == nil {
		return nil, errors.New("faults: ControlPlane is required")
	}
	if opts.Gate == nil {
		return nil, errors.New("faults: Gate is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxConcurrentRuns <= 0 {
		opts.MaxConcurrentRuns = 1
	}
	if opts.MaxRetainedRuns <= 0 {
		opts.MaxRetainedRuns = 50
	}
	if opts.DefaultDuration <= 0 {
		opts.DefaultDuration = 30 * time.Second
	}
	if opts.MaxDuration <= 0 {
		opts.MaxDuration = 10 * time.Minute
	}
	if opts.RecoveryTimeout <= 0 {
		opts.RecoveryTimeout = 3 * time.Minute
	}
	return &Injector{
		opts:    opts,
		cp:      opts.ControlPlane,
		log:     opts.Logger.With("component", "fault-injector"),
		runs:    map[string]*apiserver.ExperimentRun{},
		cancels: map[string]context.CancelFunc{},
	}, nil
}

// Close aborts every running experiment and releases every block, so a process
// shutdown cannot leave the cluster partitioned.
func (i *Injector) Close() {
	i.mu.Lock()
	for _, cancel := range i.cancels {
		cancel()
	}
	i.mu.Unlock()
	i.wg.Wait()
	i.opts.Gate.Clear()
	if i.opts.ControllerControl != nil && !i.opts.ControllerControl.ControllersRunning() {
		i.opts.ControllerControl.StartControllers()
	}
}

// List returns the experiment catalogue.
func (i *Injector) List() []apiserver.ExperimentDescriptor {
	out := make([]apiserver.ExperimentDescriptor, 0, len(catalogue))
	for _, d := range catalogue {
		if d.Kind == apiserver.ExperimentControllerCrash && i.opts.ControllerControl == nil {
			continue // not fakeable; simply not offered
		}
		out = append(out, d)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

func (i *Injector) Get(id string) (apiserver.ExperimentRun, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	run, ok := i.runs[id]
	if !ok {
		return apiserver.ExperimentRun{}, false
	}
	return *deepCopyRun(run), true
}

func (i *Injector) Runs() []apiserver.ExperimentRun {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]apiserver.ExperimentRun, 0, len(i.order))
	// Newest first: an operator is looking at the run they just started.
	for n := len(i.order) - 1; n >= 0; n-- {
		if run, ok := i.runs[i.order[n]]; ok {
			out = append(out, *deepCopyRun(run))
		}
	}
	return out
}

func (i *Injector) Abort(_ context.Context, id string) error {
	i.mu.Lock()
	cancel, ok := i.cancels[id]
	i.mu.Unlock()
	if !ok {
		return fmt.Errorf("experiment %s is not running", id)
	}
	cancel()
	return nil
}

// Start validates and launches an experiment.
func (i *Injector) Start(ctx context.Context, req apiserver.ExperimentRequest) (apiserver.ExperimentRun, error) {
	desc, ok := lookup(req.Kind)
	if !ok {
		return apiserver.ExperimentRun{}, fmt.Errorf("unknown experiment %q", req.Kind)
	}
	if desc.Kind == apiserver.ExperimentControllerCrash && i.opts.ControllerControl == nil {
		return apiserver.ExperimentRun{}, errors.New(
			"this server cannot stop its controllers, so the controller-crash experiment is unavailable")
	}
	if err := validateParams(desc, req.Params); err != nil {
		return apiserver.ExperimentRun{}, err
	}
	if err := i.validateTargets(desc, req.Params); err != nil {
		return apiserver.ExperimentRun{}, err
	}

	duration := req.Duration
	if duration <= 0 {
		duration = i.opts.DefaultDuration
	}
	if duration > i.opts.MaxDuration {
		return apiserver.ExperimentRun{}, fmt.Errorf(
			"duration %s exceeds the maximum of %s; a fault held longer than that is an outage, not an experiment",
			duration, i.opts.MaxDuration)
	}

	i.mu.Lock()
	if i.active >= i.opts.MaxConcurrentRuns {
		i.mu.Unlock()
		return apiserver.ExperimentRun{}, fmt.Errorf(
			"another experiment is already running; concurrent faults make the result impossible to attribute")
	}
	i.seq++
	id := fmt.Sprintf("exp-%d-%s", i.seq, time.Now().UTC().Format("20060102T150405"))
	run := &apiserver.ExperimentRun{
		ID:        id,
		Kind:      req.Kind,
		State:     apiserver.RunPending,
		Params:    req.Params,
		Actor:     req.Actor,
		StartedAt: time.Now().UTC(),
	}
	i.runs[id] = run
	i.order = append(i.order, id)
	if len(i.order) > i.opts.MaxRetainedRuns {
		drop := i.order[0]
		i.order = i.order[1:]
		delete(i.runs, drop)
	}
	i.active++

	// The experiment outlives the HTTP request that started it, so it gets its
	// own context rather than inheriting one that is cancelled on response.
	runCtx, cancel := context.WithCancel(context.Background())
	i.cancels[id] = cancel
	i.mu.Unlock()

	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		defer cancel()
		i.execute(runCtx, id, desc, req.Params, duration)

		i.mu.Lock()
		i.active--
		delete(i.cancels, id)
		i.mu.Unlock()
	}()

	return *deepCopyRun(run), nil
}

// execute runs one experiment through its full lifecycle.
func (i *Injector) execute(ctx context.Context, id string, desc apiserver.ExperimentDescriptor,
	params map[string]string, duration time.Duration) {

	st := i.cp.Store()
	mon := newMonitor(st, StandardInvariants())
	go mon.run()

	log := i.log.With("experiment", id, "kind", desc.Kind)
	log.Info("experiment starting", "duration", duration, "params", params)

	i.record(id, apiserver.RunInjecting, "info",
		fmt.Sprintf("injecting %s: %s", desc.Name, desc.Hypothesis))

	exp := experiments[desc.Kind]
	scope, err := exp.inject(ctx, i, id, params)
	if err != nil {
		mon.stop()
		i.finish(id, apiserver.RunFailed, mon, "", fmt.Sprintf("injection failed: %v", err))
		log.Error("injection failed", "err", err)
		return
	}
	i.setAffected(id, scope)
	i.record(id, apiserver.RunInjecting, "warn", scope.describe())

	// --- hold the fault, watching invariants -------------------------------
	i.record(id, apiserver.RunObserving, "info",
		fmt.Sprintf("holding the fault for %s while checking %d invariants",
			duration, len(mon.invariants)))
	i.setState(id, apiserver.RunObserving)

	aborted := false
	select {
	case <-time.After(duration):
	case <-ctx.Done():
		aborted = true
		i.record(id, apiserver.RunRecovering, "warn", "aborted by operator; lifting the fault early")
	}

	// --- lift the fault and measure recovery -------------------------------
	i.setState(id, apiserver.RunRecovering)
	recoveryStart := time.Now()
	if err := exp.recover(context.Background(), i, id, params, scope); err != nil {
		log.Error("could not lift the fault", "err", err)
		i.record(id, apiserver.RunRecovering, "error", fmt.Sprintf("could not lift the fault: %v", err))
	} else {
		i.record(id, apiserver.RunRecovering, "info", "fault lifted; waiting for the cluster to converge")
	}

	converged, detail := i.waitForConvergence(context.Background(), scope)
	recovery := time.Since(recoveryStart)
	mon.stop()

	switch {
	case aborted:
		i.finish(id, apiserver.RunAborted, mon, recovery.Round(time.Millisecond).String(), "")
		log.Warn("experiment aborted")
	case !converged:
		i.record(id, apiserver.RunFailed, "error", "the cluster did not converge: "+detail)
		i.finish(id, apiserver.RunFailed, mon, recovery.Round(time.Millisecond).String(),
			"the cluster did not return to desired state: "+detail)
		log.Error("experiment failed: no convergence", "detail", detail)
	case mon.violated():
		i.record(id, apiserver.RunFailed, "error", "an invariant was violated during the run")
		i.finish(id, apiserver.RunFailed, mon, recovery.Round(time.Millisecond).String(), "")
		log.Error("experiment failed: invariant violated")
	default:
		i.record(id, apiserver.RunSucceeded, "info",
			fmt.Sprintf("cluster converged in %s with every invariant held", recovery.Round(time.Millisecond)))
		i.finish(id, apiserver.RunSucceeded, mon, recovery.Round(time.Millisecond).String(), "")
		log.Info("experiment succeeded", "recovery", recovery.Round(time.Millisecond))
	}
}

// waitForConvergence blocks until the cluster returns to desired state.
//
// Convergence means every deployment has its desired replicas available and no
// node the experiment touched is still Unreachable. It is checked repeatedly
// rather than once, because a cluster that briefly looks converged mid-recovery
// would otherwise produce a falsely fast result.
func (i *Injector) waitForConvergence(ctx context.Context, scope scope) (bool, string) {
	st := i.cp.Store()
	deadline := time.Now().Add(i.opts.RecoveryTimeout)

	// Require the condition to hold for a stable window, so a momentary
	// coincidence is not reported as recovery.
	const stableFor = 2 * time.Second
	stableSince := time.Time{}
	lastProblem := "no deployments to converge"

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false, "cancelled"
		}
		problem := convergenceProblem(st, scope)
		if problem == "" {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= stableFor {
				return true, ""
			}
		} else {
			stableSince = time.Time{}
			lastProblem = problem
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false, lastProblem
}

func convergenceProblem(st *store.Store, scope scope) string {
	for _, d := range st.Deployments() {
		if d.DeletedAt != nil {
			continue
		}
		available := 0
		for _, w := range st.WorkloadsOwnedBy(d.UID) {
			if w.Ready() {
				available++
			}
		}
		if available != int(d.Spec.Replicas) {
			return fmt.Sprintf("deployment %s has %d of %d replicas available",
				d.Name, available, d.Spec.Replicas)
		}
	}
	for _, name := range scope.Nodes {
		node, ok := st.Node(name)
		if !ok {
			continue
		}
		if node.Status.Phase == v1.NodeUnreachable {
			return fmt.Sprintf("node %s is still Unreachable", name)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Run bookkeeping
// ---------------------------------------------------------------------------

// scope records what an experiment actually touched.
type scope struct {
	Nodes     []string
	Workloads []string
	Note      string
}

func (s scope) describe() string {
	switch {
	case s.Note != "":
		return s.Note
	case len(s.Nodes) > 0 && len(s.Workloads) > 0:
		return fmt.Sprintf("affected nodes %v and workloads %v", s.Nodes, s.Workloads)
	case len(s.Nodes) > 0:
		return fmt.Sprintf("affected nodes %v", s.Nodes)
	case len(s.Workloads) > 0:
		return fmt.Sprintf("affected workloads %v", s.Workloads)
	default:
		return "fault injected"
	}
}

func (i *Injector) record(id string, phase apiserver.RunState, level, message string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	run, ok := i.runs[id]
	if !ok {
		return
	}
	now := time.Now().UTC()
	run.Timeline = append(run.Timeline, apiserver.TimelineEntry{
		At:      now,
		Elapsed: now.Sub(run.StartedAt).Round(time.Millisecond).String(),
		Phase:   phase,
		Level:   level,
		Message: message,
	})
}

func (i *Injector) setState(id string, state apiserver.RunState) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if run, ok := i.runs[id]; ok {
		run.State = state
	}
}

func (i *Injector) setAffected(id string, s scope) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if run, ok := i.runs[id]; ok {
		run.AffectedNodes = s.Nodes
		run.AffectedWorkloads = s.Workloads
	}
}

func (i *Injector) finish(id string, state apiserver.RunState, mon *monitor, recovery, errMsg string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	run, ok := i.runs[id]
	if !ok {
		return
	}
	now := time.Now().UTC()
	run.State = state
	run.FinishedAt = &now
	run.Invariants = mon.snapshot()
	run.RecoveryDuration = recovery
	run.Error = errMsg
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func validateParams(desc apiserver.ExperimentDescriptor, params map[string]string) error {
	for _, p := range desc.Parameters {
		v, ok := params[p.Name]
		if (!ok || v == "") && p.Required {
			return fmt.Errorf("parameter %q is required: %s", p.Name, p.Help)
		}
		if v == "" {
			continue
		}
		switch p.Type {
		case "int":
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("parameter %q must be an integer, got %q", p.Name, v)
			}
		case "duration":
			if _, err := time.ParseDuration(v); err != nil {
				return fmt.Errorf("parameter %q must be a duration such as 30s, got %q", p.Name, v)
			}
		}
	}
	for name := range params {
		known := false
		for _, p := range desc.Parameters {
			if p.Name == name {
				known = true
			}
		}
		if !known {
			return fmt.Errorf("unknown parameter %q for experiment %s", name, desc.Kind)
		}
	}
	return nil
}

// validateTargets checks that the named objects exist and that breaking them
// will not simply destroy the cluster. An experiment that takes down the only
// node is not a reliability test; it is an outage with extra steps.
func (i *Injector) validateTargets(desc apiserver.ExperimentDescriptor, params map[string]string) error {
	st := i.cp.Store()

	if node, ok := params["node"]; ok && node != "" {
		if _, exists := st.Node(node); !exists {
			return fmt.Errorf("no node named %q", node)
		}
		ready := 0
		for _, n := range st.Nodes() {
			if n.Status.Phase == v1.NodeReady {
				ready++
			}
		}
		if ready <= 1 {
			return fmt.Errorf(
				"the cluster has only %d Ready node; failing it would leave nowhere for its workloads to go, "+
					"which tests nothing. Add a node first", ready)
		}
	}

	if workload, ok := params["workload"]; ok && workload != "" {
		w, exists := st.Workload(workload)
		if !exists {
			return fmt.Errorf("no workload named %q", workload)
		}
		if w.Status.Phase != v1.WorkloadRunning {
			return fmt.Errorf("workload %q is %s; only a Running workload can be crashed",
				workload, w.Status.Phase)
		}
	}
	_ = desc
	return nil
}

func lookup(kind apiserver.ExperimentKind) (apiserver.ExperimentDescriptor, bool) {
	for _, d := range catalogue {
		if d.Kind == kind {
			return d, true
		}
	}
	return apiserver.ExperimentDescriptor{}, false
}

// deepCopy so callers cannot mutate a run that is still being written to.
func deepCopyRun(r *apiserver.ExperimentRun) *apiserver.ExperimentRun {
	out := *r
	out.Timeline = append([]apiserver.TimelineEntry(nil), r.Timeline...)
	out.Invariants = append([]apiserver.InvariantResult(nil), r.Invariants...)
	out.AffectedNodes = append([]string(nil), r.AffectedNodes...)
	out.AffectedWorkloads = append([]string(nil), r.AffectedWorkloads...)
	if r.Params != nil {
		out.Params = make(map[string]string, len(r.Params))
		for k, v := range r.Params {
			out.Params[k] = v
		}
	}
	if r.FinishedAt != nil {
		t := *r.FinishedAt
		out.FinishedAt = &t
	}
	return &out
}
