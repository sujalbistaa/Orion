package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/runtime"
)

// Restart backoff bounds. A container that cannot start must not be retried in
// a tight loop: that turns one broken image into node-wide CPU load and a
// registry hammering.
const (
	minRestartBackoff = 1 * time.Second
	maxRestartBackoff = 5 * time.Minute
	// stableRunDuration is how long a container must run before its backoff is
	// forgiven, so a workload that crashes once an hour does not accumulate a
	// five-minute delay.
	stableRunDuration = 60 * time.Second
)

// managedWorkload is one workload as seen by this agent: what the control
// plane asked for, what the engine reports, and the local decisions in between.
type managedWorkload struct {
	name string
	uid  string
	log  *slog.Logger

	mu   sync.Mutex
	spec *orionv1.AssignedWorkload

	containerID string
	state       runtime.ContainerState

	phase   v1.WorkloadPhase
	health  v1.HealthState
	reason  string
	message string

	restartCount int32
	exitCode     *int32
	startedAt    time.Time
	finishedAt   time.Time
	usage        v1.Resources
	hostPorts    map[int32]int32

	// backoff and nextAttempt implement restart throttling.
	backoff     time.Duration
	nextAttempt time.Time

	// probe runs health checks for this workload while it is running.
	probe *prober
}

func (w *managedWorkload) setSpec(spec *orionv1.AssignedWorkload) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.spec = spec
}

func (w *managedWorkload) getSpec() *orionv1.AssignedWorkload {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.spec
}

func (w *managedWorkload) setPhase(phase v1.WorkloadPhase, reason, message string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.phase, w.reason, w.message = phase, reason, message
}

func (w *managedWorkload) terminationGrace() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.spec == nil || w.spec.GetTerminationGracePeriodMs() <= 0 {
		return 30 * time.Second
	}
	return time.Duration(w.spec.GetTerminationGracePeriodMs()) * time.Millisecond
}

// observeContainer folds an engine status into the workload's view.
func (w *managedWorkload) observeContainer(c runtime.ContainerStatus) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.containerID = c.ID
	w.state = c.State
	w.startedAt = c.StartedAt
	w.finishedAt = c.FinishedAt
	if len(c.Ports) > 0 {
		w.hostPorts = c.Ports
	}
	// The engine's restart count only moves when the engine restarts a
	// container; Orion restarts through this agent, so its own counter is
	// authoritative and must never regress.
	if c.RestartCount > w.restartCount {
		w.restartCount = c.RestartCount
	}

	switch c.State {
	case runtime.StateRunning:
		w.phase = v1.WorkloadRunning
		if w.reason == "Fenced" {
			w.reason, w.message = "", ""
		}
	case runtime.StateCreated:
		w.phase = v1.WorkloadStarting
	case runtime.StateRestarting:
		w.phase = v1.WorkloadStarting
		w.health = v1.HealthUnknown
	case runtime.StateExited, runtime.StateDead:
		code := c.ExitCode
		w.exitCode = &code
		w.health = v1.HealthUnknown
		if c.OOMKilled {
			w.reason = "OOMKilled"
			w.message = "the container exceeded its memory limit and was killed by the kernel"
		} else if c.Error != "" {
			w.message = c.Error
		}
		// The phase is decided by the reconciler, which knows the restart
		// policy; observation only records what the engine reported.
	}
}

// status renders the workload for a Sync request.
func (w *managedWorkload) status() *orionv1.WorkloadStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	st := &orionv1.WorkloadStatus{
		Name:         w.name,
		Uid:          w.uid,
		Phase:        string(w.phase),
		Health:       string(w.health),
		ContainerId:  w.containerID,
		Reason:       w.reason,
		Message:      w.message,
		RestartCount: w.restartCount,
		Usage:        toProtoResources(w.usage),
		HostPorts:    w.hostPorts,
	}
	if w.exitCode != nil {
		st.ExitCode = *w.exitCode
		st.HasExitCode = true
	}
	if !w.startedAt.IsZero() {
		st.StartedAt = toProtoTime(w.startedAt)
	}
	if !w.finishedAt.IsZero() {
		st.FinishedAt = toProtoTime(w.finishedAt)
	}
	return st
}

func (w *managedWorkload) stopProbe() {
	w.mu.Lock()
	p := w.probe
	w.probe = nil
	w.mu.Unlock()
	if p != nil {
		p.stop()
	}
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

// applyAssignment makes the node match the control plane's instruction. It is
// level-triggered: the response is the complete desired set, so anything the
// agent holds that is absent from it is no longer wanted.
func (a *Agent) applyAssignment(ctx context.Context, assigned []*orionv1.AssignedWorkload) {
	if a.fenced.Load() {
		// Contact has just been restored but the fence is lifted on the next
		// successful sync, not mid-assignment.
		return
	}

	wanted := make(map[string]*orionv1.AssignedWorkload, len(assigned))
	for _, spec := range assigned {
		wanted[spec.GetName()] = spec
	}

	a.mu.Lock()
	// Remove anything the control plane no longer lists. This is how a
	// container orphaned by a crashed agent, or one belonging to a workload
	// deleted while this node was down, gets cleaned up.
	var orphans []*managedWorkload
	for name, w := range a.workloads {
		if _, ok := wanted[name]; !ok {
			orphans = append(orphans, w)
			delete(a.workloads, name)
		}
	}
	// Attach specs and create records for newly assigned workloads.
	targets := make([]*managedWorkload, 0, len(wanted))
	for name, spec := range wanted {
		w, ok := a.workloads[name]
		if !ok {
			w = &managedWorkload{name: name, uid: spec.GetUid(), log: a.log.With("workload", name)}
			a.workloads[name] = w
		} else if w.uid != spec.GetUid() {
			// The workload was recreated with the same name. The old container
			// belongs to a different object and must not be adopted.
			orphans = append(orphans, w)
			w = &managedWorkload{name: name, uid: spec.GetUid(), log: a.log.With("workload", name)}
			a.workloads[name] = w
		}
		w.setSpec(spec)
		targets = append(targets, w)
	}
	a.mu.Unlock()

	for _, w := range orphans {
		a.removeWorkload(ctx, w, "no longer assigned to this node")
	}
	for _, w := range targets {
		if err := ctx.Err(); err != nil {
			return
		}
		a.reconcileWorkload(ctx, w)
	}
}

func (a *Agent) reconcileWorkload(ctx context.Context, w *managedWorkload) {
	spec := w.getSpec()
	if spec == nil {
		return
	}
	if spec.GetDesiredState() == "Terminated" {
		a.terminateWorkload(ctx, w)
		return
	}

	// No container yet: create and start one.
	if w.containerID == "" {
		a.startWorkload(ctx, w, spec)
		return
	}

	status, err := a.rt.Inspect(ctx, w.containerID)
	if err != nil {
		if errors.Is(err, runtime.ErrNotFound) {
			// The container vanished — an operator ran `docker rm`, or the
			// engine was pruned. Recreate it rather than reporting a workload
			// that no longer exists as Running.
			w.log.Warn("container disappeared; recreating")
			w.mu.Lock()
			w.containerID = ""
			w.mu.Unlock()
			w.stopProbe()
			a.startWorkload(ctx, w, spec)
			return
		}
		w.log.Warn("could not inspect container", "err", err)
		return
	}
	w.observeContainer(status)

	switch status.State {
	case runtime.StateRunning:
		a.ensureProbe(w, spec)

	case runtime.StateCreated:
		a.timed("start", func() error { return a.rt.Start(ctx, w.containerID) })

	case runtime.StateExited, runtime.StateDead:
		a.handleExit(ctx, w, spec, status)

	case runtime.StateRestarting, runtime.StateRemoving, runtime.StatePaused:
		// Transient engine states; the next tick will see the outcome.
	}
}

// handleExit applies the restart policy to a container that has stopped.
func (a *Agent) handleExit(ctx context.Context, w *managedWorkload, spec *orionv1.AssignedWorkload, status runtime.ContainerStatus) {
	w.stopProbe()
	policy := v1.RestartPolicy(spec.GetRestartPolicy())

	if !policy.ShouldRestart(status.ExitCode) {
		if status.ExitCode == 0 {
			w.setPhase(v1.WorkloadSucceeded, "Completed", "the container exited successfully")
		} else {
			w.setPhase(v1.WorkloadFailed, orDefault(w.reason, "Error"),
				fmt.Sprintf("the container exited with code %d and the restart policy is %s",
					status.ExitCode, policy))
		}
		return
	}

	w.mu.Lock()
	// A container that ran long enough to be considered healthy has earned a
	// clean slate; otherwise the backoff grows.
	if !status.StartedAt.IsZero() && status.FinishedAt.Sub(status.StartedAt) >= stableRunDuration {
		w.backoff = 0
	}
	if w.backoff == 0 {
		w.backoff = minRestartBackoff
	}
	if w.nextAttempt.IsZero() {
		w.nextAttempt = time.Now().Add(w.backoff)
	}
	wait := time.Until(w.nextAttempt)
	backoff := w.backoff
	w.mu.Unlock()

	if wait > 0 {
		w.setPhase(v1.WorkloadRunning, "CrashLoopBackOff",
			fmt.Sprintf("the container exited with code %d; restarting in %s (attempt %d)",
				status.ExitCode, wait.Round(time.Second), w.restartCount+1))
		return
	}

	err := a.timed("restart", func() error {
		return a.rt.Restart(ctx, w.containerID, w.terminationGrace())
	})

	w.mu.Lock()
	w.restartCount++
	w.backoff = min(backoff*2, maxRestartBackoff)
	w.nextAttempt = time.Now().Add(w.backoff)
	count := w.restartCount
	w.mu.Unlock()

	if err != nil {
		w.log.Warn("restart failed", "err", err, "restartCount", count)
		w.setPhase(v1.WorkloadRunning, "RestartFailed", err.Error())
		return
	}
	if a.metrics != nil {
		a.metrics.WorkloadRestarted(w.name)
	}
	w.log.Info("restarted container", "restartCount", count, "exitCode", status.ExitCode)
	w.setPhase(v1.WorkloadRunning, "Restarted",
		fmt.Sprintf("restarted after exit code %d", status.ExitCode))
}

func (a *Agent) startWorkload(ctx context.Context, w *managedWorkload, spec *orionv1.AssignedWorkload) {
	w.mu.Lock()
	wait := time.Until(w.nextAttempt)
	w.mu.Unlock()
	if wait > 0 {
		return // still in backoff from a previous failed start
	}

	w.setPhase(v1.WorkloadStarting, "Pulling", "pulling image "+spec.GetImage())

	// The pull gets its own generous deadline: a large image legitimately takes
	// minutes, and it must not be cut short by the sync interval.
	pullCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	err := a.timed("pull", func() error { return a.rt.PullImage(pullCtx, spec.GetImage()) })
	cancel()
	if err != nil {
		a.failStart(w, "ImagePullFailed", err)
		return
	}

	// An existing container with this name is one this agent created before a
	// restart; adopt it rather than failing forever on a name conflict.
	name := runtime.ContainerName(w.name, w.uid)
	id, err := a.rt.Create(ctx, containerSpecFor(name, a.cfg.NodeName, spec))
	if err != nil {
		if errors.Is(err, runtime.ErrAlreadyExists) {
			if adopted, ok := a.findContainerByName(ctx, name); ok {
				id = adopted.ID
				w.log.Info("adopted an existing container with the expected name", "container", id)
			} else {
				a.failStart(w, "CreateFailed", err)
				return
			}
		} else {
			a.failStart(w, "CreateFailed", err)
			return
		}
	}

	w.mu.Lock()
	w.containerID = id
	w.mu.Unlock()

	if err := a.timed("start", func() error { return a.rt.Start(ctx, id) }); err != nil {
		a.failStart(w, "StartFailed", err)
		return
	}

	w.mu.Lock()
	w.backoff = 0
	w.nextAttempt = time.Time{}
	w.mu.Unlock()

	status, err := a.rt.Inspect(ctx, id)
	if err == nil {
		w.observeContainer(status)
	}
	w.setPhase(v1.WorkloadRunning, "Started", "")
	a.ensureProbe(w, spec)
	w.log.Info("started workload", "container", id, "image", spec.GetImage())
}

func (a *Agent) failStart(w *managedWorkload, reason string, cause error) {
	w.mu.Lock()
	if w.backoff == 0 {
		w.backoff = minRestartBackoff
	} else {
		w.backoff = min(w.backoff*2, maxRestartBackoff)
	}
	w.nextAttempt = time.Now().Add(w.backoff)
	backoff := w.backoff
	w.mu.Unlock()

	w.setPhase(v1.WorkloadFailed, reason, cause.Error())
	w.log.Warn("could not start workload", "reason", reason, "err", cause, "retryIn", backoff)
}

func (a *Agent) findContainerByName(ctx context.Context, name string) (runtime.ContainerStatus, bool) {
	list, err := a.rt.List(ctx, map[string]string{runtime.LabelNode: a.cfg.NodeName})
	if err != nil {
		return runtime.ContainerStatus{}, false
	}
	for _, c := range list {
		if c.Name == name {
			return c, true
		}
	}
	return runtime.ContainerStatus{}, false
}

func (a *Agent) terminateWorkload(ctx context.Context, w *managedWorkload) {
	w.stopProbe()
	if w.containerID == "" {
		w.setPhase(v1.WorkloadTerminated, "Stopped", "")
		return
	}

	grace := w.terminationGrace()
	stopCtx, cancel := context.WithTimeout(ctx, grace+15*time.Second)
	err := a.timed("stop", func() error { return a.rt.Stop(stopCtx, w.containerID, grace) })
	cancel()
	if err != nil && !errors.Is(err, runtime.ErrNotFound) {
		w.log.Warn("could not stop container", "err", err)
		return
	}

	if err := a.timed("remove", func() error { return a.rt.Remove(ctx, w.containerID, true) }); err != nil &&
		!errors.Is(err, runtime.ErrNotFound) {
		w.log.Warn("could not remove container", "err", err)
		return
	}

	w.mu.Lock()
	w.containerID = ""
	w.mu.Unlock()
	// Terminated is only reported once the container is actually gone; the
	// control plane relies on that to release the workload's name and
	// resources.
	w.setPhase(v1.WorkloadTerminated, "Stopped", "container stopped and removed")
	w.log.Info("terminated workload")
}

func (a *Agent) removeWorkload(ctx context.Context, w *managedWorkload, why string) {
	w.stopProbe()
	if w.containerID == "" {
		return
	}
	w.log.Info("removing container", "reason", why)
	stopCtx, cancel := context.WithTimeout(ctx, w.terminationGrace()+15*time.Second)
	_ = a.rt.Stop(stopCtx, w.containerID, w.terminationGrace())
	cancel()
	if err := a.rt.Remove(ctx, w.containerID, true); err != nil && !errors.Is(err, runtime.ErrNotFound) {
		w.log.Warn("could not remove container", "err", err)
	}
}

// timed runs an engine operation and records its outcome.
func (a *Agent) timed(op string, f func() error) error {
	start := time.Now()
	err := f()
	if a.metrics != nil {
		a.metrics.ContainerOperation(op, err, time.Since(start))
	}
	return err
}

// containerSpecFor translates an assignment into an engine container spec.
func containerSpecFor(name, nodeName string, spec *orionv1.AssignedWorkload) runtime.ContainerSpec {
	env := make([]string, 0, len(spec.GetEnv()))
	for _, e := range spec.GetEnv() {
		env = append(env, e.GetName()+"="+e.GetValue())
	}
	ports := make([]runtime.PortMapping, 0, len(spec.GetPorts()))
	for _, p := range spec.GetPorts() {
		ports = append(ports, runtime.PortMapping{
			ContainerPort: p.GetContainer(), HostPort: p.GetHost(), Protocol: p.GetProtocol(),
		})
	}

	labels := map[string]string{
		runtime.LabelWorkload:    spec.GetName(),
		runtime.LabelWorkloadUID: spec.GetUid(),
		runtime.LabelNode:        nodeName,
	}
	for k, v := range spec.GetLabels() {
		labels[k] = v
	}

	limit := spec.GetLimit()
	if limit == nil || (limit.GetCpuMillis() == 0 && limit.GetMemoryBytes() == 0) {
		limit = spec.GetRequest()
	}

	return runtime.ContainerSpec{
		Name:    name,
		Image:   spec.GetImage(),
		Command: spec.GetCommand(),
		Args:    spec.GetArgs(),
		Env:     env,
		Labels:  labels,
		Ports:   ports,

		CPUMillis:   limit.GetCpuMillis(),
		MemoryBytes: limit.GetMemoryBytes(),

		// Orion's defaults, not the image's: a workload that needs a writable
		// root filesystem gets tmpfs mounts instead.
		ReadOnlyRootFS:  false,
		NoNewPrivileges: true,
		TmpfsPaths:      nil,
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
