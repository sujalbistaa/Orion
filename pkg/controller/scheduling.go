package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/scheduler"
	"github.com/sujalbistaa/orion/pkg/store"
)

// SchedulingController drains the pending-workload queue.
//
// It runs a batch per pass against a single cluster snapshot so the scheduler
// can reserve capacity across the batch. Bindings are then proposed one at a
// time; the state machine re-validates capacity when it applies each one, so a
// snapshot that has gone stale produces a clean rejection rather than an
// overcommitted node.
type SchedulingController struct {
	cp    *controlplane.ControlPlane
	sched *scheduler.Scheduler
	log   *slog.Logger

	// MaxPerPass bounds one batch so a burst of thousands of workloads cannot
	// monopolize the Raft log or delay other controllers.
	MaxPerPass int
	Interval   time.Duration

	metrics SchedulerMetrics
}

// SchedulerMetrics records scheduling outcomes. Implemented by the telemetry
// package; nil is allowed.
type SchedulerMetrics interface {
	ScheduleAttempt(result string, latency time.Duration)
	PendingWorkloads(n int)
}

func NewSchedulingController(cp *controlplane.ControlPlane, s *scheduler.Scheduler, log *slog.Logger, m SchedulerMetrics) *SchedulingController {
	return &SchedulingController{
		cp:         cp,
		sched:      s,
		log:        log.With("controller", "scheduler"),
		MaxPerPass: 256,
		Interval:   2 * time.Second,
		metrics:    m,
	}
}

func (c *SchedulingController) Name() string                  { return "scheduler" }
func (c *SchedulingController) ResyncInterval() time.Duration { return c.Interval }

func (c *SchedulingController) Reconcile(ctx context.Context) error {
	st := c.cp.Store()

	pending := st.PendingWorkloads()
	if c.metrics != nil {
		c.metrics.PendingWorkloads(len(pending))
	}
	if len(pending) == 0 {
		return nil
	}
	if len(pending) > c.MaxPerPass {
		pending = pending[:c.MaxPerPass]
	}

	nodes := st.SchedulableNodes()
	byNode := make(map[string][]*v1.Workload, len(nodes))
	for _, n := range nodes {
		byNode[n.Name] = st.WorkloadsOnNode(n.Name)
	}
	snap := scheduler.NewSnapshot(nodes, byNode)

	for _, result := range c.sched.ScheduleBatch(pending, snap) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if result.Err != nil {
			c.recordUnschedulable(ctx, result.Workload, result.Err)
			continue
		}
		c.bind(ctx, result.Workload, result.Decision)
	}
	return nil
}

func (c *SchedulingController) bind(ctx context.Context, w *v1.Workload, d *v1.PlacementDecision) {
	start := time.Now()
	_, err := c.cp.Apply(ctx, store.Command{
		Kind:      store.CmdBindWorkload,
		Name:      w.Name,
		UID:       w.UID,
		Placement: d,
		// The binding is keyed on the workload's identity, so a retry after a
		// leader change cannot bind the same workload twice.
		RequestID: "bind/" + w.UID,
	})
	latency := time.Since(start)

	switch {
	case err == nil:
		if c.metrics != nil {
			c.metrics.ScheduleAttempt("bound", latency)
		}
		c.log.Debug("bound workload", "workload", w.Name, "node", d.NodeName,
			"score", d.Score, "decisionMicros", d.LatencyMicros)

	case errors.Is(err, store.ErrConflict):
		// The snapshot was stale: another binding took the capacity. The next
		// pass re-runs against fresh state. This is expected under load, not an
		// error condition.
		if c.metrics != nil {
			c.metrics.ScheduleAttempt("conflict", latency)
		}
		c.log.Debug("binding lost a race for capacity, will retry",
			"workload", w.Name, "node", d.NodeName)

	case errors.Is(err, store.ErrInvalidState), errors.Is(err, store.ErrNotFound),
		errors.Is(err, store.ErrUIDMismatch):
		// The workload was deleted or already placed between the snapshot and
		// the write. Nothing to do.
		if c.metrics != nil {
			c.metrics.ScheduleAttempt("obsolete", latency)
		}

	default:
		if c.metrics != nil {
			c.metrics.ScheduleAttempt("error", latency)
		}
		c.log.Warn("could not bind workload", "workload", w.Name, "node", d.NodeName, "err", err)
	}
}

// recordUnschedulable stores the rejection detail on the workload so the
// console can show why it is stuck, and emits an event — but only when the
// reason changed, so a permanently unschedulable workload does not produce an
// event every two seconds.
func (c *SchedulingController) recordUnschedulable(ctx context.Context, w *v1.Workload, cause error) {
	if c.metrics != nil {
		c.metrics.ScheduleAttempt("unschedulable", 0)
	}

	var ue *scheduler.ErrUnschedulable
	decision := &v1.PlacementDecision{
		WorkloadName: w.Name,
		DecidedAt:    time.Now().UTC(),
		Reason:       cause.Error(),
	}
	if errors.As(cause, &ue) {
		decision.Rejections = ue.Rejections
	}

	if w.Status.Placement != nil && w.Status.Placement.Reason == decision.Reason {
		return // unchanged; do not churn the log
	}

	status := w.Status.DeepCopy()
	status.Placement = decision
	status.Reason = "Unschedulable"
	status.Message = cause.Error()

	if _, err := c.cp.Apply(ctx, store.Command{
		Kind:           store.CmdUpdateWorkloadStatus,
		Name:           w.Name,
		UID:            w.UID,
		WorkloadStatus: &status,
	}); err != nil && !errors.Is(err, store.ErrNotFound) {
		c.log.Warn("could not record unschedulable reason", "workload", w.Name, "err", err)
		return
	}

	c.cp.RecordEvent(ctx, v1.Event{
		Severity: v1.SeverityWarning,
		Source:   "scheduler",
		Reason:   "Unschedulable",
		Kind:     "Workload",
		Name:     w.Name,
		Message:  cause.Error(),
	})
}
