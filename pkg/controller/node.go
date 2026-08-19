package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
)

// NodeLifecycleController detects node failure and recovers the workloads that
// were running there.
//
// The timeline for a node that stops heartbeating:
//
//	t+0                 last heartbeat
//	t+HeartbeatTimeout  phase -> Unreachable; the scheduler stops using it
//	t+EvictionDelay     its workloads are terminated, freeing their names and
//	                    resources so the deployment controller can replace them
//
// The gap between the two is what stops a five-second network blip from
// rescheduling the cluster. It is also the safety window that makes Orion's
// at-most-once guarantee conditional — see the note on split brain below.
type NodeLifecycleController struct {
	cp  *controlplane.ControlPlane
	log *slog.Logger

	// HeartbeatTimeout is how long a node may go silent before it is presumed
	// unreachable.
	HeartbeatTimeout time.Duration
	// EvictionDelay is the additional grace period before its workloads are
	// terminated.
	//
	// SAFETY: an unreachable node is not necessarily a dead node — it may be
	// partitioned and still running containers. Orion closes this by having the
	// agent fence itself: if an agent cannot reach the control plane for
	// SelfFenceTimeout it stops every workload it owns. As long as
	// SelfFenceTimeout < HeartbeatTimeout + EvictionDelay, a partitioned node
	// has stopped its containers before the control plane replaces them.
	// The agent enforces its side; this controller enforces the ordering.
	EvictionDelay time.Duration

	Interval time.Duration

	metrics NodeMetrics
	now     func() time.Time
}

// NodeMetrics is implemented by the telemetry package; nil is allowed.
type NodeMetrics interface {
	NodeBecameUnreachable(node string)
	WorkloadEvicted(node string, count int)
	RecoveryObserved(node string, d time.Duration)
}

func NewNodeLifecycleController(cp *controlplane.ControlPlane, log *slog.Logger, m NodeMetrics) *NodeLifecycleController {
	return &NodeLifecycleController{
		cp:               cp,
		log:              log.With("controller", "node-lifecycle"),
		HeartbeatTimeout: 15 * time.Second,
		EvictionDelay:    15 * time.Second,
		Interval:         2 * time.Second,
		metrics:          m,
		now:              func() time.Time { return time.Now().UTC() },
	}
}

func (c *NodeLifecycleController) Name() string                  { return "node-lifecycle" }
func (c *NodeLifecycleController) ResyncInterval() time.Duration { return c.Interval }

func (c *NodeLifecycleController) Reconcile(ctx context.Context) error {
	now := c.now()

	// Phase 1: mark silent nodes unreachable.
	for _, n := range c.cp.Store().StaleNodes(now, c.HeartbeatTimeout) {
		if err := ctx.Err(); err != nil {
			return err
		}
		silence := now.Sub(n.Status.LastHeartbeat).Round(time.Second)
		_, err := c.cp.Apply(ctx, store.Command{
			Kind:   store.CmdSetNodePhase,
			Name:   n.Name,
			Phase:  v1.NodeUnreachable,
			Reason: fmt.Sprintf("no heartbeat for %s (timeout %s)", silence, c.HeartbeatTimeout),
		})
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidState) {
				continue
			}
			return err
		}
		c.log.Warn("node is unreachable", "node", n.Name, "silence", silence,
			"workloads", n.Status.WorkloadCount)
		if c.metrics != nil {
			c.metrics.NodeBecameUnreachable(n.Name)
		}
	}

	// Phase 2: evict workloads from nodes that have been unreachable long
	// enough that a partitioned agent would already have fenced itself.
	for _, n := range c.cp.Store().Nodes() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if n.Status.Phase != v1.NodeUnreachable && n.Status.Phase != v1.NodeDraining {
			continue
		}
		if n.Status.Phase == v1.NodeUnreachable &&
			now.Sub(n.Status.LastHeartbeat) < c.HeartbeatTimeout+c.EvictionDelay {
			continue
		}
		if err := c.evictWorkloads(ctx, n); err != nil {
			c.log.Warn("eviction failed", "node", n.Name, "err", err)
		}
	}
	return nil
}

// evictWorkloads terminates every active workload on a node.
//
// The agent is gone, so it will never confirm the containers stopped. The
// controller therefore drives the workloads to Terminated itself. That is
// exactly why the eviction delay must exceed the agent's self-fence timeout:
// the control plane is asserting something it cannot observe.
func (c *NodeLifecycleController) evictWorkloads(ctx context.Context, n *v1.Node) error {
	workloads := c.cp.Store().WorkloadsOnNode(n.Name)
	evicted := 0

	for _, w := range workloads {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !w.Status.Phase.Active() {
			continue
		}
		reason := "NodeUnreachable"
		if n.Status.Phase == v1.NodeDraining {
			reason = "NodeDraining"
		}

		if w.DeletedAt == nil {
			if _, err := c.cp.Apply(ctx, store.Command{
				Kind:      store.CmdDeleteWorkload,
				Name:      w.Name,
				Reason:    reason,
				RequestID: "evict/" + w.UID,
			}); err != nil && !errors.Is(err, store.ErrNotFound) {
				c.log.Warn("could not mark workload for eviction", "workload", w.Name, "err", err)
				continue
			}
		}

		// A draining node still has a live agent, so let it perform the actual
		// shutdown and report Terminated itself.
		if n.Status.Phase == v1.NodeDraining {
			evicted++
			continue
		}

		status := w.Status.DeepCopy()
		status.Phase = v1.WorkloadTerminated
		status.Reason = reason
		status.Health = v1.HealthUnknown
		if _, err := c.cp.Apply(ctx, store.Command{
			Kind:           store.CmdUpdateWorkloadStatus,
			Name:           w.Name,
			UID:            w.UID,
			WorkloadStatus: &status,
		}); err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidState) {
			c.log.Warn("could not terminate evicted workload", "workload", w.Name, "err", err)
			continue
		}
		evicted++
	}

	if evicted == 0 {
		return nil
	}
	c.log.Warn("evicted workloads from unreachable node", "node", n.Name, "count", evicted)
	if c.metrics != nil {
		c.metrics.WorkloadEvicted(n.Name, evicted)
	}
	c.cp.RecordEvent(ctx, v1.Event{
		Severity: v1.SeverityCritical,
		Source:   "node-lifecycle",
		Reason:   "WorkloadsEvicted",
		Kind:     "Node",
		Name:     n.Name,
		Message: fmt.Sprintf("terminated %d workloads after %s of silence; replacements will be scheduled",
			evicted, c.HeartbeatTimeout+c.EvictionDelay),
	})
	return nil
}
