package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
)

// GarbageCollector removes records the cluster no longer needs.
//
// It is deliberately conservative. Terminated workloads are kept for a
// retention window because "why did this fail?" is asked after the fact, and a
// record that is deleted the instant a container stops is a record nobody ever
// gets to read. Only when the window has passed, or when the count grows past a
// bound, are they purged.
type GarbageCollector struct {
	cp  *controlplane.ControlPlane
	log *slog.Logger

	// TerminatedRetention is how long a finished workload's record is kept.
	TerminatedRetention time.Duration
	// MaxTerminatedPerOwner caps retained records per deployment so a
	// crash-looping workload cannot fill the state machine.
	MaxTerminatedPerOwner int
	// MaxDeletesPerPass bounds Raft churn from a single pass.
	MaxDeletesPerPass int

	Interval time.Duration
	now      func() time.Time
}

func NewGarbageCollector(cp *controlplane.ControlPlane, log *slog.Logger) *GarbageCollector {
	return &GarbageCollector{
		cp:                    cp,
		log:                   log.With("controller", "gc"),
		TerminatedRetention:   10 * time.Minute,
		MaxTerminatedPerOwner: 20,
		MaxDeletesPerPass:     64,
		Interval:              30 * time.Second,
		now:                   func() time.Time { return time.Now().UTC() },
	}
}

func (c *GarbageCollector) Name() string                  { return "gc" }
func (c *GarbageCollector) ResyncInterval() time.Duration { return c.Interval }

func (c *GarbageCollector) Reconcile(ctx context.Context) error {
	st := c.cp.Store()
	now := c.now()
	deletes := 0

	// Index deployments so orphan detection does not rescan per workload. A
	// deployment that is being deleted does not count as a live owner: its
	// workloads should be cleaned up promptly rather than held for the full
	// diagnostic retention window.
	liveOwners := map[string]bool{}
	for _, d := range st.Deployments() {
		if d.DeletedAt == nil {
			liveOwners[d.UID] = true
		}
	}

	terminatedByOwner := map[string]int{}
	workloads := st.Workloads()

	// Newest first, so the per-owner cap keeps the most recent failures — the
	// ones an operator is actually looking at.
	for i := len(workloads) - 1; i >= 0; i-- {
		w := workloads[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if deletes >= c.MaxDeletesPerPass {
			break
		}

		// Orphans: the owning deployment is gone but this record was missed,
		// e.g. because the controller was killed mid-cascade.
		if w.OwnerRef != nil && w.OwnerRef.Kind == "Deployment" && !liveOwners[w.OwnerRef.UID] {
			if w.Status.Phase.Active() && w.DeletedAt == nil {
				if c.deleteOrphan(ctx, w) {
					deletes++
				}
				continue
			}
		}

		if w.Status.Phase.Active() {
			continue
		}

		ownerKey := "<none>"
		orphaned := false
		if w.OwnerRef != nil {
			ownerKey = w.OwnerRef.UID
			orphaned = !liveOwners[w.OwnerRef.UID]
		}
		terminatedByOwner[ownerKey]++
		overCap := terminatedByOwner[ownerKey] > c.MaxTerminatedPerOwner

		finished := w.Status.LastTransition
		if finished.IsZero() {
			finished = w.UpdatedAt
		}
		expired := now.Sub(finished) > c.TerminatedRetention

		// Retention exists so an operator can still ask "why did this fail?".
		// Once the owning deployment is gone there is nothing left to ask
		// about, so its finished workloads are purged immediately rather than
		// keeping a deleted deployment half-alive for ten minutes.
		if !expired && !overCap && !orphaned {
			continue
		}
		// Only purge records the agent has confirmed are gone, or that never
		// reached a node in the first place.
		if w.Status.Phase != v1.WorkloadTerminated && w.Status.NodeName != "" {
			continue
		}

		if _, err := c.cp.Apply(ctx, store.Command{
			Kind:      store.CmdPurgeWorkload,
			Name:      w.Name,
			UID:       w.UID,
			RequestID: "purge/" + w.UID,
		}); err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidState) {
				continue
			}
			return err
		}
		deletes++
	}

	// Deployments finish deleting once none of their workloads remain.
	for _, d := range st.Deployments() {
		if d.DeletedAt == nil || deletes >= c.MaxDeletesPerPass {
			continue
		}
		if len(st.WorkloadsOwnedBy(d.UID)) > 0 {
			continue
		}
		if _, err := c.cp.Apply(ctx, store.Command{
			Kind:      store.CmdPurgeDeployment,
			Name:      d.Name,
			RequestID: "purge-deployment/" + d.UID,
		}); err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidState) {
				continue
			}
			return err
		}
		deletes++
		c.log.Info("deployment fully removed", "deployment", d.Name)
	}
	return nil
}

func (c *GarbageCollector) deleteOrphan(ctx context.Context, w *v1.Workload) bool {
	_, err := c.cp.Apply(ctx, store.Command{
		Kind:      store.CmdDeleteWorkload,
		Name:      w.Name,
		Reason:    "OrphanedByOwnerDeletion",
		RequestID: "orphan/" + w.UID,
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		c.log.Warn("could not delete orphaned workload", "workload", w.Name, "err", err)
		return false
	}
	c.log.Info("deleted orphaned workload", "workload", w.Name, "owner", w.OwnerRef.Name)
	return true
}
