package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
)

// DeploymentController converges each deployment's actual replica set toward
// its spec.
//
// It is level-triggered: every pass counts what exists and issues the writes
// that close the gap. Self-healing is not a separate mechanism — a workload
// that crashed is simply no longer active, so the same replica arithmetic that
// handles a scale-up recreates it.
type DeploymentController struct {
	cp  *controlplane.ControlPlane
	log *slog.Logger

	Interval time.Duration
	// MaxWritesPerPass bounds how much churn one pass can cause, so a large
	// rollout is spread over several passes rather than filling the Raft log
	// in one burst.
	MaxWritesPerPass int

	metrics DeploymentMetrics
	now     func() time.Time
}

// DeploymentMetrics is implemented by the telemetry package; nil is allowed.
type DeploymentMetrics interface {
	ReplicaCreated(deployment string)
	ReplicaDeleted(deployment, reason string)
}

func NewDeploymentController(cp *controlplane.ControlPlane, log *slog.Logger, m DeploymentMetrics) *DeploymentController {
	return &DeploymentController{
		cp:               cp,
		log:              log.With("controller", "deployment"),
		Interval:         3 * time.Second,
		MaxWritesPerPass: 64,
		metrics:          m,
		now:              func() time.Time { return time.Now().UTC() },
	}
}

func (c *DeploymentController) Name() string                  { return "deployment" }
func (c *DeploymentController) ResyncInterval() time.Duration { return c.Interval }

func (c *DeploymentController) Reconcile(ctx context.Context) error {
	for _, d := range c.cp.Store().Deployments() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.DeletedAt != nil {
			// Teardown is the garbage collector's job; the replica controller
			// must not recreate what is being deleted.
			continue
		}
		if err := c.reconcileOne(ctx, d); err != nil {
			c.log.Warn("deployment reconcile failed", "deployment", d.Name, "err", err)
		}
	}
	return nil
}

// replicaSets partitions a deployment's workloads by template revision.
type replicaSets struct {
	// updated are active workloads running the current template.
	updated []*v1.Workload
	// outdated are active workloads from a previous template.
	outdated []*v1.Workload
	// available are updated workloads that are Running and healthy.
	available int
	// unschedulable counts updated workloads the scheduler could not place.
	unschedulable int
	// takenOrdinals records which name ordinals are in use, including by
	// workloads that are terminating, so a new replica never collides with a
	// container that is still shutting down.
	takenOrdinals map[int]bool
}

func (c *DeploymentController) reconcileOne(ctx context.Context, d *v1.Deployment) error {
	currentHash := v1.HashWorkloadSpec(&d.Spec.Template)
	owned := c.cp.Store().WorkloadsOwnedBy(d.UID)

	rs := replicaSets{takenOrdinals: map[int]bool{}}
	for _, w := range owned {
		if n, ok := ordinalOf(w.Name, d.Name); ok {
			rs.takenOrdinals[n] = true
		}
		if !w.Status.Phase.Active() || w.DeletedAt != nil {
			continue
		}
		if w.Labels[labelTemplateHash] == currentHash {
			rs.updated = append(rs.updated, w)
			if w.Ready() {
				rs.available++
			}
			if w.Status.Phase == v1.WorkloadPending && w.Status.Reason == "Unschedulable" {
				rs.unschedulable++
			}
		} else {
			rs.outdated = append(rs.outdated, w)
		}
	}

	desired := int(d.Spec.Replicas)
	surge, unavailable := rolloutBudget(d)
	totalActive := len(rs.updated) + len(rs.outdated)
	writes := 0

	// --- Step 1: create missing updated replicas -----------------------------
	//
	// Bounded by both the desired count and the surge budget, so a rolling
	// update never runs more than desired+maxSurge containers at once.
	allowedTotal := desired + surge
	if d.Spec.Strategy.Kind == v1.StrategyRecreate {
		// Recreate holds new replicas back until every old one is gone; there
		// is no surge budget and no overlap.
		allowedTotal = desired
		if len(rs.outdated) > 0 {
			allowedTotal = 0
		}
	}
	toCreate := min(desired-len(rs.updated), allowedTotal-totalActive)
	for i := 0; i < toCreate && writes < c.MaxWritesPerPass; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.createReplica(ctx, d, currentHash, rs.takenOrdinals); err != nil {
			// Report but keep going: one failed creation should not stall the
			// rest of the rollout.
			c.log.Warn("could not create replica", "deployment", d.Name, "err", err)
			break
		}
		writes++
		totalActive++
	}

	// --- Step 2: retire outdated replicas ------------------------------------
	//
	// Only as fast as the availability budget allows. Deleting an old replica
	// before a new one is serving is what turns a rollout into an outage.
	if len(rs.outdated) > 0 {
		minAvailable := desired - unavailable
		if minAvailable < 0 {
			minAvailable = 0
		}
		canRetire := 0
		switch d.Spec.Strategy.Kind {
		case v1.StrategyRecreate:
			canRetire = len(rs.outdated)
		default:
			// Available capacity above the floor is what we may give up. Old
			// replicas that are already unavailable cost nothing to remove.
			healthyOutdated := 0
			for _, w := range rs.outdated {
				if w.Ready() {
					healthyOutdated++
				}
			}
			totalAvailable := rs.available + healthyOutdated
			canRetire = totalAvailable - minAvailable
			if unhealthy := len(rs.outdated) - healthyOutdated; unhealthy > canRetire {
				canRetire = unhealthy
			}
		}
		canRetire = min(canRetire, len(rs.outdated))

		for _, w := range retirementOrder(rs.outdated)[:max(0, canRetire)] {
			if writes >= c.MaxWritesPerPass {
				break
			}
			if err := c.deleteReplica(ctx, w, "ReplacedByRollout"); err != nil {
				c.log.Warn("could not retire replica", "workload", w.Name, "err", err)
				continue
			}
			writes++
		}
	}

	// --- Step 3: remove excess updated replicas (scale-down) -----------------
	if excess := len(rs.updated) - desired; excess > 0 {
		for _, w := range retirementOrder(rs.updated)[:excess] {
			if writes >= c.MaxWritesPerPass {
				break
			}
			if err := c.deleteReplica(ctx, w, "ScaledDown"); err != nil {
				c.log.Warn("could not scale down replica", "workload", w.Name, "err", err)
				continue
			}
			writes++
		}
	}

	return c.updateStatus(ctx, d, rs, desired)
}

// rolloutBudget resolves maxSurge and maxUnavailable, guaranteeing the rollout
// can always make progress.
func rolloutBudget(d *v1.Deployment) (surge, unavailable int) {
	surge = int(d.Spec.Strategy.MaxSurge)
	unavailable = int(d.Spec.Strategy.MaxUnavailable)
	if d.Spec.Strategy.Kind == v1.StrategyRecreate {
		return 0, int(d.Spec.Replicas)
	}
	if surge == 0 && unavailable == 0 {
		// Validation rejects this combination on create, but a deployment
		// stored by an older build could still carry it. Surging by one is the
		// safe interpretation: it preserves capacity.
		surge = 1
	}
	return surge, unavailable
}

// labelTemplateHash marks which template revision a workload belongs to. It is
// a label rather than an annotation because the controller selects on it.
const labelTemplateHash = "orion.io/template-hash"

// labelDeployment ties a workload to its deployment for human-readable
// filtering in the console and CLI.
const labelDeployment = "orion.io/deployment"

func (c *DeploymentController) createReplica(ctx context.Context, d *v1.Deployment, hash string, taken map[int]bool) error {
	// Deterministic naming: the lowest free ordinal. Two controller passes
	// racing on the same gap therefore propose the same name, and the loser
	// gets ErrAlreadyExists instead of creating a duplicate replica.
	ordinal := 0
	for taken[ordinal] {
		ordinal++
	}
	taken[ordinal] = true
	name := fmt.Sprintf("%s-%d", d.Name, ordinal)

	labels := map[string]string{}
	for k, v := range d.Labels {
		labels[k] = v
	}
	labels[labelDeployment] = d.Name
	labels[labelTemplateHash] = hash

	w := &v1.Workload{
		ObjectMeta: v1.ObjectMeta{
			Name:     name,
			Labels:   labels,
			OwnerRef: &v1.OwnerReference{Kind: "Deployment", Name: d.Name, UID: d.UID},
		},
		Spec: d.Spec.Template.DeepCopy(),
	}

	_, err := c.cp.Apply(ctx, store.Command{
		Kind:     store.CmdCreateWorkload,
		Workload: w,
		// Keyed on deployment identity, revision and ordinal, so a retry after
		// a leader change replays instead of creating a second replica.
		RequestID: fmt.Sprintf("replica/%s/%s/%d", d.UID, hash, ordinal),
	})
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			// Another pass won the race. That is success, not failure.
			return nil
		}
		return err
	}
	if c.metrics != nil {
		c.metrics.ReplicaCreated(d.Name)
	}
	c.log.Info("created replica", "deployment", d.Name, "workload", name, "revision", hash)
	return nil
}

func (c *DeploymentController) deleteReplica(ctx context.Context, w *v1.Workload, reason string) error {
	_, err := c.cp.Apply(ctx, store.Command{
		Kind:      store.CmdDeleteWorkload,
		Name:      w.Name,
		Reason:    reason,
		RequestID: "retire/" + w.UID,
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if c.metrics != nil {
		c.metrics.ReplicaDeleted(w.Labels[labelDeployment], reason)
	}
	return nil
}

// retirementOrder decides which replicas to remove first: unhealthy before
// healthy, unplaced before running, then newest first so the longest-serving
// replicas survive.
func retirementOrder(ws []*v1.Workload) []*v1.Workload {
	out := append([]*v1.Workload(nil), ws...)
	rank := func(w *v1.Workload) int {
		switch {
		case w.Status.Phase == v1.WorkloadPending:
			return 0
		case w.Status.Health == v1.HealthUnhealthy:
			return 1
		case w.Status.Phase != v1.WorkloadRunning:
			return 2
		default:
			return 3
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if ri, rj := rank(out[i]), rank(out[j]); ri != rj {
			return ri < rj
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].Name > out[j].Name
	})
	return out
}

func (c *DeploymentController) updateStatus(ctx context.Context, d *v1.Deployment, rs replicaSets, desired int) error {
	status := v1.DeploymentStatus{
		DesiredReplicas:       int32(desired),
		CurrentReplicas:       int32(len(rs.updated) + len(rs.outdated)),
		UpdatedReplicas:       int32(len(rs.updated)),
		AvailableReplicas:     int32(rs.available),
		UnschedulableReplicas: int32(rs.unschedulable),
		ObservedGeneration:    d.Generation,
		Conditions:            d.Status.Conditions,
	}

	converged := len(rs.outdated) == 0 && len(rs.updated) == desired && rs.available == desired
	switch {
	case converged:
		status.Phase = v1.DeploymentAvailable
	case c.stalled(d, rs, desired):
		status.Phase = v1.DeploymentDegraded
	default:
		status.Phase = v1.DeploymentProgressing
	}

	status.Conditions = v1.SetCondition(status.Conditions, v1.Condition{
		Type:   "Progressing",
		Status: status.Phase != v1.DeploymentDegraded,
		Reason: string(status.Phase),
		Message: fmt.Sprintf("%d/%d replicas available, %d updated, %d outdated",
			rs.available, desired, len(rs.updated), len(rs.outdated)),
		Since: c.now(),
	})

	if statusEqual(d.Status, status) {
		return nil
	}
	_, err := c.cp.Apply(ctx, store.Command{
		Kind:         store.CmdUpdateDeploymentStatus,
		Name:         d.Name,
		DeployStatus: &status,
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

// stalled reports whether a rollout has failed to converge within its progress
// deadline. Anything unschedulable is treated as stalled immediately: waiting
// five minutes to report "there is nowhere to put this" helps nobody.
func (c *DeploymentController) stalled(d *v1.Deployment, rs replicaSets, desired int) bool {
	if rs.unschedulable > 0 {
		return true
	}
	if d.Status.Phase != v1.DeploymentProgressing {
		return false
	}
	deadline := d.Spec.ProgressDeadline.Duration()
	if deadline <= 0 {
		deadline = v1.DefaultProgressDeadline
	}
	if d.Status.LastTransition.IsZero() {
		return false
	}
	// Only stalled if there has also been no forward progress in replica count.
	if int(d.Status.AvailableReplicas) < rs.available {
		return false
	}
	return c.now().Sub(d.Status.LastTransition) > deadline && rs.available < desired
}

// statusEqual compares the fields the controller owns, ignoring timestamps and
// revision so an unchanged status does not produce a Raft write every pass.
func statusEqual(a, b v1.DeploymentStatus) bool {
	if a.Phase != b.Phase ||
		a.DesiredReplicas != b.DesiredReplicas ||
		a.CurrentReplicas != b.CurrentReplicas ||
		a.UpdatedReplicas != b.UpdatedReplicas ||
		a.AvailableReplicas != b.AvailableReplicas ||
		a.UnschedulableReplicas != b.UnschedulableReplicas ||
		a.ObservedGeneration != b.ObservedGeneration {
		return false
	}
	if len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		if a.Conditions[i].Type != b.Conditions[i].Type ||
			a.Conditions[i].Status != b.Conditions[i].Status ||
			a.Conditions[i].Message != b.Conditions[i].Message {
			return false
		}
	}
	return true
}

// ordinalOf parses "web-3" -> 3 for workloads owned by deployment "web".
func ordinalOf(workloadName, deploymentName string) (int, bool) {
	if len(workloadName) <= len(deploymentName)+1 || workloadName[:len(deploymentName)] != deploymentName {
		return 0, false
	}
	if workloadName[len(deploymentName)] != '-' {
		return 0, false
	}
	n := 0
	for _, ch := range workloadName[len(deploymentName)+1:] {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int(ch-'0')
	}
	return n, true
}
