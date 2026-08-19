package faults

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/store"
)

// experiment is the pair of operations that make up a fault: inject it, then
// lift it. Both must be safe to call when the other failed halfway, because an
// experiment that cannot be undone is an outage.
type experiment struct {
	inject  func(ctx context.Context, i *Injector, runID string, params map[string]string) (scope, error)
	recover func(ctx context.Context, i *Injector, runID string, params map[string]string, s scope) error
}

// catalogue documents every experiment. The hypothesis is mandatory: an
// experiment without a stated expectation is just breakage.
var catalogue = []apiserver.ExperimentDescriptor{
	{
		Kind: apiserver.ExperimentNodeFailure,
		Name: "Node failure",
		Description: "Stops the control plane from accepting a node's heartbeats, which is " +
			"indistinguishable from the machine losing power.",
		Hypothesis: "The control plane detects the failure within the heartbeat timeout, marks the " +
			"node Unreachable, evicts its workloads after the eviction delay, and the scheduler " +
			"places replacements on the remaining nodes. Service endpoints drop the dead backends " +
			"before the replacements are ready.",
		Invariants:  invariantNames(),
		Destructive: true,
		Parameters: parameterList{
			{Name: "node", Type: "node", Required: true, Help: "The node to fail."},
		}.toSpecs(),
	},
	{
		Kind: apiserver.ExperimentNetworkPartition,
		Name: "Network partition",
		Description: "Isolates a node from the control plane while its agent keeps running. " +
			"Unlike node failure, the containers are still alive on the other side.",
		Hypothesis: "The control plane treats the node as failed and schedules replacements. " +
			"Meanwhile the agent, unable to reach the control plane, fences itself and stops its " +
			"own workloads before the eviction deadline — so the cluster never runs two copies of " +
			"the same workload.",
		Invariants:  invariantNames(),
		Destructive: true,
		Parameters: parameterList{
			{Name: "node", Type: "node", Required: true, Help: "The node to isolate."},
		}.toSpecs(),
	},
	{
		Kind:        apiserver.ExperimentWorkloadCrash,
		Name:        "Workload crash",
		Description: "Terminates a running workload's container.",
		Hypothesis: "The agent observes the exit and applies the restart policy; if the workload " +
			"belongs to a deployment, the controller restores the replica count. The deployment " +
			"never exceeds its desired replica count during the recovery.",
		Invariants:  invariantNames(),
		Destructive: true,
		Parameters: parameterList{
			{Name: "workload", Type: "workload", Required: true, Help: "The workload to crash."},
		}.toSpecs(),
	},
	{
		Kind: apiserver.ExperimentLeaderFailure,
		Name: "Control-plane leader failure",
		Description: "Forces the Raft leader to step down, triggering an election while the " +
			"cluster is under load.",
		Hypothesis: "A new leader is elected, every committed write survives, controllers restart " +
			"on the new leader without duplicating work, and in-flight writes either commit or " +
			"fail cleanly — never partially.",
		Invariants: invariantNames(),
		Parameters: nil,
	},
	{
		Kind:        apiserver.ExperimentControllerCrash,
		Name:        "Controller manager crash",
		Description: "Stops reconciliation entirely, then restarts it.",
		Hypothesis: "Nothing changes while the controllers are down — the cluster is level- " +
			"triggered, not event-driven — and on restart they reconcile from current state " +
			"without recreating work that already exists.",
		Invariants: invariantNames(),
		Parameters: nil,
	},
	{
		Kind:        apiserver.ExperimentResourceExhaustion,
		Name:        "Resource exhaustion",
		Description: "Creates workloads until the cluster runs out of schedulable capacity.",
		Hypothesis: "Excess workloads stay Pending with a per-node explanation of why they could " +
			"not be placed, no node is overcommitted, and the affected deployments report " +
			"Degraded rather than silently claiming to be available.",
		Invariants:  invariantNames(),
		Destructive: true,
		Parameters: parameterList{
			{Name: "count", Type: "int", Required: false, Default: "20",
				Help: "How many filler workloads to create."},
			{Name: "cpu", Type: "int", Required: false, Default: "1000",
				Help: "CPU request per filler workload, in millicores."},
		}.toSpecs(),
	},
}

// parameterList lets the catalogue above be written as a readable table
// instead of a wall of struct literals.
type parameterList []apiserver.ParameterSpec

func (l parameterList) toSpecs() []apiserver.ParameterSpec { return l }

func invariantNames() []string {
	invs := StandardInvariants()
	out := make([]string, 0, len(invs))
	for _, inv := range invs {
		out = append(out, inv.Name)
	}
	return out
}

// experiments maps a kind to its implementation.
var experiments = map[apiserver.ExperimentKind]experiment{
	apiserver.ExperimentNodeFailure: {
		inject: func(_ context.Context, i *Injector, runID string, params map[string]string) (scope, error) {
			node := params["node"]
			// Blocking the node's traffic is a real fault: from every other
			// component's perspective the heartbeats simply stop.
			i.opts.Gate.Block(node, "fault injection: simulated node failure", runID, true)
			workloads := activeWorkloadNames(i.cp.Store(), node)
			return scope{
				Nodes:     []string{node},
				Workloads: workloads,
				Note: fmt.Sprintf("blocked all traffic from node %s, which is running %d workloads; "+
					"the control plane will now see it stop heartbeating", node, len(workloads)),
			}, nil
		},
		recover: func(_ context.Context, i *Injector, runID string, params map[string]string, _ scope) error {
			i.opts.Gate.Unblock(params["node"])
			return nil
		},
	},

	apiserver.ExperimentNetworkPartition: {
		inject: func(_ context.Context, i *Injector, runID string, params map[string]string) (scope, error) {
			node := params["node"]
			i.opts.Gate.Block(node, "fault injection: network partition", runID, false)
			workloads := activeWorkloadNames(i.cp.Store(), node)
			return scope{
				Nodes:     []string{node},
				Workloads: workloads,
				Note: fmt.Sprintf("partitioned node %s from the control plane; its agent is still "+
					"running %d workloads and must fence itself before the eviction deadline",
					node, len(workloads)),
			}, nil
		},
		recover: func(_ context.Context, i *Injector, runID string, params map[string]string, _ scope) error {
			i.opts.Gate.Unblock(params["node"])
			return nil
		},
	},

	apiserver.ExperimentWorkloadCrash: {
		inject: func(ctx context.Context, i *Injector, _ string, params map[string]string) (scope, error) {
			name := params["workload"]
			w, ok := i.cp.Store().Workload(name)
			if !ok {
				return scope{}, fmt.Errorf("workload %s no longer exists", name)
			}
			// Deleting the workload makes the agent actually stop the
			// container; the deployment controller then has to notice the
			// missing replica and create a replacement.
			if _, err := i.cp.Apply(ctx, store.Command{
				Kind:   store.CmdDeleteWorkload,
				Name:   name,
				Reason: "FaultInjection",
				Actor:  "fault-injector",
			}); err != nil {
				return scope{}, err
			}
			return scope{
				Nodes:     []string{w.Status.NodeName},
				Workloads: []string{name},
				Note: fmt.Sprintf("terminated workload %s on node %s", name,
					orUnplaced(w.Status.NodeName)),
			}, nil
		},
		recover: func(context.Context, *Injector, string, map[string]string, scope) error {
			// Nothing to undo: recovery is the system's job, which is the point.
			return nil
		},
	},

	apiserver.ExperimentLeaderFailure: {
		inject: func(ctx context.Context, i *Injector, _ string, _ map[string]string) (scope, error) {
			status := i.cp.Raft().Status()
			if len(status.Voters) < 3 {
				return scope{}, fmt.Errorf(
					"the control plane has %d replica(s); a leader failure needs at least 3 to keep quorum",
					len(status.Voters))
			}
			if !i.cp.IsLeader() {
				return scope{}, errors.New("this replica is not the leader; run the experiment against the leader")
			}
			return scope{}, errors.New(
				"leader failure is only meaningful against a multi-replica control plane started with " +
					"--peers; see docs/FAILURES.md for how to run it")
		},
		recover: func(context.Context, *Injector, string, map[string]string, scope) error { return nil },
	},

	apiserver.ExperimentControllerCrash: {
		inject: func(_ context.Context, i *Injector, _ string, _ map[string]string) (scope, error) {
			if i.opts.ControllerControl == nil {
				return scope{}, errors.New("controller control is not available on this server")
			}
			i.opts.ControllerControl.StopControllers()
			return scope{Note: "stopped the controller manager; no reconciliation is running"}, nil
		},
		recover: func(_ context.Context, i *Injector, _ string, _ map[string]string, _ scope) error {
			i.opts.ControllerControl.StartControllers()
			return nil
		},
	},

	apiserver.ExperimentResourceExhaustion: {
		inject: func(ctx context.Context, i *Injector, runID string, params map[string]string) (scope, error) {
			count := paramInt(params, "count", 20)
			cpu := paramInt(params, "cpu", 1000)
			if count > 200 {
				return scope{}, fmt.Errorf("count %d is too large; 200 is the cap", count)
			}

			created := make([]string, 0, count)
			for n := 0; n < count; n++ {
				name := fmt.Sprintf("fault-filler-%s-%d", shortID(runID), n)
				w := &v1.Workload{
					ObjectMeta: v1.ObjectMeta{
						Name:   name,
						Labels: map[string]string{"orion.io/fault-experiment": runID},
					},
					Spec: v1.WorkloadSpec{
						// A container that exits immediately would be restarted
						// forever; pausing holds the reservation without doing
						// anything.
						Image:         "busybox:1.36",
						Command:       []string{"sh", "-c"},
						Args:          []string{"sleep 3600"},
						RestartPolicy: v1.RestartNever,
						Resources: v1.ResourceSpec{
							Request: v1.Resources{CPU: v1.MilliCPU(cpu), Memory: 64 << 20},
						},
					},
				}
				v1.SetWorkloadSpecDefaults(&w.Spec)
				if _, err := i.cp.Apply(ctx, store.Command{
					Kind: store.CmdCreateWorkload, Workload: w, Actor: "fault-injector",
				}); err != nil {
					break // the cluster refused; that is a legitimate outcome
				}
				created = append(created, name)
			}
			if len(created) == 0 {
				return scope{}, errors.New("could not create any filler workloads")
			}
			return scope{
				Workloads: created,
				Note: fmt.Sprintf("created %d workloads requesting %dm CPU each to exhaust capacity",
					len(created), cpu),
			}, nil
		},
		recover: func(ctx context.Context, i *Injector, _ string, _ map[string]string, s scope) error {
			// Every filler must be removed, even if some deletions fail, or the
			// cluster is left permanently full.
			var firstErr error
			for _, name := range s.Workloads {
				if _, err := i.cp.Apply(ctx, store.Command{
					Kind:   store.CmdDeleteWorkload,
					Name:   name,
					Reason: "FaultInjectionCleanup",
					Actor:  "fault-injector",
				}); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			return firstErr
		},
	},
}

func activeWorkloadNames(st *store.Store, node string) []string {
	var out []string
	for _, w := range st.WorkloadsOnNode(node) {
		if w.Status.Phase.Active() {
			out = append(out, w.Name)
		}
	}
	return out
}

func paramInt(params map[string]string, key string, def int) int {
	v, ok := params[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func orUnplaced(node string) string {
	if node == "" {
		return "(unplaced)"
	}
	return node
}

func shortID(runID string) string {
	if len(runID) > 8 {
		return runID[4:8]
	}
	return runID
}
