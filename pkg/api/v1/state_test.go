package v1

import "testing"

var allWorkloadPhases = []WorkloadPhase{
	WorkloadPending, WorkloadScheduled, WorkloadStarting, WorkloadRunning,
	WorkloadSucceeded, WorkloadFailed, WorkloadTerminating, WorkloadTerminated,
}

var allNodePhases = []NodePhase{
	NodeRegistering, NodeReady, NodeNotReady, NodeUnreachable, NodeDraining, NodeDecommissioned,
}

// A terminal phase must have no outgoing edges, otherwise "terminal" is a lie
// that controllers will eventually violate.
func TestTerminalPhasesHaveNoOutgoingTransitions(t *testing.T) {
	for _, from := range allWorkloadPhases {
		if !from.Terminal() {
			continue
		}
		for _, to := range allWorkloadPhases {
			if from != to && from.CanTransitionTo(to) {
				t.Errorf("terminal workload phase %s allows transition to %s", from, to)
			}
		}
	}
	for _, from := range allNodePhases {
		if !from.Terminal() {
			continue
		}
		for _, to := range allNodePhases {
			if from != to && from.CanTransitionTo(to) {
				t.Errorf("terminal node phase %s allows transition to %s", from, to)
			}
		}
	}
}

// Every non-terminal phase must be able to reach a terminal phase, or objects
// can get permanently stuck and leak resources.
func TestEveryPhaseCanReachTerminal(t *testing.T) {
	reaches := func(start WorkloadPhase) bool {
		seen := map[WorkloadPhase]bool{start: true}
		queue := []WorkloadPhase{start}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if cur.Terminal() {
				return true
			}
			for _, next := range allWorkloadPhases {
				if next != cur && cur.CanTransitionTo(next) && !seen[next] {
					seen[next] = true
					queue = append(queue, next)
				}
			}
		}
		return false
	}
	for _, p := range allWorkloadPhases {
		if !reaches(p) {
			t.Errorf("workload phase %s cannot reach a terminal phase", p)
		}
	}
}

func TestWorkloadTransitionsRejectIllegalEdges(t *testing.T) {
	illegal := []struct{ from, to WorkloadPhase }{
		{WorkloadPending, WorkloadRunning},     // must be scheduled first
		{WorkloadRunning, WorkloadPending},     // never rewind; reschedule creates a new workload
		{WorkloadTerminated, WorkloadRunning},  // terminal
		{WorkloadFailed, WorkloadRunning},      // in-place restart keeps phase Running
		{WorkloadTerminating, WorkloadRunning}, // deletion is not cancellable
	}
	for _, tc := range illegal {
		if tc.from.CanTransitionTo(tc.to) {
			t.Errorf("expected %s -> %s to be rejected", tc.from, tc.to)
		}
	}

	legal := []struct{ from, to WorkloadPhase }{
		{WorkloadPending, WorkloadScheduled},
		{WorkloadScheduled, WorkloadStarting},
		{WorkloadScheduled, WorkloadRunning}, // fast start, image already present
		{WorkloadStarting, WorkloadRunning},
		{WorkloadRunning, WorkloadFailed},
		{WorkloadRunning, WorkloadTerminating},
		{WorkloadTerminating, WorkloadTerminated},
	}
	for _, tc := range legal {
		if !tc.from.CanTransitionTo(tc.to) {
			t.Errorf("expected %s -> %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestSelfTransitionAlwaysAllowed(t *testing.T) {
	for _, p := range allWorkloadPhases {
		if !p.CanTransitionTo(p) {
			t.Errorf("workload phase %s rejects self-transition", p)
		}
	}
	for _, p := range allNodePhases {
		if !p.CanTransitionTo(p) {
			t.Errorf("node phase %s rejects self-transition", p)
		}
	}
}

func TestOnlyReadyNodesAreSchedulable(t *testing.T) {
	for _, p := range allNodePhases {
		if got, want := p.Schedulable(), p == NodeReady; got != want {
			t.Errorf("%s.Schedulable() = %v, want %v", p, got, want)
		}
	}
}

func TestActiveAndDonePhasesArePartition(t *testing.T) {
	for _, p := range allWorkloadPhases {
		if p.Active() && p.Done() {
			t.Errorf("phase %s is both active and done", p)
		}
		if !p.Active() && !p.Done() && p != WorkloadTerminating {
			t.Errorf("phase %s is neither active nor done", p)
		}
	}
}

func TestRestartPolicyDecisions(t *testing.T) {
	cases := []struct {
		policy   RestartPolicy
		exitCode int32
		want     bool
	}{
		{RestartAlways, 0, true},
		{RestartAlways, 137, true},
		{RestartOnFailure, 0, false},
		{RestartOnFailure, 1, true},
		{RestartNever, 0, false},
		{RestartNever, 1, false},
	}
	for _, tc := range cases {
		if got := tc.policy.ShouldRestart(tc.exitCode); got != tc.want {
			t.Errorf("%s.ShouldRestart(%d) = %v, want %v", tc.policy, tc.exitCode, got, tc.want)
		}
	}
}

// Unknown health must not serve traffic: endpoints are proven healthy, not
// assumed healthy.
func TestUnknownHealthDoesNotServe(t *testing.T) {
	if HealthUnknown.Serving() {
		t.Error("HealthUnknown must not be considered serving")
	}
	if HealthUnhealthy.Serving() {
		t.Error("HealthUnhealthy must not be considered serving")
	}
	if !HealthHealthy.Serving() {
		t.Error("HealthHealthy must be considered serving")
	}
}
