// Package faults implements Orion's fault injection framework.
//
// The design constraint that shapes everything here: an injected fault must be
// a *real* fault, not a simulation. Marking a node "failed" in a database and
// asserting that the database now says "failed" proves nothing. So every
// experiment produces a condition the rest of the system cannot distinguish
// from the genuine article:
//
//	node failure        the control plane stops accepting that agent's syncs,
//	                    which is exactly what a dead machine looks like from
//	                    the control plane's side
//	network partition   the same, except the agent is still alive — so this is
//	                    the experiment that exercises self-fencing
//	workload crash      the container is actually stopped
//	leader failure      the Raft leader actually steps down
//	controller crash    the controller manager is actually stopped
//	resource exhaustion real workloads are created until scheduling fails
//
// Each experiment states a hypothesis and a set of invariants. The invariants
// are sampled continuously while the fault is held and while the cluster
// recovers, so a violation is caught at the moment it happens rather than
// inferred from the wreckage.
package faults

import (
	"sync"
	"time"
)

// Gate decides whether the control plane will talk to a node's agent.
//
// It is consulted by the node service on every Register and Sync. Blocking a
// node here produces, from every other component's perspective, an ordinary
// unreachable node: heartbeats stop arriving, the failure detector fires, and
// the eviction path runs for real.
type Gate struct {
	mu      sync.RWMutex
	blocked map[string]blockRecord
}

type blockRecord struct {
	Reason     string
	Since      time.Time
	Experiment string
	// Bidirectional means the agent is also considered unreachable *by* the
	// control plane, which is the distinction between "the machine died" and
	// "the network broke in one direction".
	Bidirectional bool
}

func NewGate() *Gate { return &Gate{blocked: map[string]blockRecord{}} }

// Allowed reports whether the control plane should serve this node.
func (g *Gate) Allowed(node string) bool {
	if g == nil {
		return true
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, blocked := g.blocked[node]
	return !blocked
}

// Block starts dropping a node's traffic.
func (g *Gate) Block(node, reason, experiment string, bidirectional bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.blocked[node] = blockRecord{
		Reason: reason, Since: time.Now(), Experiment: experiment, Bidirectional: bidirectional,
	}
}

// Unblock restores a node.
func (g *Gate) Unblock(node string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.blocked, node)
}

// UnblockExperiment restores every node an experiment blocked. It is called on
// abort and on process shutdown, so a crashed experiment cannot leave the
// cluster permanently partitioned.
func (g *Gate) UnblockExperiment(experiment string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for node, rec := range g.blocked {
		if rec.Experiment == experiment {
			delete(g.blocked, node)
		}
	}
}

// BlockedNodes lists currently blocked nodes, so the console can show that a
// node's "failure" is deliberate rather than sending someone to investigate a
// machine that is fine.
func (g *Gate) BlockedNodes() map[string]string {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string]string, len(g.blocked))
	for node, rec := range g.blocked {
		out[node] = rec.Reason
	}
	return out
}

// Clear releases every block. Called during shutdown.
func (g *Gate) Clear() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.blocked = map[string]blockRecord{}
}
