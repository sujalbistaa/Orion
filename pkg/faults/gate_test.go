package faults

import "testing"

func TestGateNilIsAlwaysAllowed(t *testing.T) {
	var g *Gate
	if !g.Allowed("worker-01") {
		t.Fatal("a nil Gate must allow every node, so faults are strictly opt-in")
	}
	if g.BlockedNodes() != nil {
		t.Fatal("a nil Gate must report no blocked nodes")
	}
}

func TestGateBlockAndUnblock(t *testing.T) {
	g := NewGate()
	if !g.Allowed("worker-01") {
		t.Fatal("a fresh Gate must allow every node")
	}

	g.Block("worker-01", "network partition", "exp-1", false)
	if g.Allowed("worker-01") {
		t.Fatal("a blocked node must not be allowed")
	}
	if !g.Allowed("worker-02") {
		t.Fatal("blocking one node must not affect another")
	}

	g.Unblock("worker-01")
	if !g.Allowed("worker-01") {
		t.Fatal("unblocking must restore the node")
	}
}

func TestGateUnblockExperimentOnlyReleasesItsOwnBlocks(t *testing.T) {
	g := NewGate()
	g.Block("worker-01", "reason", "exp-1", false)
	g.Block("worker-02", "reason", "exp-2", false)

	g.UnblockExperiment("exp-1")

	if !g.Allowed("worker-01") {
		t.Fatal("exp-1's block should have been released")
	}
	if g.Allowed("worker-02") {
		t.Fatal("exp-2's block must survive releasing exp-1")
	}
}

func TestGateBlockedNodesReportsReason(t *testing.T) {
	g := NewGate()
	g.Block("worker-01", "simulated crash", "exp-1", true)

	blocked := g.BlockedNodes()
	if got, want := blocked["worker-01"], "simulated crash"; got != want {
		t.Fatalf("BlockedNodes()[worker-01] = %q, want %q", got, want)
	}
	if len(blocked) != 1 {
		t.Fatalf("expected exactly one blocked node, got %v", blocked)
	}
}

func TestGateClearReleasesEverything(t *testing.T) {
	g := NewGate()
	g.Block("worker-01", "r1", "exp-1", false)
	g.Block("worker-02", "r2", "exp-2", false)

	g.Clear()

	if !g.Allowed("worker-01") || !g.Allowed("worker-02") {
		t.Fatal("Clear must release every block")
	}
	if len(g.BlockedNodes()) != 0 {
		t.Fatal("Clear must leave no blocked nodes")
	}
}
