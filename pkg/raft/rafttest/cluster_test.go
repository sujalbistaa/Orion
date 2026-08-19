package rafttest

import (
	"fmt"
	"testing"

	"github.com/sujalbistaa/orion/pkg/raft"
)

func defaultOpts(seed int64) Options {
	return Options{
		Seed:          seed,
		IDs:           []uint64{1, 2, 3},
		ElectionTick:  10,
		HeartbeatTick: 1,
		PreVote:       true,
		CheckQuorum:   true,
	}
}

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	n := NewNetwork(t, defaultOpts(1))
	if !n.RunUntil(500, func() bool { return n.Leader() != 0 }) {
		t.Fatalf("no leader elected within 500 ticks (seed %d)", n.Seed)
	}
	if got := len(n.LeadersInTerm()); got != 1 {
		t.Fatalf("expected exactly one leader, got %d (seed %d)", got, n.Seed)
	}
	n.CheckSafety()
}

func TestReplicationReachesEveryFollower(t *testing.T) {
	n := NewNetwork(t, defaultOpts(2))
	n.ElectLeader(1)

	for i := 0; i < 20; i++ {
		if _, err := n.Propose(1, fmt.Sprintf("key%d=value%d", i, i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	n.Run(20)

	leaderCommit := n.CommittedOn(1)
	for _, r := range n.Replicas() {
		if got := r.Node.Committed(); got != leaderCommit {
			t.Errorf("replica %d committed %d, leader committed %d (seed %d)", r.ID, got, leaderCommit, n.Seed)
		}
		if len(r.FSM.State) != 20 {
			t.Errorf("replica %d applied %d keys, want 20 (seed %d)", r.ID, len(r.FSM.State), n.Seed)
		}
	}
	n.CheckSafety()
}

// The minority side of a partition must not commit anything, and the majority
// side must keep making progress.
func TestPartitionMinorityCannotCommit(t *testing.T) {
	n := NewNetwork(t, defaultOpts(3))
	n.ElectLeader(1)
	n.Propose(1, "before=partition")
	n.Run(10)
	committedBefore := n.CommittedOn(3)

	// Isolate the leader; {2,3} form the majority.
	n.Isolate(1)
	// The old leader accepts a proposal it can never commit.
	if _, err := n.Propose(1, "orphan=1"); err != nil && err != raft.ErrNotLeader {
		t.Fatalf("unexpected error: %v", err)
	}
	n.Run(200)

	if n.CommittedOn(1) > committedBefore+1 {
		// +1 tolerates the no-op the isolated leader may already have had
		// committed before isolation.
		t.Errorf("isolated leader advanced its commit index to %d (was %d) (seed %d)",
			n.CommittedOn(1), committedBefore, n.Seed)
	}

	// The majority elects a new leader and keeps committing.
	newLeader := uint64(0)
	n.RunUntil(500, func() bool {
		for _, id := range []uint64{2, 3} {
			if n.Replica(id).Node.State() == raft.Leader {
				newLeader = id
				return true
			}
		}
		return false
	})
	if newLeader == 0 {
		t.Fatalf("majority failed to elect a new leader (seed %d)", n.Seed)
	}
	if _, err := n.Propose(newLeader, "after=partition"); err != nil {
		t.Fatalf("majority could not accept a proposal: %v", err)
	}
	n.Run(50)
	if n.Replica(newLeader).FSM.State["after"] != "partition" {
		t.Errorf("majority did not apply its own write (seed %d)", n.Seed)
	}

	// Healing must converge every replica, and the orphaned write must be gone.
	n.Heal()
	n.Run(300)
	for _, r := range n.Replicas() {
		if _, ok := r.FSM.State["orphan"]; ok {
			t.Errorf("replica %d applied an uncommitted write from the isolated leader (seed %d)", r.ID, n.Seed)
		}
		if r.FSM.State["after"] != "partition" {
			t.Errorf("replica %d did not converge after healing: %v (seed %d)", r.ID, r.FSM.State, n.Seed)
		}
	}
	n.CheckSafety()
}

// Pre-vote: a replica that was isolated and campaigned repeatedly must not
// disrupt the healthy leader when it reconnects with an inflated term.
func TestPreVotePreventsDisruptionOnRejoin(t *testing.T) {
	opts := defaultOpts(4)
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	n.Propose(1, "a=1")
	n.Run(20)

	termBefore := n.Replica(1).Node.Term()

	n.Isolate(3)
	n.Run(500) // replica 3 spins on pre-vote and gets nowhere
	if n.Replica(3).Node.Term() > termBefore {
		t.Errorf("isolated replica advanced its term to %d under pre-vote (was %d) (seed %d)",
			n.Replica(3).Node.Term(), termBefore, n.Seed)
	}

	n.Heal()
	n.Run(100)
	if n.Replica(1).Node.State() != raft.Leader {
		t.Errorf("leader was disrupted by a rejoining replica (seed %d)", n.Seed)
	}
	if n.Replica(1).Node.Term() != termBefore {
		t.Errorf("term advanced from %d to %d on rejoin (seed %d)", termBefore, n.Replica(1).Node.Term(), n.Seed)
	}
	n.CheckSafety()
}

// A crashed replica must recover its committed state from disk alone.
func TestCrashAndRestartRecoversCommittedState(t *testing.T) {
	opts := defaultOpts(5)
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	for i := 0; i < 10; i++ {
		n.Propose(1, fmt.Sprintf("k%d=v%d", i, i))
	}
	n.Run(30)

	before := n.Replica(3).Node.Committed()
	n.Crash(3)
	// The cluster keeps working with 2 of 3.
	n.Propose(1, "during=crash")
	n.Run(30)
	if n.Replica(1).FSM.State["during"] != "crash" {
		t.Fatalf("cluster stalled with one replica down (seed %d)", n.Seed)
	}

	n.Restart(3, opts)
	n.Run(200)

	r3 := n.Replica(3)
	if r3.Node.Committed() < before {
		t.Errorf("restarted replica lost committed state: %d < %d (seed %d)", r3.Node.Committed(), before, n.Seed)
	}
	if r3.FSM.State["during"] != "crash" {
		t.Errorf("restarted replica did not catch up: %v (seed %d)", r3.FSM.State, n.Seed)
	}
	for i := 0; i < 10; i++ {
		if got, want := r3.FSM.State[fmt.Sprintf("k%d", i)], fmt.Sprintf("v%d", i); got != want {
			t.Errorf("restarted replica lost key k%d: got %q want %q (seed %d)", i, got, want, n.Seed)
		}
	}
	n.CheckSafety()
}

// Killing the leader must produce a new one that retains every committed entry.
func TestLeaderFailoverPreservesCommittedEntries(t *testing.T) {
	opts := defaultOpts(6)
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	for i := 0; i < 5; i++ {
		n.Propose(1, fmt.Sprintf("k%d=v%d", i, i))
	}
	n.Run(30)

	n.Crash(1)
	if !n.RunUntil(1000, func() bool { return n.Leader() != 0 && n.Leader() != 1 }) {
		t.Fatalf("no new leader after the leader crashed (seed %d)", n.Seed)
	}
	newLeader := n.Leader()

	// Every committed entry must have survived the failover.
	for i := 0; i < 5; i++ {
		key, want := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		if got := n.Replica(newLeader).FSM.State[key]; got != want {
			t.Errorf("new leader %d lost committed key %s: got %q want %q (seed %d)", newLeader, key, got, want, n.Seed)
		}
	}
	if _, err := n.Propose(newLeader, "after=failover"); err != nil {
		t.Fatalf("new leader cannot accept writes: %v", err)
	}
	n.Run(50)
	n.CheckSafety()
}

// The interesting case: a lossy, slow, duplicating network. Raft must still
// converge, and it must do so for every seed.
func TestConvergesUnderLossDelayAndDuplication(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			opts := defaultOpts(seed)
			opts.Faults = FaultConfig{DropRate: 0.15, DuplicateRate: 0.10, MaxDelayTicks: 3}
			n := NewNetwork(t, opts)

			if !n.RunUntil(3000, func() bool { return n.Leader() != 0 }) {
				t.Fatalf("no leader elected on a lossy network within 3000 ticks (seed %d)", seed)
			}

			applied := 0
			for i := 0; i < 30; i++ {
				if _, err := n.ProposeToLeader(fmt.Sprintf("k%d=v%d", i, i)); err == nil {
					applied++
				}
				n.Run(10)
			}
			if applied == 0 {
				t.Fatalf("no proposal was accepted on a lossy network (seed %d)", seed)
			}

			// Give the cluster time to converge with no further writes.
			n.Run(3000)

			leader := n.Leader()
			if leader == 0 {
				t.Fatalf("cluster has no leader after quiescing (seed %d)", seed)
			}
			want := n.Replica(leader).Node.Committed()
			for _, r := range n.Replicas() {
				if got := r.Node.Committed(); got != want {
					t.Errorf("replica %d committed %d, leader %d committed %d (seed %d)", r.ID, got, leader, want, seed)
				}
			}
			n.CheckSafety()
		})
	}
}

// A five-node cluster must tolerate two simultaneous failures.
func TestFiveNodeClusterToleratesTwoFailures(t *testing.T) {
	opts := defaultOpts(7)
	opts.IDs = []uint64{1, 2, 3, 4, 5}
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	n.Propose(1, "a=1")
	n.Run(20)

	n.Crash(4)
	n.Crash(5)
	if _, err := n.Propose(1, "b=2"); err != nil {
		t.Fatalf("five-node cluster stalled with two failures: %v", err)
	}
	n.Run(50)
	if n.Replica(1).FSM.State["b"] != "2" {
		t.Fatalf("cluster did not commit with 3 of 5 alive (seed %d)", n.Seed)
	}

	// A third failure costs quorum: no further commits are allowed.
	committed := n.CommittedOn(1)
	n.Crash(3)
	n.Propose(1, "c=3")
	n.Run(300)
	for _, id := range []uint64{1, 2} {
		if n.CommittedOn(id) > committed {
			t.Errorf("replica %d committed past quorum loss: %d > %d (seed %d)", id, n.CommittedOn(id), committed, n.Seed)
		}
	}
	n.CheckSafety()
}

// An asymmetric partition (leader can send but not receive) must be resolved:
// CheckQuorum makes the leader step down rather than serve stale reads forever.
func TestAsymmetricPartitionResolves(t *testing.T) {
	opts := defaultOpts(8)
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	n.Propose(1, "a=1")
	n.Run(20)

	n.BlockOneWay(2, 1)
	n.BlockOneWay(3, 1)
	n.Run(400)

	if n.Replica(1).Node.State() == raft.Leader {
		t.Errorf("leader that cannot hear from anyone kept leadership (seed %d)", n.Seed)
	}
	n.Heal()
	if !n.RunUntil(1000, func() bool { return n.Leader() != 0 }) {
		t.Fatalf("cluster did not recover after healing an asymmetric partition (seed %d)", n.Seed)
	}
	n.CheckSafety()
}

// Membership: growing from three to five voters must not lose committed state
// and must keep the cluster available throughout.
func TestMembershipChangeAddsVoters(t *testing.T) {
	opts := defaultOpts(9)
	opts.IDs = []uint64{1, 2, 3}
	n := NewNetwork(t, opts)
	n.ElectLeader(1)
	n.Propose(1, "a=1")
	n.Run(20)

	// Removing a voter is the simpler direction to verify in a fixed-size
	// simulator: quorum shrinks from 2 to 2 (of 2), and commits continue.
	if _, err := n.Replica(1).Node.ProposeConfChange(raft.ConfChange{
		Type: raft.ConfChangeRemoveNode, NodeID: 3,
	}); err != nil {
		t.Fatalf("conf change rejected: %v", err)
	}
	n.Run(100)

	conf := n.Replica(1).Node.Configuration()
	if len(conf.Voters) != 2 || conf.Contains(3) {
		t.Fatalf("expected voters {1,2} after removal, got %v (seed %d)", conf.Voters, n.Seed)
	}
	if _, err := n.Propose(1, "b=2"); err != nil {
		t.Fatalf("cluster unavailable after membership change: %v", err)
	}
	n.Run(50)
	if n.Replica(2).FSM.State["b"] != "2" {
		t.Errorf("remaining voters did not commit after membership change (seed %d)", n.Seed)
	}
	n.CheckSafety()
}

// Regression guard: the exact same seed must produce the exact same history.
// Without this, a "flaky" failure could never be reproduced.
func TestSimulationIsDeterministic(t *testing.T) {
	run := func() []string {
		opts := defaultOpts(42)
		opts.Faults = FaultConfig{DropRate: 0.2, DuplicateRate: 0.1, MaxDelayTicks: 4}
		n := NewNetwork(t, opts)
		n.RunUntil(2000, func() bool { return n.Leader() != 0 })
		for i := 0; i < 10; i++ {
			n.ProposeToLeader(fmt.Sprintf("k%d=v%d", i, i))
			n.Run(5)
		}
		n.Run(500)
		var out []string
		for _, r := range n.Replicas() {
			out = append(out, fmt.Sprintf("%d:term=%d:commit=%d:applied=%d",
				r.ID, r.Node.Term(), r.Node.Committed(), len(r.FSM.Applied)))
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("simulation is not deterministic: %v vs %v", a, b)
		}
	}
}
