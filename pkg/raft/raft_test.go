package raft

import (
	"log/slog"
	"math/rand"
	"testing"
)

// These tests exercise the consensus core directly. Whole-cluster behaviour
// (elections under partition, crash recovery, lossy links) lives in
// pkg/raft/rafttest.

func testConfig(t *testing.T, id uint64, peers []uint64) Config {
	t.Helper()
	return Config{
		ID:            id,
		Peers:         peers,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       NewMemoryStorage(),
		Rand:          rand.New(rand.NewSource(1)).Intn,
		Logger:        slog.New(slog.DiscardHandler),
	}
}

func newTestRaft(t *testing.T, id uint64, peers ...uint64) *raft {
	t.Helper()
	r, err := newRaft(testConfig(t, id, peers))
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	return r
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"zero ID", func(c *Config) { c.ID = 0 }},
		{"election not greater than heartbeat", func(c *Config) { c.ElectionTick = 1; c.HeartbeatTick = 1 }},
		{"zero heartbeat", func(c *Config) { c.HeartbeatTick = 0 }},
		{"no storage", func(c *Config) { c.Storage = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t, 1, []uint64{1})
			tc.mut(&c)
			if _, err := newRaft(c); err == nil {
				t.Fatal("expected configuration to be rejected")
			}
		})
	}
}

// A single-voter cluster must elect itself and commit without any peer.
func TestSingleVoterElectsAndCommits(t *testing.T) {
	r := newTestRaft(t, 1, 1)
	r.campaign()
	if r.state != Leader {
		t.Fatalf("expected Leader, got %s", r.state)
	}
	idx, err := r.propose([]byte("k=v"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if r.log.committed < idx {
		t.Fatalf("single voter must commit immediately: committed=%d idx=%d", r.log.committed, idx)
	}
}

func TestFollowerRejectsProposals(t *testing.T) {
	r := newTestRaft(t, 1, 1, 2, 3)
	if _, err := r.propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	if err := r.readIndex([]byte("t")); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader for read, got %v", err)
	}
}

// The election restriction (§5.4.1): a candidate whose log is behind must not
// win, even if it has the higher term.
func TestVoteDeniedToCandidateWithStaleLog(t *testing.T) {
	r := newTestRaft(t, 1, 1, 2, 3)
	r.becomeCandidate()
	r.becomeLeader()
	r.appendEntries(Entry{Type: EntryNormal, Data: []byte("a")})
	r.appendEntries(Entry{Type: EntryNormal, Data: []byte("b")})
	lastTerm := r.log.lastTerm()

	r.becomeFollower(r.term+1, None)
	r.msgs = nil

	// Candidate with a shorter log at the same term.
	r.Step(Message{Type: MsgVote, From: 2, Term: r.term, LastLogIndex: 1, LastLogTerm: lastTerm})
	if len(r.msgs) != 1 || !r.msgs[0].Reject {
		t.Fatalf("expected vote rejection for a stale log, got %v", r.msgs)
	}

	// Candidate that is at least as up to date wins the vote.
	r.msgs = nil
	r.Step(Message{Type: MsgVote, From: 3, Term: r.term, LastLogIndex: r.log.lastIndex(), LastLogTerm: lastTerm})
	if len(r.msgs) != 1 || r.msgs[0].Reject {
		t.Fatalf("expected vote to be granted, got %v", r.msgs)
	}
}

// One vote per term, but re-granting to the same candidate must be allowed so a
// lost response can be retried.
func TestVoteIsGrantedOncePerTermButIsIdempotent(t *testing.T) {
	r := newTestRaft(t, 1, 1, 2, 3)
	r.Step(Message{Type: MsgVote, From: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	if r.vote != 2 {
		t.Fatalf("expected vote for 2, got %d", r.vote)
	}
	r.msgs = nil
	r.Step(Message{Type: MsgVote, From: 3, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	if len(r.msgs) != 1 || !r.msgs[0].Reject {
		t.Fatalf("expected second candidate in the same term to be rejected, got %v", r.msgs)
	}
	r.msgs = nil
	r.Step(Message{Type: MsgVote, From: 2, Term: 5, LastLogIndex: 0, LastLogTerm: 0})
	if len(r.msgs) != 1 || r.msgs[0].Reject {
		t.Fatalf("expected repeat request from the same candidate to be granted, got %v", r.msgs)
	}
}

// Pre-vote must not advance the responder's term; that is the whole point.
func TestPreVoteDoesNotAdvanceTerm(t *testing.T) {
	c := testConfig(t, 1, []uint64{1, 2, 3})
	c.PreVote = true
	r, err := newRaft(c)
	if err != nil {
		t.Fatal(err)
	}
	before := r.term
	r.Step(Message{Type: MsgPreVote, From: 2, Term: before + 5, LastLogIndex: 0, LastLogTerm: 0})
	if r.term != before {
		t.Fatalf("pre-vote advanced term from %d to %d", before, r.term)
	}
	if r.vote != None {
		t.Fatalf("pre-vote recorded a vote for %d", r.vote)
	}
}

// Figure 8: a leader must not commit an entry from a previous term by counting
// replicas. Only an entry from its own term may advance the commit index.
func TestCommitRequiresCurrentTermEntry(t *testing.T) {
	r := newTestRaft(t, 1, 1, 2, 3)

	// Simulate a log carrying an entry from term 1 while we lead in term 2.
	r.log.append(Entry{Term: 1, Index: 1, Type: EntryNormal, Data: []byte("old")})
	r.becomeFollower(1, None)
	r.becomeCandidate() // term 2
	r.state = Leader
	r.lead = r.id
	r.progress = map[uint64]*progress{
		1: {Match: 1, Next: 2, State: replicateState},
		2: {Match: 1, Next: 2, State: replicateState},
		3: {Match: 0, Next: 2, State: probeState},
	}

	// A majority stores index 1, but it is from an older term.
	if r.maybeCommit() {
		t.Fatal("committed an entry from a previous term by replica count")
	}
	if r.log.committed != 0 {
		t.Fatalf("commit index advanced to %d", r.log.committed)
	}

	// Appending and replicating an entry from the current term commits both.
	r.appendEntries(Entry{Type: EntryNormal, Data: []byte("new")})
	r.progress[2].Match = 2
	if !r.maybeCommit() {
		t.Fatal("expected commit once a current-term entry was replicated")
	}
	if r.log.committed != 2 {
		t.Fatalf("expected commit index 2, got %d", r.log.committed)
	}
}

// A duplicated AppendEntries must not truncate entries the follower already
// holds. This is the property that makes RPC duplication harmless.
func TestDuplicateAppendIsIdempotent(t *testing.T) {
	r := newTestRaft(t, 2, 1, 2, 3)
	entries := []Entry{
		{Term: 1, Index: 1, Type: EntryNormal, Data: []byte("a")},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("b")},
	}
	msg := Message{Type: MsgApp, From: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: 2}

	r.Step(msg)
	if r.log.lastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", r.log.lastIndex())
	}
	committed := r.log.committed

	// Deliver the same message twice more, then an older prefix.
	r.Step(msg)
	r.Step(msg)
	r.Step(Message{Type: MsgApp, From: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries[:1], LeaderCommit: 1})

	if r.log.lastIndex() != 2 {
		t.Fatalf("duplicate append changed the log: last index %d", r.log.lastIndex())
	}
	if r.log.committed != committed {
		t.Fatalf("duplicate append moved commit index from %d to %d", committed, r.log.committed)
	}
	got, _ := r.log.slice(1, 3)
	if len(got) != 2 || string(got[0].Data) != "a" || string(got[1].Data) != "b" {
		t.Fatalf("log contents corrupted by duplicates: %v", got)
	}
}

// A follower with a divergent suffix must overwrite it, and the leader must
// find the divergence point using the conflict hint rather than one index per
// round trip.
func TestConflictingSuffixIsOverwrittenUsingHint(t *testing.T) {
	f := newTestRaft(t, 2, 1, 2, 3)
	// Follower's log: three entries from term 1.
	f.Step(Message{Type: MsgApp, From: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 1, Index: 3, Data: []byte("c")},
	}})

	// New leader in term 2 whose log diverges at index 2.
	f.msgs = nil
	f.Step(Message{Type: MsgApp, From: 1, Term: 2, PrevLogIndex: 3, PrevLogTerm: 2, Entries: nil})
	if len(f.msgs) != 1 || !f.msgs[0].Reject {
		t.Fatalf("expected rejection, got %v", f.msgs)
	}
	hint := f.msgs[0]
	if hint.RejectHintIndex != 1 || hint.RejectHintTerm != 1 {
		t.Fatalf("expected hint to point at the start of the conflicting term (1,1), got (%d,%d)",
			hint.RejectHintIndex, hint.RejectHintTerm)
	}

	// The leader retries from the hint and overwrites the divergent suffix.
	f.msgs = nil
	f.Step(Message{Type: MsgApp, From: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{
		{Term: 2, Index: 2, Data: []byte("x")},
	}, LeaderCommit: 2})
	if f.log.lastIndex() != 2 {
		t.Fatalf("expected the divergent suffix to be truncated, last index is %d", f.log.lastIndex())
	}
	got, _ := f.log.slice(2, 3)
	if string(got[0].Data) != "x" || got[0].Term != 2 {
		t.Fatalf("expected entry 2 to be overwritten, got %v", got[0])
	}
}

// A follower must never adopt a commit index higher than the entries it holds,
// even when the leader reports one.
func TestFollowerCommitIsBoundedByItsOwnLog(t *testing.T) {
	r := newTestRaft(t, 2, 1, 2, 3)
	r.Step(Message{
		Type: MsgApp, From: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []Entry{{Term: 1, Index: 1, Data: []byte("a")}},
		LeaderCommit: 99,
	})
	if r.log.committed != 1 {
		t.Fatalf("follower commit index must be bounded by its log; got %d", r.log.committed)
	}
}

// A stale leader that returns after a partition must be told the current term
// so it steps down instead of retrying forever.
func TestStaleLeaderIsRejectedWithCurrentTerm(t *testing.T) {
	r := newTestRaft(t, 2, 1, 2, 3)
	r.becomeFollower(5, 1)
	r.msgs = nil
	r.Step(Message{Type: MsgApp, From: 3, Term: 2, PrevLogIndex: 0, PrevLogTerm: 0})
	if len(r.msgs) != 1 {
		t.Fatalf("expected one response, got %v", r.msgs)
	}
	if !r.msgs[0].Reject || r.msgs[0].Term != 5 {
		t.Fatalf("expected rejection carrying term 5, got %v", r.msgs[0])
	}
}

// Membership changes must be serialized: a second change while one is
// uncommitted could break the overlapping-majority guarantee.
func TestOnlyOneConfChangeInFlight(t *testing.T) {
	r := newTestRaft(t, 1, 1)
	r.campaign()
	if _, err := r.proposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 2}); err != nil {
		t.Fatalf("first conf change rejected: %v", err)
	}
	if _, err := r.proposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 3}); err != ErrConfChangeInFlight {
		t.Fatalf("expected ErrConfChangeInFlight, got %v", err)
	}
	r.applyConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 2})
	if _, err := r.proposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 3}); err != nil {
		t.Fatalf("expected the next change to be accepted after the first applied: %v", err)
	}
}

func TestConfChangeRoundTrip(t *testing.T) {
	in := ConfChange{Type: ConfChangeRemoveNode, NodeID: 42, Context: []byte("10.0.0.7:7300")}
	out, err := DecodeConfChange(encodeConfChange(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Type != in.Type || out.NodeID != in.NodeID || string(out.Context) != string(in.Context) {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
	if _, err := DecodeConfChange([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected truncated payload to be rejected")
	}
}

// A read index must not be served before the leader has committed an entry in
// its own term, because until then it does not know the true commit index.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	r := newTestRaft(t, 1, 1, 2, 3)
	r.becomeCandidate()
	r.becomeLeader() // appends a no-op that is not yet committed
	if err := r.readIndex([]byte("t1")); err != ErrNoLeader {
		t.Fatalf("expected ErrNoLeader before the term's first commit, got %v", err)
	}

	// Replicate the no-op to a majority.
	r.progress[2].maybeUpdate(r.log.lastIndex())
	r.maybeCommit()
	if err := r.readIndex([]byte("t2")); err != nil {
		t.Fatalf("expected read index to be accepted: %v", err)
	}
	// It is confirmed by a heartbeat quorum, not immediately.
	if len(r.readStates) != 0 {
		t.Fatalf("read confirmed without a heartbeat quorum: %v", r.readStates)
	}
	r.Step(Message{Type: MsgHeartbeatResp, From: 2, Term: r.term, ReadCtx: []byte("t2")})
	if len(r.readStates) != 1 {
		t.Fatalf("expected one confirmed read after quorum, got %d", len(r.readStates))
	}
}

func TestCheckQuorumStepsDownIsolatedLeader(t *testing.T) {
	c := testConfig(t, 1, []uint64{1, 2, 3})
	c.CheckQuorum = true
	r, err := newRaft(c)
	if err != nil {
		t.Fatal(err)
	}
	r.becomeCandidate()
	r.becomeLeader()
	if r.state != Leader {
		t.Fatal("expected leader")
	}
	// No follower ever responds.
	for i := 0; i < c.ElectionTick*2; i++ {
		r.Tick()
	}
	if r.state == Leader {
		t.Fatal("expected an isolated leader to step down under CheckQuorum")
	}
}

func TestProgressDoesNotRegressOnDuplicateResponses(t *testing.T) {
	p := &progress{Match: 5, Next: 6, State: replicateState}
	if p.maybeUpdate(3) {
		t.Fatal("a stale match index must not be reported as progress")
	}
	if p.Match != 5 {
		t.Fatalf("match index regressed to %d", p.Match)
	}
	if !p.maybeUpdate(7) {
		t.Fatal("expected a higher match index to register")
	}
	if p.Match != 7 || p.Next != 8 {
		t.Fatalf("unexpected progress state: match=%d next=%d", p.Match, p.Next)
	}
}
