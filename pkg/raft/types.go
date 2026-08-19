// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout,
// 2014) together with the pre-vote, snapshot, single-server membership change
// and read-index extensions from Ongaro's dissertation.
//
// The core (raft struct in core.go) is a pure state machine: it performs no
// I/O, spawns no goroutines and reads no clock. Callers drive it with Tick and
// Step and collect its outputs with Ready. Everything that touches the disk,
// the network or the wall clock lives in Node (node.go) or in the caller.
//
// This split exists for one reason: it makes the interesting failure modes
// testable. A whole cluster can be advanced instruction by instruction inside a
// single goroutine, with a seeded network that drops, delays, duplicates and
// reorders messages, and the run is bit-for-bit reproducible from its seed.
//
// Safety properties the implementation is responsible for, using the names from
// Figure 3 of the paper:
//
//	Election Safety     at most one leader per term
//	Leader Append-Only  a leader never overwrites or deletes its own entries
//	Log Matching        two logs agreeing on (index, term) agree on all
//	                    preceding entries
//	Leader Completeness a committed entry is present in every future leader
//	State Machine Safety no two nodes apply different entries at the same index
//
// The deterministic simulator in cluster_test.go asserts all five after every
// scenario it runs.
package raft

import (
	"errors"
	"fmt"
)

// State is a Raft server's role.
type State uint8

const (
	Follower State = iota
	// PreCandidate solicits votes without incrementing its term. It prevents a
	// node that was partitioned away from disrupting a healthy leader when it
	// rejoins with an inflated term (dissertation §9.6).
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case PreCandidate:
		return "PreCandidate"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// None is the zero node ID, meaning "no leader" or "voted for nobody".
const None uint64 = 0

// EntryType distinguishes replicated payloads from internal bookkeeping.
type EntryType uint8

const (
	// EntryNormal carries an opaque application command.
	EntryNormal EntryType = iota
	// EntryNoOp is committed by a new leader at the start of its term. Raft
	// forbids committing entries from previous terms by counting replicas
	// (paper §5.4.2); committing a no-op from the current term advances the
	// commit index over those older entries safely.
	EntryNoOp
	// EntryConfChange changes the voter set by one server.
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "Normal"
	case EntryNoOp:
		return "NoOp"
	case EntryConfChange:
		return "ConfChange"
	}
	return "Unknown"
}

// Entry is a single replicated log record.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("{i=%d t=%d %s len=%d}", e.Index, e.Term, e.Type, len(e.Data))
}

// HardState is the subset of server state that Raft requires to be on stable
// storage before any message that depends on it is sent.
type HardState struct {
	Term    uint64
	VoteFor uint64
	Commit  uint64
}

func (h HardState) IsEmpty() bool { return h.Term == 0 && h.VoteFor == None && h.Commit == 0 }

// SoftState is volatile role information. It is never persisted; it is reported
// so the surrounding system can react to leadership changes.
type SoftState struct {
	Leader uint64
	State  State
}

// Configuration is the set of voting members. Orion performs membership changes
// one server at a time, which keeps old and new majorities overlapping and
// removes the need for joint consensus (dissertation §4.1).
type Configuration struct {
	Voters []uint64
}

func (c Configuration) Contains(id uint64) bool {
	for _, v := range c.Voters {
		if v == id {
			return true
		}
	}
	return false
}

// Quorum is the number of votes needed for a majority.
func (c Configuration) Quorum() int { return len(c.Voters)/2 + 1 }

func (c Configuration) Clone() Configuration {
	v := make([]uint64, len(c.Voters))
	copy(v, c.Voters)
	return Configuration{Voters: v}
}

// ConfChangeType is the kind of single-server membership change.
type ConfChangeType uint8

const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
)

// ConfChange is the payload of an EntryConfChange entry.
type ConfChange struct {
	Type   ConfChangeType
	NodeID uint64
	// Context carries the member's network address so that peers learn how to
	// reach a newly added server from the log itself, rather than from
	// out-of-band configuration that could disagree between replicas.
	Context []byte
}

// Snapshot replaces the log prefix up to and including Index.
type Snapshot struct {
	Index uint64
	Term  uint64
	// Config is the membership as of Index. A restarting server recovers its
	// voter set from here, not from static configuration.
	Config Configuration
	Data   []byte
}

func (s *Snapshot) IsEmpty() bool { return s == nil || s.Index == 0 }

// MessageType enumerates the Raft RPCs. Requests and responses are modelled as
// separate message types moving through one transport, which keeps the core a
// pure Step function instead of a set of blocking RPC handlers.
type MessageType uint8

const (
	MsgVote MessageType = iota
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
	MsgSnapResp
	// MsgReadIndex asks the leader to confirm leadership so a linearizable read
	// can be served without appending to the log (dissertation §6.4).
	MsgReadIndex
	MsgReadIndexResp
)

var msgNames = map[MessageType]string{
	MsgVote: "Vote", MsgVoteResp: "VoteResp",
	MsgPreVote: "PreVote", MsgPreVoteResp: "PreVoteResp",
	MsgApp: "App", MsgAppResp: "AppResp",
	MsgHeartbeat: "Heartbeat", MsgHeartbeatResp: "HeartbeatResp",
	MsgSnap: "Snap", MsgSnapResp: "SnapResp",
	MsgReadIndex: "ReadIndex", MsgReadIndexResp: "ReadIndexResp",
}

func (t MessageType) String() string {
	if n, ok := msgNames[t]; ok {
		return n
	}
	return "Unknown"
}

// Message is a single Raft RPC or its response.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64

	// AppendEntries request.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// Vote request.
	LastLogIndex uint64
	LastLogTerm  uint64

	// Responses.
	Reject bool
	// MatchIndex is the highest index the follower has accepted, echoed so the
	// leader can advance nextIndex without an extra round trip.
	MatchIndex uint64
	// RejectHintTerm/RejectHintIndex let a leader skip an entire conflicting
	// term in one round trip instead of decrementing nextIndex one at a time
	// (paper §5.3, "Optimization"). Without this, catching up a follower that
	// diverged by N entries costs N round trips.
	RejectHintTerm  uint64
	RejectHintIndex uint64

	Snapshot *Snapshot

	// ReadIndex correlation token, opaque to Raft.
	ReadCtx   []byte
	ReadIndex uint64
}

func (m Message) String() string {
	return fmt.Sprintf("%s %d->%d term=%d prev=(%d,%d) n=%d commit=%d reject=%v match=%d",
		m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit, m.Reject, m.MatchIndex)
}

// ReadState pairs a caller's opaque token with the log index that must be
// applied before the corresponding read is linearizable.
type ReadState struct {
	Index uint64
	Ctx   []byte
}

// Ready is the batch of work the core produced since the last Advance.
//
// The caller MUST honour this order, because Raft's safety proof assumes a
// server never sends a message that depends on state it has not made durable:
//
//  1. persist Snapshot, if any
//  2. append Entries to stable storage and fsync
//  3. persist HardState and fsync
//  4. send Messages
//  5. apply CommittedEntries to the application state machine
//  6. call Advance
//
// Steps 4 and 5 may run concurrently with each other but never before 1-3.
type Ready struct {
	// SoftState is non-nil only when the role or known leader changed.
	SoftState *SoftState
	// HardState is non-nil only when it changed and must be persisted.
	HardState *HardState
	// Entries are new log entries to append before sending Messages.
	Entries []Entry
	// Snapshot must be persisted and applied before CommittedEntries.
	Snapshot *Snapshot
	// CommittedEntries are safe to apply to the application state machine.
	CommittedEntries []Entry
	// Messages to deliver to peers.
	Messages []Message
	// ReadStates are read-index confirmations, in request order.
	ReadStates []ReadState
}

func (r Ready) isEmpty() bool {
	return r.SoftState == nil && r.HardState == nil && len(r.Entries) == 0 &&
		r.Snapshot.IsEmpty() && len(r.CommittedEntries) == 0 && len(r.Messages) == 0 &&
		len(r.ReadStates) == 0
}

// Errors returned by the core and by Node.
var (
	// ErrNotLeader is returned for a proposal made to a non-leader. Callers
	// should redirect to the address reported by Node.Leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrNoLeader is returned when there is no known leader, typically during
	// an election or when quorum is lost.
	ErrNoLeader = errors.New("raft: no leader elected")
	// ErrStopped is returned once the node has been shut down.
	ErrStopped = errors.New("raft: node stopped")
	// ErrProposalDropped means the proposal was discarded, e.g. because
	// leadership was lost before it could be committed. It is safe to retry;
	// Orion's proposals carry idempotency keys.
	ErrProposalDropped = errors.New("raft: proposal dropped")
	// ErrConfChangeInFlight rejects a second membership change while one is
	// uncommitted. Overlapping single-server changes can break the overlapping
	// majority guarantee.
	ErrConfChangeInFlight = errors.New("raft: configuration change already in flight")
	// ErrCompacted means the requested index is below the snapshot boundary.
	ErrCompacted = errors.New("raft: requested index is compacted")
	// ErrUnavailable means the requested entries are beyond the log.
	ErrUnavailable = errors.New("raft: requested entry is unavailable")
	// ErrSnapshotOutOfDate rejects a snapshot older than local state.
	ErrSnapshotOutOfDate = errors.New("raft: snapshot is out of date")
)
