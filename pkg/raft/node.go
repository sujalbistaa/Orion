package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// FSM is the application state machine replicated by Raft.
//
// Apply is called exactly once per committed entry, in index order, from a
// single goroutine. Its return value is delivered to the caller that proposed
// the entry. Apply must be deterministic: given the same entry sequence every
// replica must reach the same state, or the cluster diverges silently.
type FSM interface {
	Apply(e Entry) any
	// Snapshot serializes the current state. It is called from the Raft
	// goroutine, so implementations should keep it cheap or copy-on-write.
	Snapshot() ([]byte, error)
	// Restore replaces all state with a snapshot's contents.
	Restore(data []byte) error
}

// Transport delivers messages to peers. Send is best effort and must not
// block: Raft tolerates loss, delay, duplication and reordering, but a
// transport that blocks the consensus loop turns a slow peer into a cluster-
// wide stall.
type Transport interface {
	Send(m Message)
	AddPeer(id uint64, addr string)
	RemovePeer(id uint64)
	Close() error
}

// NodeOptions configures the Raft driver.
type NodeOptions struct {
	Config Config
	FSM    FSM

	Transport Transport

	// TickInterval is the wall-clock duration of one logical tick.
	TickInterval time.Duration
	// SnapshotThreshold is how many applied entries may accumulate past the
	// last snapshot before a new one is taken and the log is compacted.
	SnapshotThreshold uint64
	// InitialPeerAddrs seeds the transport for the bootstrap configuration.
	InitialPeerAddrs map[uint64]string
}

// Status is a point-in-time view of the consensus layer, surfaced through the
// API server so operators can see leadership and replication lag directly.
type Status struct {
	ID           uint64
	Term         uint64
	State        State
	Leader       uint64
	Commit       uint64
	Applied      uint64
	LastIndex    uint64
	SnapshotIdx  uint64
	Voters       []uint64
	Progress     map[uint64]PeerStatus
	QuorumSize   int
	HasQuorum    bool
	LastLeaderAt time.Time
}

type PeerStatus struct {
	Match  uint64
	Next   uint64
	State  string
	Active bool
}

type proposalRequest struct {
	data   []byte
	cc     *ConfChange
	result chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

type readRequest struct {
	result chan error
}

type waiter struct {
	term   uint64
	result chan proposalResult
}

// Node drives a raft core: it owns the goroutine, the storage writes, the
// transport and the application of committed entries.
type Node struct {
	opts NodeOptions
	r    *raft
	fsm  FSM
	tr   Transport
	log  *slog.Logger

	proposeC chan proposalRequest
	recvC    chan Message
	readC    chan readRequest
	statusC  chan chan Status

	stopOnce sync.Once
	stopC    chan struct{}
	doneC    chan struct{}

	// leaderID is published atomically so hot paths (request routing) never
	// have to round-trip through the consensus goroutine.
	leaderID atomic.Uint64
	termV    atomic.Uint64
	stateV   atomic.Uint32
	appliedV atomic.Uint64

	// leaderCh is signalled on every leadership change so controllers can start
	// and stop with leadership.
	leaderMu  sync.Mutex
	leaderSub []chan bool

	waitersMu sync.Mutex
	waiters   map[uint64]*waiter

	pendingReads map[string]chan error
	readIndexes  map[string]uint64
	readSeq      uint64

	snapshotIndex uint64
	peerAddrs     map[uint64]string

	// applyErr records a fatal application error; the node stops rather than
	// diverging from its peers.
	applyErr error
}

// NewNode creates a Raft node. It restores any persisted snapshot into the FSM
// before returning, so callers observe a consistent starting state.
func NewNode(opts NodeOptions) (*Node, error) {
	if opts.FSM == nil {
		return nil, errors.New("raft: FSM is required")
	}
	if opts.Transport == nil {
		return nil, errors.New("raft: Transport is required")
	}
	if opts.TickInterval <= 0 {
		opts.TickInterval = 100 * time.Millisecond
	}
	if opts.SnapshotThreshold == 0 {
		opts.SnapshotThreshold = 8192
	}

	// Restore state from the newest snapshot before the core reads the log, so
	// FSM state and the log's applied index agree.
	snap, err := opts.Config.Storage.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: reading snapshot: %w", err)
	}
	if snap.Index > 0 {
		if err := opts.FSM.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("raft: restoring snapshot at index %d: %w", snap.Index, err)
		}
	}

	r, err := newRaft(opts.Config)
	if err != nil {
		return nil, err
	}

	n := &Node{
		opts:          opts,
		r:             r,
		fsm:           opts.FSM,
		tr:            opts.Transport,
		log:           opts.Config.Logger.With("component", "raft-node", "node", opts.Config.ID),
		proposeC:      make(chan proposalRequest),
		recvC:         make(chan Message, 1024),
		readC:         make(chan readRequest),
		statusC:       make(chan chan Status),
		stopC:         make(chan struct{}),
		doneC:         make(chan struct{}),
		waiters:       make(map[uint64]*waiter),
		pendingReads:  make(map[string]chan error),
		readIndexes:   make(map[string]uint64),
		snapshotIndex: snap.Index,
		peerAddrs:     make(map[uint64]string),
	}
	for id, addr := range opts.InitialPeerAddrs {
		n.peerAddrs[id] = addr
		if id != opts.Config.ID {
			opts.Transport.AddPeer(id, addr)
		}
	}
	n.termV.Store(r.term)
	n.stateV.Store(uint32(Follower))
	n.appliedV.Store(r.log.applied)
	return n, nil
}

// Start launches the consensus goroutine. It returns immediately.
func (n *Node) Start() {
	go n.run()
}

// Stop shuts the node down and waits for the goroutine to exit. It is safe to
// call more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopC) })
	<-n.doneC
}

// Step delivers an inbound message from a peer. It never blocks indefinitely:
// if the consensus loop is saturated the message is dropped, which Raft treats
// as ordinary network loss.
func (n *Node) Step(m Message) {
	select {
	case n.recvC <- m:
	case <-n.stopC:
	default:
		n.log.Warn("dropping inbound raft message: receive queue full", "type", m.Type, "from", m.From)
	}
}

// Propose replicates data and waits for it to be applied, returning the FSM's
// result. It fails fast when this node is not the leader so callers can
// redirect rather than time out.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	return n.submit(ctx, proposalRequest{data: data, result: make(chan proposalResult, 1)})
}

// ProposeConfChange adds or removes a control-plane member.
func (n *Node) ProposeConfChange(ctx context.Context, cc ConfChange) error {
	_, err := n.submit(ctx, proposalRequest{cc: &cc, result: make(chan proposalResult, 1)})
	return err
}

func (n *Node) submit(ctx context.Context, req proposalRequest) (any, error) {
	if State(n.stateV.Load()) != Leader {
		return nil, ErrNotLeader
	}
	select {
	case n.proposeC <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopC:
		return nil, ErrStopped
	}
	select {
	case res := <-req.result:
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopC:
		return nil, ErrStopped
	}
}

// LinearizableRead blocks until this node's state machine is guaranteed to
// reflect every write committed before the call. It uses the read-index
// protocol: a heartbeat round confirms leadership, and the caller waits for the
// commit index observed at request time to be applied. No log write occurs.
func (n *Node) LinearizableRead(ctx context.Context) error {
	if State(n.stateV.Load()) != Leader {
		return ErrNotLeader
	}
	req := readRequest{result: make(chan error, 1)}
	select {
	case n.readC <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopC:
		return ErrStopped
	}
}

// IsLeader reports leadership without touching the consensus goroutine.
func (n *Node) IsLeader() bool { return State(n.stateV.Load()) == Leader }

// Leader returns the current leader's ID, or None.
func (n *Node) Leader() uint64 { return n.leaderID.Load() }

// LeaderAddress returns the transport address of the current leader, if known.
// The API server uses it to redirect writes that reach a follower.
func (n *Node) LeaderAddress() (string, bool) {
	leader := n.leaderID.Load()
	if leader == None {
		return "", false
	}
	n.leaderMu.Lock()
	defer n.leaderMu.Unlock()
	addr, ok := n.peerAddrs[leader]
	return addr, ok
}

// AppliedIndex is the highest log index applied to the FSM.
func (n *Node) AppliedIndex() uint64 { return n.appliedV.Load() }

// LeadershipChanges returns a channel that receives true when this node becomes
// leader and false when it loses leadership. Controllers subscribe so that
// exactly one replica runs reconciliation at a time.
func (n *Node) LeadershipChanges() <-chan bool {
	ch := make(chan bool, 8)
	n.leaderMu.Lock()
	n.leaderSub = append(n.leaderSub, ch)
	n.leaderMu.Unlock()
	return ch
}

func (n *Node) publishLeadership(isLeader bool) {
	n.leaderMu.Lock()
	subs := append([]chan bool(nil), n.leaderSub...)
	n.leaderMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- isLeader:
		default:
		}
	}
}

// Status returns a snapshot of consensus state.
func (n *Node) Status() Status {
	reply := make(chan Status, 1)
	select {
	case n.statusC <- reply:
		select {
		case st := <-reply:
			return st
		case <-n.doneC:
		}
	case <-n.doneC:
	}
	// The node is stopped; report what the atomics still hold.
	return Status{ID: n.opts.Config.ID, Term: n.termV.Load(), State: State(n.stateV.Load()), Applied: n.appliedV.Load()}
}

// ---------------------------------------------------------------------------
// Consensus loop
// ---------------------------------------------------------------------------

func (n *Node) run() {
	defer close(n.doneC)
	ticker := time.NewTicker(n.opts.TickInterval)
	defer ticker.Stop()

	for {
		// Draining outputs before blocking keeps latency low: a proposal that
		// arrives just after a tick is replicated in the same iteration.
		if err := n.processReady(); err != nil {
			n.log.Error("fatal raft error, stopping node", "err", err)
			n.applyErr = err
			n.failAllWaiters(err)
			return
		}

		select {
		case <-ticker.C:
			n.r.Tick()

		case m := <-n.recvC:
			if err := n.r.Step(m); err != nil {
				n.log.Warn("raft step failed", "type", m.Type, "from", m.From, "err", err)
			}

		case req := <-n.proposeC:
			n.handleProposal(req)

		case req := <-n.readC:
			n.handleRead(req)

		case reply := <-n.statusC:
			reply <- n.status()

		case <-n.stopC:
			n.failAllWaiters(ErrStopped)
			_ = n.opts.Config.Storage.Close()
			return
		}
	}
}

func (n *Node) handleProposal(req proposalRequest) {
	var (
		index uint64
		err   error
	)
	if req.cc != nil {
		index, err = n.r.proposeConfChange(*req.cc)
	} else {
		index, err = n.r.propose(req.data)
	}
	if err != nil {
		req.result <- proposalResult{err: err}
		return
	}
	n.waitersMu.Lock()
	// A waiter already registered at this index belongs to a proposal from an
	// earlier term whose entry was overwritten; fail it rather than leak it.
	if old, ok := n.waiters[index]; ok {
		old.result <- proposalResult{err: ErrProposalDropped}
	}
	n.waiters[index] = &waiter{term: n.r.term, result: req.result}
	n.waitersMu.Unlock()
}

func (n *Node) handleRead(req readRequest) {
	n.readSeq++
	token := fmt.Sprintf("%d-%d", n.opts.Config.ID, n.readSeq)
	if err := n.r.readIndex([]byte(token)); err != nil {
		req.result <- err
		return
	}
	n.pendingReads[token] = req.result
}

func (n *Node) processReady() error {
	rd := n.r.makeReady()
	if rd.isEmpty() {
		return nil
	}

	// Order matters and is load-bearing: nothing may be sent that depends on
	// state we have not made durable. See the Ready doc comment.
	if !rd.Snapshot.IsEmpty() {
		if err := n.opts.Config.Storage.SaveSnapshot(*rd.Snapshot); err != nil && !errors.Is(err, ErrSnapshotOutOfDate) {
			return fmt.Errorf("persisting snapshot: %w", err)
		}
		if err := n.fsm.Restore(rd.Snapshot.Data); err != nil {
			return fmt.Errorf("restoring snapshot into state machine: %w", err)
		}
		if err := n.opts.Config.Storage.SetConfiguration(rd.Snapshot.Config); err != nil {
			return fmt.Errorf("persisting configuration from snapshot: %w", err)
		}
		n.snapshotIndex = rd.Snapshot.Index
		n.appliedV.Store(rd.Snapshot.Index)
	}
	if len(rd.Entries) > 0 {
		if err := n.opts.Config.Storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("appending entries: %w", err)
		}
	}
	if rd.HardState != nil {
		if err := n.opts.Config.Storage.SetHardState(*rd.HardState); err != nil {
			return fmt.Errorf("persisting hard state: %w", err)
		}
		n.termV.Store(rd.HardState.Term)
	}

	if rd.SoftState != nil {
		n.onSoftStateChange(*rd.SoftState)
	}

	for _, m := range rd.Messages {
		n.tr.Send(m)
	}

	for _, rs := range rd.ReadStates {
		n.readIndexes[string(rs.Ctx)] = rs.Index
	}

	if err := n.applyEntries(rd.CommittedEntries); err != nil {
		return err
	}

	n.r.advance(rd)
	n.resolveReads()

	if err := n.maybeSnapshot(); err != nil {
		return err
	}
	return nil
}

func (n *Node) onSoftStateChange(ss SoftState) {
	prevLeader := n.leaderID.Swap(ss.Leader)
	wasLeader := State(n.stateV.Swap(uint32(ss.State))) == Leader
	isLeader := ss.State == Leader

	if wasLeader && !isLeader {
		// Everything we proposed but did not commit may or may not survive.
		// Reporting it as dropped is the honest answer; Orion's proposals are
		// idempotent, so the caller retries against the new leader.
		n.failAllWaiters(ErrNotLeader)
		n.failAllReads(ErrNotLeader)
	}
	if wasLeader != isLeader {
		n.publishLeadership(isLeader)
	}
	if prevLeader != ss.Leader {
		n.log.Info("leadership changed", "leader", ss.Leader, "term", n.r.term, "state", ss.State)
	}
}

func (n *Node) applyEntries(entries []Entry) error {
	for _, e := range entries {
		switch e.Type {
		case EntryNoOp:
			// Nothing to apply; the entry exists to establish the leader's term.

		case EntryConfChange:
			cc, err := DecodeConfChange(e.Data)
			if err != nil {
				// A malformed entry is in the replicated log, so every replica
				// sees it. Failing loudly is correct: silently skipping would
				// let replicas disagree about membership.
				return fmt.Errorf("decoding conf change at index %d: %w", e.Index, err)
			}
			conf := n.r.applyConfChange(cc)
			if err := n.opts.Config.Storage.SetConfiguration(conf); err != nil {
				return fmt.Errorf("persisting configuration: %w", err)
			}
			n.applyMembership(cc)
			n.log.Info("applied membership change", "type", cc.Type, "member", cc.NodeID, "voters", conf.Voters)

		default:
			result := n.fsm.Apply(e)
			n.resolveWaiter(e, result)
			continue
		}
		n.resolveWaiter(e, nil)
	}
	if len(entries) > 0 {
		n.appliedV.Store(entries[len(entries)-1].Index)
	}
	return nil
}

func (n *Node) applyMembership(cc ConfChange) {
	switch cc.Type {
	case ConfChangeAddNode:
		addr := string(cc.Context)
		n.leaderMu.Lock()
		n.peerAddrs[cc.NodeID] = addr
		n.leaderMu.Unlock()
		if cc.NodeID != n.opts.Config.ID && addr != "" {
			n.tr.AddPeer(cc.NodeID, addr)
		}
	case ConfChangeRemoveNode:
		n.leaderMu.Lock()
		delete(n.peerAddrs, cc.NodeID)
		n.leaderMu.Unlock()
		n.tr.RemovePeer(cc.NodeID)
	}
}

func (n *Node) resolveWaiter(e Entry, result any) {
	n.waitersMu.Lock()
	w, ok := n.waiters[e.Index]
	if ok {
		delete(n.waiters, e.Index)
	}
	n.waitersMu.Unlock()
	if !ok {
		return
	}
	if w.term != e.Term {
		// A different entry won this index: our proposal was overwritten after
		// a leader change.
		w.result <- proposalResult{err: ErrProposalDropped}
		return
	}
	w.result <- proposalResult{value: result}
}

func (n *Node) failAllWaiters(err error) {
	n.waitersMu.Lock()
	waiters := n.waiters
	n.waiters = make(map[uint64]*waiter)
	n.waitersMu.Unlock()
	for _, w := range waiters {
		w.result <- proposalResult{err: err}
	}
}

func (n *Node) failAllReads(err error) {
	for token, ch := range n.pendingReads {
		ch <- err
		delete(n.pendingReads, token)
		delete(n.readIndexes, token)
	}
}

func (n *Node) resolveReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	applied := n.r.appliedIndex()
	for token, idx := range n.readIndexes {
		if idx > applied {
			continue
		}
		if ch, ok := n.pendingReads[token]; ok {
			ch <- nil
			delete(n.pendingReads, token)
		}
		delete(n.readIndexes, token)
	}
}

// maybeSnapshot compacts the log once enough entries have been applied. The
// snapshot is taken from the FSM synchronously; Orion's cluster state is small
// enough (tens of MB at cluster scale) that a copy is cheaper than the
// bookkeeping an asynchronous snapshot would require.
func (n *Node) maybeSnapshot() error {
	applied := n.r.appliedIndex()
	if applied < n.snapshotIndex+n.opts.SnapshotThreshold {
		return nil
	}
	term, err := n.r.log.term(applied)
	if err != nil {
		return nil
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("taking state machine snapshot: %w", err)
	}
	snap := Snapshot{Index: applied, Term: term, Config: n.r.config.Clone(), Data: data}
	if err := n.opts.Config.Storage.SaveSnapshot(snap); err != nil {
		if errors.Is(err, ErrSnapshotOutOfDate) {
			return nil
		}
		return fmt.Errorf("saving snapshot: %w", err)
	}
	n.snapshotIndex = applied
	n.log.Info("compacted raft log", "snapshotIndex", applied, "bytes", len(data))
	return nil
}

func (n *Node) status() Status {
	st := Status{
		ID:          n.r.id,
		Term:        n.r.term,
		State:       n.r.state,
		Leader:      n.r.lead,
		Commit:      n.r.log.committed,
		Applied:     n.r.log.applied,
		LastIndex:   n.r.log.lastIndex(),
		SnapshotIdx: n.snapshotIndex,
		Voters:      n.r.config.Clone().Voters,
		QuorumSize:  n.r.quorum(),
		Progress:    make(map[uint64]PeerStatus, len(n.r.progress)),
	}
	for id, p := range n.r.progress {
		st.Progress[id] = PeerStatus{Match: p.Match, Next: p.Next, State: p.State.String(), Active: p.RecentActive}
	}
	st.HasQuorum = n.r.state != Leader || n.r.quorumActive()
	return st
}
