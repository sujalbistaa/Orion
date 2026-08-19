package raft

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
)

// Config configures a Raft core.
type Config struct {
	// ID is this server's identifier. Must be non-zero and stable across
	// restarts.
	ID uint64

	// Peers is the initial voter set, used only when Storage has no persisted
	// configuration (i.e. on first boot of a fresh cluster). A server joining
	// an existing cluster leaves this empty and is added via ConfChange.
	Peers []uint64

	// ElectionTick is how many Tick calls a follower waits without hearing from
	// a leader before starting an election. The actual timeout is randomized in
	// [ElectionTick, 2*ElectionTick) to avoid split votes.
	ElectionTick int
	// HeartbeatTick is how many Tick calls between leader heartbeats. It must
	// be well below ElectionTick; the paper's guidance is an order of magnitude.
	HeartbeatTick int

	Storage Storage

	// PreVote runs an extra vote round that does not increment the term, so a
	// server returning from a partition cannot force a healthy leader to step
	// down. Recommended; enabled by orion-server.
	PreVote bool

	// CheckQuorum makes a leader step down if it has not heard from a majority
	// within one election timeout. Without it, a partitioned leader keeps
	// believing it leads and would answer reads with stale data.
	CheckQuorum bool

	// MaxEntriesPerAppend bounds how many entries one AppendEntries carries.
	MaxEntriesPerAppend int
	// MaxCommittedEntriesPerReady bounds how much the application must apply in
	// one batch, so a large catch-up does not block the event loop.
	MaxCommittedEntriesPerReady int

	// Rand supplies randomness for election timeouts. Tests inject a seeded
	// source to make whole-cluster runs reproducible.
	Rand func(n int) int

	Logger *slog.Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: ID must not be zero")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return fmt.Errorf("raft: ElectionTick (%d) must exceed HeartbeatTick (%d)", c.ElectionTick, c.HeartbeatTick)
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("raft: HeartbeatTick must be positive")
	}
	if c.Storage == nil {
		return errors.New("raft: Storage is required")
	}
	if c.MaxEntriesPerAppend <= 0 {
		c.MaxEntriesPerAppend = 64
	}
	if c.MaxCommittedEntriesPerReady <= 0 {
		c.MaxCommittedEntriesPerReady = 256
	}
	if c.Rand == nil {
		c.Rand = rand.Intn
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// progressState tracks how the leader is currently feeding one follower.
type progressState uint8

const (
	// probeState sends at most one AppendEntries per heartbeat interval while
	// the leader searches for the follower's divergence point. Without this
	// throttle, a follower that is far behind would be flooded with rejected
	// appends.
	probeState progressState = iota
	// replicateState streams entries optimistically; Next runs ahead of Match.
	replicateState
	// snapshotState means the follower needs a snapshot because the entries it
	// requires have been compacted away.
	snapshotState
)

func (s progressState) String() string {
	switch s {
	case probeState:
		return "Probe"
	case replicateState:
		return "Replicate"
	case snapshotState:
		return "Snapshot"
	}
	return "Unknown"
}

// progress is the leader's view of one follower.
type progress struct {
	// Match is the highest index known to be replicated on this follower.
	Match uint64
	// Next is the index of the next entry to send.
	Next  uint64
	State progressState
	// PendingSnapshot is the index of a snapshot currently being installed.
	PendingSnapshot uint64
	// RecentActive is set when a message arrives from this follower and cleared
	// each election timeout; CheckQuorum counts it.
	RecentActive bool
	// ProbeSent throttles probeState to one message per heartbeat.
	ProbeSent bool
}

func (p *progress) becomeProbe() {
	if p.State == snapshotState {
		// Resume just past whatever the snapshot delivered.
		p.Next = max(p.Match+1, p.PendingSnapshot+1)
	} else {
		p.Next = p.Match + 1
	}
	p.State = probeState
	p.PendingSnapshot = 0
	p.ProbeSent = false
}

func (p *progress) becomeReplicate() {
	p.State = replicateState
	p.PendingSnapshot = 0
	p.Next = p.Match + 1
	p.ProbeSent = false
}

func (p *progress) becomeSnapshot(index uint64) {
	p.State = snapshotState
	p.PendingSnapshot = index
	p.ProbeSent = false
}

// maybeUpdate advances Match/Next after a successful append. It returns false
// for a stale or duplicated response, which must not move Match backwards.
func (p *progress) maybeUpdate(matchIndex uint64) bool {
	if matchIndex <= p.Match {
		// Still refresh Next: a duplicate success can arrive after a probe
		// reset Next below Match+1.
		if p.Next < matchIndex+1 {
			p.Next = matchIndex + 1
		}
		return false
	}
	p.Match = matchIndex
	if p.Next < matchIndex+1 {
		p.Next = matchIndex + 1
	}
	return true
}

// maybeDecrTo backs Next up after a rejection, using the follower's hint to
// skip a whole conflicting term at once.
func (p *progress) maybeDecrTo(rejected, hintIndex, hintTerm uint64, lastIndex uint64) bool {
	if p.State == replicateState {
		// In replicate state, Match is authoritative; ignore stale rejections
		// for entries at or below it.
		if rejected <= p.Match {
			return false
		}
		p.Next = p.Match + 1
		return true
	}
	if p.Next-1 != rejected {
		// Stale rejection for a probe we have already moved past.
		return false
	}
	next := hintIndex
	if next == 0 {
		next = 1
	}
	next = min(next, lastIndex+1)
	p.Next = max(next, 1)
	p.ProbeSent = false
	_ = hintTerm
	return true
}

// readIndexStatus tracks quorum confirmation for one linearizable read.
type readIndexStatus struct {
	index uint64
	ctx   []byte
	acks  map[uint64]bool
}

// raft is the pure consensus state machine. It has no locks, no goroutines and
// no clock; all inputs arrive through Tick and Step.
type raft struct {
	id  uint64
	cfg Config

	term  uint64
	vote  uint64
	state State
	lead  uint64

	log    *raftLog
	config Configuration

	// progress is populated only while this server is the leader.
	progress map[uint64]*progress
	// votes records responses to the current (pre)election.
	votes map[uint64]bool

	electionElapsed  int
	heartbeatElapsed int
	// randomizedElectionTimeout is re-drawn on every state change so that two
	// servers rarely time out together.
	randomizedElectionTimeout int

	// pendingConfIndex is the log index of the most recent EntryConfChange. A
	// new membership change is refused until it is applied, which is what keeps
	// old and new majorities overlapping.
	pendingConfIndex uint64
	// uncommittedConf is true while a conf change is appended but not applied.
	uncommittedConf bool

	readPending []*readIndexStatus

	msgs       []Message
	readStates []ReadState

	prevSoftState SoftState
	prevHardState HardState

	logger *slog.Logger
}

func newRaft(c Config) (*raft, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	rlog, err := newRaftLog(c.Storage)
	if err != nil {
		return nil, err
	}
	hs, conf, err := c.Storage.InitialState()
	if err != nil {
		return nil, err
	}
	if len(conf.Voters) == 0 {
		conf = Configuration{Voters: append([]uint64(nil), c.Peers...)}
	}

	r := &raft{
		id:       c.ID,
		cfg:      c,
		log:      rlog,
		config:   conf,
		progress: make(map[uint64]*progress),
		votes:    make(map[uint64]bool),
		logger:   c.Logger.With("component", "raft", "node", c.ID),
	}
	if !hs.IsEmpty() {
		r.term = hs.Term
		r.vote = hs.VoteFor
		if hs.Commit > 0 {
			r.log.commitTo(hs.Commit)
		}
	}
	r.becomeFollower(r.term, None)
	// Report the state we recovered with, so the driver persists nothing new
	// but observers see the correct starting point.
	r.prevSoftState = SoftState{Leader: None, State: Follower}
	r.prevHardState = r.hardState()
	return r, nil
}

func (r *raft) hardState() HardState {
	return HardState{Term: r.term, VoteFor: r.vote, Commit: r.log.committed}
}

func (r *raft) softState() SoftState {
	return SoftState{Leader: r.lead, State: r.state}
}

func (r *raft) quorum() int { return r.config.Quorum() }

func (r *raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *raft) resetTimers() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// [ElectionTick, 2*ElectionTick)
	r.randomizedElectionTimeout = r.cfg.ElectionTick + r.cfg.Rand(r.cfg.ElectionTick)
}

func (r *raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.lead = None
	r.resetTimers()
	r.votes = make(map[uint64]bool)
	r.progress = make(map[uint64]*progress)
	r.readPending = nil
	r.uncommittedConf = false
	r.pendingConfIndex = 0
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

func (r *raft) becomeFollower(term, lead uint64) {
	r.reset(term)
	r.state = Follower
	r.lead = lead
}

func (r *raft) becomePreCandidate() {
	if r.state == Leader {
		panic("raft: leader cannot become pre-candidate")
	}
	// A pre-candidate does NOT increment its term and does not record a vote;
	// that is the entire point of pre-vote.
	r.state = PreCandidate
	r.votes = make(map[uint64]bool)
	r.lead = None
	r.resetTimers()
}

func (r *raft) becomeCandidate() {
	if r.state == Leader {
		panic("raft: leader cannot become candidate")
	}
	r.reset(r.term + 1)
	r.state = Candidate
	r.vote = r.id
	r.votes[r.id] = true
}

func (r *raft) becomeLeader() {
	if r.state == Follower {
		panic("raft: follower cannot become leader without an election")
	}
	r.reset(r.term)
	r.state = Leader
	r.lead = r.id
	r.vote = r.id

	last := r.log.lastIndex()
	for _, id := range r.config.Voters {
		p := &progress{Next: last + 1, State: probeState}
		if id == r.id {
			p.Match = last
			p.State = replicateState
			p.RecentActive = true
		}
		r.progress[id] = p
	}

	// Committing a no-op from the new term is what makes entries carried over
	// from previous terms safely committable (paper §5.4.2). It also gives the
	// leader a fast way to learn its true commit index.
	r.appendEntries(Entry{Type: EntryNoOp})
	r.logger.Info("became leader", "term", r.term, "lastIndex", r.log.lastIndex())
}

// ---------------------------------------------------------------------------
// Driving inputs
// ---------------------------------------------------------------------------

// Tick advances the logical clock by one unit.
func (r *raft) Tick() {
	switch r.state {
	case Leader:
		r.tickLeader()
	default:
		r.tickElection()
	}
}

func (r *raft) tickElection() {
	// A server that is not a voter (removed from the configuration, or still
	// catching up) must never start an election.
	if !r.config.Contains(r.id) {
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.campaign()
	}
}

func (r *raft) tickLeader() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.cfg.CheckQuorum && r.electionElapsed >= r.cfg.ElectionTick {
		r.electionElapsed = 0
		if !r.quorumActive() {
			r.logger.Warn("stepping down: no quorum contact within election timeout", "term", r.term)
			r.becomeFollower(r.term, None)
			return
		}
		for id, p := range r.progress {
			if id != r.id {
				p.RecentActive = false
			}
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTick {
		r.heartbeatElapsed = 0
		r.broadcastHeartbeat(nil)
	}
}

func (r *raft) quorumActive() bool {
	active := 0
	for _, id := range r.config.Voters {
		if id == r.id {
			active++
			continue
		}
		if p, ok := r.progress[id]; ok && p.RecentActive {
			active++
		}
	}
	return active >= r.quorum()
}

// campaign starts an election, or a pre-election when PreVote is enabled.
func (r *raft) campaign() {
	if !r.config.Contains(r.id) {
		return
	}
	if r.cfg.PreVote {
		r.becomePreCandidate()
		r.solicitVotes(MsgPreVote, r.term+1)
	} else {
		r.becomeCandidate()
		r.solicitVotes(MsgVote, r.term)
	}
}

// solicitVotes sends vote requests and handles the single-voter case where the
// election is already decided.
func (r *raft) solicitVotes(t MessageType, term uint64) {
	r.votes[r.id] = true
	if r.tallyVotes() == voteWon {
		r.electionWon()
		return
	}
	li, lt := r.log.lastIndex(), r.log.lastTerm()
	for _, id := range r.config.Voters {
		if id == r.id {
			continue
		}
		r.send(Message{
			Type: t, To: id, Term: term,
			LastLogIndex: li, LastLogTerm: lt,
		})
	}
}

type voteResult uint8

const (
	votePending voteResult = iota
	voteWon
	voteLost
)

func (r *raft) tallyVotes() voteResult {
	granted, rejected := 0, 0
	for _, id := range r.config.Voters {
		v, ok := r.votes[id]
		if !ok {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= r.quorum():
		return voteWon
	case rejected >= r.quorum():
		return voteLost
	default:
		return votePending
	}
}

func (r *raft) electionWon() {
	if r.state == PreCandidate {
		// Pre-vote succeeded: now run the real election, incrementing the term.
		r.becomeCandidate()
		r.solicitVotes(MsgVote, r.term)
		return
	}
	r.becomeLeader()
	r.broadcastAppend()
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

// Step feeds one message into the state machine.
func (r *raft) Step(m Message) error {
	switch {
	case m.Term == 0:
		// Local/termless message; fall through to state handlers.
	case m.Term > r.term:
		if m.Type == MsgPreVote {
			// Never advance the term for a pre-vote request: doing so would
			// hand a partitioned server exactly the disruption pre-vote exists
			// to prevent.
			break
		}
		if m.Type == MsgPreVoteResp && !m.Reject {
			// A granted pre-vote carries the future term; do not adopt it.
			break
		}
		lead := None
		if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
			lead = m.From
		}
		r.logger.Debug("observed higher term", "from", m.From, "msgTerm", m.Term, "term", r.term)
		r.becomeFollower(m.Term, lead)

	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat, MsgSnap:
			// Tell the stale leader our term so it steps down promptly rather
			// than waiting for its own election timeout.
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Reject: true})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
		}
		// Everything else from an older term is ignored.
		return nil
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		r.handleVoteRequest(m)
		return nil
	}

	switch r.state {
	case Leader:
		return r.stepLeader(m)
	case Candidate, PreCandidate:
		return r.stepCandidate(m)
	default:
		return r.stepFollower(m)
	}
}

func (r *raft) handleVoteRequest(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}

	// CheckQuorum: ignore a vote request from a server that is not more up to
	// date while we still have a live leader, so a single flapping node cannot
	// force repeated elections.
	if r.cfg.CheckQuorum && m.Type == MsgVote && r.lead != None && r.electionElapsed < r.cfg.ElectionTick {
		r.send(Message{Type: respType, To: m.From, Term: r.term, Reject: true})
		return
	}

	upToDate := r.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)
	var grant bool
	if m.Type == MsgPreVote {
		// Grant a pre-vote if we would plausibly grant the real vote: the
		// candidate's term must be at least ours and its log up to date.
		grant = m.Term >= r.term && upToDate
	} else {
		// One vote per term (paper §5.2). Re-granting to the same candidate is
		// required so a lost response can be retried.
		canVote := r.vote == None || r.vote == m.From
		grant = canVote && upToDate
	}

	if grant {
		if m.Type == MsgVote {
			r.vote = m.From
			// Only a real granted vote resets the election timer; otherwise a
			// stream of pre-votes could keep a follower from ever campaigning.
			r.electionElapsed = 0
		}
		r.send(Message{Type: respType, To: m.From, Term: m.Term})
		return
	}
	r.send(Message{Type: respType, To: m.From, Term: r.term, Reject: true})
}

func (r *raft) stepLeader(m Message) error {
	p := r.progress[m.From]
	if p != nil {
		p.RecentActive = true
	}

	switch m.Type {
	case MsgAppResp:
		if p == nil {
			return nil
		}
		if m.Reject {
			if p.maybeDecrTo(m.PrevLogIndex, m.RejectHintIndex, m.RejectHintTerm, r.log.lastIndex()) {
				if p.State == replicateState {
					p.becomeProbe()
				}
				r.sendAppend(m.From)
			}
			return nil
		}
		if p.maybeUpdate(m.MatchIndex) {
			if p.State == probeState {
				p.becomeReplicate()
			}
			if r.maybeCommit() {
				r.broadcastAppend()
			} else if p.Next <= r.log.lastIndex() {
				// Keep feeding a follower that is still catching up.
				r.sendAppend(m.From)
			}
		} else if p.State == probeState {
			p.ProbeSent = false
		}

	case MsgHeartbeatResp:
		if p != nil {
			p.ProbeSent = false
			if p.Match < r.log.lastIndex() {
				r.sendAppend(m.From)
			}
		}
		r.recordReadAck(m)

	case MsgSnapResp:
		if p == nil {
			return nil
		}
		if m.Reject {
			// The follower refused the snapshot (out of date); fall back to
			// probing rather than retrying the same snapshot forever.
			p.becomeProbe()
			return nil
		}
		p.Match = max(p.Match, p.PendingSnapshot)
		p.becomeProbe()

	case MsgReadIndex:
		r.handleReadIndex(m)
	}
	return nil
}

func (r *raft) stepCandidate(m Message) error {
	switch m.Type {
	case MsgPreVoteResp:
		if r.state != PreCandidate {
			return nil
		}
		r.votes[m.From] = !m.Reject
		switch r.tallyVotes() {
		case voteWon:
			r.electionWon()
		case voteLost:
			r.becomeFollower(r.term, None)
		}

	case MsgVoteResp:
		if r.state != Candidate {
			return nil
		}
		r.votes[m.From] = !m.Reject
		switch r.tallyVotes() {
		case voteWon:
			r.electionWon()
		case voteLost:
			r.becomeFollower(r.term, None)
		}

	case MsgApp, MsgHeartbeat, MsgSnap:
		// A leader exists for this term; concede.
		r.becomeFollower(m.Term, m.From)
		return r.stepFollower(m)
	}
	return nil
}

func (r *raft) stepFollower(m Message) error {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.log.commitTo(min(m.LeaderCommit, r.log.lastIndex()))
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, ReadCtx: m.ReadCtx})
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgReadIndex:
		// Followers cannot answer linearizable reads; the driver is expected to
		// forward to the leader. Dropping here is correct and visible: the read
		// times out rather than returning stale data.
	}
	return nil
}

func (r *raft) handleAppend(m Message) {
	if m.PrevLogIndex < r.log.committed {
		// Everything up to committed is already durable and agreed; answer with
		// our commit index so the leader fast-forwards.
		r.send(Message{Type: MsgAppResp, To: m.From, MatchIndex: r.log.committed})
		return
	}
	if lastNew, ok := r.log.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit, m.Entries); ok {
		r.send(Message{Type: MsgAppResp, To: m.From, MatchIndex: lastNew})
		return
	}

	// Rejection: include a hint so the leader can skip the whole conflicting
	// term instead of walking back one index per round trip.
	hintIndex, hintTerm := r.conflictHint(m.PrevLogIndex)
	r.send(Message{
		Type: MsgAppResp, To: m.From, Reject: true,
		PrevLogIndex:    m.PrevLogIndex,
		RejectHintIndex: hintIndex,
		RejectHintTerm:  hintTerm,
	})
}

// conflictHint returns the first index of the conflicting term, or our last
// index + 1 when the log is simply too short.
func (r *raft) conflictHint(prevIndex uint64) (uint64, uint64) {
	last := r.log.lastIndex()
	if prevIndex > last {
		return last + 1, 0
	}
	term, err := r.log.term(prevIndex)
	if err != nil {
		return r.log.firstIndex(), 0
	}
	// Walk back to the first index carrying this term.
	first := r.log.firstIndex()
	idx := prevIndex
	for idx > first {
		t, err := r.log.term(idx - 1)
		if err != nil || t != term {
			break
		}
		idx--
	}
	return idx, term
}

func (r *raft) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil {
		return
	}
	if s.Index <= r.log.committed {
		// Already have everything this snapshot covers.
		r.send(Message{Type: MsgSnapResp, To: m.From, MatchIndex: r.log.committed})
		return
	}
	r.log.restore(s)
	r.config = s.Config.Clone()
	r.logger.Info("restored from leader snapshot", "index", s.Index, "term", s.Term)
	r.send(Message{Type: MsgSnapResp, To: m.From, MatchIndex: s.Index})
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

func (r *raft) broadcastAppend() {
	for _, id := range r.config.Voters {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *raft) broadcastHeartbeat(ctx []byte) {
	for _, id := range r.config.Voters {
		if id == r.id {
			continue
		}
		p := r.progress[id]
		commit := r.log.committed
		if p != nil {
			// Never tell a follower to commit past what it holds.
			commit = min(commit, p.Match)
		}
		r.send(Message{Type: MsgHeartbeat, To: id, LeaderCommit: commit, ReadCtx: ctx})
	}
}

func (r *raft) sendAppend(to uint64) {
	p := r.progress[to]
	if p == nil {
		return
	}
	if p.State == snapshotState {
		return
	}
	if p.State == probeState && p.ProbeSent {
		return
	}

	prevIndex := p.Next - 1
	prevTerm, err := r.log.term(prevIndex)
	if err != nil {
		// The entry the follower needs has been compacted; send a snapshot.
		r.sendSnapshot(to, p)
		return
	}
	entries, err := r.log.slice(p.Next, min(r.log.lastIndex()+1, p.Next+uint64(r.cfg.MaxEntriesPerAppend)))
	if err != nil {
		r.sendSnapshot(to, p)
		return
	}

	if p.State == probeState {
		p.ProbeSent = true
	} else if len(entries) > 0 {
		// Optimistically assume success so the next batch can be pipelined.
		p.Next = entries[len(entries)-1].Index + 1
	}

	r.send(Message{
		Type: MsgApp, To: to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.log.committed,
	})
}

func (r *raft) sendSnapshot(to uint64, p *progress) {
	snap, err := r.cfg.Storage.Snapshot()
	if err != nil || snap.Index == 0 {
		r.logger.Warn("follower needs a snapshot but none is available", "follower", to, "err", err)
		return
	}
	p.becomeSnapshot(snap.Index)
	s := snap
	r.send(Message{Type: MsgSnap, To: to, Snapshot: &s})
	r.logger.Info("sending snapshot to follower", "follower", to, "index", snap.Index)
}

// maybeCommit advances the commit index to the highest index replicated on a
// majority, subject to the current-term restriction from paper §5.4.2.
func (r *raft) maybeCommit() bool {
	if len(r.config.Voters) == 0 {
		return false
	}
	matches := make([]uint64, 0, len(r.config.Voters))
	for _, id := range r.config.Voters {
		if p, ok := r.progress[id]; ok {
			matches = append(matches, p.Match)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	// The quorum-th largest match index is replicated on a majority.
	candidate := matches[r.quorum()-1]

	if candidate <= r.log.committed {
		return false
	}
	// A leader may never conclude that an entry from a *previous* term is
	// committed merely because it is stored on a majority. Only entries from
	// the current term can be counted; older ones are committed transitively.
	if term, err := r.log.term(candidate); err != nil || term != r.term {
		return false
	}
	r.log.commitTo(candidate)
	return true
}

// appendEntries appends leader-authored entries and updates its own progress.
func (r *raft) appendEntries(entries ...Entry) {
	last := r.log.lastIndex()
	for i := range entries {
		entries[i].Term = r.term
		entries[i].Index = last + 1 + uint64(i)
		if entries[i].Type == EntryConfChange {
			r.pendingConfIndex = entries[i].Index
			r.uncommittedConf = true
		}
	}
	r.log.append(entries...)
	if p := r.progress[r.id]; p != nil {
		p.maybeUpdate(r.log.lastIndex())
	}
	// A single-voter cluster commits immediately; there is nobody to wait for.
	r.maybeCommit()
}

// ---------------------------------------------------------------------------
// Proposals
// ---------------------------------------------------------------------------

func (r *raft) propose(data []byte) (uint64, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	r.appendEntries(Entry{Type: EntryNormal, Data: data})
	r.broadcastAppend()
	return r.log.lastIndex(), nil
}

func (r *raft) proposeConfChange(cc ConfChange) (uint64, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	if r.uncommittedConf {
		return 0, ErrConfChangeInFlight
	}
	// A leader must have committed an entry from its own term before changing
	// membership, otherwise it may not yet know the true committed prefix.
	if r.log.committed < r.log.firstIndex() {
		return 0, ErrProposalDropped
	}
	r.appendEntries(Entry{Type: EntryConfChange, Data: encodeConfChange(cc)})
	r.broadcastAppend()
	return r.log.lastIndex(), nil
}

// applyConfChange updates membership. The driver calls this when the
// corresponding entry is applied, not when it is appended.
//
// Applying on commit (rather than on append, as the dissertation describes)
// means an uncommitted change is never in force. Combined with the rule that
// only one change may be in flight, the old and new majorities always overlap,
// which is the property the restriction exists to guarantee.
func (r *raft) applyConfChange(cc ConfChange) Configuration {
	switch cc.Type {
	case ConfChangeAddNode:
		if !r.config.Contains(cc.NodeID) {
			r.config.Voters = append(r.config.Voters, cc.NodeID)
			sort.Slice(r.config.Voters, func(i, j int) bool { return r.config.Voters[i] < r.config.Voters[j] })
			if r.state == Leader {
				r.progress[cc.NodeID] = &progress{Next: r.log.lastIndex() + 1, State: probeState}
			}
		}
	case ConfChangeRemoveNode:
		voters := r.config.Voters[:0]
		for _, v := range r.config.Voters {
			if v != cc.NodeID {
				voters = append(voters, v)
			}
		}
		r.config.Voters = voters
		delete(r.progress, cc.NodeID)
		// Removing a member shrinks the quorum, which can immediately make
		// pending entries committable.
		if r.state == Leader {
			if r.maybeCommit() {
				r.broadcastAppend()
			}
		}
	}
	r.uncommittedConf = false
	return r.config.Clone()
}

// ---------------------------------------------------------------------------
// Linearizable reads
// ---------------------------------------------------------------------------

// readIndex begins a linearizable read. The caller may serve the read once the
// returned ReadState's index has been applied to its state machine.
func (r *raft) readIndex(ctx []byte) error {
	if r.state != Leader {
		return ErrNotLeader
	}
	// Until the leader has committed an entry from its own term it does not
	// know the true commit index, so it cannot safely bound a read.
	if term, err := r.log.term(r.log.committed); err != nil || term != r.term {
		return ErrNoLeader
	}
	if len(r.config.Voters) == 1 && r.config.Contains(r.id) {
		// Single voter: this server is the majority.
		r.readStates = append(r.readStates, ReadState{Index: r.log.committed, Ctx: ctx})
		return nil
	}
	st := &readIndexStatus{
		index: r.log.committed,
		ctx:   append([]byte(nil), ctx...),
		acks:  map[uint64]bool{r.id: true},
	}
	r.readPending = append(r.readPending, st)
	// Confirm leadership with a heartbeat round rather than by appending to the
	// log: reads must not cost a disk write.
	r.broadcastHeartbeat(st.ctx)
	return nil
}

func (r *raft) recordReadAck(m Message) {
	if len(m.ReadCtx) == 0 {
		return
	}
	for i, st := range r.readPending {
		if !bytes.Equal(st.ctx, m.ReadCtx) {
			continue
		}
		st.acks[m.From] = true
		if len(st.acks) >= r.quorum() {
			// Confirm this read and every earlier one: they were issued at
			// indices no greater than this one and are confirmed by the same
			// heartbeat round.
			for _, done := range r.readPending[:i+1] {
				r.readStates = append(r.readStates, ReadState{Index: done.index, Ctx: done.ctx})
			}
			r.readPending = r.readPending[i+1:]
		}
		return
	}
}

func (r *raft) handleReadIndex(m Message) {
	// A follower forwarded a read; confirm leadership then answer it.
	if err := r.readIndex(m.ReadCtx); err != nil {
		return
	}
	r.send(Message{Type: MsgReadIndexResp, To: m.From, ReadIndex: r.log.committed, ReadCtx: m.ReadCtx})
}

// ---------------------------------------------------------------------------
// Ready / Advance
// ---------------------------------------------------------------------------

func (r *raft) makeReady() Ready {
	rd := Ready{
		Entries:          r.log.unstableEntries(),
		CommittedEntries: r.log.nextCommittedEntries(r.cfg.MaxCommittedEntriesPerReady),
		Messages:         r.msgs,
		ReadStates:       r.readStates,
	}
	if ss := r.softState(); ss != r.prevSoftState {
		s := ss
		rd.SoftState = &s
		r.prevSoftState = ss
	}
	if hs := r.hardState(); hs != r.prevHardState {
		h := hs
		rd.HardState = &h
		r.prevHardState = hs
	}
	if !r.log.pendingSnapshot.IsEmpty() {
		rd.Snapshot = r.log.pendingSnapshot
	}
	r.msgs = nil
	r.readStates = nil
	return rd
}

func (r *raft) advance(rd Ready) {
	if !rd.Snapshot.IsEmpty() {
		r.log.snapshotPersisted(rd.Snapshot.Index)
	}
	if n := len(rd.Entries); n > 0 {
		last := rd.Entries[n-1]
		r.log.stableTo(last.Index, last.Term)
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.log.appliedTo(rd.CommittedEntries[n-1].Index)
	}
}

func (r *raft) appliedIndex() uint64 { return r.log.applied }
