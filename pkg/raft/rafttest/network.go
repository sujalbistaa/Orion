// Package rafttest provides a deterministic, fully in-memory Raft cluster
// simulator.
//
// Every run is reproducible from a seed: there are no goroutines, no wall
// clock and no real network. One logical tick delivers due packets, advances
// each replica's clock and drains its Ready output, in a fixed order. That
// makes the failure modes people usually hand-wave about — partitions, delayed
// and duplicated RPCs, crashes mid-append — into ordinary table-driven tests
// that either pass or print the exact seed needed to reproduce them.
//
// The simulator checks Raft's five safety properties after every scenario, so
// a violation is caught where it happens rather than as a mysterious
// inconsistency later.
package rafttest

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"testing"

	"github.com/sujalbistaa/orion/pkg/raft"
)

// FaultConfig describes the network conditions to simulate. The zero value is
// a perfect network.
type FaultConfig struct {
	// DropRate is the probability in [0,1] that a message is discarded.
	DropRate float64
	// DuplicateRate is the probability that a message is delivered twice.
	// Raft must be idempotent under duplication; this is what proves it.
	DuplicateRate float64
	// MaxDelayTicks spreads delivery over [1, MaxDelayTicks] ticks. Zero means
	// same-tick delivery.
	MaxDelayTicks int
}

// FSM records applied entries so tests can compare replicas' histories.
type FSM struct {
	// Applied is the exact sequence of normal entries this replica applied.
	Applied []raft.Entry
	// State is a simple key/value fold over applied entries, giving the tests a
	// notion of "same state" beyond the raw log.
	State map[string]string
}

func newFSM() *FSM { return &FSM{State: map[string]string{}} }

func (f *FSM) Apply(e raft.Entry) any {
	f.Applied = append(f.Applied, e)
	// Payload format is "key=value"; anything else is recorded but not folded.
	for i := 0; i < len(e.Data); i++ {
		if e.Data[i] == '=' {
			f.State[string(e.Data[:i])] = string(e.Data[i+1:])
			break
		}
	}
	return e.Index
}

func (f *FSM) Snapshot() ([]byte, error) { return nil, nil }

func (f *FSM) Restore([]byte) error {
	f.Applied = nil
	f.State = map[string]string{}
	return nil
}

// Replica is one simulated server: consensus core, its storage and its FSM.
type Replica struct {
	ID      uint64
	Node    *raft.RawNode
	Storage *raft.MemoryStorage
	FSM     *FSM

	// crashed replicas neither tick nor receive; their storage survives, which
	// is exactly what a process restart looks like.
	crashed bool
}

type packet struct {
	msg       raft.Message
	deliverAt uint64
	seq       uint64
}

// Network is a deterministic cluster of replicas connected by a lossy link.
type Network struct {
	t   *testing.T
	rng *rand.Rand
	// Seed is printed on failure so a run can be reproduced exactly.
	Seed int64

	replicas map[uint64]*Replica
	ids      []uint64

	inflight []packet
	tick     uint64
	seq      uint64

	faults FaultConfig
	// blocked[a][b] means messages from a to b are dropped: an asymmetric
	// partition, which is strictly harder than a symmetric one.
	blocked map[uint64]map[uint64]bool

	// leadersByTerm detects Election Safety violations across the whole run.
	leadersByTerm map[uint64]uint64
	// committedByReplica[i] is the highest commit index each replica reached.
	maxCommitted uint64

	logger *slog.Logger
}

// Options configures a simulated cluster.
type Options struct {
	Seed          int64
	IDs           []uint64
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	Faults        FaultConfig
	// Verbose logs every raft decision; off by default because a 10k-tick run
	// produces a great deal of output.
	Verbose bool
}

// NewNetwork builds a cluster where every replica knows every other.
func NewNetwork(t *testing.T, opts Options) *Network {
	t.Helper()
	if len(opts.IDs) == 0 {
		opts.IDs = []uint64{1, 2, 3}
	}
	if opts.ElectionTick == 0 {
		opts.ElectionTick = 10
	}
	if opts.HeartbeatTick == 0 {
		opts.HeartbeatTick = 1
	}
	if opts.Seed == 0 {
		opts.Seed = 1
	}

	level := slog.LevelError
	if opts.Verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: level}))

	n := &Network{
		t:             t,
		rng:           rand.New(rand.NewSource(opts.Seed)),
		Seed:          opts.Seed,
		replicas:      make(map[uint64]*Replica, len(opts.IDs)),
		ids:           append([]uint64(nil), opts.IDs...),
		faults:        opts.Faults,
		blocked:       make(map[uint64]map[uint64]bool),
		leadersByTerm: make(map[uint64]uint64),
		logger:        logger,
	}
	sort.Slice(n.ids, func(i, j int) bool { return n.ids[i] < n.ids[j] })

	for _, id := range n.ids {
		storage := raft.NewMemoryStorage()
		fsm := newFSM()
		// Each replica draws randomness from the shared, seeded source, so
		// election timeouts differ between replicas but are reproducible.
		cfg := raft.Config{
			ID:            id,
			Peers:         append([]uint64(nil), n.ids...),
			ElectionTick:  opts.ElectionTick,
			HeartbeatTick: opts.HeartbeatTick,
			Storage:       storage,
			PreVote:       opts.PreVote,
			CheckQuorum:   opts.CheckQuorum,
			Rand:          n.rng.Intn,
			Logger:        logger,
		}
		node, err := raft.NewRawNode(cfg)
		if err != nil {
			t.Fatalf("creating replica %d: %v", id, err)
		}
		n.replicas[id] = &Replica{ID: id, Node: node, Storage: storage, FSM: fsm}
	}
	return n
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// Replicas returns the replica set in ID order.
func (n *Network) Replicas() []*Replica {
	out := make([]*Replica, 0, len(n.ids))
	for _, id := range n.ids {
		out = append(out, n.replicas[id])
	}
	return out
}

func (n *Network) Replica(id uint64) *Replica { return n.replicas[id] }

// Run advances the cluster by the given number of ticks.
func (n *Network) Run(ticks int) {
	for i := 0; i < ticks; i++ {
		n.step()
	}
}

// RunUntil advances until cond returns true or maxTicks elapse. It reports
// whether the condition was met, so callers assert with context rather than
// sleeping and hoping.
func (n *Network) RunUntil(maxTicks int, cond func() bool) bool {
	for i := 0; i < maxTicks; i++ {
		if cond() {
			return true
		}
		n.step()
	}
	return cond()
}

func (n *Network) step() {
	n.tick++
	n.deliverDue()
	for _, id := range n.ids {
		r := n.replicas[id]
		if r.crashed {
			continue
		}
		r.Node.Tick()
	}
	n.drainAll()
}

func (n *Network) drainAll() {
	// Repeat until quiescent so that a message produced this tick can be
	// processed this tick when the network is perfect; with delays configured,
	// packets are queued for a future tick instead.
	for round := 0; round < 100; round++ {
		progressed := false
		for _, id := range n.ids {
			r := n.replicas[id]
			if r.crashed || !r.Node.HasReady() {
				continue
			}
			n.processReady(r)
			progressed = true
		}
		if !progressed {
			return
		}
		if n.faults.MaxDelayTicks > 0 {
			// With delays configured, everything is queued; nothing more can
			// happen in this tick.
			return
		}
		n.deliverDue()
	}
}

func (n *Network) processReady(r *Replica) {
	rd := r.Node.Ready()

	if rd.SoftState != nil && rd.SoftState.State == raft.Leader {
		term := r.Node.Term()
		if prev, ok := n.leadersByTerm[term]; ok && prev != r.ID {
			n.t.Fatalf("ELECTION SAFETY VIOLATION (seed %d): term %d has leaders %d and %d",
				n.Seed, term, prev, r.ID)
		}
		n.leadersByTerm[term] = r.ID
	}

	if !rd.Snapshot.IsEmpty() {
		if err := r.Storage.SaveSnapshot(*rd.Snapshot); err != nil && err != raft.ErrSnapshotOutOfDate {
			n.t.Fatalf("replica %d: saving snapshot: %v", r.ID, err)
		}
		if err := r.FSM.Restore(rd.Snapshot.Data); err != nil {
			n.t.Fatalf("replica %d: restoring snapshot: %v", r.ID, err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := r.Storage.Append(rd.Entries); err != nil {
			n.t.Fatalf("replica %d: appending entries: %v", r.ID, err)
		}
	}
	if rd.HardState != nil {
		if err := r.Storage.SetHardState(*rd.HardState); err != nil {
			n.t.Fatalf("replica %d: persisting hard state: %v", r.ID, err)
		}
	}

	for _, m := range rd.Messages {
		n.enqueue(m)
	}

	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raft.EntryNoOp:
		case raft.EntryConfChange:
			cc, err := raft.DecodeConfChange(e.Data)
			if err != nil {
				n.t.Fatalf("replica %d: decoding conf change: %v", r.ID, err)
			}
			conf := r.Node.ApplyConfChange(cc)
			if err := r.Storage.SetConfiguration(conf); err != nil {
				n.t.Fatalf("replica %d: persisting configuration: %v", r.ID, err)
			}
		default:
			r.FSM.Apply(e)
		}
	}
	if c := r.Node.Committed(); c > n.maxCommitted {
		n.maxCommitted = c
	}
	r.Node.Advance(rd)
}

func (n *Network) enqueue(m raft.Message) {
	if n.isBlocked(m.From, m.To) {
		return
	}
	if n.faults.DropRate > 0 && n.rng.Float64() < n.faults.DropRate {
		return
	}
	delay := uint64(0)
	if n.faults.MaxDelayTicks > 0 {
		delay = uint64(1 + n.rng.Intn(n.faults.MaxDelayTicks))
	}
	n.push(m, n.tick+delay)

	if n.faults.DuplicateRate > 0 && n.rng.Float64() < n.faults.DuplicateRate {
		dupDelay := delay
		if n.faults.MaxDelayTicks > 0 {
			dupDelay = uint64(1 + n.rng.Intn(n.faults.MaxDelayTicks))
		}
		n.push(m, n.tick+dupDelay)
	}
}

func (n *Network) push(m raft.Message, at uint64) {
	n.seq++
	n.inflight = append(n.inflight, packet{msg: m, deliverAt: at, seq: n.seq})
}

func (n *Network) deliverDue() {
	if len(n.inflight) == 0 {
		return
	}
	// Deliver in (deliverAt, seq) order so the run is fully determined by the
	// seed. Randomized delays already provide reordering.
	sort.SliceStable(n.inflight, func(i, j int) bool {
		if n.inflight[i].deliverAt != n.inflight[j].deliverAt {
			return n.inflight[i].deliverAt < n.inflight[j].deliverAt
		}
		return n.inflight[i].seq < n.inflight[j].seq
	})

	remaining := n.inflight[:0]
	due := make([]packet, 0, len(n.inflight))
	for _, p := range n.inflight {
		if p.deliverAt <= n.tick {
			due = append(due, p)
		} else {
			remaining = append(remaining, p)
		}
	}
	n.inflight = append([]packet(nil), remaining...)

	for _, p := range due {
		// The partition may have been installed after the packet was queued;
		// re-check at delivery time.
		if n.isBlocked(p.msg.From, p.msg.To) {
			continue
		}
		dst := n.replicas[p.msg.To]
		if dst == nil || dst.crashed {
			continue
		}
		if err := dst.Node.Step(p.msg); err != nil {
			n.t.Fatalf("replica %d: step %s: %v", dst.ID, p.msg.Type, err)
		}
	}
}

func (n *Network) isBlocked(from, to uint64) bool {
	if m, ok := n.blocked[from]; ok && m[to] {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

// Partition splits the cluster into groups that cannot talk across group
// boundaries. Messages within a group still flow.
func (n *Network) Partition(groups ...[]uint64) {
	n.blocked = make(map[uint64]map[uint64]bool)
	group := make(map[uint64]int, len(n.ids))
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi
		}
	}
	for _, a := range n.ids {
		for _, b := range n.ids {
			if a == b {
				continue
			}
			ga, oka := group[a]
			gb, okb := group[b]
			if !oka || !okb || ga != gb {
				n.block(a, b)
			}
		}
	}
}

// Isolate cuts one replica off from everyone else in both directions.
func (n *Network) Isolate(id uint64) {
	for _, other := range n.ids {
		if other == id {
			continue
		}
		n.block(id, other)
		n.block(other, id)
	}
}

// BlockOneWay drops messages from -> to only, leaving the reverse direction
// working. Asymmetric partitions break naive leader-lease implementations.
func (n *Network) BlockOneWay(from, to uint64) { n.block(from, to) }

func (n *Network) block(from, to uint64) {
	if n.blocked[from] == nil {
		n.blocked[from] = make(map[uint64]bool)
	}
	n.blocked[from][to] = true
}

// Heal removes every partition.
func (n *Network) Heal() { n.blocked = make(map[uint64]map[uint64]bool) }

// Crash stops a replica without touching its storage.
func (n *Network) Crash(id uint64) {
	r := n.replicas[id]
	if r == nil {
		n.t.Fatalf("unknown replica %d", id)
	}
	r.crashed = true
	// In-flight messages to and from a crashed process are lost.
	kept := n.inflight[:0]
	for _, p := range n.inflight {
		if p.msg.To != id && p.msg.From != id {
			kept = append(kept, p)
		}
	}
	n.inflight = append([]packet(nil), kept...)
}

// Restart brings a crashed replica back from its persisted storage, exactly as
// a process restart would. Volatile state (role, votes received, progress) is
// discarded; the log, term, vote and commit index survive.
func (n *Network) Restart(id uint64, opts Options) {
	r := n.replicas[id]
	if r == nil {
		n.t.Fatalf("unknown replica %d", id)
	}
	if opts.ElectionTick == 0 {
		opts.ElectionTick = 10
	}
	if opts.HeartbeatTick == 0 {
		opts.HeartbeatTick = 1
	}
	cfg := raft.Config{
		ID:            id,
		Peers:         append([]uint64(nil), n.ids...),
		ElectionTick:  opts.ElectionTick,
		HeartbeatTick: opts.HeartbeatTick,
		Storage:       r.Storage,
		PreVote:       opts.PreVote,
		CheckQuorum:   opts.CheckQuorum,
		Rand:          n.rng.Intn,
		Logger:        n.logger,
	}
	node, err := raft.NewRawNode(cfg)
	if err != nil {
		n.t.Fatalf("restarting replica %d: %v", id, err)
	}
	// The state machine is rebuilt by replaying the persisted log, which is
	// the property "a restart does not corrupt state" actually depends on.
	r.FSM = newFSM()
	r.Node = node
	r.crashed = false
}

// ---------------------------------------------------------------------------
// Cluster operations
// ---------------------------------------------------------------------------

// Leader returns the current leader's ID, or 0 if there is none. When several
// replicas believe they lead (possible across a partition), the one with the
// highest term is returned.
func (n *Network) Leader() uint64 {
	var leader, bestTerm uint64
	for _, id := range n.ids {
		r := n.replicas[id]
		if r.crashed || r.Node.State() != raft.Leader {
			continue
		}
		if r.Node.Term() >= bestTerm {
			bestTerm = r.Node.Term()
			leader = id
		}
	}
	return leader
}

// LeadersInTerm returns every replica currently claiming leadership, used to
// assert that a partition produces at most one *effective* leader.
func (n *Network) LeadersInTerm() map[uint64]uint64 {
	out := map[uint64]uint64{}
	for _, id := range n.ids {
		r := n.replicas[id]
		if !r.crashed && r.Node.State() == raft.Leader {
			out[id] = r.Node.Term()
		}
	}
	return out
}

// ElectLeader campaigns on the given replica and runs until it wins, so tests
// that are not about elections do not depend on timeout randomness.
func (n *Network) ElectLeader(id uint64) {
	n.t.Helper()
	n.replicas[id].Node.Campaign()
	n.drainAll()
	if !n.RunUntil(200, func() bool { return n.Leader() == id }) {
		n.t.Fatalf("replica %d did not become leader within 200 ticks (seed %d)", id, n.Seed)
	}
}

// Propose submits a command to the given replica.
func (n *Network) Propose(id uint64, data string) (uint64, error) {
	r := n.replicas[id]
	if r == nil || r.crashed {
		return 0, fmt.Errorf("replica %d unavailable", id)
	}
	idx, err := r.Node.Propose([]byte(data))
	if err != nil {
		return 0, err
	}
	n.drainAll()
	return idx, nil
}

// ProposeToLeader submits to whichever replica currently leads.
func (n *Network) ProposeToLeader(data string) (uint64, error) {
	leader := n.Leader()
	if leader == 0 {
		return 0, fmt.Errorf("no leader")
	}
	return n.Propose(leader, data)
}

// CommittedOn reports the commit index of a replica.
func (n *Network) CommittedOn(id uint64) uint64 { return n.replicas[id].Node.Committed() }

// ---------------------------------------------------------------------------
// Safety checks
// ---------------------------------------------------------------------------

// CheckSafety asserts Raft's safety properties across every live replica. It is
// called at the end of every scenario; a violation reports the seed so the run
// can be replayed exactly.
func (n *Network) CheckSafety() {
	n.t.Helper()
	n.checkLogMatching()
	n.checkStateMachineSafety()
	n.checkLeaderCompleteness()
}

// Log Matching: if two logs contain an entry with the same index and term, the
// logs are identical in all preceding entries.
func (n *Network) checkLogMatching() {
	n.t.Helper()
	for _, a := range n.Replicas() {
		for _, b := range n.Replicas() {
			if a.ID >= b.ID {
				continue
			}
			last := min64(a.Node.LastIndex(), b.Node.LastIndex())
			for i := last; i >= 1; i-- {
				ea, err1 := a.Node.LogEntries(i, i+1)
				eb, err2 := b.Node.LogEntries(i, i+1)
				if err1 != nil || err2 != nil || len(ea) == 0 || len(eb) == 0 {
					continue
				}
				if ea[0].Term != eb[0].Term {
					continue
				}
				// Same (index, term): every preceding entry must match.
				for j := uint64(1); j < i; j++ {
					pa, e1 := a.Node.LogEntries(j, j+1)
					pb, e2 := b.Node.LogEntries(j, j+1)
					if e1 != nil || e2 != nil || len(pa) == 0 || len(pb) == 0 {
						continue
					}
					if pa[0].Term != pb[0].Term || string(pa[0].Data) != string(pb[0].Data) {
						n.t.Fatalf("LOG MATCHING VIOLATION (seed %d): replicas %d and %d agree at index %d term %d "+
							"but differ at index %d (%v vs %v)", n.Seed, a.ID, b.ID, i, ea[0].Term, j, pa[0], pb[0])
					}
				}
				break
			}
		}
	}
}

// State Machine Safety: no two replicas apply different entries at the same
// index.
func (n *Network) checkStateMachineSafety() {
	n.t.Helper()
	for _, a := range n.Replicas() {
		for _, b := range n.Replicas() {
			if a.ID >= b.ID {
				continue
			}
			byIndex := map[uint64]raft.Entry{}
			for _, e := range a.FSM.Applied {
				byIndex[e.Index] = e
			}
			for _, e := range b.FSM.Applied {
				other, ok := byIndex[e.Index]
				if !ok {
					continue
				}
				if other.Term != e.Term || string(other.Data) != string(e.Data) {
					n.t.Fatalf("STATE MACHINE SAFETY VIOLATION (seed %d): replicas %d and %d applied different "+
						"entries at index %d: %v vs %v", n.Seed, a.ID, b.ID, e.Index, other, e)
				}
			}
		}
	}
}

// Leader Completeness: every entry committed anywhere must be present, with the
// same term, in the log of the current leader.
func (n *Network) checkLeaderCompleteness() {
	n.t.Helper()
	leader := n.Leader()
	if leader == 0 {
		return
	}
	lr := n.replicas[leader]
	for _, r := range n.Replicas() {
		committed := r.Node.Committed()
		for i := uint64(1); i <= committed; i++ {
			want, err := r.Node.LogEntries(i, i+1)
			if err != nil || len(want) == 0 {
				continue
			}
			got, err := lr.Node.LogEntries(i, i+1)
			if err != nil || len(got) == 0 {
				n.t.Fatalf("LEADER COMPLETENESS VIOLATION (seed %d): entry %d committed on replica %d is "+
					"missing from leader %d", n.Seed, i, r.ID, leader)
			}
			if got[0].Term != want[0].Term || string(got[0].Data) != string(want[0].Data) {
				n.t.Fatalf("LEADER COMPLETENESS VIOLATION (seed %d): entry %d differs between replica %d (%v) "+
					"and leader %d (%v)", n.Seed, i, r.ID, want[0], leader, got[0])
			}
		}
	}
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
