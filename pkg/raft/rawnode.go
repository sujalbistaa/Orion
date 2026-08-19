package raft

// RawNode is a synchronous, single-goroutine view of the consensus core. It
// performs no I/O and never blocks: the caller supplies ticks and messages and
// is responsible for persisting and sending whatever Ready returns.
//
// Node (node.go) is the driver used in production. RawNode exists so that a
// whole cluster can be advanced deterministically inside one goroutine — see
// pkg/raft/rafttest — which is what makes partition, delay, duplication and
// crash scenarios reproducible from a seed instead of flaky.
type RawNode struct {
	r *raft
}

func NewRawNode(c Config) (*RawNode, error) {
	r, err := newRaft(c)
	if err != nil {
		return nil, err
	}
	return &RawNode{r: r}, nil
}

// Tick advances the logical clock by one unit.
func (rn *RawNode) Tick() { rn.r.Tick() }

// Step processes one inbound message.
func (rn *RawNode) Step(m Message) error { return rn.r.Step(m) }

// Campaign forces an election. Tests use it to make leadership deterministic
// instead of waiting for a randomized timeout.
func (rn *RawNode) Campaign() { rn.r.campaign() }

// Propose appends an application command. It returns the assigned log index.
func (rn *RawNode) Propose(data []byte) (uint64, error) { return rn.r.propose(data) }

// ProposeConfChange appends a single-server membership change.
func (rn *RawNode) ProposeConfChange(cc ConfChange) (uint64, error) {
	return rn.r.proposeConfChange(cc)
}

// ApplyConfChange installs a committed membership change.
func (rn *RawNode) ApplyConfChange(cc ConfChange) Configuration { return rn.r.applyConfChange(cc) }

// ReadIndex starts a linearizable read; the confirmation arrives as a
// ReadState in a later Ready.
func (rn *RawNode) ReadIndex(ctx []byte) error { return rn.r.readIndex(ctx) }

// Ready returns pending output. The caller must honour the ordering documented
// on the Ready type and then call Advance.
func (rn *RawNode) Ready() Ready { return rn.r.makeReady() }

// HasReady reports whether Ready would return anything, so a simulator can
// avoid allocating on idle iterations.
func (rn *RawNode) HasReady() bool {
	return len(rn.r.msgs) > 0 || len(rn.r.readStates) > 0 ||
		len(rn.r.log.unstableEntries()) > 0 ||
		!rn.r.log.pendingSnapshot.IsEmpty() ||
		rn.r.log.committed > rn.r.log.applied ||
		rn.r.softState() != rn.r.prevSoftState ||
		rn.r.hardState() != rn.r.prevHardState
}

// Advance acknowledges that the caller has persisted and applied a Ready.
func (rn *RawNode) Advance(rd Ready) { rn.r.advance(rd) }

func (rn *RawNode) ID() uint64                   { return rn.r.id }
func (rn *RawNode) Term() uint64                 { return rn.r.term }
func (rn *RawNode) State() State                 { return rn.r.state }
func (rn *RawNode) Leader() uint64               { return rn.r.lead }
func (rn *RawNode) Committed() uint64            { return rn.r.log.committed }
func (rn *RawNode) Applied() uint64              { return rn.r.log.applied }
func (rn *RawNode) LastIndex() uint64            { return rn.r.log.lastIndex() }
func (rn *RawNode) Configuration() Configuration { return rn.r.config.Clone() }

// LogEntries returns entries in [lo, hi) for test assertions about log
// equivalence across replicas.
func (rn *RawNode) LogEntries(lo, hi uint64) ([]Entry, error) { return rn.r.log.slice(lo, hi) }
