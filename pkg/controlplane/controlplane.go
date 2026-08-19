// Package controlplane binds the consensus layer to the cluster state machine
// and exposes the one write path every other component uses.
//
// Nothing else in Orion talks to Raft directly. Controllers, the API server and
// the agent-facing gRPC service all go through Apply, which handles proposal
// encoding, leader checks, retry on dropped proposals and the difference
// between "the cluster rejected this" and "we could not reach the cluster".
// That distinction matters: the first is a 4xx, the second is a 503.
package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/raft"
	"github.com/sujalbistaa/orion/pkg/store"
)

// Errors callers distinguish on.
var (
	// ErrNotLeader means this replica cannot accept writes. LeaderAddress
	// tells the caller where to go instead.
	ErrNotLeader = errors.New("controlplane: this replica is not the leader")
	// ErrUnavailable means the write could not be committed — no quorum, or
	// leadership changed mid-flight. Retrying is safe: every command carries an
	// idempotency key.
	ErrUnavailable = errors.New("controlplane: cluster is unavailable")
)

// PeerInfo describes a control-plane replica for status reporting.
type PeerInfo struct {
	ID      uint64
	Address string
}

// Options configures the control plane.
type Options struct {
	NodeID uint64
	// Peers maps replica ID to its raft address, used for bootstrap and for
	// redirecting writes.
	Peers map[uint64]string

	Raft      raft.Config
	Transport raft.Transport
	Store     *store.Store

	TickInterval      time.Duration
	SnapshotThreshold uint64

	// ProposeTimeout bounds a single write. It must exceed a typical election
	// so that a write issued during a leader change has a chance to land after
	// one retry rather than failing the user's request.
	ProposeTimeout time.Duration

	Logger *slog.Logger
}

// ControlPlane is the write path plus the read snapshot.
type ControlPlane struct {
	node  *raft.Node
	store *store.Store
	log   *slog.Logger

	nodeID         uint64
	peers          map[uint64]string
	proposeTimeout time.Duration

	startedAt time.Time
	clusterID string
}

func New(opts Options) (*ControlPlane, error) {
	if opts.Store == nil {
		return nil, errors.New("controlplane: Store is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.ProposeTimeout == 0 {
		opts.ProposeTimeout = 10 * time.Second
	}
	opts.Raft.Logger = opts.Logger

	node, err := raft.NewNode(raft.NodeOptions{
		Config:            opts.Raft,
		FSM:               opts.Store,
		Transport:         opts.Transport,
		TickInterval:      opts.TickInterval,
		SnapshotThreshold: opts.SnapshotThreshold,
		InitialPeerAddrs:  opts.Peers,
	})
	if err != nil {
		return nil, fmt.Errorf("controlplane: creating raft node: %w", err)
	}

	return &ControlPlane{
		node:           node,
		store:          opts.Store,
		log:            opts.Logger.With("component", "controlplane"),
		nodeID:         opts.NodeID,
		peers:          opts.Peers,
		proposeTimeout: opts.ProposeTimeout,
		startedAt:      time.Now(),
		clusterID:      fmt.Sprintf("orion-%d", opts.NodeID),
	}, nil
}

func (cp *ControlPlane) Start() { cp.node.Start() }

func (cp *ControlPlane) Stop() { cp.node.Stop() }

// Store returns the local read view. Reads from it are served from this
// replica's applied state; use ReadIndex first when staleness is unacceptable.
func (cp *ControlPlane) Store() *store.Store { return cp.store }

// Raft exposes the consensus node for status reporting and membership changes.
func (cp *ControlPlane) Raft() *raft.Node { return cp.node }

func (cp *ControlPlane) IsLeader() bool { return cp.node.IsLeader() }

// LeaderAddress returns where writes should be sent, if a leader is known.
func (cp *ControlPlane) LeaderAddress() (string, bool) { return cp.node.LeaderAddress() }

// LeadershipChanges lets controllers start and stop with leadership.
func (cp *ControlPlane) LeadershipChanges() <-chan bool { return cp.node.LeadershipChanges() }

// ReadIndex blocks until this replica's state machine reflects every write
// committed before the call. Use it for reads that must not be stale — the
// alternative would be to route every read through the leader, which costs a
// network hop for data this replica already has.
func (cp *ControlPlane) ReadIndex(ctx context.Context) error {
	err := cp.node.LinearizableRead(ctx)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrNoLeader):
		return ErrNotLeader
	case errors.Is(err, raft.ErrStopped):
		return ErrUnavailable
	default:
		return err
	}
}

// Apply replicates a command and returns the state machine's result.
//
// Three outcomes are distinguished on purpose:
//
//	nil error                  the command was committed and applied
//	a store error (wrapped)    the cluster agreed to reject it — a conflict,
//	                           a missing object, an illegal transition. Every
//	                           replica computed the same rejection.
//	ErrNotLeader/ErrUnavailable the command may or may not have been committed;
//	                           retry with the same RequestID.
func (cp *ControlPlane) Apply(ctx context.Context, cmd store.Command) (store.Result, error) {
	if cmd.Timestamp.IsZero() {
		// The timestamp is replicated so that every replica applies the same
		// value. Stamping it here, once, on the proposer is the only place it
		// can be read from a clock.
		cmd.Timestamp = time.Now().UTC()
	}
	if cmd.RequestID == "" && requiresIdempotency(cmd.Kind) {
		cmd.RequestID = NewRequestID()
	}

	data, err := cmd.Encode()
	if err != nil {
		return store.Result{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, cp.proposeTimeout)
	defer cancel()

	// One retry. A proposal is dropped when leadership changes mid-flight;
	// since the command carries an idempotency key, retrying is safe and turns
	// a routine leader change into a slower request rather than a failed one.
	const attempts = 2
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return store.Result{}, ErrUnavailable
			}
		}

		raw, err := cp.node.Propose(ctx, data)
		if err != nil {
			lastErr = err
			switch {
			case errors.Is(err, raft.ErrNotLeader):
				return store.Result{}, ErrNotLeader
			case errors.Is(err, raft.ErrProposalDropped):
				continue // retry: safe because of RequestID
			case errors.Is(err, raft.ErrStopped), errors.Is(err, context.DeadlineExceeded):
				return store.Result{}, ErrUnavailable
			default:
				return store.Result{}, err
			}
		}

		res, ok := raw.(store.Result)
		if !ok {
			return store.Result{}, fmt.Errorf("controlplane: state machine returned %T, want store.Result", raw)
		}
		if res.Err != nil {
			// An agreed rejection. It is the caller's problem, not the
			// cluster's, so it is returned as a normal error and never retried.
			return res, res.Err
		}
		return res, nil
	}
	cp.log.Warn("proposal exhausted retries", "kind", cmd.Kind, "err", lastErr)
	return store.Result{}, ErrUnavailable
}

// requiresIdempotency reports whether a command kind must carry a request ID.
// Status updates are naturally idempotent — applying one twice produces the
// same state — so they are exempt and save an entry in the dedup table.
func requiresIdempotency(kind store.CommandKind) bool {
	switch kind {
	case store.CmdUpdateNodeStatus, store.CmdUpdateWorkloadStatus,
		store.CmdUpdateDeploymentStatus, store.CmdUpdateServiceEndpoint,
		store.CmdRecordEvent, store.CmdSetNodePhase:
		return false
	}
	return true
}

// NewRequestID returns a random idempotency key.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not a recoverable condition; a non-unique
		// request ID would silently suppress a legitimate write.
		panic("controlplane: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// RecordEvent appends an operational event. Failures are logged rather than
// returned: an event is a diagnostic, and losing one must never fail the
// operation that produced it.
func (cp *ControlPlane) RecordEvent(ctx context.Context, e v1.Event) {
	// The store stamps the event's actor from the command, so it has to be
	// carried there too or the audit trail records an anonymous action.
	if _, err := cp.Apply(ctx, store.Command{
		Kind: store.CmdRecordEvent, Event: &e, Actor: e.Actor,
	}); err != nil {
		cp.log.Warn("could not record event", "reason", e.Reason, "kind", e.Kind, "name", e.Name, "err", err)
	}
}

// ClusterStatus is the consensus view surfaced by the API.
func (cp *ControlPlane) ClusterStatus() v1.Cluster {
	st := cp.node.Status()

	members := make([]v1.Member, 0, len(st.Voters))
	for _, id := range st.Voters {
		m := v1.Member{ID: fmt.Sprintf("%d", id), Address: cp.peers[id], Role: "Follower"}
		switch {
		case id == st.Leader:
			m.Role = "Leader"
		case id == cp.nodeID:
			m.Role = st.State.String()
		}
		if id == cp.nodeID {
			m.Reachable = true
		} else if p, ok := st.Progress[id]; ok {
			m.Reachable = p.Active
		}
		members = append(members, m)
	}

	return v1.Cluster{
		Name:          "orion",
		ID:            cp.clusterID,
		ControlPlane:  members,
		LeaderID:      fmt.Sprintf("%d", st.Leader),
		RaftTerm:      st.Term,
		CommitIndex:   st.Commit,
		AppliedIndex:  st.Applied,
		Quorum:        st.QuorumSize,
		QuorumHealthy: st.HasQuorum && st.Leader != raft.None,
		CreatedAt:     cp.startedAt,
	}
}

// AddMember grows the control plane by one replica.
func (cp *ControlPlane) AddMember(ctx context.Context, id uint64, addr string) error {
	return cp.node.ProposeConfChange(ctx, raft.ConfChange{
		Type: raft.ConfChangeAddNode, NodeID: id, Context: []byte(addr),
	})
}

// RemoveMember shrinks the control plane by one replica.
func (cp *ControlPlane) RemoveMember(ctx context.Context, id uint64) error {
	return cp.node.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: id})
}
