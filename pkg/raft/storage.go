package raft

import (
	"sync"
)

// Storage is the durable half of a Raft server. Implementations must make a
// write visible to subsequent reads only after it is durable: the consensus
// safety argument assumes a server never forgets something it acknowledged.
//
// Index semantics: the log holds entries in [FirstIndex, LastIndex]. After a
// snapshot at index S, FirstIndex is S+1 and Term(S) must still be answerable
// because the next AppendEntries consistency check will ask for it.
type Storage interface {
	// InitialState returns the persisted HardState and membership. On a fresh
	// server both are zero values.
	InitialState() (HardState, Configuration, error)

	// SetHardState persists term, vote and commit index durably.
	SetHardState(HardState) error

	// Append persists entries. If entries[0].Index is at or below an existing
	// entry's index, the implementation MUST discard the conflicting suffix
	// first — a follower whose log diverged is required to overwrite it.
	Append([]Entry) error

	// Entries returns entries in [lo, hi).
	Entries(lo, hi uint64) ([]Entry, error)

	// Term returns the term of the entry at i. It must succeed for the
	// snapshot index even though that entry is compacted away.
	Term(i uint64) (uint64, error)

	// FirstIndex is the lowest index available, i.e. snapshotIndex+1.
	FirstIndex() (uint64, error)

	// LastIndex is the highest index in the log.
	LastIndex() (uint64, error)

	// SaveSnapshot persists a snapshot and compacts the log prefix it covers.
	SaveSnapshot(Snapshot) error

	// Snapshot returns the most recent snapshot, or an empty one.
	Snapshot() (Snapshot, error)

	// SetConfiguration persists the current voter set. It is stored separately
	// from the log so a restarting server knows its membership before it has
	// replayed anything.
	SetConfiguration(Configuration) error

	Close() error
}

// MemoryStorage is a Storage kept entirely in RAM. It is used by the
// deterministic simulator and by single-process tests. It is not durable and is
// never used by orion-server, which uses FileStorage.
type MemoryStorage struct {
	mu sync.RWMutex

	hardState HardState
	config    Configuration
	snapshot  Snapshot

	// entries[0] is a sentinel carrying the index and term of the last
	// compacted entry, so Term(snapshotIndex) stays answerable.
	entries []Entry
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{entries: []Entry{{Index: 0, Term: 0}}}
}

func (m *MemoryStorage) InitialState() (HardState, Configuration, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hardState, m.config.Clone(), nil
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hardState = hs
	return nil
}

func (m *MemoryStorage) SetConfiguration(c Configuration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = c.Clone()
	return nil
}

func (m *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	base := m.entries[0].Index
	first := entries[0].Index
	last := entries[len(entries)-1].Index

	// Entirely below the compaction boundary: nothing to do.
	if last < base {
		return nil
	}
	if first < base {
		entries = entries[base-first:]
		first = entries[0].Index
	}

	offset := first - base
	switch {
	case uint64(len(m.entries)) > offset:
		// Overlap: truncate the divergent suffix and rewrite.
		m.entries = append(m.entries[:offset:offset], entries...)
	case uint64(len(m.entries)) == offset:
		m.entries = append(m.entries, entries...)
	default:
		return ErrUnavailable
	}
	return nil
}

func (m *MemoryStorage) Entries(lo, hi uint64) ([]Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	base := m.entries[0].Index
	if lo <= base {
		return nil, ErrCompacted
	}
	if hi > base+uint64(len(m.entries)) {
		return nil, ErrUnavailable
	}
	out := make([]Entry, hi-lo)
	copy(out, m.entries[lo-base:hi-base])
	return out, nil
}

func (m *MemoryStorage) Term(i uint64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	base := m.entries[0].Index
	if i < base {
		return 0, ErrCompacted
	}
	if i-base >= uint64(len(m.entries)) {
		return 0, ErrUnavailable
	}
	return m.entries[i-base].Term, nil
}

func (m *MemoryStorage) FirstIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries[0].Index + 1, nil
}

func (m *MemoryStorage) LastIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entries[0].Index + uint64(len(m.entries)) - 1, nil
}

func (m *MemoryStorage) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.Index <= m.snapshot.Index {
		return ErrSnapshotOutOfDate
	}
	m.snapshot = s
	m.config = s.Config.Clone()

	base := m.entries[0].Index
	if s.Index >= base+uint64(len(m.entries)) {
		// The snapshot is ahead of everything we hold (installed from a
		// leader); reset the log to the snapshot boundary.
		m.entries = []Entry{{Index: s.Index, Term: s.Term}}
		return nil
	}
	// Compact the prefix, keeping a sentinel at the snapshot index.
	keep := m.entries[s.Index-base:]
	compacted := make([]Entry, len(keep))
	copy(compacted, keep)
	compacted[0] = Entry{Index: s.Index, Term: s.Term}
	m.entries = compacted
	return nil
}

func (m *MemoryStorage) Snapshot() (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshot, nil
}

func (m *MemoryStorage) Close() error { return nil }
