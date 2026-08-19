package raft

import "fmt"

// raftLog is the view of the replicated log used by the core state machine. It
// stitches together entries already handed to stable storage and entries that
// are still in memory awaiting persistence.
//
// Indices are 1-based and contiguous. Index 0 is the sentinel meaning "before
// the first entry"; its term is 0.
type raftLog struct {
	storage Storage

	// unstable holds entries appended by the core but not yet persisted by the
	// caller. unstableOffset is the log index of unstable[0].
	unstable       []Entry
	unstableOffset uint64

	// pendingSnapshot is a snapshot received from a leader that the caller has
	// not yet persisted.
	pendingSnapshot *Snapshot

	// committed is the highest index known to be replicated on a majority.
	committed uint64
	// applied is the highest index handed to the application state machine.
	applied uint64
}

func newRaftLog(storage Storage) (*raftLog, error) {
	first, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	l := &raftLog{
		storage:        storage,
		unstableOffset: last + 1,
	}
	// Everything already on disk at or below the snapshot boundary is applied
	// by definition; the caller restores the state machine from the snapshot.
	l.committed = first - 1
	l.applied = first - 1
	return l, nil
}

func (l *raftLog) firstIndex() uint64 {
	if !l.pendingSnapshot.IsEmpty() {
		return l.pendingSnapshot.Index + 1
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(fmt.Sprintf("raft: storage.FirstIndex failed: %v", err))
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if n := len(l.unstable); n > 0 {
		return l.unstableOffset + uint64(n) - 1
	}
	if !l.pendingSnapshot.IsEmpty() {
		return l.pendingSnapshot.Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("raft: storage.LastIndex failed: %v", err))
	}
	return i
}

// term returns the term of the entry at index i, or ErrCompacted/ErrUnavailable.
func (l *raftLog) term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	if i > l.lastIndex() {
		return 0, ErrUnavailable
	}
	if n := len(l.unstable); n > 0 && i >= l.unstableOffset {
		return l.unstable[i-l.unstableOffset].Term, nil
	}
	if !l.pendingSnapshot.IsEmpty() && i == l.pendingSnapshot.Index {
		return l.pendingSnapshot.Term, nil
	}
	return l.storage.Term(i)
}

// mustTerm is term() for indices the caller has already bounds-checked. A
// failure here means the log invariants are broken, which must not be silently
// tolerated.
func (l *raftLog) mustTerm(i uint64) uint64 {
	t, err := l.term(i)
	if err != nil {
		panic(fmt.Sprintf("raft: term(%d) failed on a log with first=%d last=%d: %v",
			i, l.firstIndex(), l.lastIndex(), err))
	}
	return t
}

func (l *raftLog) lastTerm() uint64 { return l.mustTerm(l.lastIndex()) }

// slice returns entries in [lo, hi). Callers must keep the range within
// [firstIndex, lastIndex+1).
func (l *raftLog) slice(lo, hi uint64) ([]Entry, error) {
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid slice %d > %d", lo, hi)
	}
	if lo < l.firstIndex() {
		return nil, ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}

	var out []Entry
	// Portion still on stable storage.
	if lo < l.unstableOffset {
		stableHi := min(hi, l.unstableOffset)
		ents, err := l.storage.Entries(lo, stableHi)
		if err != nil {
			return nil, err
		}
		out = append(out, ents...)
	}
	// Portion still in memory.
	if hi > l.unstableOffset {
		unstableLo := max(lo, l.unstableOffset)
		out = append(out, l.unstable[unstableLo-l.unstableOffset:hi-l.unstableOffset]...)
	}
	return out, nil
}

// matchesTerm implements the Log Matching consistency check: does this log
// contain an entry at index i whose term is term?
func (l *raftLog) matchesTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

// isUpToDate implements the election restriction from paper §5.4.1: a candidate
// may only win if its log is at least as up to date as the voter's, comparing
// last term first and length second.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= l.lastIndex()
}

// append adds entries authored locally by a leader and returns the new last
// index.
func (l *raftLog) append(entries ...Entry) uint64 {
	if len(entries) == 0 {
		return l.lastIndex()
	}
	if entries[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: non-contiguous append at %d, log last index is %d",
			entries[0].Index, l.lastIndex()))
	}
	if len(l.unstable) == 0 {
		l.unstableOffset = entries[0].Index
	}
	l.unstable = append(l.unstable, entries...)
	return l.lastIndex()
}

// maybeAppend applies a leader's AppendEntries payload. It returns the index of
// the last new entry and true on success, or false if the consistency check at
// (prevIndex, prevTerm) failed.
//
// Entries already present with a matching term are skipped rather than
// rewritten; only a genuine conflict truncates the log. This preserves the
// Leader Append-Only property and, just as importantly, keeps a duplicated or
// reordered AppendEntries from destroying entries the follower already has.
func (l *raftLog) maybeAppend(prevIndex, prevTerm, leaderCommit uint64, entries []Entry) (uint64, bool) {
	if !l.matchesTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew := prevIndex + uint64(len(entries))

	conflict := l.findConflict(entries)
	switch {
	case conflict == 0:
		// Every entry is already present with a matching term: a duplicate.
	case conflict <= l.committed:
		panic(fmt.Sprintf("raft: leader conflicts with committed entry at index %d (committed=%d)",
			conflict, l.committed))
	default:
		l.truncateAndAppend(entries[conflict-(prevIndex+1):])
	}

	// A follower's commit index can never exceed the last entry it actually
	// holds, even if the leader's commit index is higher, because the leader
	// may have committed entries this follower has not yet received.
	l.commitTo(min(leaderCommit, lastNew))
	return lastNew, true
}

// findConflict returns the index of the first entry that conflicts with the
// local log, or 0 if there is no conflict. An entry conflicts when it has the
// same index but a different term; entries past the end of the log are
// "conflicting" in the sense that they must be appended.
func (l *raftLog) findConflict(entries []Entry) uint64 {
	for _, e := range entries {
		if !l.matchesTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

func (l *raftLog) truncateAndAppend(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	switch {
	case first == l.unstableOffset+uint64(len(l.unstable)):
		l.unstable = append(l.unstable, entries...)
	case first <= l.unstableOffset:
		// The conflict reaches into entries already persisted; the caller's
		// storage Append is responsible for truncating on disk when it sees a
		// rewind.
		l.unstableOffset = first
		l.unstable = append([]Entry(nil), entries...)
	default:
		keep := l.unstable[:first-l.unstableOffset]
		l.unstable = append(append([]Entry(nil), keep...), entries...)
	}
}

func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit index %d is beyond last index %d", i, l.lastIndex()))
		}
		l.committed = i
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if i > l.committed || i < l.applied {
		panic(fmt.Sprintf("raft: applied index %d out of range (applied=%d committed=%d)",
			i, l.applied, l.committed))
	}
	l.applied = i
}

// nextCommittedEntries returns entries that are committed but not yet applied.
func (l *raftLog) nextCommittedEntries(maxEntries int) []Entry {
	lo := max(l.applied+1, l.firstIndex())
	if l.committed < lo {
		return nil
	}
	hi := l.committed + 1
	if maxEntries > 0 && hi-lo > uint64(maxEntries) {
		hi = lo + uint64(maxEntries)
	}
	ents, err := l.slice(lo, hi)
	if err != nil {
		panic(fmt.Sprintf("raft: unexpected error fetching committed entries [%d,%d): %v", lo, hi, err))
	}
	return ents
}

// unstableEntries returns entries the caller must persist.
func (l *raftLog) unstableEntries() []Entry { return l.unstable }

// stableTo records that entries up to index i (with matching term) reached
// stable storage, so they can be dropped from the in-memory buffer.
func (l *raftLog) stableTo(i, term uint64) {
	if len(l.unstable) == 0 || i < l.unstableOffset {
		return
	}
	// The term check guards against a stale Advance: if the log was truncated
	// and rewritten while a persist was in flight, the entry at i is no longer
	// the one that was persisted and must not be discarded.
	if i > l.lastIndex() || l.mustTerm(i) != term {
		return
	}
	n := i - l.unstableOffset + 1
	l.unstable = l.unstable[n:]
	l.unstableOffset = i + 1
	if len(l.unstable) == 0 {
		l.unstable = nil
	}
}

// restore resets the log to a snapshot received from a leader.
func (l *raftLog) restore(s *Snapshot) {
	l.pendingSnapshot = s
	l.unstable = nil
	l.unstableOffset = s.Index + 1
	l.committed = s.Index
	l.applied = s.Index
}

// snapshotPersisted clears the pending snapshot once the caller has written it
// to storage.
func (l *raftLog) snapshotPersisted(index uint64) {
	if !l.pendingSnapshot.IsEmpty() && l.pendingSnapshot.Index == index {
		l.pendingSnapshot = nil
	}
}
