package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FileStorage is a crash-safe, append-only implementation of Storage.
//
// Layout inside the data directory:
//
//	wal-<firstIndex>.log     write-ahead log segments, rotated by size
//	snapshot-<index>-<term>  state machine snapshots, newest wins
//
// Record framing is [crc32c u32][type u8][length u32][payload]. Every record is
// checksummed, so a torn write at the tail of the log — the normal outcome of a
// power loss mid-append — is detected on replay and truncated rather than
// silently interpreted as data. Records before the torn one are unaffected,
// which is the entire reason for an append-only design.
//
// Log entries are also held in memory for reads. The disk is the authority on
// restart; memory is the authority while running.
type FileStorage struct {
	mu  sync.RWMutex
	dir string

	// segment is the currently open WAL segment.
	segment     *os.File
	segmentPath string
	segmentSize int64

	// entries[0] is a sentinel holding the index/term of the last compacted
	// entry so Term(snapshotIndex) remains answerable.
	entries   []Entry
	hardState HardState
	config    Configuration
	snapshot  Snapshot

	// syncWrites controls whether each batch is fsynced. It is true in
	// production. Benchmarks that measure consensus throughput without disk
	// latency set it false and say so in their methodology.
	syncWrites bool

	maxSegmentBytes int64
}

var _ Storage = (*FileStorage)(nil)

type recordType uint8

const (
	recEntry recordType = iota + 1
	recHardState
	recConfig
)

const (
	recordHeaderSize   = 9 // crc32 + type + length
	defaultSegmentSize = 64 << 20
	// maxRecordSize bounds a single record so a corrupt length field cannot
	// make replay allocate an arbitrary amount of memory.
	maxRecordSize = 64 << 20
)

// FileStorageOptions configures durability behaviour.
type FileStorageOptions struct {
	// NoSync disables fsync. Only for benchmarks and tests; a crash will lose
	// acknowledged writes.
	NoSync bool
	// MaxSegmentBytes overrides the segment rotation threshold.
	MaxSegmentBytes int64
}

// OpenFileStorage opens or creates a Raft data directory and replays it.
func OpenFileStorage(dir string, opts FileStorageOptions) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("raft: creating data directory: %w", err)
	}
	fs := &FileStorage{
		dir:             dir,
		entries:         []Entry{{Index: 0, Term: 0}},
		syncWrites:      !opts.NoSync,
		maxSegmentBytes: opts.MaxSegmentBytes,
	}
	if fs.maxSegmentBytes <= 0 {
		fs.maxSegmentBytes = defaultSegmentSize
	}
	if err := fs.recover(); err != nil {
		return nil, err
	}
	return fs, nil
}

// recover loads the newest valid snapshot, then replays WAL segments on top.
func (fs *FileStorage) recover() error {
	if err := fs.loadSnapshot(); err != nil {
		return err
	}
	if fs.snapshot.Index > 0 {
		fs.entries = []Entry{{Index: fs.snapshot.Index, Term: fs.snapshot.Term}}
		fs.config = fs.snapshot.Config.Clone()
	}

	segments, err := fs.walSegments()
	if err != nil {
		return err
	}
	for _, path := range segments {
		if err := fs.replaySegment(path); err != nil {
			return err
		}
	}
	return fs.openSegmentForAppend()
}

func (fs *FileStorage) walSegments() ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(fs.dir, "wal-*.log"))
	if err != nil {
		return nil, err
	}
	type seg struct {
		path  string
		start uint64
	}
	segs := make([]seg, 0, len(matches))
	for _, m := range matches {
		base := strings.TrimSuffix(filepath.Base(m), ".log")
		n, err := strconv.ParseUint(strings.TrimPrefix(base, "wal-"), 10, 64)
		if err != nil {
			// An unparseable name is not ours; leaving it alone is safer than
			// guessing.
			continue
		}
		segs = append(segs, seg{path: m, start: n})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].start < segs[j].start })
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = s.path
	}
	return out, nil
}

// replaySegment applies every intact record in a segment. On encountering a
// corrupt or truncated record it truncates the file at that offset and stops:
// anything after a torn write cannot be trusted to be contiguous.
func (fs *FileStorage) replaySegment(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("raft: reading %s: %w", path, err)
	}

	offset := 0
	for offset+recordHeaderSize <= len(data) {
		header := data[offset : offset+recordHeaderSize]
		want := binary.LittleEndian.Uint32(header[0:4])
		rt := recordType(header[4])
		length := int(binary.LittleEndian.Uint32(header[5:9]))

		if length < 0 || length > maxRecordSize || offset+recordHeaderSize+length > len(data) {
			break // truncated tail
		}
		payload := data[offset+recordHeaderSize : offset+recordHeaderSize+length]
		if checksum(payload) != want {
			break // torn or corrupt record
		}

		if err := fs.applyRecord(rt, payload); err != nil {
			return fmt.Errorf("raft: replaying %s at offset %d: %w", path, offset, err)
		}
		offset += recordHeaderSize + length
	}

	if offset < len(data) {
		// Drop the unreadable tail so future appends start from a clean
		// boundary. This is the expected outcome of a crash mid-write.
		if err := os.Truncate(path, int64(offset)); err != nil {
			return fmt.Errorf("raft: truncating torn tail of %s: %w", path, err)
		}
	}
	return nil
}

func (fs *FileStorage) applyRecord(rt recordType, payload []byte) error {
	switch rt {
	case recEntry:
		e, _, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		fs.appendInMemory([]Entry{e})
	case recHardState:
		if len(payload) < 24 {
			return io.ErrUnexpectedEOF
		}
		fs.hardState = HardState{
			Term:    binary.LittleEndian.Uint64(payload[0:8]),
			VoteFor: binary.LittleEndian.Uint64(payload[8:16]),
			Commit:  binary.LittleEndian.Uint64(payload[16:24]),
		}
	case recConfig:
		c, _, err := decodeConfiguration(payload)
		if err != nil {
			return err
		}
		fs.config = c
	default:
		return fmt.Errorf("unknown record type %d", rt)
	}
	return nil
}

func (fs *FileStorage) openSegmentForAppend() error {
	segments, err := fs.walSegments()
	if err != nil {
		return err
	}
	var path string
	if len(segments) == 0 {
		first := fs.entries[0].Index + 1
		path = filepath.Join(fs.dir, fmt.Sprintf("wal-%020d.log", first))
	} else {
		path = segments[len(segments)-1]
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("raft: opening wal segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	fs.segment = f
	fs.segmentPath = path
	fs.segmentSize = info.Size()
	return nil
}

func (fs *FileStorage) writeRecords(records []struct {
	rt      recordType
	payload []byte
}) error {
	if fs.segment == nil {
		return errors.New("raft: storage is closed")
	}
	buf := make([]byte, 0, 4096)
	for _, r := range records {
		buf = binary.LittleEndian.AppendUint32(buf, checksum(r.payload))
		buf = append(buf, byte(r.rt))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.payload)))
		buf = append(buf, r.payload...)
	}
	n, err := fs.segment.Write(buf)
	fs.segmentSize += int64(n)
	if err != nil {
		return fmt.Errorf("raft: writing wal: %w", err)
	}
	if fs.syncWrites {
		if err := fs.segment.Sync(); err != nil {
			return fmt.Errorf("raft: fsync wal: %w", err)
		}
	}
	return fs.maybeRotate()
}

func (fs *FileStorage) maybeRotate() error {
	if fs.segmentSize < fs.maxSegmentBytes {
		return nil
	}
	if err := fs.segment.Close(); err != nil {
		return err
	}
	next := fs.lastIndexLocked() + 1
	path := filepath.Join(fs.dir, fmt.Sprintf("wal-%020d.log", next))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("raft: rotating wal segment: %w", err)
	}
	fs.segment = f
	fs.segmentPath = path
	fs.segmentSize = 0
	// A fresh segment must be able to stand alone after older segments are
	// deleted by compaction, so restate the durable metadata immediately.
	return fs.writeMetaRecords()
}

func (fs *FileStorage) writeMetaRecords() error {
	hs := make([]byte, 0, 24)
	hs = binary.LittleEndian.AppendUint64(hs, fs.hardState.Term)
	hs = binary.LittleEndian.AppendUint64(hs, fs.hardState.VoteFor)
	hs = binary.LittleEndian.AppendUint64(hs, fs.hardState.Commit)
	return fs.writeRecords([]struct {
		rt      recordType
		payload []byte
	}{
		{recHardState, hs},
		{recConfig, encodeConfiguration(fs.config)},
	})
}

// ---------------------------------------------------------------------------
// Storage interface
// ---------------------------------------------------------------------------

func (fs *FileStorage) InitialState() (HardState, Configuration, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.hardState, fs.config.Clone(), nil
}

func (fs *FileStorage) SetHardState(hs HardState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	payload := make([]byte, 0, 24)
	payload = binary.LittleEndian.AppendUint64(payload, hs.Term)
	payload = binary.LittleEndian.AppendUint64(payload, hs.VoteFor)
	payload = binary.LittleEndian.AppendUint64(payload, hs.Commit)
	if err := fs.writeRecords([]struct {
		rt      recordType
		payload []byte
	}{{recHardState, payload}}); err != nil {
		return err
	}
	fs.hardState = hs
	return nil
}

func (fs *FileStorage) SetConfiguration(c Configuration) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fs.writeRecords([]struct {
		rt      recordType
		payload []byte
	}{{recConfig, encodeConfiguration(c)}}); err != nil {
		return err
	}
	fs.config = c.Clone()
	return nil
}

func (fs *FileStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()

	records := make([]struct {
		rt      recordType
		payload []byte
	}, 0, len(entries))
	for _, e := range entries {
		records = append(records, struct {
			rt      recordType
			payload []byte
		}{recEntry, appendEntry(nil, e)})
	}
	if err := fs.writeRecords(records); err != nil {
		return err
	}
	fs.appendInMemory(entries)
	return nil
}

// appendInMemory applies Raft's overwrite semantics to the in-memory view. A
// record for index i that arrives after an existing index i replaces it and
// discards everything after, which is exactly what replay must reproduce.
func (fs *FileStorage) appendInMemory(entries []Entry) {
	base := fs.entries[0].Index
	for _, e := range entries {
		if e.Index <= base {
			continue // already compacted
		}
		offset := e.Index - base
		switch {
		case offset < uint64(len(fs.entries)):
			fs.entries = append(fs.entries[:offset:offset], e)
		case offset == uint64(len(fs.entries)):
			fs.entries = append(fs.entries, e)
		default:
			// A gap would break the contiguity invariant. This can only happen
			// if the log was corrupted in a way CRCs did not catch, so failing
			// loudly is the only safe response.
			panic(fmt.Sprintf("raft: non-contiguous entry %d, log ends at %d",
				e.Index, base+uint64(len(fs.entries))-1))
		}
	}
}

func (fs *FileStorage) Entries(lo, hi uint64) ([]Entry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	base := fs.entries[0].Index
	if lo <= base {
		return nil, ErrCompacted
	}
	if hi > base+uint64(len(fs.entries)) {
		return nil, ErrUnavailable
	}
	out := make([]Entry, hi-lo)
	copy(out, fs.entries[lo-base:hi-base])
	return out, nil
}

func (fs *FileStorage) Term(i uint64) (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	base := fs.entries[0].Index
	if i < base {
		return 0, ErrCompacted
	}
	if i-base >= uint64(len(fs.entries)) {
		return 0, ErrUnavailable
	}
	return fs.entries[i-base].Term, nil
}

func (fs *FileStorage) FirstIndex() (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.entries[0].Index + 1, nil
}

func (fs *FileStorage) LastIndex() (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.lastIndexLocked(), nil
}

func (fs *FileStorage) lastIndexLocked() uint64 {
	return fs.entries[0].Index + uint64(len(fs.entries)) - 1
}

// SaveSnapshot writes a snapshot atomically (temp file + rename + directory
// fsync) and then removes WAL segments the snapshot has superseded. The order
// matters: the snapshot must be durable before any log is discarded.
func (fs *FileStorage) SaveSnapshot(s Snapshot) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if s.Index <= fs.snapshot.Index {
		return ErrSnapshotOutOfDate
	}

	meta := encodeSnapshotMeta(s)
	payload := make([]byte, 0, len(meta)+len(s.Data)+8)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(meta)))
	payload = append(payload, meta...)
	payload = append(payload, s.Data...)

	body := binary.LittleEndian.AppendUint32(nil, checksum(payload))
	body = append(body, payload...)

	name := fmt.Sprintf("snapshot-%020d-%020d.snap", s.Index, s.Term)
	final := filepath.Join(fs.dir, name)
	tmp := final + ".tmp"

	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		return fmt.Errorf("raft: writing snapshot: %w", err)
	}
	if fs.syncWrites {
		f, err := os.Open(tmp)
		if err == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("raft: installing snapshot: %w", err)
	}
	if fs.syncWrites {
		if d, err := os.Open(fs.dir); err == nil {
			_ = d.Sync()
			_ = d.Close()
		}
	}

	fs.snapshot = s
	fs.config = s.Config.Clone()
	fs.compactLocked(s.Index, s.Term)
	return fs.pruneSegmentsLocked(s.Index)
}

func (fs *FileStorage) compactLocked(index, term uint64) {
	base := fs.entries[0].Index
	if index >= base+uint64(len(fs.entries)) {
		fs.entries = []Entry{{Index: index, Term: term}}
		return
	}
	if index <= base {
		return
	}
	keep := fs.entries[index-base:]
	compacted := make([]Entry, len(keep))
	copy(compacted, keep)
	compacted[0] = Entry{Index: index, Term: term}
	fs.entries = compacted
}

// pruneSegmentsLocked deletes WAL segments entirely superseded by a snapshot.
// A segment is only removed when the segment that follows it starts at or below
// the snapshot index, so the segment covering the boundary is always kept.
func (fs *FileStorage) pruneSegmentsLocked(snapshotIndex uint64) error {
	segments, err := fs.walSegments()
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(segments); i++ {
		nextStart, err := segmentStart(segments[i+1])
		if err != nil {
			continue
		}
		if nextStart > snapshotIndex+1 {
			break
		}
		if segments[i] == fs.segmentPath {
			continue
		}
		if err := os.Remove(segments[i]); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("raft: pruning wal segment: %w", err)
		}
	}
	// Older snapshots are no longer needed once a newer one is durable.
	snaps, _ := filepath.Glob(filepath.Join(fs.dir, "snapshot-*.snap"))
	for _, p := range snaps {
		idx, _, err := snapshotIndexTerm(p)
		if err == nil && idx < snapshotIndex {
			_ = os.Remove(p)
		}
	}
	return nil
}

func segmentStart(path string) (uint64, error) {
	base := strings.TrimSuffix(filepath.Base(path), ".log")
	return strconv.ParseUint(strings.TrimPrefix(base, "wal-"), 10, 64)
}

func snapshotIndexTerm(path string) (uint64, uint64, error) {
	base := strings.TrimSuffix(filepath.Base(path), ".snap")
	parts := strings.Split(strings.TrimPrefix(base, "snapshot-"), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed snapshot name %q", path)
	}
	idx, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	term, err := strconv.ParseUint(parts[1], 10, 64)
	return idx, term, err
}

func (fs *FileStorage) Snapshot() (Snapshot, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.snapshot, nil
}

// loadSnapshot picks the highest-indexed snapshot whose checksum verifies,
// falling back to older ones. A snapshot that fails verification is left on
// disk for forensics rather than deleted.
func (fs *FileStorage) loadSnapshot() error {
	paths, err := filepath.Glob(filepath.Join(fs.dir, "snapshot-*.snap"))
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, p := range paths {
		s, err := readSnapshotFile(p)
		if err != nil {
			continue
		}
		fs.snapshot = s
		return nil
	}
	// Clean up any interrupted snapshot writes.
	tmps, _ := filepath.Glob(filepath.Join(fs.dir, "snapshot-*.snap.tmp"))
	for _, p := range tmps {
		_ = os.Remove(p)
	}
	return nil
}

func readSnapshotFile(path string) (Snapshot, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	if len(body) < 8 {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	want := binary.LittleEndian.Uint32(body[0:4])
	payload := body[4:]
	if checksum(payload) != want {
		return Snapshot{}, fmt.Errorf("raft: snapshot %s failed checksum verification", path)
	}
	metaLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	if 4+metaLen > len(payload) {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	s, _, err := decodeSnapshotMeta(payload[4 : 4+metaLen])
	if err != nil {
		return Snapshot{}, err
	}
	s.Data = append([]byte(nil), payload[4+metaLen:]...)
	return s, nil
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.segment == nil {
		return nil
	}
	err := fs.segment.Close()
	fs.segment = nil
	return err
}
