package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func openTestStorage(t *testing.T, dir string) *FileStorage {
	t.Helper()
	fs, err := OpenFileStorage(dir, FileStorageOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	t.Cleanup(func() { fs.Close() })
	return fs
}

func entries(from, to uint64, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Type: EntryNormal, Data: []byte{byte(i)}})
	}
	return out
}

func TestFileStorageSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)

	if err := fs.Append(entries(1, 100, 3)); err != nil {
		t.Fatalf("append: %v", err)
	}
	hs := HardState{Term: 3, VoteFor: 7, Commit: 100}
	if err := fs.SetHardState(hs); err != nil {
		t.Fatalf("set hard state: %v", err)
	}
	if err := fs.SetConfiguration(Configuration{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatalf("set configuration: %v", err)
	}
	fs.Close()

	reopened := openTestStorage(t, dir)
	gotHS, gotConf, err := reopened.InitialState()
	if err != nil {
		t.Fatalf("initial state: %v", err)
	}
	if gotHS != hs {
		t.Errorf("hard state not durable: got %+v want %+v", gotHS, hs)
	}
	if len(gotConf.Voters) != 3 {
		t.Errorf("configuration not durable: %v", gotConf.Voters)
	}
	last, _ := reopened.LastIndex()
	if last != 100 {
		t.Fatalf("last index after reopen = %d, want 100", last)
	}
	got, err := reopened.Entries(1, 101)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	for i, e := range got {
		if e.Index != uint64(i+1) || e.Term != 3 {
			t.Fatalf("entry %d corrupted after reopen: %+v", i, e)
		}
	}
}

// A power loss mid-append leaves a partial record. Replay must discard exactly
// that record and keep every complete one before it.
func TestFileStorageTruncatesTornTailOnRecovery(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)
	if err := fs.Append(entries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segments) != 1 {
		t.Fatalf("expected one segment, got %v", segments)
	}
	info, err := os.Stat(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	// Chop the file mid-record: the last entry becomes unreadable.
	if err := os.Truncate(segments[0], info.Size()-8); err != nil {
		t.Fatal(err)
	}

	reopened := openTestStorage(t, dir)
	last, _ := reopened.LastIndex()
	if last != 9 {
		t.Fatalf("expected the torn record to be dropped leaving last index 9, got %d", last)
	}
	// The log must be usable again: appending from the truncation point works.
	if err := reopened.Append(entries(10, 12, 2)); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	last, _ = reopened.LastIndex()
	if last != 12 {
		t.Fatalf("last index after recovery append = %d, want 12", last)
	}
}

// A corrupt record body (bit flip, not truncation) must be caught by the
// checksum and not replayed as data.
func TestFileStorageDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)
	if err := fs.Append(entries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	data, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit in the payload of the last record.
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(segments[0], data, 0o640); err != nil {
		t.Fatal(err)
	}

	reopened := openTestStorage(t, dir)
	last, _ := reopened.LastIndex()
	if last != 4 {
		t.Fatalf("corrupt record was replayed: last index %d, want 4", last)
	}
}

// Raft requires a follower to overwrite a divergent suffix. On an append-only
// log this means the later record wins on replay.
func TestFileStorageOverwritesDivergentSuffix(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)
	if err := fs.Append(entries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	// A new leader in term 2 rewrites from index 3.
	if err := fs.Append([]Entry{
		{Index: 3, Term: 2, Data: []byte("x")},
		{Index: 4, Term: 2, Data: []byte("y")},
	}); err != nil {
		t.Fatal(err)
	}

	check := func(s Storage, label string) {
		last, _ := s.LastIndex()
		if last != 4 {
			t.Fatalf("%s: last index = %d, want 4", label, last)
		}
		got, err := s.Entries(3, 5)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if got[0].Term != 2 || string(got[0].Data) != "x" || got[1].Term != 2 {
			t.Fatalf("%s: divergent suffix not overwritten: %+v", label, got)
		}
	}
	check(fs, "in memory")
	fs.Close()
	check(openTestStorage(t, dir), "after replay")
}

func TestFileStorageSnapshotCompactsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)
	if err := fs.Append(entries(1, 200, 1)); err != nil {
		t.Fatal(err)
	}
	if err := fs.SetHardState(HardState{Term: 1, Commit: 200}); err != nil {
		t.Fatal(err)
	}

	snap := Snapshot{Index: 150, Term: 1, Config: Configuration{Voters: []uint64{1, 2, 3}}, Data: []byte("state")}
	if err := fs.SaveSnapshot(snap); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if _, err := fs.Entries(1, 10); err != ErrCompacted {
		t.Errorf("expected compacted entries to be unavailable, got %v", err)
	}
	// The snapshot boundary term must remain answerable for the next
	// AppendEntries consistency check.
	if term, err := fs.Term(150); err != nil || term != 1 {
		t.Errorf("Term(150) = %d, %v; want 1, nil", term, err)
	}
	if first, _ := fs.FirstIndex(); first != 151 {
		t.Errorf("FirstIndex = %d, want 151", first)
	}
	fs.Close()

	reopened := openTestStorage(t, dir)
	got, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 150 || string(got.Data) != "state" || len(got.Config.Voters) != 3 {
		t.Fatalf("snapshot not recovered: %+v", got)
	}
	if last, _ := reopened.LastIndex(); last != 200 {
		t.Errorf("entries after the snapshot were lost: last index %d, want 200", last)
	}
	if _, err := reopened.Entries(151, 201); err != nil {
		t.Errorf("post-snapshot entries unreadable: %v", err)
	}
}

func TestFileStorageRejectsStaleSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir)
	if err := fs.Append(entries(1, 50, 1)); err != nil {
		t.Fatal(err)
	}
	if err := fs.SaveSnapshot(Snapshot{Index: 40, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := fs.SaveSnapshot(Snapshot{Index: 30, Term: 1}); err != ErrSnapshotOutOfDate {
		t.Fatalf("expected ErrSnapshotOutOfDate, got %v", err)
	}
}

func TestFileStorageRotatesSegments(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStorage(t, dir) // 4 KiB segments

	for i := uint64(1); i <= 500; i++ {
		if err := fs.Append([]Entry{{Index: i, Term: 1, Data: make([]byte, 64)}}); err != nil {
			t.Fatal(err)
		}
	}
	segments, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(segments) < 2 {
		t.Fatalf("expected the log to rotate into multiple segments, got %d", len(segments))
	}
	fs.Close()

	reopened := openTestStorage(t, dir)
	if last, _ := reopened.LastIndex(); last != 500 {
		t.Fatalf("last index across segments = %d, want 500", last)
	}
}

func BenchmarkFileStorageAppendSync(b *testing.B) {
	dir := b.TempDir()
	fs, err := OpenFileStorage(dir, FileStorageOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()
	entry := Entry{Term: 1, Type: EntryNormal, Data: make([]byte, 256)}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry.Index = uint64(i + 1)
		if err := fs.Append([]Entry{entry}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStorageAppendBatch(b *testing.B) {
	dir := b.TempDir()
	fs, err := OpenFileStorage(dir, FileStorageOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()

	const batch = 64
	b.ResetTimer()
	b.ReportAllocs()
	idx := uint64(0)
	for i := 0; i < b.N; i++ {
		es := make([]Entry, batch)
		for j := range es {
			idx++
			es[j] = Entry{Index: idx, Term: 1, Type: EntryNormal, Data: make([]byte, 256)}
		}
		if err := fs.Append(es); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "entries/s")
}
