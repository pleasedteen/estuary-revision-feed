// Package store holds unit tests for sequence allocation and downstream
// read-position semantics.
package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"estuary-collector/internal/domain"
)

func sampleChanges(prefix string, n int) []domain.Change {
	out := make([]domain.Change, 0, n)
	for i := 0; i < n; i++ {
		id := prefix + string(rune('a'+i))
		out = append(out, domain.Change{
			Op: domain.OpUpsert, Institution: "alpha", SampleID: id, SampledOn: "2026-11-10", Revision: 1,
			Record: &domain.CanonicalRecord{Institution: "alpha", SampleID: id, Revision: 1, SampledOn: "2026-11-10", Unit: "mg/L", MgPerL: "1"},
		})
	}
	return out
}

func TestPublishAndReadAfter(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	changes := []domain.Change{
		{Op: domain.OpUpsert, Institution: "alpha", SampleID: "A-1", SampledOn: "2026-11-10", Revision: 1,
			Record: &domain.CanonicalRecord{Institution: "alpha", SampleID: "A-1", Revision: 1, SampledOn: "2026-11-10", Unit: "mg/L", MgPerL: "1"}},
		{Op: domain.OpWithdraw, Institution: "alpha", SampleID: "A-2", SampledOn: "2026-11-10", Revision: 2},
		{Op: domain.OpUpsert, Institution: "alpha", SampleID: "A-3", SampledOn: "2026-11-11", Revision: 1,
			Record: &domain.CanonicalRecord{Institution: "alpha", SampleID: "A-3", Revision: 1, SampledOn: "2026-11-11", Unit: "mg/L", MgPerL: "2"}},
	}
	env, err := st.Publish("batch-1", changes)
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 1 || len(env.Changes) != 3 || env.Changes[2].Seq != 3 {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if st.MaxSeq() != 3 {
		t.Fatalf("max seq = %d", st.MaxSeq())
	}

	// Empty publish is a no-op.
	if _, err := st.Publish("empty", nil); err != nil {
		t.Fatal(err)
	}

	// Read from the middle: only later sequences come back and the envelope
	// is trimmed to the changes past the saved position.
	page, err := st.ReadAfter(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 {
		t.Fatalf("expected one envelope, got %d", len(page))
	}
	if len(page[0].Changes) != 2 || page[0].Changes[0].Seq != 2 {
		t.Fatalf("unexpected trimmed page: %+v", page[0])
	}
	if _, err := st.ReadAfter(st.MaxSeq(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestReopenRebuildsSequence(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Publish("b", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s", SampledOn: "d", Revision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(filepath.Join(dir))
	if err != nil {
		t.Fatal(err)
	}
	if st2.MaxSeq() != 1 {
		t.Fatalf("rebuilt max seq = %d, want 1", st2.MaxSeq())
	}
	env, err := st2.Publish("b2", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s2", SampledOn: "d", Revision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 2 {
		t.Fatalf("sequence did not continue across reopen: %d", env.Seq)
	}
}

// TestFailedAppendRollsBack verifies that a failed or short append never
// leaves a visible partial batch and that a retry appends a clean line.
func TestFailedAppendRollsBack(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.Publish("b1", sampleChanges("x", 2))
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || st.MaxSeq() != 2 {
		t.Fatalf("unexpected first publish: %+v max=%d", first, st.MaxSeq())
	}

	// A short write must be reported as an error and leave no fragment.
	st.SetWriteFault(func(line []byte) (int, error) { return len(line) / 2, nil })
	if _, err := st.Publish("b2", sampleChanges("y", 2)); err == nil {
		t.Fatal("short write should surface as an error")
	}
	// A hard failure must also be reported.
	st.SetWriteFault(func([]byte) (int, error) { return 0, errors.New("disk full") })
	if _, err := st.Publish("b2", sampleChanges("y", 2)); err == nil {
		t.Fatal("injected write error should surface")
	}
	// Sequence is unchanged: the failed batches never existed.
	if st.MaxSeq() != 2 {
		t.Fatalf("failed appends advanced the sequence to %d", st.MaxSeq())
	}

	// The file must still be clean JSONL and readable end to end.
	envs, err := st.ReadAfter(0, 0)
	if err != nil {
		t.Fatalf("readable after failed appends: %v", err)
	}
	if len(envs) != 1 || len(envs[0].Changes) != 2 {
		t.Fatalf("expected exactly the first batch, got %+v", envs)
	}

	// Lifting the fault lets a retry append a full line that parses cleanly.
	st.SetWriteFault(nil)
	third, err := st.Publish("b3", sampleChanges("z", 1))
	if err != nil {
		t.Fatal(err)
	}
	if third.Seq != 3 {
		t.Fatalf("retry should continue at seq 3, got %d", third.Seq)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	envs, err = st2.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 2 || st2.MaxSeq() != 3 {
		t.Fatalf("reopened state wrong: envs=%d max=%d", len(envs), st2.MaxSeq())
	}
}

// TestReopenTruncatesIncompleteTail models a crash in the middle of an
// append: bytes reached the file without their trailing newline. Earlier
// complete batches stay readable and the fragment is discarded.
func TestReopenTruncatesIncompleteTail(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish("b1", sampleChanges("x", 2)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	active := filepath.Join(dir, activeName)
	// Simulate a half-written second batch: no newline, invalid JSON on its
	// own, exactly what a crash mid-Sync can leave behind.
	frag := []byte(`{"seq":3,"batch_id":"partial","changes":[{"seq":3,"op":"ups`)
	if err := os.WriteFile(active, append(mustRead(t, active), frag...), 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with incomplete tail: %v", err)
	}
	if st2.MaxSeq() != 2 {
		t.Fatalf("incomplete tail advanced max seq to %d", st2.MaxSeq())
	}
	envs, err := st2.ReadAfter(0, 0)
	if err != nil {
		t.Fatalf("completed batch must stay readable: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("expected one complete batch, got %d", len(envs))
	}
	// The file no longer contains the fragment, so the next append is a
	// valid line and readers stay happy.
	env, err := st2.Publish("b2", sampleChanges("y", 1))
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 3 {
		t.Fatalf("post-recovery sequence = %d, want 3", env.Seq)
	}
	if _, err := st2.ReadAfter(0, 0); err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	raw := mustRead(t, active)
	var okCount int
	for _, line := range splitLines(raw) {
		var e domain.Envelope
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("corrupt line after recovery: %v (%s)", err, line)
		}
		okCount++
	}
	if okCount != 2 {
		t.Fatalf("expected 2 complete lines, got %d", okCount)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i := range b {
		if b[i] == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return out
}
