// Package store holds unit tests for sequence allocation and downstream
// read-position semantics.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"estuary-collector/internal/domain"
)

// publish spills and commits one batch through the streaming writer.
func publish(t *testing.T, st *Store, batchID string, changes []domain.Change) domain.Envelope {
	t.Helper()
	w, err := st.BeginBatch(batchID)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if err := w.Add(c); err != nil {
			t.Fatal(err)
		}
	}
	env, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return env
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
	env := publish(t, st, "batch-1", changes)
	if env.Seq != 1 {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if st.MaxSeq() != 3 {
		t.Fatalf("max seq = %d", st.MaxSeq())
	}

	// The committed batch is a single env file carrying its sequence range.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var envFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "env-") {
			envFiles = append(envFiles, e.Name())
		}
	}
	if len(envFiles) != 1 || !strings.HasPrefix(envFiles[0], "env-000000000001-000000000003-batch-1") {
		t.Fatalf("unexpected batch files: %v", envFiles)
	}

	// Empty commit is a no-op and leaves no file behind.
	if env := publish(t, st, "empty", nil); env.BatchID != "" {
		t.Fatalf("empty commit should publish nothing, got %+v", env)
	}
	if st.MaxSeq() != 3 {
		t.Fatalf("empty commit moved max seq to %d", st.MaxSeq())
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
	if got := page[0].Changes[2-1].Record.MgPerL; got != "2" {
		t.Fatalf("record content corrupted: %q", got)
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
	publish(t, st, "b", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s", SampledOn: "d", Revision: 1}})

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st2.MaxSeq() != 1 {
		t.Fatalf("rebuilt max seq = %d, want 1", st2.MaxSeq())
	}
	env := publish(t, st2, "b2", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s2", SampledOn: "d", Revision: 1}})
	if env.Seq != 2 {
		t.Fatalf("sequence did not continue across reopen: %d", env.Seq)
	}
}

// TestLegacyLayoutReadable writes batches in the historical layout (an
// active batches.jsonl plus a rotated archive) and verifies the new store
// reads them in sequence order and continues the sequence above them.
func TestLegacyLayoutReadable(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"seq":1,"batch_id":"old-1","changed_at":"2026-11-10T00:00:00Z","changes":[{"seq":1,"op":"upsert","institution":"alpha","sample_id":"A-1","sampled_on":"2026-11-10","revision":1}]}
{"seq":2,"batch_id":"old-1","changed_at":"2026-11-10T00:00:00Z","changes":[{"seq":2,"op":"withdraw","institution":"alpha","sample_id":"A-2","sampled_on":"2026-11-10","revision":2}]}
`
	archive := `{"seq":3,"batch_id":"old-2","changed_at":"2026-11-10T00:00:01Z","changes":[{"seq":3,"op":"upsert","institution":"bravo","sample_id":"B-1","sampled_on":"2026-11-11","revision":1}]}
`
	// The rotated archive holds the earlier sequences, the active file the
	// later ones — exactly what the historical rotating writer produced.
	if err := os.WriteFile(filepath.Join(dir, "batch-000000000001.jsonl"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "batches.jsonl"), []byte(archive), 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if st.MaxSeq() != 3 {
		t.Fatalf("legacy max seq = %d, want 3", st.MaxSeq())
	}
	page, err := st.ReadAfter(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].Changes[0].Seq != 2 || page[1].Changes[0].Seq != 3 {
		t.Fatalf("legacy read order wrong: %+v", page)
	}

	// A saved downstream position keeps working, and new batches continue
	// the sequence above the legacy files.
	env := publish(t, st, "new-1", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s", SampledOn: "d", Revision: 1}})
	if env.Seq != 4 {
		t.Fatalf("new batch should continue at seq 4, got %d", env.Seq)
	}
	page, err = st.ReadAfter(3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Changes[0].Seq != 4 {
		t.Fatalf("continuation across layout boundary wrong: %+v", page)
	}
}

// TestStreamChangesLargeBatch spills more changes than memory would hold
// comfortably and verifies streaming recovery sees every one of them.
func TestStreamChangesLargeBatch(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20000
	w, err := st.BeginBatch("big")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		err := w.Add(domain.Change{
			Op: domain.OpUpsert, Institution: "alpha",
			SampleID: fmt.Sprintf("S-%d", i%8), SampledOn: "2026-11-10", Revision: int64(i/8 + 1),
			Record: &domain.CanonicalRecord{Institution: "alpha", SampleID: fmt.Sprintf("S-%d", i%8), Revision: int64(i/8 + 1), SampledOn: "2026-11-10", Unit: "mg/L", MgPerL: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	env, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if env.Seq != 1 || st.MaxSeq() != n {
		t.Fatalf("env seq %d, max %d", env.Seq, st.MaxSeq())
	}

	// Streaming replay sees all n changes in order, with contiguous seqs.
	var seen int64
	var lastSeq int64
	err = st.StreamChanges(0, func(c domain.Change) error {
		seen++
		if c.Seq != lastSeq+1 {
			t.Fatalf("sequence gap: got %d after %d", c.Seq, lastSeq)
		}
		lastSeq = c.Seq
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != n {
		t.Fatalf("streamed %d changes, want %d", seen, n)
	}

	// The same batch is readable through the paged API as one envelope.
	page, err := st.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || len(page[0].Changes) != n {
		t.Fatalf("read back %d envelopes / %d changes", len(page), len(page[0].Changes))
	}
}

// TestAbortLeavesNoTrace verifies an aborted batch publishes nothing and its
// spill file is cleaned up.
func TestAbortLeavesNoTrace(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.BeginBatch("doomed")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(domain.Change{Op: domain.OpWithdraw, Institution: "a", SampleID: "s", SampledOn: "d", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if st.MaxSeq() != 0 {
		t.Fatalf("aborted batch moved max seq to %d", st.MaxSeq())
	}
	page, err := st.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 0 {
		t.Fatalf("aborted batch visible: %+v", page)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".frag") || strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("stale spill file left behind: %s", e.Name())
		}
	}
}
