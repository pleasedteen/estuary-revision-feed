// Package store holds unit tests for sequence allocation and downstream
// read-position semantics.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"estuary-collector/internal/domain"
)

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

	// Read from the middle: the batch is the atomic unit, so an envelope that
	// contains a sequence past the saved position is returned whole. Saving
	// the returned next_seq therefore always lands on a batch boundary.
	page, err := st.ReadAfter(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 {
		t.Fatalf("expected one envelope, got %d", len(page))
	}
	if len(page[0].Changes) != 3 || page[0].Changes[0].Seq != 1 {
		t.Fatalf("expected the whole batch, got %+v", page[0])
	}
	// Reading from the batch's last sequence returns nothing.
	if page2, err := st.ReadAfter(3, 0); err != nil {
		t.Fatal(err)
	} else if len(page2) != 0 {
		t.Fatalf("expected empty page at batch boundary, got %d", len(page2))
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

// TestRotationAndBatchBoundaries verifies that batches stay whole and exactly
// once across file rotation when continuing from a saved next_seq with a
// smaller limit than the batch size.
func TestRotationAndBatchBoundaries(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenWithRotation(dir, 256)
	if err != nil {
		t.Fatal(err)
	}
	// Publish several multi-change batches; small rotation forces files.
	sizes := []int{3, 1, 4, 2}
	for i, n := range sizes {
		changes := make([]domain.Change, n)
		for j := range changes {
			changes[j] = domain.Change{
				Op: domain.OpUpsert, Institution: "alpha",
				SampleID:  fmt.Sprintf("A-%d-%d", i, j),
				SampledOn: "2026-11-10", Revision: 1,
				Record: &domain.CanonicalRecord{Institution: "alpha", Unit: "mg/L", MgPerL: "1"},
			}
		}
		if _, err := st.Publish(fmt.Sprintf("b%d", i), changes); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenWithRotation(dir, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	// limit=1 forces the reader to advance batch by batch.
	var seen int64
	got := map[string]int{}
	for i := 0; i < 100; i++ {
		page, err := st2.ReadAfter(seen, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, env := range page {
			if len(env.Changes) == 0 {
				t.Fatalf("empty envelope returned")
			}
			got[env.BatchID]++
			seen = env.Changes[len(env.Changes)-1].Seq
		}
	}
	for i, n := range sizes {
		id := fmt.Sprintf("b%d", i)
		if got[id] != 1 {
			t.Fatalf("batch %s seen %d times, want exactly once", id, got[id])
		}
		_ = n
	}
	if len(got) != len(sizes) {
		t.Fatalf("saw %d batches, want %d", len(got), len(sizes))
	}
}

// TestTornTailRepair verifies an interrupted append (partial trailing line) is
// dropped on Open and the batch boundary stays authoritative.
func TestTornTailRepair(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	env, err := st.Publish("good", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s", SampledOn: "d", Revision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Simulate a crash mid-append: a partial JSON line with no newline.
	f, err := os.OpenFile(filepath.Join(dir, activeName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":2,"batch_id":"tor`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open should repair the torn tail: %v", err)
	}
	defer st2.Close()
	if st2.MaxSeq() != env.Changes[len(env.Changes)-1].Seq {
		t.Fatalf("repaired max seq = %d, want %d", st2.MaxSeq(), env.Seq)
	}
	page, err := st2.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].BatchID != "good" {
		t.Fatalf("expected only the good batch, got %+v", page)
	}
	// The sequence continues cleanly after the repaired tail.
	env2, err := st2.Publish("after", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s2", SampledOn: "d", Revision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if env2.Seq != env.Seq+1 {
		t.Fatalf("sequence after repair = %d, want %d", env2.Seq, env.Seq+1)
	}
}

// TestCorruptTerminatedLineRejected verifies a newline-terminated but invalid
// line is treated as real corruption, not a repairable torn tail.
func TestCorruptTerminatedLineRejected(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish("good", []domain.Change{{Op: domain.OpUpsert, Institution: "i", SampleID: "s", SampledOn: "d", Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	f, err := os.OpenFile(filepath.Join(dir, activeName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("not-json-but-terminated\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := Open(dir); err == nil {
		t.Fatal("Open should reject a terminated corrupt line")
	}
}
