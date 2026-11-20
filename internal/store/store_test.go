// Package store holds unit tests for sequence allocation and downstream
// read-position semantics.
package store

import (
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
