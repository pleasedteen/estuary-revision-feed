package collector

import (
	"testing"

	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

func newTestEngine(t *testing.T) (*Engine, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	stt, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Engine{store: st, states: stt, index: map[string]*indexEntry{}}, st
}

func rawRecord(id string, rev int64) map[string]any {
	return map[string]any{
		"institution":   "alpha",
		"sample_id":     id,
		"rev":           rev,
		"sampled_on":    "2026-11-10",
		"site":          "北滩",
		"substance":     "悬浮物",
		"concentration": "1",
		"unit":          "mg/L",
		"status":        "active",
		"published_at":  "2026-11-10T00:00:00Z",
	}
}

// TestStagerDoesNotMutateSharedIndex is the core regression: a page that is
// fetched but never committed must leave the shared index untouched, so a
// same-process retry still sees the records as new.
func TestStagerDoesNotMutateSharedIndex(t *testing.T) {
	e, _ := newTestEngine(t)
	st := newStager(e)
	if err := st.derive(rawRecord("A-1", 1)); err != nil {
		t.Fatal(err)
	}
	if len(e.index) != 0 {
		t.Fatal("staging leaked into the shared index before commit")
	}
	if len(e.Materialized()) != 0 {
		t.Fatal("staged record visible before publication")
	}
	// A discarded run (e.g. page 2 returned 429) leaves nothing behind.
	// Retrying derives the same record again.
	st2 := newStager(e)
	if err := st2.derive(rawRecord("A-1", 1)); err != nil {
		t.Fatal(err)
	}
	if len(st2.changes) != 1 {
		t.Fatalf("retry lost page-1 change: got %d changes", len(st2.changes))
	}
}

// TestCommitPublishesThenAdvancesIndex verifies commit is visible in both
// /v1/batches and /v1/records only after the batch is durable, and that a
// duplicate revision is absorbed afterwards.
func TestCommitPublishesThenAdvancesIndex(t *testing.T) {
	e, st := newTestEngine(t)
	stager := newStager(e)
	if err := stager.derive(rawRecord("A-1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.commit("alpha", "b1", stager, state.Position{Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if got := len(e.Materialized()); got != 1 {
		t.Fatalf("record not visible after commit: %d", got)
	}
	envs, err := st.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 1 || envs[0].BatchID != "b1" {
		t.Fatalf("batch not published: %+v", envs)
	}
	// Re-deriving the same revision after commit is a no-op.
	again := newStager(e)
	if err := again.derive(rawRecord("A-1", 1)); err != nil {
		t.Fatal(err)
	}
	if len(again.changes) != 0 {
		t.Fatalf("duplicate revision re-emitted: %v", again.changes)
	}
}

// TestCommitPublishFailureLeavesIndexUntouched ensures a publish error does
// not advance visibility: the position and index stay consistent (both stay
// at the previous state), so the next run retries cleanly.
func TestCommitPublishFailureLeavesIndexUntouched(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	stt, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{store: st, states: stt, index: map[string]*indexEntry{}}

	stager := newStager(e)
	if err := stager.derive(rawRecord("A-1", 1)); err != nil {
		t.Fatal(err)
	}
	// Break the active file so Publish fails.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.commit("alpha", "b1", stager, state.Position{Offset: 1}); err == nil {
		t.Fatal("expected publish to fail on a closed store")
	}
	if len(e.index) != 0 {
		t.Fatal("index advanced despite a failed publish")
	}
	if pos := e.states.Get("alpha"); pos.Offset != 0 {
		t.Fatalf("position advanced despite a failed publish: %+v", pos)
	}
}
