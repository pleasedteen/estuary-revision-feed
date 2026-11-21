package state

import (
	"os"
	"path/filepath"
	"testing"

	"estuary-collector/internal/domain"
)

func TestUpdateAndReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Get("alpha"); got.Offset != 0 || got.Cursor != "" || got.Windows != nil {
		t.Fatalf("new store should have zero position, got %+v", got)
	}
	if err := st.Update("alpha", Position{Offset: 9}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update("bravo", Position{Cursor: "abc", CursorOffset: 4}); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(filepath.Join(dir))
	if err != nil {
		t.Fatal(err)
	}
	if got := st2.Get("alpha"); got.Offset != 9 {
		t.Fatalf("alpha position lost: %+v", got)
	}
	if got := st2.Get("bravo"); got.Cursor != "abc" || got.CursorOffset != 4 {
		t.Fatalf("bravo position lost: %+v", got)
	}
}

// TestCheckpointPendingRoundTrip verifies position and pending journal are
// persisted atomically and an empty journal clears the entry.
func TestCheckpointPendingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos := Position{Windows: map[string]int{"w1": 2, "w2": 5}}
	pending := Pending{
		BatchID: "charlie-1-000",
		Changes: []domain.Change{{
			Op: domain.OpUpsert, Institution: "charlie", SampleID: "C-1",
			SampledOn: "2026-11-10", Revision: 2,
		}},
	}
	if err := st.Checkpoint("charlie", pos, pending); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotPos := st2.Get("charlie")
	if gotPos.Windows["w1"] != 2 || gotPos.Windows["w2"] != 5 {
		t.Fatalf("window offsets lost: %+v", gotPos)
	}
	gotPending := st2.Pending("charlie")
	if gotPending.BatchID != "charlie-1-000" || len(gotPending.Changes) != 1 {
		t.Fatalf("pending journal lost: %+v", gotPending)
	}
	if gotPending.Changes[0].Revision != 2 {
		t.Fatalf("pending change corrupted: %+v", gotPending.Changes[0])
	}

	// An empty journal clears the entry while the position is kept.
	if err := st2.Checkpoint("charlie", pos, Pending{}); err != nil {
		t.Fatal(err)
	}
	st3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := st3.Pending("charlie"); !got.Empty() {
		t.Fatalf("pending journal should be cleared, got %+v", got)
	}
	if got := st3.Get("charlie"); got.Windows["w2"] != 5 {
		t.Fatalf("position lost on journal clear: %+v", got)
	}
}

// TestLegacyStateFileReadable verifies a state file written by the previous
// version (window index + inner offset, no pending section) still opens, so
// an old data directory can be upgraded on first read.
func TestLegacyStateFileReadable(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"positions": {"alpha": {"offset": 8}, "charlie": {"window_index": 2, "window_offset": 0}}}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("legacy state file must remain readable: %v", err)
	}
	got := st.Get("charlie")
	if got.WindowIndex != 2 || got.Windows != nil {
		t.Fatalf("legacy charlie position misread: %+v", got)
	}
	if p := st.Pending("charlie"); !p.Empty() {
		t.Fatalf("legacy file should have no pending journal, got %+v", p)
	}
}
