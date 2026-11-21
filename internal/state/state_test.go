package state

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestUpdateAndReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Get("alpha"); !reflect.DeepEqual(got, Position{}) {
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
	if got := st2.Get("alpha"); got.Offset != 9 || len(got.Windows) != 0 {
		t.Fatalf("alpha position lost: %+v", got)
	}
	if got := st2.Get("bravo"); got.Cursor != "abc" || got.CursorOffset != 4 {
		t.Fatalf("bravo position lost: %+v", got)
	}
}

// TestWindowCheckpointsRoundTrip persists Charlie window checkpoints,
// including a retransmitted window version, and reads them back.
func TestWindowCheckpointsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos := Position{
		WindowIndex: 2,
		Windows: []WindowCheckpoint{
			{Window: "2026-11-10T00:00:00Z", Version: 2},
			{Window: "2026-11-11T00:00:00Z", Version: 1},
		},
	}
	if err := st.Update("charlie", pos); err != nil {
		t.Fatal(err)
	}
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := st2.Get("charlie")
	if !reflect.DeepEqual(got, pos) {
		t.Fatalf("charlie checkpoint round trip mismatch: %+v", got)
	}
}
