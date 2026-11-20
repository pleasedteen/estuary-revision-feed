package state

import (
	"path/filepath"
	"testing"
)

func TestUpdateAndReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Get("alpha"); got != (Position{}) {
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
