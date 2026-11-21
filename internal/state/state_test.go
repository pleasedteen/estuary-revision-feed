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
	if got := st.Get("alpha"); got.Offset != 0 || got.Cursor != "" {
		t.Fatalf("new store should have a zero position, got %+v", got)
	}
	if err := st.Update("alpha", Position{Offset: 9}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update("bravo", Position{Cursor: "abc", CursorOffset: 4}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetWindowVersion("charlie", "2026-11-10T00:00:00Z", 2); err != nil {
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
	if got := st2.WindowVersions("charlie"); got["2026-11-10T00:00:00Z"] != 2 {
		t.Fatalf("charlie window version lost: %+v", got)
	}
}

func TestWindowVersionsAreCopied(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetWindowVersion("charlie", "w1", 1); err != nil {
		t.Fatal(err)
	}
	got := st.WindowVersions("charlie")
	got["w1"] = 99 // mutating the copy must not affect stored state
	if v := st.WindowVersions("charlie")["w1"]; v != 1 {
		t.Fatalf("window version map leaked: %d", v)
	}
}
