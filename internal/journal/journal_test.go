package journal

import (
	"path/filepath"
	"testing"
)

func TestSaveLoadClear(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.Load("charlie"); err != nil || ok {
		t.Fatalf("fresh journal should be empty: ok=%v err=%v", ok, err)
	}
	entry := Entry{
		Source:  "charlie",
		Window:  "2026-11-10T00:00:00Z",
		Version: 2,
		Offset:  4,
		Records: []map[string]any{{"sample_id": "C-300", "rev": float64(1)}},
	}
	if err := st.Save(entry); err != nil {
		t.Fatal(err)
	}
	// Reopening reads the durable entry back.
	st2, err := Open(filepath.Join(dir))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := st2.Load("charlie")
	if err != nil || !ok {
		t.Fatalf("journal entry lost: ok=%v err=%v", ok, err)
	}
	if got.Window != entry.Window || got.Version != entry.Version || got.Offset != entry.Offset || len(got.Records) != 1 {
		t.Fatalf("journal entry mismatch: %+v", got)
	}
	if err := st2.Clear("charlie"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st2.Load("charlie"); err != nil || ok {
		t.Fatalf("clear failed: ok=%v err=%v", ok, err)
	}
}

func TestEntriesAreIndependentPerSource(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Save(Entry{Source: "a", Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.Save(Entry{Source: "b", Offset: 2}); err != nil {
		t.Fatal(err)
	}
	a, ok, err := st.Load("a")
	if err != nil || !ok || a.Offset != 1 {
		t.Fatalf("source a entry wrong: %+v ok=%v err=%v", a, ok, err)
	}
	if err := st.Clear("a"); err != nil {
		t.Fatal(err)
	}
	b, ok, err := st.Load("b")
	if err != nil || !ok || b.Offset != 2 {
		t.Fatalf("clearing a disturbed b: %+v ok=%v err=%v", b, ok, err)
	}
}
