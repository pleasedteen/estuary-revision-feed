package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"estuary-collector/internal/state"
)

func TestRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	run := j.BeginRun([]string{"alpha", "bravo"})
	if run.Round != 1 {
		t.Fatalf("first round = %d, want 1", run.Round)
	}
	j.UpdateSource(run.Round, "alpha", func(sr *SourceRun) {
		sr.PagesFetched = 2
		sr.Stage = StagePublish
		sr.BatchID = "alpha-1-002"
		sr.BatchSeq = 4
	})
	j.UpdateSource(run.Round, "alpha", func(sr *SourceRun) {
		sr.Stage = StageCheckpoint
		sr.PositionSaved = true
		sr.Position = &state.Position{Offset: 8}
		sr.Status = StatusOK
	})
	j.EndRun(run.Round)

	// Reopen: round numbering continues, entries survive.
	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cur, runs := j2.Snapshot()
	if cur != 1 || len(runs) != 1 {
		t.Fatalf("snapshot = round %d, %d runs; want 1, 1", cur, len(runs))
	}
	got := runs[0]
	if got.Status != RunDone {
		t.Fatalf("run status = %q, want done", got.Status)
	}
	alpha := got.Sources[0]
	if !alpha.PositionSaved || alpha.Position == nil || alpha.Position.Offset != 8 {
		t.Fatalf("alpha position not preserved: %+v", alpha)
	}
	if alpha.BatchID != "alpha-1-002" || alpha.BatchSeq != 4 {
		t.Fatalf("alpha batch not preserved: %+v", alpha)
	}
	next := j2.BeginRun([]string{"alpha"})
	if next.Round != 2 {
		t.Fatalf("round after reopen = %d, want 2", next.Round)
	}
}

func TestInterruptedOnReopen(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	run := j.BeginRun([]string{"alpha", "bravo"})
	// Bravo publishes but the process dies before saving the position.
	j.UpdateSource(run.Round, "bravo", func(sr *SourceRun) {
		sr.Stage = StagePublish
		sr.BatchID = "bravo-1-003"
	})

	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := j2.Find(run.Round)
	if !ok {
		t.Fatal("round lost after reopen")
	}
	if got.Status != RunInterrupted {
		t.Fatalf("run status = %q, want interrupted", got.Status)
	}
	var bravo *SourceRun
	for i := range got.Sources {
		if got.Sources[i].Source == "bravo" {
			bravo = &got.Sources[i]
		}
	}
	if bravo == nil {
		t.Fatal("bravo entry missing")
	}
	if bravo.Status != StatusInterrupted {
		t.Fatalf("bravo status = %q, want interrupted", bravo.Status)
	}
	// The stop point is still visible: published, position not saved.
	if bravo.BatchID != "bravo-1-003" || bravo.PositionSaved {
		t.Fatalf("bravo boundary fields lost: %+v", bravo)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	run := j.BeginRun([]string{"alpha"})
	_, runs := j.Snapshot()
	runs[0].Sources[0].PagesFetched = 999
	got, _ := j.Find(run.Round)
	if got.Sources[0].PagesFetched == 999 {
		t.Fatal("snapshot aliases journal memory")
	}
}

func TestHistoryIsBounded(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keepRuns+10; i++ {
		r := j.BeginRun([]string{"alpha"})
		j.EndRun(r.Round)
	}
	cur, runs := j.Snapshot()
	if cur != keepRuns+10 {
		t.Fatalf("current round = %d, want %d", cur, keepRuns+10)
	}
	if len(runs) != keepRuns {
		t.Fatalf("retained runs = %d, want %d", len(runs), keepRuns)
	}
	// Oldest retained round is the most recent window.
	if runs[len(runs)-1].Round != int64(11) {
		t.Fatalf("oldest retained round = %d, want 11", runs[len(runs)-1].Round)
	}
	if _, ok := j.Find(1); ok {
		t.Fatal("round 1 should have been trimmed")
	}
}

func TestCorruptFileFailsOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "runs.json"), []byte("{nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestFileFormat(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	run := j.BeginRun([]string{"alpha"})
	j.EndRun(run.Round)
	b, err := os.ReadFile(filepath.Join(dir, "runs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		NextRound int64 `json:"next_round"`
		Runs      []struct {
			Round  int64  `json:"round"`
			Status string `json:"status"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.NextRound != 2 || len(doc.Runs) != 1 || doc.Runs[0].Status != RunDone {
		t.Fatalf("unexpected file content: %s", b)
	}
}
