package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"estuary-collector/internal/mockdata"
	"estuary-collector/internal/mockserver"
	"estuary-collector/internal/service"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// TestBravoExpiryBudget proves a cursor that keeps expiring can no longer trap
// a bravo run in an endless head-restart loop. The round must stop after the
// fixed detail-request budget, report a failure without publishing anything
// and leave the bookmark untouched so a later round can recover once the
// source is healthy again.
func TestBravoExpiryBudget(t *testing.T) {
	mock := mockserver.New()
	base := httptest.NewServer(mock.Handler())
	defer base.Close()

	// Every continuation cursor is treated as permanently expired, while head
	// requests (no cursor) keep succeeding. This reproduces a source whose
	// cursors never survive between requests.
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGone)
			_, _ = w.Write([]byte(`{"error":"cursor expired"}`))
			return
		}
		mock.Handler().ServeHTTP(w, r)
	}))
	defer flaky.Close()

	dir := t.TempDir()
	cfg := service.Config{
		DataDir:      dir,
		MockBaseURL:  flaky.URL,
		AuthURL:      base.URL + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	}
	svc, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Seed a mid-log continuation bookmark so the first request already hits
	// the permanently-expired cursor.
	if err := svc.Engine.OverridePositionForTest(mockdata.InstitutionBravo,
		state.Position{Cursor: "expired-mid-log-cursor", CursorOffset: 3}); err != nil {
		t.Fatal(err)
	}

	batchStore, err := store.Open(filepath.Join(dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	seqBefore := batchStore.MaxSeq()

	rep := svc.Engine.Sync(context.Background())
	br := sourceResult(rep, mockdata.InstitutionBravo)

	if br.Status != "failed" {
		t.Fatalf("expected a failed bravo run under persistent expiry, got %+v", br)
	}
	if !br.CursorRestart {
		t.Fatal("expected at least one head restart to be recorded")
	}
	// One round may issue at most six bravo detail requests (the 410s count).
	if br.PagesFetched != 6 {
		t.Fatalf("expected the run to stop at 6 detail requests, got %d", br.PagesFetched)
	}
	// Nothing may be published before the tail is reached.
	if got := batchStore.MaxSeq(); got != seqBefore {
		t.Fatalf("budget-exhausted run published batches: seq %d -> %d", seqBefore, got)
	}
	// The bookmark must remain untouched so recovery is possible later.
	if pos := loadState(t, dir)["bravo"]; pos.Cursor != "expired-mid-log-cursor" {
		t.Fatalf("bookmark changed after a non-tail run: %+v", pos)
	}

	// Point a fresh engine at the healthy mock: the same bookmark now resolves
	// and the run completes, proving the cap does not permanently block work.
	healthy, err := service.New(service.Config{
		DataDir:      dir,
		MockBaseURL:  base.URL,
		AuthURL:      base.URL + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	rep = healthy.Engine.Sync(context.Background())
	if r := sourceResult(rep, mockdata.InstitutionBravo); r.Status != "ok" {
		t.Fatalf("bravo failed to recover after the source healed: %+v", r)
	}
	if r := sourceResult(rep, mockdata.InstitutionBravo); r.Changes != 7 {
		t.Fatalf("expected the seven initial bravo records after recovery, got %d", r.Changes)
	}
}

// TestBravoOldBookmarkCompatibility verifies a pre-upgrade bookmark that only
// carries a cursor offset (no resume cursor) still reads from the head once,
// emits no duplicate changes against published history, and is upgraded to a
// tail bookmark with a persisted resume cursor.
func TestBravoOldBookmarkCompatibility(t *testing.T) {
	h := newHarness(t)
	h.sync()

	// Wipe only the resume fields, mimicking a state file written by the
	// previous implementation at the tail (empty cursor, offset only).
	pos := loadState(t, h.dir)["bravo"]
	if err := h.svc.Engine.OverridePositionForTest(mockdata.InstitutionBravo,
		state.Position{CursorOffset: pos.CursorOffset}); err != nil {
		t.Fatal(err)
	}

	rep := h.sync()
	br := sourceResult(rep, mockdata.InstitutionBravo)
	if br.Status != "ok" || br.Changes != 0 {
		t.Fatalf("old bookmark head readback must absorb published revisions, got %+v", br)
	}
	upgraded := loadState(t, h.dir)["bravo"]
	if upgraded.Cursor != "" || upgraded.ResumeCursor == "" {
		t.Fatalf("old bookmark not upgraded to tail+resume form: %+v", upgraded)
	}

	// Subsequent rounds use the resume cursor and re-read only the final page.
	rep = h.sync()
	if r := sourceResult(rep, mockdata.InstitutionBravo); r.PagesFetched != 1 || r.Changes != 0 {
		t.Fatalf("upgraded bookmark did not resume from the final page: %+v", r)
	}
}

// accumulates already-published revisions in its staging area, and that an
// interrupted readback leaves the durable index untouched (the fresh revision
// is still published on a later complete run).
func TestBravoStagingNotPollutedByOldRecords(t *testing.T) {
	h := newHarness(t)
	h.sync()

	// Stage 1 adds a fresh bravo change set (withdrawal, reschedule, new
	// sample). Interrupt the very first bravo page of the next run.
	h.advanceStage()
	var interrupted bool
	h.svc.Engine.SetPageHook(func(src string, pageNo int, _ source.Next) {
		if src == mockdata.InstitutionBravo && pageNo == 0 && !interrupted {
			interrupted = true
			panic("simulated crash during head readback")
		}
	})
	func() {
		defer func() { _ = recover() }()
		h.sync()
	}()
	h.svc.Engine.SetPageHook(nil)

	bravoEnvelopeCount := func() int {
		envs := h.batchesAfter(0)
		n := 0
		for _, env := range envs {
			bid, _ := env["batch_id"].(string)
			if len(bid) >= 5 && bid[:5] == "bravo" {
				n++
			}
		}
		return n
	}
	bravoBatchesBefore := bravoEnvelopeCount()

	// Restart from on-disk state and complete the run.
	svc2, err := service.New(service.Config{
		DataDir:      h.dir,
		MockBaseURL:  h.mockURL,
		AuthURL:      h.mockURL + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc2

	// The interrupted, non-tail run must not have published a bravo batch.
	if got := bravoEnvelopeCount(); got != bravoBatchesBefore {
		t.Fatalf("interrupted readback published a partial bravo batch: %d -> %d envelopes",
			bravoBatchesBefore, got)
	}

	rep := h.sync()
	br := sourceResult(rep, mockdata.InstitutionBravo)
	if br.Status != "ok" {
		t.Fatalf("bravo run after interrupted readback failed: %+v", br)
	}
	// Stage 1 carries four bravo changes; none was lost by the interruption.
	if br.Changes != 4 {
		t.Fatalf("expected all 4 stage-1 bravo changes, got %d", br.Changes)
	}
	// A subsequent idempotent run emits nothing: no revision was double-counted
	// by staging/index leakage.
	rep = h.sync()
	if r := sourceResult(rep, mockdata.InstitutionBravo); r.Changes != 0 {
		t.Fatalf("rerun after readback should be empty, got %+v", r)
	}
}
