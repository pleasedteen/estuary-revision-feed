package tests

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"estuary-collector/internal/collector"
	"estuary-collector/internal/mockserver"
	"estuary-collector/internal/service"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// charlieC300Revisions collects every published C-300 revision in seq order.
func charlieC300Revisions(t *testing.T, h *harness) []int64 {
	t.Helper()
	envs := h.batchesAfter(0)
	var revs []int64
	for _, env := range envs {
		for _, c := range env["changes"].([]any) {
			cm := c.(map[string]any)
			if cm["institution"] == "charlie" && cm["sample_id"] == "C-300" {
				revs = append(revs, int64(cm["revision"].(float64)))
			}
		}
	}
	return revs
}

func charlieResult(t *testing.T, rep collector.Report) collector.SourceResult {
	t.Helper()
	return sourceResult(rep, "charlie")
}

// Test12_CharlieRetransmitImmediate covers a whole-window retransmission
// arriving after the window was already confirmed: the manifest version
// bumps, the window is re-read, the higher revision is captured and the
// unchanged rows produce no changes. A subsequent run only consults the
// manifest (zero page requests) and stays idempotent.
func Test12_CharlieRetransmitImmediate(t *testing.T) {
	h := newHarness(t)
	h.sync()

	// Retransmit the first window immediately (version 1 -> 2, C-300 rev 2).
	out := h.mockPost("/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z", map[string]any{})
	if out["version"].(float64) != 2 {
		t.Fatalf("expected retransmitted window version 2, got %v", out["version"])
	}

	rep := h.sync()
	cr := charlieResult(t, rep)
	if cr.Status != "ok" || cr.Changes != 1 {
		t.Fatalf("retransmitted window should yield exactly one change, got %+v", cr)
	}
	if cr.WindowRestart {
		t.Fatal("a manifest-level retransmission must not flag a mid-window restart")
	}
	// The initial run published rev1; the retransmitted window appends rev2.
	// Both revisions are real history, each published exactly once.
	revs := charlieC300Revisions(t, h)
	if len(revs) != 2 || revs[0] != 1 || revs[1] != 2 {
		t.Fatalf("C-300 history should be rev1 then rev2, each once, got %v", revs)
	}
	recs := materializedMap(h)
	c300 := recs["charlie|C-300|2026-11-10"]
	if c300["revision"].(float64) != 2 || c300["mg_per_l"] != "9.9" {
		t.Fatalf("materialized C-300 should be the retransmitted rev2 row: %v", c300)
	}

	// The confirmed window at version 2 now needs the manifest only: the next
	// run issues no inside-window page requests and emits nothing.
	before := h.mockGet("/mock/stats")["request_counts"].(map[string]any)["charlie"].(float64)
	rep = h.sync()
	if cr := charlieResult(t, rep); cr.Changes != 0 || cr.PagesFetched != 0 {
		t.Fatalf("confirmed window rerun must be manifest-only, got %+v", cr)
	}
	after := h.mockGet("/mock/stats")["request_counts"].(map[string]any)["charlie"].(float64)
	if after-before != 1 {
		t.Fatalf("rerun should add exactly one manifest request, delta %v", after-before)
	}

	// The checkpoint persists the read window versions.
	positions := loadState(t, h.dir)
	cp := positions["charlie"]
	want := []state.WindowCheckpoint{
		{Window: "2026-11-10T00:00:00Z", Version: 2},
		{Window: "2026-11-11T00:00:00Z", Version: 1},
	}
	if len(cp.Windows) != len(want) {
		t.Fatalf("expected %d window checkpoints, got %+v", len(want), cp.Windows)
	}
	for i, w := range want {
		if cp.Windows[i] != w {
			t.Fatalf("checkpoint %d = %+v, want %+v", i, cp.Windows[i], w)
		}
	}
}

// Test13_CharlieRetransmitBetweenPages arms the retransmission to fire
// between the two pages of the first window. The collector must detect the
// version change, restart the window from its first page and publish only
// the new version: C-300 appears solely at revision 2, never at revision 1.
func Test13_CharlieRetransmitBetweenPages(t *testing.T) {
	h := newHarness(t)
	// Fire before the second page request of the first window (three rows,
	// two rows per page).
	h.mockPost("/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z&arm_page=2", map[string]any{})

	rep := h.sync()
	cr := charlieResult(t, rep)
	if cr.Status != "ok" {
		t.Fatalf("sync with mid-window retransmission failed: %+v", cr)
	}
	if !cr.WindowRestart {
		t.Fatal("expected window_restart to be reported after a mid-pages retransmission")
	}
	// Initial dataset still has seven distinct Charlie records; the restarted
	// window contributes the retransmitted C-300 rev2 instead of its rev1.
	if cr.Changes != 7 {
		t.Fatalf("expected seven Charlie changes, got %d", cr.Changes)
	}
	revs := charlieC300Revisions(t, h)
	if len(revs) != 1 || revs[0] != 2 {
		t.Fatalf("no mixed versions allowed: C-300 must appear once at rev2, got %v", revs)
	}
	// The other two sources are unaffected by Charlie's retransmission.
	for _, name := range []string{"alpha", "bravo"} {
		if r := sourceResult(rep, name); r.Status != "ok" || r.Changes == 0 {
			t.Fatalf("source %s should collect normally: %+v", name, r)
		}
	}
	// The window served four page requests in total (v1 page 1, the boundary
	// page that revealed the new version, then v2 pages 1 and 2).
	pages := h.mockGet("/mock/stats")["charlie_window_page_requests"].(map[string]any)
	if pages["2026-11-10T00:00:00Z"].(float64) != 4 {
		t.Fatalf("expected four page requests for the retransmitted window, got %v", pages)
	}

	// A second run is empty and never re-emits the published revisions.
	rep = h.sync()
	if cr := charlieResult(t, rep); cr.Changes != 0 {
		t.Fatalf("rerun after mid-pages retransmission must be empty, got %+v", cr)
	}
	if revs := charlieC300Revisions(t, h); len(revs) != 1 || revs[0] != 2 {
		t.Fatalf("C-300 duplicated on rerun: %v", revs)
	}
}

// Test14_CharlieRetransmitCrashResume crashes the process inside the first
// Charlie window, has the window retransmitted while the collector is down,
// then restarts against the same data directory. The interrupted window is
// re-read from the head, the higher revision is captured exactly once and
// already-published revisions never re-emit.
func Test14_CharlieRetransmitCrashResume(t *testing.T) {
	mock := mockserver.New()
	srv := httptest.NewServer(mock.Handler())
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	cfg := service.Config{
		DataDir:      dir,
		MockBaseURL:  srv.URL,
		AuthURL:      srv.URL + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	}
	svc, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var pages atomic.Int64
	svc.Engine.SetPageHook(func(src string, _ int, _ source.Next) {
		if src == "charlie" && pages.Add(1) == 1 {
			panic("simulated crash inside charlie window")
		}
	})
	func() {
		defer func() { _ = recover() }()
		svc.Engine.Sync(context.Background())
	}()
	svc.Engine.SetPageHook(nil)

	// The window is retransmitted while no collector is running.
	if _, ok := mock.Provider.CharlieRetransmit("2026-11-10T00:00:00Z"); !ok {
		t.Fatal("retransmit of existing window should succeed")
	}

	// Restart reusing the original data directory.
	svc2, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep := svc2.Engine.Sync(context.Background())
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("resume run not ok: %+v", r)
		}
	}

	batchStore, err := store.Open(filepath.Join(dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	envs, err := batchStore.ReadAfter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	c300 := map[int64]int{}
	for _, env := range envs {
		for _, c := range env.Changes {
			total++
			if c.Institution == "charlie" && c.SampleID == "C-300" {
				c300[c.Revision]++
			}
		}
	}
	if total != 22 {
		t.Fatalf("expected all 22 initial records after resume, got %d changes", total)
	}
	if len(c300) != 1 || c300[2] != 1 {
		t.Fatalf("C-300 must be published exactly once at rev2, got %v", c300)
	}
	if got := len(svc2.Engine.Materialized()); got != 22 {
		t.Fatalf("expected 22 materialized records, got %d", got)
	}

	// A final run proves the resume point is stable: nothing re-publishes.
	rep2 := svc2.Engine.Sync(context.Background())
	for _, r := range rep2.Sources {
		if r.Changes != 0 {
			t.Fatalf("post-resume rerun not empty: %+v", r)
		}
	}
}

// Test15_CharlieRetransmitOtherSourcesUnaffected runs the mid-pages
// retransmission together with the normal stage progression and checks the
// reschedule semantics of the later window still publish atomically.
func Test15_CharlieRetransmitWithStageProgress(t *testing.T) {
	h := newHarness(t)
	h.mockPost("/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z&arm_page=2", map[string]any{})
	h.advanceStage() // opens the 2026-11-12 window including the C-301 reschedule

	rep := h.sync()
	cr := charlieResult(t, rep)
	if cr.Status != "ok" || !cr.WindowRestart {
		t.Fatalf("expected ok with window restart, got %+v", cr)
	}
	// Seven stage-0 rows (C-300 as retransmitted rev2) plus three stage-1
	// rows: the C-301 old-day withdraw, its new-day upsert and C-307.
	if cr.Changes != 10 {
		t.Fatalf("expected 10 charlie changes across both windows, got %d", cr.Changes)
	}
	envs := h.batchesAfter(0)
	found := false
	for _, env := range envs {
		hasOld, hasNew := false, false
		for _, c := range env["changes"].([]any) {
			cm := c.(map[string]any)
			if cm["sample_id"] != "C-301" {
				continue
			}
			if cm["op"] == "withdraw" && cm["sampled_on"] == "2026-11-10" {
				hasOld = true
			}
			if cm["op"] == "upsert" && cm["sampled_on"] == "2026-11-14" {
				hasNew = true
			}
		}
		if hasOld && hasNew {
			found = true
		}
	}
	if !found {
		t.Fatal("C-301 reschedule halves must still publish in one batch")
	}
	recs := materializedMap(h)
	if recs["charlie|C-300|2026-11-10"]["revision"].(float64) != 2 {
		t.Fatal("retransmitted C-300 rev2 missing after staged progress")
	}
	if _, ok := recs["charlie|C-301|2026-11-10"]; ok {
		t.Fatal("old-day C-301 must be withdrawn after the reschedule")
	}
	if _, ok := recs["charlie|C-301|2026-11-14"]; !ok {
		t.Fatal("new-day C-301 missing after the reschedule")
	}
}

// Test16_CharlieRetransmitStormFailsFast arms a sticky retransmission that
// bumps the window version before every page request, so no single version
// can ever be read cover-to-cover. The run must fail explicitly instead of
// looping forever and must never publish a partial window; once the arm is
// disarmed the next run completes normally.
func Test16_CharlieRetransmitStormFailsFast(t *testing.T) {
	h := newHarness(t)
	// Sticky arm: retransmit before every page request of the first window.
	h.mockPost("/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z&arm_page=-1", map[string]any{})

	rep := h.sync()
	cr := charlieResult(t, rep)
	if cr.Status != "failed" || cr.Error == "" {
		t.Fatalf("a window retransmitted on every page must fail the run, got %+v", cr)
	}
	if cr.WindowRestart != true {
		t.Fatalf("expected window_restart flag on the storm run, got %+v", cr)
	}
	// The other two sources are isolated from Charlie's failure.
	for _, name := range []string{"alpha", "bravo"} {
		if r := sourceResult(rep, name); r.Status != "ok" || r.Changes == 0 {
			t.Fatalf("source %s should still complete: %+v", name, r)
		}
	}
	// Nothing of the failed Charlie run may have been published: its changes
	// only ever existed in the discarded per-window overlay.
	for _, env := range h.batchesAfter(0) {
		for _, c := range env["changes"].([]any) {
			if c.(map[string]any)["institution"] == "charlie" {
				t.Fatal("failed charlie run must not publish partial window changes")
			}
		}
	}

	// Disarm the storm; the next run reads every window normally and completes.
	h.mockPost("/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z&arm_page=0", map[string]any{})
	rep = h.sync()
	cr = charlieResult(t, rep)
	if cr.Status != "ok" || cr.Changes != 7 {
		t.Fatalf("charlie should recover with seven changes after disarm, got %+v", cr)
	}
	recs := materializedMap(h)
	if len(recs) != 22 {
		t.Fatalf("expected 22 materialized records after recovery, got %d", len(recs))
	}
	// A final rerun is empty: recovery left a stable, confirmed checkpoint.
	if cr = charlieResult(t, h.sync()); cr.Changes != 0 {
		t.Fatalf("rerun after storm recovery must be empty, got %+v", cr)
	}
}
