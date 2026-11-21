// Package tests contains end-to-end tests that run the collector against
// the in-process mock institutions. No external services are required.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"estuary-collector/internal/collector"
	"estuary-collector/internal/domain"
	"estuary-collector/internal/mockdata"
	"estuary-collector/internal/mockserver"
	"estuary-collector/internal/service"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// harness bundles a mock server and a collector pointed at an isolated dir.
type harness struct {
	t       *testing.T
	mock    *mockserver.Server
	mockSrv *httptest.Server
	dir     string
	svc     *service.Service
	mockURL string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
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
		t.Fatalf("new service: %v", err)
	}
	return &harness{t: t, mock: mock, mockSrv: srv, dir: dir, svc: svc, mockURL: srv.URL}
}

func (h *harness) sync() collector.Report {
	return h.svc.Engine.Sync(context.Background())
}

func (h *harness) advanceStage() {
	h.mock.Provider.Advance()
}

func (h *harness) mockPost(path string, body any) map[string]any {
	h.t.Helper()
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	resp, err := http.Post(h.mockURL+path, "application/json", &buf)
	if err != nil {
		h.t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func (h *harness) mockGet(path string) map[string]any {
	h.t.Helper()
	resp, err := http.Get(h.mockURL + path)
	if err != nil {
		h.t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func (h *harness) batchesAfter(seq int64) []map[string]any {
	st, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		h.t.Fatal(err)
	}
	envs, err := st.ReadAfter(seq, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	out := make([]map[string]any, 0)
	for _, env := range envs {
		b, _ := json.Marshal(env)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		out = append(out, m)
	}
	return out
}

// loadState decodes the persisted collector state file.
func loadState(t *testing.T, dir string) map[string]state.Position {
	t.Helper()
	st, err := state.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]state.Position{
		"alpha":   st.Get("alpha"),
		"bravo":   st.Get("bravo"),
		"charlie": st.Get("charlie"),
	}
}

// allChanges flattens every published change.
func allChanges(t *testing.T, h *harness) []map[string]any {
	t.Helper()
	envs := h.batchesAfter(0)
	var out []map[string]any
	for _, env := range envs {
		for _, c := range env["changes"].([]any) {
			out = append(out, c.(map[string]any))
		}
	}
	return out
}

// changeByKey returns the latest matching change (a sample can legitimately
// appear more than once across runs as revisions accumulate).
func changeByKey(changes []map[string]any, inst, sample, day, op string) (map[string]any, bool) {
	var found map[string]any
	for _, c := range changes {
		if c["institution"] == inst && c["sample_id"] == sample && c["sampled_on"] == day && c["op"] == op {
			found = c
		}
	}
	return found, found != nil
}

func materializedMap(h *harness) map[string]map[string]any {
	recs := h.svc.Engine.Materialized()
	out := map[string]map[string]any{}
	for _, r := range recs {
		b, _ := json.Marshal(r)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		out[r.Institution+"|"+r.SampleID+"|"+r.SampledOn] = m
	}
	return out
}

func sourceResult(rep collector.Report, name string) collector.SourceResult {
	for _, s := range rep.Sources {
		if s.Source == name {
			return s
		}
	}
	return collector.SourceResult{}
}

// Test1_FullPipeline performs an initial sync and verifies all initial
// records from all three pagination styles land as canonical batches.
func Test1_FullPipeline(t *testing.T) {
	h := newHarness(t)
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("source %s not ok: %+v", r.Source, r)
		}
		if r.PagesFetched == 0 {
			t.Fatalf("source %s fetched no pages", r.Source)
		}
	}
	changes := allChanges(t, h)
	if len(changes) != 8+7+7 {
		t.Fatalf("expected 22 initial changes, got %d", len(changes))
	}
	recs := materializedMap(h)
	if len(recs) != 22 {
		t.Fatalf("expected 22 materialized records, got %d", len(recs))
	}
	// All concentrations are canonicalized to mg/L.
	for key, rec := range recs {
		if rec["unit"] != "mg/L" {
			t.Fatalf("%s not canonicalized to mg/L: %v", key, rec["unit"])
		}
	}
}

// Test2_MixedFormats verifies new/legacy field names and g/m3 values are
// normalized into the same canonical shape.
func Test2_MixedFormats(t *testing.T) {
	h := newHarness(t)
	h.sync()
	recs := materializedMap(h)

	alpha, ok := recs["alpha|A-102|2026-11-11"] // new format, g/m3
	if !ok {
		t.Fatal("missing alpha A-102")
	}
	if alpha["mg_per_l"] != "15" || alpha["location"] != "北滩" || alpha["material"] != "悬浮物" {
		t.Fatalf("new-format normalization wrong: %v", alpha)
	}
	bravo, ok := recs["bravo|B-200|2026-11-10"] // legacy format, mg/L
	if !ok {
		t.Fatal("missing bravo B-200")
	}
	if bravo["mg_per_l"] != "6.4" || bravo["location"] != "南港" || bravo["sample_id"] != "B-200" {
		t.Fatalf("legacy-format normalization wrong: %v", bravo)
	}
	charlie, ok := recs["charlie|C-302|2026-11-10"] // new format, g/m3
	if !ok {
		t.Fatal("missing charlie C-302")
	}
	if charlie["mg_per_l"] != "7.7" {
		t.Fatalf("g/m3 -> mg/L conversion wrong: %v", charlie)
	}
}

// Test3_RevisionAndWithdrawal applies stage 1: plain revision, plain
// withdrawal, a new sample and a reschedule.
func Test3_RevisionAndWithdrawal(t *testing.T) {
	h := newHarness(t)
	h.sync()

	h.advanceStage()
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("stage-1 sync failed: %+v", r)
		}
	}
	changes := allChanges(t, h)

	// A-100 revision 2 supersedes revision 1.
	up, ok := changeByKey(changes, "alpha", "A-100", "2026-11-10", "upsert")
	if !ok || up["revision"].(float64) != 2 {
		t.Fatalf("missing A-100 rev2 upsert: %v", ok)
	}
	// A-104 withdrawal.
	wd, ok := changeByKey(changes, "alpha", "A-104", "2026-11-11", "withdraw")
	if !ok || wd["revision"].(float64) != 2 {
		t.Fatalf("missing A-104 withdrawal at revision 2: %v", wd)
	}
	recs := materializedMap(h)
	if _, alive := recs["alpha|A-104|2026-11-11"]; alive {
		t.Fatal("withdrawn A-104 still materialized")
	}
	// New sample A-108.
	if _, ok := recs["alpha|A-108|2026-11-12"]; !ok {
		t.Fatal("missing new sample A-108")
	}
}

// Test4_RescheduleAtomic verifies old-day withdrawal and new-day insert are
// published inside the same batch (one envelope).
func Test4_RescheduleAtomic(t *testing.T) {
	for _, inst := range []struct {
		name, sample, oldDay, newDay string
	}{
		{"alpha", "A-101", "2026-11-10", "2026-11-12"},
		{"bravo", "B-203", "2026-11-11", "2026-11-13"},
		{"charlie", "C-301", "2026-11-10", "2026-11-14"},
	} {
		t.Run(inst.name, func(t *testing.T) {
			h := newHarness(t)
			h.sync()
			h.advanceStage()
			h.sync()

			envs := h.batchesAfter(0)
			found := false
			for _, env := range envs {
				hasOld, hasNew := false, false
				for _, c := range env["changes"].([]any) {
					cm := c.(map[string]any)
					if cm["sample_id"] != inst.sample {
						continue
					}
					if cm["op"] == "withdraw" && cm["sampled_on"] == inst.oldDay {
						hasOld = true
					}
					if cm["op"] == "upsert" && cm["sampled_on"] == inst.newDay {
						hasNew = true
					}
				}
				if hasOld && hasNew {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s reschedule halves not in the same batch", inst.name)
			}
			recs := materializedMap(h)
			if _, old := recs[inst.name+"|"+inst.sample+"|"+inst.oldDay]; old {
				t.Fatalf("%s old-day entry still visible", inst.sample)
			}
			if _, neu := recs[inst.name+"|"+inst.sample+"|"+inst.newDay]; !neu {
				t.Fatalf("%s new-day entry missing", inst.sample)
			}
		})
	}
}

// Test5_IdempotentRerun verifies a second run emits nothing new.
func Test5_IdempotentRerun(t *testing.T) {
	h := newHarness(t)
	h.sync()
	firstCount := len(allChanges(t, h))
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Changes != 0 || r.Status != "ok" {
			t.Fatalf("rerun should be a no-op, got %+v", r)
		}
	}
	if got := len(allChanges(t, h)); got != firstCount {
		t.Fatalf("rerun changed change count: %d -> %d", firstCount, got)
	}
}

// Test6_DownstreamReadPosition verifies downstream consumers can read from
// a saved sequence and continue later without losing or repeating changes.
func Test6_DownstreamReadPosition(t *testing.T) {
	h := newHarness(t)
	h.sync()
	st, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.ReadAfter(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("limit=1 should return one envelope, got %d", len(first))
	}
	saved := first[0].Seq - 1 // position before first envelope
	page1, err := st.ReadAfter(saved, 0)
	if err != nil {
		t.Fatal(err)
	}
	var maxSeq int64
	for _, env := range page1 {
		for _, c := range env.Changes {
			maxSeq = max(maxSeq, c.Seq)
		}
	}
	// Continue from the saved position after stage 1 adds more changes.
	h.advanceStage()
	h.sync()
	page2, err := st.ReadAfter(maxSeq, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) == 0 {
		t.Fatal("expected new batches after advancing the stage")
	}
	// Nothing is re-returned when the position is already current. A fresh
	// store handle is used so its sequence counter reflects every batch.
	finalStore, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	page3, err := finalStore.ReadAfter(finalStore.MaxSeq(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 0 {
		t.Fatalf("reading at max seq should be empty, got %d envelopes", len(page3))
	}
}

// Test7_CursorExpiryReplay verifies an expired bravo cursor restarts from
// the head, replays old revisions without duplicate changes, and that an
// old revision cannot resurrect a withdrawn record.
func Test7_CursorExpiryReplay(t *testing.T) {
	h := newHarness(t)
	// Capture a mid-log bravo cursor during the initial synchronization; the
	// end-of-log cursor is empty, so a cursor seen while paging is used.
	var seenCursor string
	var seenOffset int
	h.svc.Engine.SetPageHook(func(src string, _ int, next source.Next) {
		if src == mockdata.InstitutionBravo && next.Cursor != "" && seenCursor == "" {
			seenCursor = next.Cursor
			seenOffset = next.CursorOffset
		}
	})
	h.sync()
	h.svc.Engine.SetPageHook(nil)
	if seenCursor == "" {
		t.Fatal("expected to observe a non-empty bravo cursor while paging")
	}

	// Stage 1 withdraws B-201; stage 2 re-publishes its stale rev1 (active).
	h.advanceStage()
	rep := h.sync()
	if r := sourceResult(rep, mockdata.InstitutionBravo); r.Status != "ok" {
		t.Fatalf("bravo stage1: %+v", r)
	}
	// Force the persisted position back to the captured mid-log cursor and
	// mark it expired, then advance to stage 2.
	posStore, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := posStore.Update(mockdata.InstitutionBravo,
		state.Position{Cursor: seenCursor, CursorOffset: seenOffset}); err != nil {
		t.Fatal(err)
	}
	h.mockPost("/mock/control/cursor/expire", map[string]string{"cursor": seenCursor})
	// Stage 2 appends the stale B-201 rev1; stage 3 appends the fresh
	// B-200 rev2. Both are read during the head replay below.
	h.advanceStage()
	h.advanceStage()

	rep = h.sync()
	br := sourceResult(rep, mockdata.InstitutionBravo)
	if br.Status != "ok" {
		t.Fatalf("bravo after expiry: %+v", br)
	}
	if !br.CursorRestart {
		t.Fatal("expected a cursor restart to be reported")
	}
	// Only the fresh B-200 revision is a change; every replayed old revision
	// (including the stale active B-201 rev1) is absorbed.
	if br.Changes != 1 {
		t.Fatalf("head replay must only emit the fresh B-200 revision, got %d changes", br.Changes)
	}
	recs := materializedMap(h)
	if _, alive := recs["bravo|B-201|2026-11-10"]; alive {
		t.Fatal("stale active revision resurrected withdrawn B-201")
	}
	if recs["bravo|B-200|2026-11-10"]["revision"].(float64) != 2 {
		t.Fatalf("B-200 should be at revision 2: %v", recs["bravo|B-200|2026-11-10"])
	}
}

// Test8_TokenSingleRefresh invalidates the shared access token so that both
// token-sharing clients (alpha, charlie) hit 401 inside one run and asserts
// the auth endpoint sees exactly one refresh for the generation.
func Test8_TokenSingleRefresh(t *testing.T) {
	h := newHarness(t)
	// First run obtains access-gen1 and reaches the end of every source.
	h.sync()

	statsBefore := h.mockGet("/mock/stats")
	before := int64(statsBefore["refresh_count"].(float64))

	// Invalidate access-gen1 out of band: the auth endpoint now only accepts
	// access-gen2, which must be obtained through one refresh call.
	h.mockPost("/auth/token", map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     mockserver.SharedClient,
		"refresh_token": "refresh-token-v1",
	})

	// Clear the persisted read positions before rebuilding the collector so
	// the new engine re-reads every source from the head with the stale
	// token. Published batches stay in place, so the replay emits no changes.
	posStore, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		if err := posStore.Update(name, state.Position{}); err != nil {
			t.Fatal(err)
		}
	}

	// Rebuild the collector process: on-disk positions survive, while the
	// in-memory token manager still holds the now-dead access-gen1.
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

	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" || r.Changes != 0 {
			t.Fatalf("token-refresh run should be an empty ok run: %+v", r)
		}
	}
	statsAfter := h.mockGet("/mock/stats")
	after := int64(statsAfter["refresh_count"].(float64))
	// One generation was rotated manually above; the collector's own run may
	// add exactly one more refresh (coalesced across alpha and charlie).
	if after-before != 2 {
		t.Fatalf("expected manual refresh plus exactly one collector refresh, got diff %d (before=%d after=%d)", after-before, before, after)
	}
}

// Test9_RateLimitIsolation throttles alpha and verifies bravo and charlie
// still complete successfully in the same run.
func Test9_RateLimitIsolation(t *testing.T) {
	h := newHarness(t)
	h.sync()
	h.advanceStage()

	h.mockPost("/mock/control/ratelimit?source=alpha&on=true", map[string]any{})
	rep := h.sync()

	ar := sourceResult(rep, mockdata.InstitutionAlpha)
	if ar.Status != "rate_limited" {
		t.Fatalf("alpha should be rate limited, got %+v", ar)
	}
	for _, name := range []string{mockdata.InstitutionBravo, mockdata.InstitutionCharlie} {
		r := sourceResult(rep, name)
		if r.Status != "ok" {
			t.Fatalf("%s should keep running while alpha is throttled: %+v", name, r)
		}
	}
	// Lifting the limit lets alpha catch up without affecting the others.
	h.mockPost("/mock/control/ratelimit?source=alpha&on=false", map[string]any{})
	rep = h.sync()
	if r := sourceResult(rep, mockdata.InstitutionAlpha); r.Status != "ok" {
		t.Fatalf("alpha recovery failed: %+v", r)
	}
}

// Test10_InterruptResume simulates a crash after a few pages and verifies a
// restart resumes from the saved position, downstream cursors stay valid and
// no change is emitted twice.
func Test10_InterruptResume(t *testing.T) {
	h := newHarness(t)
	var crash sync.Once
	var pages atomic.Int64
	h.svc.Engine.SetPageHook(func(string, int, source.Next) {
		n := pages.Add(1)
		if n == 4 {
			crash.Do(func() {
				panic("simulated crash mid-sync")
			})
		}
	})
	func() {
		defer func() { _ = recover() }()
		h.sync()
	}()
	h.svc.Engine.SetPageHook(nil)

	// Restart the process: rebuild everything from on-disk state.
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

	batchStore, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	seqBefore := batchStore.MaxSeq()

	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("resume run not ok: %+v", r)
		}
	}
	// A final idempotent run proves no revision is ever re-published.
	rep2 := h.sync()
	for _, r := range rep2.Sources {
		if r.Changes != 0 {
			t.Fatalf("post-resume rerun not empty: %+v", r)
		}
	}
	recs := h.svc.Engine.Materialized()
	if len(recs) != 22 {
		t.Fatalf("expected all 22 initial records after resume, got %d", len(recs))
	}
	// Downstream position saved before the crash still reads correctly.
	envs, err := batchStore.ReadAfter(seqBefore, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) == 0 {
		t.Fatal("expected remaining batches to be readable from pre-crash position")
	}
}

// Test11_AllStagesAcrossRestarts drives every stage, restarting the service
// between runs, and checks the final materialized truth end to end.
func Test11_AllStagesAcrossRestarts(t *testing.T) {
	h := newHarness(t)
	restart := func() {
		svc, err := service.New(service.Config{
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
		h.svc = svc
	}

	h.sync()
	for stage := 1; stage <= 3; stage++ {
		h.advanceStage()
		restart()
		rep := h.sync()
		for _, r := range rep.Sources {
			if r.Status != "ok" {
				t.Fatalf("stage %d source %s failed: %+v", stage, r.Source, r)
			}
		}
	}
	recs := materializedMap(h)

	expectVisible := []string{
		"alpha|A-100|2026-11-10", // rev2
		"alpha|A-101|2026-11-12", // rescheduled
		"alpha|A-102|2026-11-11", // rev2
		"alpha|A-105|2026-11-11", // rev2
		"alpha|A-108|2026-11-12",
		"bravo|B-203|2026-11-13", // rescheduled
		"bravo|B-207|2026-11-12",
		"charlie|C-301|2026-11-14", // rescheduled
		"charlie|C-307|2026-11-12",
	}
	for _, key := range expectVisible {
		if _, ok := recs[key]; !ok {
			t.Errorf("expected %s visible", key)
		}
	}
	expectGone := []string{
		"alpha|A-104|2026-11-11", // withdrawn
		"alpha|A-101|2026-11-10", // old day after reschedule
		"bravo|B-201|2026-11-10", // withdrawn, stale replay blocked
		"bravo|B-203|2026-11-11", // old day after reschedule
		"charlie|C-301|2026-11-10",
	}
	for _, key := range expectGone {
		if _, ok := recs[key]; ok {
			t.Errorf("expected %s absent", key)
		}
	}
	if recs["alpha|A-102|2026-11-11"]["mg_per_l"] != "16.3" {
		t.Errorf("A-102 rev2 concentration wrong: %v", recs["alpha|A-102|2026-11-11"])
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// charlieFetches returns the served charlie page requests as "window@offset"
// strings for compact assertions.
func charlieFetches(h *harness) []string {
	var out []string
	for _, f := range h.mock.Provider.CharlieFetches() {
		out = append(out, f.Window+"@"+fmt.Sprint(f.Offset))
	}
	return out
}

// syncThroughStages syncs stage 0 and then advances and syncs n times.
func syncThroughStages(t *testing.T, h *harness, n int) {
	t.Helper()
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("stage-0 sync failed: %+v", r)
		}
	}
	for i := 0; i < n; i++ {
		h.advanceStage()
		rep = h.sync()
		for _, r := range rep.Sources {
			if r.Status != "ok" {
				t.Fatalf("stage-%d sync failed: %+v", i+1, r)
			}
		}
	}
}

// Test12_CharlieBackfillAndOldWindowAppend drives stage 4: charlie registers
// an upload window that sorts before every known window and appends detail
// rows to an already-consumed window. The round must request only the new
// ranges, and the appended cross-page reschedule must leave in one batch.
func Test12_CharlieBackfillAndOldWindowAppend(t *testing.T) {
	h := newHarness(t)
	syncThroughStages(t, h, 3)

	h.mock.Provider.ResetCharlieFetches()
	batchStore, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	seqBefore := batchStore.MaxSeq()

	h.advanceStage() // stage 4: backfill window 11-09 + append to window 11-10
	rep := h.sync()
	cr := sourceResult(rep, mockdata.InstitutionCharlie)
	if cr.Status != "ok" {
		t.Fatalf("charlie stage-4 sync failed: %+v", cr)
	}

	// Only the new ranges are requested: the whole back-registered window
	// (2 rows, one page) and offsets 3..6 of the appended old window.
	want := []string{
		"2026-11-09T00:00:00Z@0",
		"2026-11-10T00:00:00Z@3",
		"2026-11-10T00:00:00Z@5",
	}
	if got := charlieFetches(h); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("charlie fetches = %v, want %v", got, want)
	}

	// Five changes in exactly one new envelope: two backfilled samples, the
	// appended revision and the two halves of the appended reschedule.
	envs := h.batchesAfter(seqBefore)
	if len(envs) != 1 {
		t.Fatalf("expected exactly one new batch, got %d", len(envs))
	}
	changes := envs[0]["changes"].([]any)
	if len(changes) != 5 {
		t.Fatalf("expected 5 changes in the batch, got %d", len(changes))
	}
	hasOld, hasNew := false, false
	for _, c := range changes {
		cm := c.(map[string]any)
		if cm["institution"] != "charlie" || cm["sample_id"] != "C-302" {
			continue
		}
		if cm["op"] == "withdraw" && cm["sampled_on"] == "2026-11-10" {
			hasOld = true
		}
		if cm["op"] == "upsert" && cm["sampled_on"] == "2026-11-15" {
			hasNew = true
		}
	}
	if !hasOld || !hasNew {
		t.Fatal("cross-page reschedule halves of C-302 not in the same batch")
	}

	recs := materializedMap(h)
	for _, key := range []string{"charlie|C-308|2026-11-09", "charlie|C-309|2026-11-09"} {
		if _, ok := recs[key]; !ok {
			t.Fatalf("backfilled record %s missing", key)
		}
	}
	if _, old := recs["charlie|C-302|2026-11-10"]; old {
		t.Fatal("C-302 old-day entry still visible after appended reschedule")
	}
	if _, ok := recs["charlie|C-302|2026-11-15"]; !ok {
		t.Fatal("C-302 new-day entry missing after appended reschedule")
	}
	if got := recs["charlie|C-300|2026-11-10"]["mg_per_l"]; got != "9.8" {
		t.Fatalf("appended revision of C-300 not applied: %v", got)
	}

	// The next round is a no-op: no fetches, no changes.
	h.mock.Provider.ResetCharlieFetches()
	rep = h.sync()
	if cr := sourceResult(rep, mockdata.InstitutionCharlie); cr.Changes != 0 {
		t.Fatalf("rerun after catch-up produced %d changes", cr.Changes)
	}
	if got := charlieFetches(h); len(got) != 0 {
		t.Fatalf("rerun after catch-up fetched pages: %v", got)
	}
}

// Test13_CharlieAppendDuringFetch verifies rows appended to a window while a
// round is mid-flight are left for the next round, and the next round
// requests only the appended range.
func Test13_CharlieAppendDuringFetch(t *testing.T) {
	h := newHarness(t)
	// After the first served page, one generated row (C-800) lands in the
	// 3-row window 11-10 — while the round is fetching it.
	h.mock.Provider.ArmCharlieAppend("2026-11-10T00:00:00Z", 1)

	rep := h.sync()
	cr := sourceResult(rep, mockdata.InstitutionCharlie)
	if cr.Status != "ok" || cr.Changes != 7 {
		t.Fatalf("first sync should publish the 7 snapshot rows, got %+v", cr)
	}
	if _, ok := materializedMap(h)["charlie|C-800|2026-11-10"]; ok {
		t.Fatal("row appended mid-fetch must be left for the next round")
	}
	want := []string{
		"2026-11-10T00:00:00Z@0",
		"2026-11-10T00:00:00Z@2",
		"2026-11-11T00:00:00Z@0",
		"2026-11-11T00:00:00Z@2",
	}
	if got := charlieFetches(h); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("first-round fetches = %v, want %v", got, want)
	}

	// Next round: only the appended range (offset 3) of the old window is
	// requested, and the late row is published.
	h.mock.Provider.ResetCharlieFetches()
	rep = h.sync()
	cr = sourceResult(rep, mockdata.InstitutionCharlie)
	if cr.Status != "ok" || cr.Changes != 1 {
		t.Fatalf("second sync should publish only the late row, got %+v", cr)
	}
	want = []string{"2026-11-10T00:00:00Z@3"}
	if got := charlieFetches(h); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("second-round fetches = %v, want %v", got, want)
	}
	if _, ok := materializedMap(h)["charlie|C-800|2026-11-10"]; !ok {
		t.Fatal("late row missing after the follow-up round")
	}
}

// Test14_CharlieCheckpointResume crashes the charlie run between two pages
// of an appended window and verifies the restart resumes at the missing
// page, and that the changes derived before the crash are published in the
// same single batch as the ones derived after it.
func Test14_CharlieCheckpointResume(t *testing.T) {
	h := newHarness(t)
	syncThroughStages(t, h, 3)
	h.advanceStage() // stage 4: backfill window + appended cross-page reschedule

	// Crash after the second charlie page of the round (the page carrying
	// the first half of the C-302 reschedule).
	var crash sync.Once
	h.svc.Engine.SetPageHook(func(src string, pageNo int, _ source.Next) {
		if src == mockdata.InstitutionCharlie && pageNo == 1 {
			crash.Do(func() { panic("simulated crash mid-window") })
		}
	})
	rep := h.sync()
	h.svc.Engine.SetPageHook(nil)
	if cr := sourceResult(rep, mockdata.InstitutionCharlie); cr.Status != "failed" {
		t.Fatalf("expected crashed charlie run, got %+v", cr)
	}

	// The crash left a checkpoint: journaled changes under a reserved batch id.
	st, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	pending := st.Pending(mockdata.InstitutionCharlie)
	if len(pending.Changes) == 0 || pending.BatchID == "" {
		t.Fatalf("expected a pending journal after the crash, got %+v", pending)
	}
	batchStore, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	seqBefore := batchStore.MaxSeq()

	// Restart the process and resume.
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
	h.mock.Provider.ResetCharlieFetches()
	rep = h.sync()
	cr := sourceResult(rep, mockdata.InstitutionCharlie)
	if cr.Status != "ok" {
		t.Fatalf("resume run failed: %+v", cr)
	}

	// Only the missing page is fetched — no rescan of completed windows.
	want := []string{"2026-11-10T00:00:00Z@5"}
	if got := charlieFetches(h); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("resume fetches = %v, want %v", got, want)
	}

	// Everything — pre-crash and post-resume changes alike — lands in one
	// batch under the batch id reserved before the crash.
	envs := h.batchesAfter(seqBefore)
	if len(envs) != 1 {
		t.Fatalf("expected exactly one resumed batch, got %d", len(envs))
	}
	if envs[0]["batch_id"] != pending.BatchID {
		t.Fatalf("resumed batch id %v != journaled %v", envs[0]["batch_id"], pending.BatchID)
	}
	changes := envs[0]["changes"].([]any)
	if len(changes) != 5 {
		t.Fatalf("resumed batch should hold all 5 changes, got %d", len(changes))
	}
	hasOld, hasNew := false, false
	for _, c := range changes {
		cm := c.(map[string]any)
		if cm["sample_id"] != "C-302" {
			continue
		}
		if cm["op"] == "withdraw" && cm["sampled_on"] == "2026-11-10" {
			hasOld = true
		}
		if cm["op"] == "upsert" && cm["sampled_on"] == "2026-11-15" {
			hasNew = true
		}
	}
	if !hasOld || !hasNew {
		t.Fatal("reschedule halves separated by the crash not read back from the same batch")
	}
	if _, ok := materializedMap(h)["charlie|C-302|2026-11-15"]; !ok {
		t.Fatal("C-302 new-day entry missing after resume")
	}

	// The journal is cleared and a rerun is a no-op.
	st2, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if p := st2.Pending(mockdata.InstitutionCharlie); !p.Empty() {
		t.Fatalf("pending journal not cleared after resume: %+v", p)
	}
	rep = h.sync()
	if cr := sourceResult(rep, mockdata.InstitutionCharlie); cr.Changes != 0 {
		t.Fatalf("post-resume rerun produced %d changes", cr.Changes)
	}
}

// Test16_CharliePublishedPendingNotReemitted simulates a crash in the narrow
// window between publishing the batch and clearing the pending journal: the
// journal still holds an already-published change. The next run must drop it
// instead of publishing it a second time.
func Test16_CharliePublishedPendingNotReemitted(t *testing.T) {
	h := newHarness(t)
	h.sync()

	// Take an already-published charlie change and plant it into the pending
	// journal, as a crash between publish and journal-clear would leave it.
	var dup map[string]any
	for _, c := range allChanges(t, h) {
		if c["institution"] == "charlie" && c["sample_id"] == "C-300" {
			dup = c
			break
		}
	}
	if dup == nil {
		t.Fatal("expected a published charlie C-300 change")
	}
	b, _ := json.Marshal(dup)
	var ch domain.Change
	if err := json.Unmarshal(b, &ch); err != nil {
		t.Fatal(err)
	}
	st, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	pos := st.Get(mockdata.InstitutionCharlie)
	if err := st.Checkpoint(mockdata.InstitutionCharlie, pos,
		state.Pending{BatchID: "charlie-crash-window", Changes: []domain.Change{ch}}); err != nil {
		t.Fatal(err)
	}

	// Restart: the journaled change is already reflected in the replayed
	// batch history and must be dropped, and the journal cleared.
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
	rep := h.sync()
	if cr := sourceResult(rep, mockdata.InstitutionCharlie); cr.Status != "ok" || cr.Changes != 0 {
		t.Fatalf("already-published journaled change must not be re-emitted: %+v", cr)
	}
	if got := len(allChanges(t, h)); got != 22 {
		t.Fatalf("published change was re-emitted from the journal: total %d, want 22", got)
	}
	st2, err := state.Open(filepath.Join(h.dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if p := st2.Pending(mockdata.InstitutionCharlie); !p.Empty() {
		t.Fatalf("journal not cleared after deduplicated reload: %+v", p)
	}
}

// Test15_CharlieStateUpgrade opens a data directory whose state file was
// written by the previous version (window index + inner offset) and verifies
// the first run after the upgrade re-reads nothing and republishes nothing,
// while later stages still flow.
func Test15_CharlieStateUpgrade(t *testing.T) {
	h := newHarness(t)
	h.sync() // stage 0 published with the current format

	// Rewrite the charlie position in the legacy v1 shape: both windows of
	// the stage-0 directory fully consumed (index past the end).
	statePath := filepath.Join(h.dir, "state", "state.json")
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["positions"].(map[string]any)["charlie"] = map[string]any{"window_index": 2, "window_offset": 0}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// First run after the upgrade: the legacy position is migrated against
	// the live directory, nothing is re-fetched or republished.
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
	h.mock.Provider.ResetCharlieFetches()
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" || r.Changes != 0 {
			t.Fatalf("upgrade run should be an empty ok run: %+v", r)
		}
	}
	if got := charlieFetches(h); len(got) != 0 {
		t.Fatalf("upgrade run re-fetched pages: %v", got)
	}
	if got := len(allChanges(t, h)); got != 22 {
		t.Fatalf("upgrade republished changes: total %d, want 22", got)
	}
	// The persisted position is now the per-window map.
	pos := loadState(t, h.dir)["charlie"]
	if pos.Windows == nil || pos.Windows["2026-11-10T00:00:00Z"] != 3 || pos.Windows["2026-11-11T00:00:00Z"] != 4 {
		t.Fatalf("legacy position not migrated to per-window map: %+v", pos)
	}

	// Later stages still flow through the migrated position.
	syncThroughStages(t, h, 4)
	recs := materializedMap(h)
	if _, ok := recs["charlie|C-308|2026-11-09"]; !ok {
		t.Fatal("backfilled window not picked up after upgrade")
	}
	if _, ok := recs["charlie|C-302|2026-11-15"]; !ok {
		t.Fatal("appended reschedule not picked up after upgrade")
	}
}

// TestHTTPLayer exercises the real HTTP mux including conflict and
// downstream read endpoints.
func TestHTTPLayer(t *testing.T) {
	h := newHarness(t)
	httpSrv := httptest.NewServer(h.svc.Handler())
	defer httpSrv.Close()

	post := func(path string) (int, map[string]any) {
		resp, err := http.Post(httpSrv.URL+path, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(httpSrv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	status, rep := post("/v1/sync")
	if status != http.StatusOK {
		t.Fatalf("sync status %d: %v", status, rep)
	}
	// First page is capped by limit; continuation starts at its last seq.
	status, batches := get("/v1/batches?after_seq=0&limit=2")
	if status != http.StatusOK || batches["count"].(float64) == 0 {
		t.Fatalf("batches endpoint wrong: %d %v", status, batches)
	}
	// Fetch without a limit to reach the current end of the stream.
	status, all := get("/v1/batches?after_seq=0")
	if status != http.StatusOK || all["count"].(float64) == 0 {
		t.Fatalf("full batches endpoint wrong: %d %v", status, all)
	}
	nextSeq := int64(all["next_seq"].(float64))
	status, page2 := get(fmt.Sprintf("/v1/batches?after_seq=%d", nextSeq))
	if status != http.StatusOK {
		t.Fatalf("continuation failed: %d", status)
	}
	if len(page2["batches"].([]any)) != 0 {
		t.Fatal("continuation at current position should be empty")
	}
	status, _ = get("/v1/records")
	if status != http.StatusOK {
		t.Fatalf("records endpoint: %d", status)
	}
	status, _ = get("/v1/status")
	if status != http.StatusOK {
		t.Fatalf("status endpoint: %d", status)
	}

	// Concurrent sync: hold one run inside the engine, then confirm a
	// second request is rejected with 409 while the first is still active.
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	h.svc.Engine.SetPageHook(func(string, int, source.Next) {
		enterOnce.Do(func() {
			close(entered)
			<-release // keep the first run in flight
		})
	})
	wg.Add(2)
	go func() { defer wg.Done(); c, _ := post("/v1/sync"); codes <- c }()
	go func() {
		defer wg.Done()
		<-entered
		// Poll briefly until the engine advertises the in-flight run.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if st, body := get("/v1/status"); st == http.StatusOK && body["syncing"] == true {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		c, _ := post("/v1/sync")
		codes <- c
		close(release)
	}()
	wg.Wait()
	h.svc.Engine.SetPageHook(nil)
	close(codes)
	got := sort.IntSlice{}
	for c := range codes {
		got = append(got, c)
	}
	sort.Ints(got)
	if len(got) != 2 || got[0] != http.StatusOK || got[1] != http.StatusConflict {
		t.Fatalf("expected one 200 and one 409 for concurrent sync, got %v", got)
	}
}

var _ = source.Page{}
