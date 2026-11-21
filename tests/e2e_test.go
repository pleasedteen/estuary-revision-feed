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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"estuary-collector/internal/collector"
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

// restart rebuilds the collector process from on-disk state, keeping the
// mock server and the published batches in place.
func (h *harness) restart() {
	h.t.Helper()
	svc, err := service.New(service.Config{
		DataDir:      h.dir,
		MockBaseURL:  h.mockURL,
		AuthURL:      h.mockURL + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	})
	if err != nil {
		h.t.Fatalf("restart service: %v", err)
	}
	h.svc = svc
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

// maxSeq returns the current highest published sequence.
func (h *harness) maxSeq() int64 {
	h.t.Helper()
	st, err := store.Open(filepath.Join(h.dir, "batches"))
	if err != nil {
		h.t.Fatal(err)
	}
	return st.MaxSeq()
}

// charlieRow builds one raw charlie record for mock control appends.
func charlieRow(sample string, rev int64, day, mgPerL string) map[string]any {
	return map[string]any{
		"institution": "charlie", "sample_id": sample, "rev": rev,
		"sampled_on": day, "site": "河口东", "substance": "悬浮物",
		"concentration": mgPerL, "unit": "mg/L", "status": "active",
		"published_at": "2026-11-16T01:00:00Z",
	}
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

// Test12_CharlieLateAndRevisedWindows covers a late-attached window whose
// upload time sorts before windows already read, and existing windows that
// re-publish revisions in place. Every newly attached and every re-versioned
// window must be fully collected, while unchanged windows are skipped after
// only reading the manifest.
func Test12_CharlieLateAndRevisedWindows(t *testing.T) {
	h := newHarness(t)
	h.sync()

	// Two stages apply: the 2026-11-12 window opens, then 2026-11-09 (older
	// than every existing window) is attached late and the already-read
	// 2026-11-10 window re-publishes revisions in place (a plain revision
	// plus a reschedule of C-302 from 2026-11-10 to 2026-11-13).
	h.advanceStage()
	h.advanceStage()
	rep := h.sync()
	for _, r := range rep.Sources {
		if r.Status != "ok" {
			t.Fatalf("stage-1 sync source %s failed: %+v", r.Source, r)
		}
	}

	recs := materializedMap(h)
	for _, key := range []string{
		"charlie|C-290|2026-11-09", // late-attached window, sorts first
		"charlie|C-291|2026-11-09",
		"charlie|C-300|2026-11-10", // revised in place
		"charlie|C-302|2026-11-13", // rescheduled inside an already-read window
		"charlie|C-307|2026-11-12", // window opened in this stage
	} {
		if _, ok := recs[key]; !ok {
			t.Errorf("expected %s visible after stage 1", key)
		}
	}
	if _, gone := recs["charlie|C-302|2026-11-10"]; gone {
		t.Fatal("C-302 old archive day still visible after reschedule")
	}
	if recs["charlie|C-300|2026-11-10"]["revision"].(float64) != 2 {
		t.Fatalf("C-300 should be at revision 2: %v", recs["charlie|C-300|2026-11-10"])
	}

	// The in-place reschedule halves must share one batch: a batch is one
	// complete window-version pass.
	for _, env := range h.batchesAfter(0) {
		hasOld, hasNew := false, false
		for _, c := range env["changes"].([]any) {
			cm := c.(map[string]any)
			if cm["institution"] != "charlie" || cm["sample_id"] != "C-302" {
				continue
			}
			if cm["op"] == "withdraw" && cm["sampled_on"] == "2026-11-10" {
				hasOld = true
			}
			if cm["op"] == "upsert" && cm["sampled_on"] == "2026-11-13" {
				hasNew = true
			}
		}
		if hasOld != hasNew {
			t.Fatalf("charlie C-302 reschedule split across batches: envelope %v", env["batch_id"])
		}
	}

	// Unchanged windows are skipped after the manifest: the following sync
	// must read the manifest but fetch no rows at all.
	before := h.mockGet("/mock/stats")
	h.sync()
	after := h.mockGet("/mock/stats")
	manifest := after["charlie_manifest_calls"].(float64) - before["charlie_manifest_calls"].(float64)
	pages := after["charlie_page_calls"].(float64) - before["charlie_page_calls"].(float64)
	if manifest < 1 {
		t.Fatalf("expected at least one manifest read, got %v", manifest)
	}
	if pages != 0 {
		t.Fatalf("unchanged windows fetched %v pages, want 0", pages)
	}

	// The next stage revises another already-read window in place.
	h.advanceStage()
	rep = h.sync()
	if r := sourceResult(rep, mockdata.InstitutionCharlie); r.Status != "ok" {
		t.Fatalf("stage-2 charlie failed: %+v", r)
	}
	recs = materializedMap(h)
	if recs["charlie|C-303|2026-11-11"]["revision"].(float64) != 2 {
		t.Fatalf("C-303 should be revision 2: %v", recs["charlie|C-303|2026-11-11"])
	}
}

// Test13_CharlieInterruptResumeSameVersion makes a window grow to three pages,
// crashes after its second page while the saved version is still readable, and
// verifies the restart continues at the saved page instead of re-fetching the
// earlier ones. No change is published twice and old downstream bookmarks
// still read normally.
func Test13_CharlieInterruptResumeSameVersion(t *testing.T) {
	h := newHarness(t)
	h.sync()
	savedSeq := h.maxSeq()

	// Grow the 2026-11-11 window from 4 to 7 rows: three pages of two.
	appendRows := []map[string]any{
		charlieRow("C-308", 1, "2026-11-11", "6.2"),
		charlieRow("C-309", 1, "2026-11-11", "9.6"),
		charlieRow("C-310", 1, "2026-11-11", "13.4"),
	}
	h.mockPost("/mock/control/charlie/append", map[string]any{
		"window": "2026-11-11T00:00:00Z", "records": appendRows,
	})

	var charliePages atomic.Int64
	var crash sync.Once
	h.svc.Engine.SetPageHook(func(src string, _ int, _ source.Next) {
		if src != mockdata.InstitutionCharlie {
			return
		}
		if charliePages.Add(1) == 2 {
			crash.Do(func() { panic("simulated crash mid-window") })
		}
	})
	before := h.mockGet("/mock/stats")
	crashPagesBefore := before["charlie_page_calls"].(float64)
	func() {
		defer func() { _ = recover() }()
		h.sync()
	}()
	h.svc.Engine.SetPageHook(nil)

	// Restart the process; the journal and state are on disk.
	h.restart()

	// Record the next offset reported by the first page of the resumed run;
	// it must be 6 (the page read started at row 4), not 2.
	var firstResumeNext atomic.Int64
	firstResumeNext.Store(-1)
	h.svc.Engine.SetPageHook(func(src string, _ int, next source.Next) {
		if src == mockdata.InstitutionCharlie {
			firstResumeNext.CompareAndSwap(-1, int64(next.WindowOffset))
		}
	})
	before = h.mockGet("/mock/stats")
	pagesBefore := before["charlie_page_calls"].(float64)
	rep := h.sync()
	h.svc.Engine.SetPageHook(nil)
	if r := sourceResult(rep, mockdata.InstitutionCharlie); r.Status != "ok" {
		t.Fatalf("resume charlie failed: %+v", r)
	}
	after := h.mockGet("/mock/stats")
	pagesAfter := after["charlie_page_calls"].(float64)
	crashPages := pagesBefore - crashPagesBefore
	// The grown window has 7 rows in pages of 2: offsets 0,2,4,6. The crash
	// hit after page two (offset 2), so the resume continues at offset 4 and
	// reads the remaining two pages, never the first two.
	if crashPages != 2 {
		t.Fatalf("crash run read %v pages, expected 2", crashPages)
	}
	if used := pagesAfter - pagesBefore; used != 2 {
		t.Fatalf("resume read %v pages, expected the 2 remaining", used)
	}
	if got := firstResumeNext.Load(); got != 6 {
		t.Fatalf("resumed run's first page ended at offset %d, expected 6 (page read started at row 4)", got)
	}
	if r := sourceResult(rep, mockdata.InstitutionCharlie); r.WindowRestart != 0 {
		t.Fatalf("readable version should not be restarted: %+v", r)
	}

	// The three new rows are published exactly once, the old 22 remain.
	changes := 0
	for _, c := range allChanges(t, h) {
		if c["institution"] == "charlie" {
			changes++
		}
	}
	if changes != 10 {
		t.Fatalf("expected 7 base + 3 new charlie changes, got %d", changes)
	}
	if got := len(h.svc.Engine.Materialized()); got != 25 {
		t.Fatalf("expected 25 materialized records, got %d", got)
	}

	// A bookmark taken before the crash still sees every later batch.
	late := h.batchesAfter(savedSeq)
	if len(late) == 0 {
		t.Fatal("downstream position from before the crash saw no new batches")
	}
}

// Test14_CharlieVersionRetiredRestartsWindow crashes mid-window, then retires
// the saved version so the restart must begin the window again from its first
// page under the current version, still yielding one complete version per
// batch and exactly one published change per record.
func Test14_CharlieVersionRetiredRestartsWindow(t *testing.T) {
	h := newHarness(t)
	var charliePages atomic.Int64
	var crash sync.Once
	h.svc.Engine.SetPageHook(func(src string, _ int, _ source.Next) {
		if src != mockdata.InstitutionCharlie {
			return
		}
		if charliePages.Add(1) == 2 {
			crash.Do(func() { panic("simulated crash mid-window") })
		}
	})
	func() {
		defer func() { _ = recover() }()
		h.sync()
	}()
	h.svc.Engine.SetPageHook(nil)

	// The window's next version is published and its older snapshot retired.
	h.mockPost("/mock/control/charlie/append", map[string]any{
		"window": "2026-11-10T00:00:00Z",
		"records": []map[string]any{{
			"institution": "charlie", "sample_id": "C-300", "rev": 2,
			"sampled_on": "2026-11-10", "site": "河口东", "substance": "悬浮物",
			"concentration": "9.8", "unit": "mg/L", "status": "active",
			"published_at": "2026-11-13T01:00:00Z",
		}},
	})
	h.mockPost("/mock/control/charlie/expire", map[string]any{"window": "2026-11-10T00:00:00Z"})
	h.restart()

	rep := h.sync()
	cr := sourceResult(rep, mockdata.InstitutionCharlie)
	if cr.Status != "ok" {
		t.Fatalf("resume charlie failed: %+v", cr)
	}
	if cr.WindowRestart < 1 {
		t.Fatalf("expected a reported window restart, got %+v", cr)
	}
	recs := materializedMap(h)
	if len(recs) != 22 {
		t.Fatalf("expected 22 materialized records, got %d", len(recs))
	}
	if recs["charlie|C-300|2026-11-10"]["mg_per_l"] != "9.8" {
		t.Fatalf("revised C-300 not applied: %v", recs["charlie|C-300|2026-11-10"])
	}
	// Each charlie revision is published at most once: the crash between
	// pages must never cause a change to be emitted twice.
	counts := map[string]int{}
	for _, c := range allChanges(t, h) {
		if c["institution"] == "charlie" {
			k := fmt.Sprintf("%v|%v|%v", c["sample_id"], c["sampled_on"], c["revision"])
			counts[k]++
		}
	}
	for k, n := range counts {
		if n > 1 {
			t.Fatalf("charlie change %s published %d times", k, n)
		}
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
