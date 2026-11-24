// Run-journal acceptance tests: per-round source progress query, failure
// stages, persistence-boundary stops and restart behavior.
package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"estuary-collector/internal/domain"
	"estuary-collector/internal/journal"
	"estuary-collector/internal/mockdata"
	"estuary-collector/internal/mockserver"
	"estuary-collector/internal/service"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// restart rebuilds the service from the on-disk state, simulating a process
// restart (batches, positions and the run journal survive; memory does not).
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
		h.t.Fatal(err)
	}
	h.svc = svc
}

// runSource returns the journal entry of one source within a round.
func runSource(t *testing.T, run journal.Run, name string) journal.SourceRun {
	t.Helper()
	for _, s := range run.Sources {
		if s.Source == name {
			return s
		}
	}
	t.Fatalf("round %d has no entry for source %s", run.Round, name)
	return journal.SourceRun{}
}

// runsResponse is the typed shape of GET /v1/sync/runs.
type runsResponse struct {
	Syncing      bool          `json:"syncing"`
	CurrentRound int64         `json:"current_round"`
	Runs         []journal.Run `json:"runs"`
}

// Test12_RunQueryRateLimitSecondPage arms the mock so alpha is throttled
// from its second page on, then checks the run journal over HTTP: the
// throttled source shows its failure stage and fetched pages, the other
// sources are fully done, and recovery re-reads everything.
func Test12_RunQueryRateLimitSecondPage(t *testing.T) {
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
	getRuns := func(path string) (int, runsResponse) {
		resp, err := http.Get(httpSrv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out runsResponse
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// Alpha answers its first page, then 429 from the second page on.
	h.mockPost("/mock/control/ratelimit_page?source=alpha&after=1", map[string]any{})

	status, _ := post("/v1/sync")
	if status != http.StatusPartialContent {
		t.Fatalf("sync with a throttled source should be 206, got %d", status)
	}

	status, body := getRuns("/v1/sync/runs")
	if status != http.StatusOK {
		t.Fatalf("runs endpoint: %d", status)
	}
	if body.CurrentRound != 1 || len(body.Runs) != 1 {
		t.Fatalf("current_round=%d runs=%d, want 1/1", body.CurrentRound, len(body.Runs))
	}
	round := body.Runs[0]
	if round.Status != journal.RunDone {
		t.Fatalf("round status = %q, want done", round.Status)
	}

	// The throttled source: stopped in the fetch stage after one page,
	// nothing published, resume position not advanced.
	alpha := runSource(t, round, mockdata.InstitutionAlpha)
	if alpha.Status != journal.StatusRateLimited {
		t.Fatalf("alpha status = %q, want rate_limited", alpha.Status)
	}
	if alpha.Stage != journal.StageFetch {
		t.Fatalf("alpha failure stage = %q, want fetch", alpha.Stage)
	}
	if alpha.PagesFetched != 1 || alpha.RecordsSeen != 3 {
		t.Fatalf("alpha progress = %d pages/%d records, want 1/3", alpha.PagesFetched, alpha.RecordsSeen)
	}
	if alpha.BatchID != "" || alpha.PositionSaved {
		t.Fatalf("alpha must not publish or checkpoint: %+v", alpha)
	}

	// The other two sources completed with a published batch and a saved
	// resume position each.
	for _, name := range []string{mockdata.InstitutionBravo, mockdata.InstitutionCharlie} {
		sr := runSource(t, round, name)
		if sr.Status != journal.StatusOK || sr.Stage != journal.StageDone {
			t.Fatalf("%s = %q/%q, want ok/done", name, sr.Status, sr.Stage)
		}
		if sr.BatchID == "" || sr.BatchSeq == 0 || sr.BatchChanges == 0 {
			t.Fatalf("%s missing published batch: %+v", name, sr)
		}
		if !sr.PositionSaved || sr.Position == nil {
			t.Fatalf("%s missing saved position: %+v", name, sr)
		}
	}

	// A specific round can be queried; unknown rounds are 404.
	resp1, err := http.Get(httpSrv.URL + "/v1/sync/runs?round=1")
	if err != nil {
		t.Fatal(err)
	}
	var single struct {
		Syncing bool        `json:"syncing"`
		Run     journal.Run `json:"run"`
	}
	_ = json.NewDecoder(resp1.Body).Decode(&single)
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK || single.Run.Round != 1 {
		t.Fatalf("round=1 query: %d %+v", resp1.StatusCode, single)
	}
	if sr := runSource(t, single.Run, mockdata.InstitutionAlpha); sr.Status != journal.StatusRateLimited {
		t.Fatalf("round=1 alpha = %q, want rate_limited", sr.Status)
	}
	resp, err := http.Get(httpSrv.URL + "/v1/sync/runs?round=99")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("round=99 should be 404, got %d", resp.StatusCode)
	}

	// Lift the quota: alpha catches up and re-reads everything it had seen
	// before the throttle (no record is lost downstream).
	h.mockPost("/mock/control/ratelimit_page?source=alpha&after=-1", map[string]any{})
	status, _ = post("/v1/sync")
	if status != http.StatusOK {
		t.Fatalf("recovery sync should be 200, got %d", status)
	}
	_, body = getRuns("/v1/sync/runs")
	if body.CurrentRound != 2 {
		t.Fatalf("current_round = %d, want 2", body.CurrentRound)
	}
	alpha2 := runSource(t, body.Runs[0], mockdata.InstitutionAlpha)
	if alpha2.Status != journal.StatusOK || alpha2.Changes != 8 || !alpha2.PositionSaved {
		t.Fatalf("alpha recovery = %+v, want ok with 8 changes and saved position", alpha2)
	}
	// Downstream sees every alpha record exactly once.
	seen := map[string]int64{}
	for _, c := range allChanges(t, h) {
		if c["institution"] == "alpha" {
			seen[c["sample_id"].(string)] = int64(c["revision"].(float64))
		}
	}
	if len(seen) != 8 {
		t.Fatalf("downstream saw %d distinct alpha samples, want 8", len(seen))
	}
}

// Test13_RunQueryWhileSourceStuck hangs one source in the mock and verifies
// the run query keeps answering with the other sources' live progress.
func Test13_RunQueryWhileSourceStuck(t *testing.T) {
	h := newHarness(t)
	httpSrv := httptest.NewServer(h.svc.Handler())
	defer httpSrv.Close()

	h.mockPost("/mock/control/hang?source=alpha&on=true", map[string]any{})

	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		h.svc.Engine.Sync(context.Background())
	}()

	// While alpha is stuck, the query endpoint must answer and show bravo
	// and charlie done. Poll until the journal reflects that.
	deadline := time.Now().Add(10 * time.Second)
	var snap runsResponse
	for {
		resp, err := http.Get(httpSrv.URL + "/v1/sync/runs")
		if err != nil {
			t.Fatalf("query while source stuck: %v", err)
		}
		var body runsResponse
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		snap = body
		if len(body.Runs) == 1 {
			bravo := runSource(t, body.Runs[0], mockdata.InstitutionBravo)
			charlie := runSource(t, body.Runs[0], mockdata.InstitutionCharlie)
			if bravo.Status == journal.StatusOK && charlie.Status == journal.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("other sources did not finish while alpha was stuck: %+v", body.Runs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !snap.Syncing {
		t.Fatal("syncing flag should be true while a source is stuck")
	}
	round := snap.Runs[0]
	if round.Status != journal.RunRunning {
		t.Fatalf("round status = %q, want running", round.Status)
	}
	alpha := runSource(t, round, mockdata.InstitutionAlpha)
	if alpha.Status != journal.StatusRunning || alpha.Stage != journal.StageFetch {
		t.Fatalf("stuck alpha = %q/%q, want running/fetch", alpha.Status, alpha.Stage)
	}
	if alpha.PagesFetched != 0 {
		t.Fatalf("stuck alpha fetched %d pages, want 0", alpha.PagesFetched)
	}
	// The finished sources already show batch + saved position.
	bravo := runSource(t, round, mockdata.InstitutionBravo)
	if bravo.BatchID == "" || !bravo.PositionSaved {
		t.Fatalf("bravo should be fully done while alpha is stuck: %+v", bravo)
	}

	// Release the source; the round completes.
	h.mockPost("/mock/control/hang?source=alpha&on=false", map[string]any{})
	select {
	case <-syncDone:
	case <-time.After(15 * time.Second):
		t.Fatal("sync did not finish after lifting the hang")
	}
	_, runs := h.svc.Engine.Runs()
	alpha = runSource(t, runs[0], mockdata.InstitutionAlpha)
	if alpha.Status != journal.StatusOK || !alpha.PositionSaved {
		t.Fatalf("alpha after release = %+v, want ok with saved position", alpha)
	}
}

// Test14_RunBoundariesAcrossRestart stops the process at each persistence
// boundary and verifies the run journal — read after restart, with the
// original round numbering — shows exactly where the round stopped.
func Test14_RunBoundariesAcrossRestart(t *testing.T) {
	// (a) Stop before publishing: alpha dies mid-fetch. After the restart
	// the round shows fetched pages but no batch and no saved position.
	t.Run("before_publish", func(t *testing.T) {
		h := newHarness(t)
		var once sync.Once
		h.svc.Engine.SetPageHook(func(src string, pageNo int, _ source.Next) {
			if src == mockdata.InstitutionAlpha && pageNo == 1 {
				once.Do(func() { panic("simulated crash before publish") })
			}
		})
		h.sync()
		h.restart()

		_, runs := h.svc.Engine.Runs()
		if len(runs) != 1 || runs[0].Round != 1 {
			t.Fatalf("expected round 1 after restart, got %+v", runs)
		}
		alpha := runSource(t, runs[0], mockdata.InstitutionAlpha)
		if alpha.Status != journal.StatusFailed {
			t.Fatalf("alpha status = %q, want failed", alpha.Status)
		}
		if alpha.Stage != journal.StageFetch || alpha.PagesFetched != 2 {
			t.Fatalf("alpha stopped at %q after %d pages, want fetch/2", alpha.Stage, alpha.PagesFetched)
		}
		if alpha.BatchID != "" || alpha.PositionSaved {
			t.Fatalf("alpha must not have published or checkpointed: %+v", alpha)
		}
		// Recovery: the next round re-reads and publishes everything.
		rep := h.sync()
		if r := sourceResult(rep, mockdata.InstitutionAlpha); r.Status != "ok" || r.Changes != 8 {
			t.Fatalf("alpha recovery = %+v", r)
		}
	})

	// (b) Stop after publishing but before saving the resume position.
	// After the restart the round shows the published batch and
	// position_saved=false; the batch is on disk for downstream, the
	// position was never advanced, and the next round emits no duplicate.
	t.Run("published_position_not_saved", func(t *testing.T) {
		h := newHarness(t)
		h.svc.Engine.SetAfterPublishHook(func(src string, _ domain.Envelope) {
			if src == mockdata.InstitutionBravo {
				panic("simulated crash after publish")
			}
		})
		h.sync()
		h.restart()

		_, runs := h.svc.Engine.Runs()
		bravo := runSource(t, runs[0], mockdata.InstitutionBravo)
		if bravo.Stage != journal.StagePublish {
			t.Fatalf("bravo stage = %q, want publish", bravo.Stage)
		}
		if bravo.BatchID == "" || bravo.BatchSeq == 0 || bravo.BatchChanges != 7 {
			t.Fatalf("bravo batch not journaled: %+v", bravo)
		}
		if bravo.PositionSaved {
			t.Fatalf("bravo position must not be saved: %+v", bravo)
		}
		// The position on disk was never advanced...
		pos := loadState(t, h.dir)[mockdata.InstitutionBravo]
		if pos != (state.Position{}) {
			t.Fatalf("bravo on-disk position = %+v, want zero", pos)
		}
		// ...but the batch is durable and readable by downstream.
		st, err := store.Open(filepath.Join(h.dir, "batches"))
		if err != nil {
			t.Fatal(err)
		}
		envs, err := st.ReadAfter(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, env := range envs {
			if env.BatchID == bravo.BatchID {
				found = true
			}
		}
		if !found {
			t.Fatalf("journaled batch %s not found on disk", bravo.BatchID)
		}
		// The next round re-reads from the head; the already-published
		// revisions are absorbed, nothing is emitted twice.
		rep := h.sync()
		if r := sourceResult(rep, mockdata.InstitutionBravo); r.Status != "ok" || r.Changes != 0 {
			t.Fatalf("bravo re-run = %+v, want ok with 0 changes", r)
		}
	})

	// (c) Stop after the resume position is saved: the round shows
	// position_saved=true even though the process died right afterwards.
	t.Run("position_saved", func(t *testing.T) {
		h := newHarness(t)
		h.svc.Engine.SetAfterCheckpointHook(func(src string, _ state.Position) {
			if src == mockdata.InstitutionCharlie {
				panic("simulated crash after checkpoint")
			}
		})
		h.sync()
		h.restart()

		_, runs := h.svc.Engine.Runs()
		charlie := runSource(t, runs[0], mockdata.InstitutionCharlie)
		if charlie.Stage != journal.StageCheckpoint {
			t.Fatalf("charlie stage = %q, want checkpoint", charlie.Stage)
		}
		if !charlie.PositionSaved || charlie.Position == nil {
			t.Fatalf("charlie position should be saved: %+v", charlie)
		}
		if charlie.BatchID == "" {
			t.Fatalf("charlie batch should be journaled: %+v", charlie)
		}
		// Recovery emits nothing twice.
		rep := h.sync()
		if r := sourceResult(rep, mockdata.InstitutionCharlie); r.Status != "ok" || r.Changes != 0 {
			t.Fatalf("charlie re-run = %+v, want ok with 0 changes", r)
		}
	})

	// (d) A change-free round with an unchanged position is still fully
	// journaled: after another restart it is recognizable as completed
	// (position saved), and round numbering continues across restarts.
	t.Run("empty_round_across_restart", func(t *testing.T) {
		h := newHarness(t)
		h.sync() // round 1: everything published
		h.restart()
		rep := h.sync() // round 2: no-op
		for _, r := range rep.Sources {
			if r.Changes != 0 {
				t.Fatalf("round 2 should be change-free: %+v", r)
			}
		}
		h.restart()

		cur, runs := h.svc.Engine.Runs()
		if cur != 2 {
			t.Fatalf("current round after restarts = %d, want 2", cur)
		}
		round2, ok := h.svc.Engine.Run(2)
		if !ok {
			t.Fatal("round 2 not found after restart")
		}
		if round2.Status != journal.RunDone {
			t.Fatalf("round 2 status = %q, want done", round2.Status)
		}
		for _, name := range []string{mockdata.InstitutionAlpha, mockdata.InstitutionBravo, mockdata.InstitutionCharlie} {
			sr := runSource(t, round2, name)
			if sr.Status != journal.StatusOK || sr.Stage != journal.StageDone {
				t.Fatalf("round 2 %s = %q/%q, want ok/done", name, sr.Status, sr.Stage)
			}
			if sr.BatchID != "" || sr.Changes != 0 {
				t.Fatalf("round 2 %s should have no batch: %+v", name, sr)
			}
			if !sr.PositionSaved || sr.Position == nil {
				t.Fatalf("round 2 %s should show the saved position: %+v", name, sr)
			}
		}
		if len(runs) != 2 || runs[1].Round != 1 {
			t.Fatalf("round history broken across restarts: %+v", runs)
		}
	})
}

// Test15_RunQueryHTTPMethod guards the method handling of the runs endpoint.
func Test15_RunQueryHTTPMethod(t *testing.T) {
	h := newHarness(t)
	httpSrv := httptest.NewServer(h.svc.Handler())
	defer httpSrv.Close()
	resp, err := http.Post(httpSrv.URL+"/v1/sync/runs", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/sync/runs = %d, want 405", resp.StatusCode)
	}
	resp, err = http.Get(httpSrv.URL + "/v1/sync/runs?round=abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad round = %d, want 400", resp.StatusCode)
	}
}
