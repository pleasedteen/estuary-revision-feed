// Package collector orchestrates synchronization runs: it pages each source
// independently, normalizes every record, derives monotonic changes against
// the already-published history, and appends crash-safe batches.
//
// Durability model
//
//	Batches (JSON Lines) are the authoritative record. The read-position
//	state file only records where the next run starts. On startup the
//	per-record index is rebuilt by replaying every published batch, so:
//	  - an interrupted run simply re-reads from the last saved position;
//	  - replayed revisions never produce changes twice (revisions are
//	    monotonic per sample);
//	  - an old revision of a withdrawn record can never resurrect it.
//
//	Within a run, revisions are staged in a per-source pending overlay and
//	committed to the index only once their batch is durable, so a source
//	failing mid-run never poisons the index with unpublished records.
//
// A reschedule arrives as adjacent revisions (withdrawal under the old day,
// insert under the new day). All changes of one source run are published in
// a single batch, so the withdrawal and the insert become visible together.
//
// Every round is recorded in the run journal (package journal): per-source
// pages fetched, current/failure stage, published batch and resume-position
// save state, queryable while the round runs and after a restart.
package collector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"estuary-collector/internal/domain"
	"estuary-collector/internal/journal"
	"estuary-collector/internal/mockdata"
	"estuary-collector/internal/normalize"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// indexEntry is the current known truth for one archive key.
type indexEntry struct {
	revision  int64
	withdrawn bool
	record    *domain.CanonicalRecord
}

// SourceResult summarizes one source's run.
type SourceResult struct {
	Source        string `json:"source"`
	Status        string `json:"status"` // ok, rate_limited, failed
	PagesFetched  int    `json:"pages_fetched"`
	RecordsSeen   int    `json:"records_seen"`
	Changes       int    `json:"changes"`
	BatchID       string `json:"batch_id,omitempty"`
	CursorRestart bool   `json:"cursor_restart,omitempty"`
	Error         string `json:"error,omitempty"`
}

// Report is the full synchronization report.
type Report struct {
	StartedAt string         `json:"started_at"`
	EndedAt   string         `json:"ended_at"`
	Sources   []SourceResult `json:"sources"`
}

// Engine wires stores, state and the three source clients.
type Engine struct {
	store   *store.Store
	states  *state.Store
	journal *journal.Journal
	alpha   *source.AlphaClient
	bravo   *source.BravoClient
	charlie *source.CharlieClient

	idxMu sync.RWMutex
	index map[string]*indexEntry

	syncing atomic.Bool

	// Hook used by tests to inject failures mid-run. It receives the source
	// name, the zero-based page number just processed within that source and
	// the next read position returned by the page.
	hookAfterPage func(source string, pageNo int, next source.Next)
	// hookAfterPublish fires after a batch is durable and recorded in the
	// journal, but before the resume position is saved. hookAfterCheckpoint
	// fires after the position is saved and recorded. Both are test/chaos
	// knobs for stopping exactly at a persistence boundary.
	hookAfterPublish    func(source string, env domain.Envelope)
	hookAfterCheckpoint func(source string, pos state.Position)
}

// New builds an engine and rebuilds the record index from published batches.
func New(st *store.Store, stt *state.Store, jr *journal.Journal, a *source.AlphaClient, b *source.BravoClient, c *source.CharlieClient) (*Engine, error) {
	e := &Engine{
		store:   st,
		states:  stt,
		journal: jr,
		alpha:   a,
		bravo:   b,
		charlie: c,
		index:   map[string]*indexEntry{},
	}
	if err := e.rebuildIndex(); err != nil {
		return nil, err
	}
	return e, nil
}

// SetPageHook installs a test hook invoked after every fetched page.
func (e *Engine) SetPageHook(h func(source string, pageNo int, next source.Next)) {
	e.hookAfterPage = h
}

// SetAfterPublishHook installs a hook fired once a batch is durable and
// journaled but before the resume position is saved.
func (e *Engine) SetAfterPublishHook(h func(source string, env domain.Envelope)) {
	e.hookAfterPublish = h
}

// SetAfterCheckpointHook installs a hook fired once the resume position is
// saved and journaled.
func (e *Engine) SetAfterCheckpointHook(h func(source string, pos state.Position)) {
	e.hookAfterCheckpoint = h
}

// Syncing reports whether a run is currently in progress.
func (e *Engine) Syncing() bool { return e.syncing.Load() }

// Runs returns the current round number and the retained per-round journal,
// newest first. It is served from the journal only and never scans batches.
func (e *Engine) Runs() (int64, []journal.Run) { return e.journal.Snapshot() }

// Run returns one round of the journal.
func (e *Engine) Run(round int64) (journal.Run, bool) { return e.journal.Find(round) }

func (e *Engine) rebuildIndex() error {
	envelopes, err := e.store.ReadAfter(0, 0)
	if err != nil {
		return err
	}
	for _, env := range envelopes {
		for _, ch := range env.Changes {
			key := ch.Institution + "|" + ch.SampleID + "|" + ch.SampledOn
			switch ch.Op {
			case domain.OpUpsert:
				if ch.Record != nil {
					rec := *ch.Record
					e.index[key] = &indexEntry{revision: ch.Revision, record: &rec}
				}
			case domain.OpWithdraw:
				e.index[key] = &indexEntry{revision: ch.Revision, withdrawn: true}
			}
		}
	}
	return nil
}

// Materialized returns the currently visible (non-withdrawn) records, sorted
// by archive day and sample id.
func (e *Engine) Materialized() []domain.CanonicalRecord {
	e.idxMu.RLock()
	defer e.idxMu.RUnlock()
	out := make([]domain.CanonicalRecord, 0, len(e.index))
	for _, ent := range e.index {
		if !ent.withdrawn && ent.record != nil {
			out = append(out, *ent.record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SampledOn != out[j].SampledOn {
			return out[i].SampledOn < out[j].SampledOn
		}
		if out[i].Institution != out[j].Institution {
			return out[i].Institution < out[j].Institution
		}
		return out[i].SampleID < out[j].SampleID
	})
	return out
}

// Sync runs all three sources concurrently. A failure of one source (rate
// limit, transport error, ...) never blocks the other two. Every round is
// journaled so its progress can be queried while it runs and inspected after
// a restart.
func (e *Engine) Sync(ctx context.Context) Report {
	if !e.syncing.CompareAndSwap(false, true) {
		return Report{StartedAt: time.Now().UTC().Format(time.RFC3339Nano), EndedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	defer e.syncing.Store(false)

	round := e.journal.BeginRun([]string{
		mockdata.InstitutionAlpha, mockdata.InstitutionBravo, mockdata.InstitutionCharlie,
	}).Round
	report := Report{
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Sources:   make([]SourceResult, 3),
	}
	var wg sync.WaitGroup
	run := func(idx int, name string, fn func(context.Context, int64) SourceResult) {
		defer wg.Done()
		res := SourceResult{Source: name, Status: "ok"}
		func() {
			defer func() {
				if r := recover(); r != nil {
					res.Status = "failed"
					res.Error = fmt.Sprintf("panic: %v", r)
					// Record the death but keep the stage/batch/position
					// fields already journaled: they show where it stopped.
					e.journal.UpdateSource(round, name, func(sr *journal.SourceRun) {
						sr.Status = journal.StatusFailed
						sr.Error = res.Error
					})
				}
			}()
			res = fn(ctx, round)
		}()
		report.Sources[idx] = res
	}
	wg.Add(3)
	go run(0, mockdata.InstitutionAlpha, e.syncAlpha)
	go run(1, mockdata.InstitutionBravo, e.syncBravo)
	go run(2, mockdata.InstitutionCharlie, e.syncCharlie)
	wg.Wait()
	e.journal.EndRun(round)
	report.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return report
}

// derive applies one raw record and returns the change it causes, or nil
// when the revision is stale or a no-op. Newer revisions are staged in the
// run's pending overlay, not in the authoritative index: the index only
// reflects what is actually published (see commitPending), so a source that
// fails mid-run re-derives those records on its next run instead of
// silently swallowing them.
func (e *Engine) derive(pending map[string]*indexEntry, raw map[string]any) (*domain.Change, error) {
	rec, err := normalize.Record(raw)
	if err != nil {
		return nil, err
	}
	key := rec.Key()

	e.idxMu.RLock()
	existing, known := e.index[key]
	e.idxMu.RUnlock()
	// A revision staged earlier in this same run supersedes the published one.
	if p, ok := pending[key]; ok {
		existing, known = p, true
	}

	if known && rec.Revision <= existing.revision {
		// Stale revision. In particular, an old active revision re-read after
		// a cursor restart must not resurrect a withdrawn record.
		return nil, nil
	}

	var ch *domain.Change
	if rec.Withdrawn {
		ch = &domain.Change{
			Op:          domain.OpWithdraw,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
		}
		pending[key] = &indexEntry{revision: rec.Revision, withdrawn: true}
	} else {
		cp := rec
		ch = &domain.Change{
			Op:          domain.OpUpsert,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
			Record:      &cp,
		}
		pending[key] = &indexEntry{revision: rec.Revision, record: &cp}
	}
	return ch, nil
}

// commitPending merges a run's staged revisions into the authoritative
// index. It is called once the batch carrying them is durable, so the index
// never advances past the published stream.
func (e *Engine) commitPending(pending map[string]*indexEntry) {
	if len(pending) == 0 {
		return
	}
	e.idxMu.Lock()
	defer e.idxMu.Unlock()
	for k, v := range pending {
		e.index[k] = v
	}
}

// publishAndCheckpoint writes the run's batch and then persists the next
// read position. The position is only advanced after durable publication,
// and the run's staged revisions are committed to the index only once the
// batch is durable. The journal is persisted at both boundaries — batch
// durable, then position saved — so a process stopped between them is
// diagnosable after restart.
func (e *Engine) publishAndCheckpoint(round int64, source, batchID string, changes []domain.Change, pending map[string]*indexEntry, pos state.Position) (string, error) {
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) { sr.Stage = journal.StagePublish })
	if len(changes) > 0 {
		env, err := e.store.Publish(batchID, changes)
		if err != nil {
			return "", fmt.Errorf("publish batch: %w", err)
		}
		batchID = env.BatchID
		e.commitPending(pending)
		e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) {
			sr.BatchID = env.BatchID
			sr.BatchSeq = env.Seq
			sr.BatchChanges = len(env.Changes)
			sr.PublishedAt = env.ChangedAt
		})
		if e.hookAfterPublish != nil {
			e.hookAfterPublish(source, env)
		}
	}
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) { sr.Stage = journal.StageCheckpoint })
	if err := e.states.Update(source, pos); err != nil {
		return "", fmt.Errorf("save position: %w", err)
	}
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) {
		p := pos
		sr.PositionSaved = true
		sr.Position = &p
	})
	if e.hookAfterCheckpoint != nil {
		e.hookAfterCheckpoint(source, pos)
	}
	return batchID, nil
}

// noteError records a source's failure in the journal. The stage already
// journaled (fetch/derive/publish/checkpoint) is the failure stage.
func (e *Engine) noteError(round int64, source string, res *SourceResult) {
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) {
		sr.Status = res.Status
		sr.Error = res.Error
	})
}

// noteProgress journals live fetch progress after every page, so an operator
// can watch a running round (or diagnose a stuck one) page by page.
func (e *Engine) noteProgress(round int64, source string, pages, records int) {
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) {
		sr.PagesFetched = pages
		sr.RecordsSeen = records
	})
}

// noteStage moves a source to a different stage (used when the failure
// happens outside the fetch loop).
func (e *Engine) noteStage(round int64, source, stage string) {
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) { sr.Stage = stage })
}

// noteDone marks a source finished in the journal.
func (e *Engine) noteDone(round int64, source string, res *SourceResult) {
	e.journal.UpdateSource(round, source, func(sr *journal.SourceRun) {
		sr.Status = journal.StatusOK
		sr.Stage = journal.StageDone
		sr.PagesFetched = res.PagesFetched
		sr.RecordsSeen = res.RecordsSeen
		sr.Changes = res.Changes
		sr.CursorRestart = res.CursorRestart
	})
}

func (e *Engine) newBatchID(source string, n int) string {
	return fmt.Sprintf("%s-%d-%03d", source, time.Now().UTC().UnixNano(), n)
}

func classifyError(err error) (string, string) {
	var rl *source.RateLimitedError
	if errors.As(err, &rl) {
		return "rate_limited", err.Error()
	}
	return "failed", err.Error()
}

// ---------- alpha: offset ----------

func (e *Engine) syncAlpha(ctx context.Context, round int64) (res SourceResult) {
	res.Source = mockdata.InstitutionAlpha
	pos := e.states.Get(mockdata.InstitutionAlpha)
	offset := pos.Offset
	var changes []domain.Change
	pending := map[string]*indexEntry{}
	run := 0

	for {
		if err := ctx.Err(); err != nil {
			res.Status, res.Error = "interrupted", err.Error()
			e.noteError(round, res.Source, &res)
			return res
		}
		page, err := e.alpha.Fetch(ctx, offset)
		if err != nil {
			res.Status, res.Error = classifyError(err)
			e.noteError(round, res.Source, &res)
			return res
		}
		res.PagesFetched++
		for _, raw := range page.Records {
			res.RecordsSeen++
			ch, derr := e.derive(pending, raw)
			if derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				e.noteStage(round, res.Source, journal.StageDerive)
				e.noteError(round, res.Source, &res)
				return res
			}
			if ch != nil {
				changes = append(changes, *ch)
			}
		}
		e.noteProgress(round, res.Source, res.PagesFetched, res.RecordsSeen)
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionAlpha, run, page.Position)
		}
		offset = page.Position.Offset
		run++
		if page.Done {
			break
		}
	}
	bid, err := e.publishAndCheckpoint(round, mockdata.InstitutionAlpha, e.newBatchID(mockdata.InstitutionAlpha, run), changes, pending,
		state.Position{Offset: offset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		e.noteError(round, res.Source, &res)
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	e.noteDone(round, res.Source, &res)
	return res
}

// ---------- bravo: cursor ----------

func (e *Engine) syncBravo(ctx context.Context, round int64) (res SourceResult) {
	res.Source = mockdata.InstitutionBravo
	pos := e.states.Get(mockdata.InstitutionBravo)
	cursor := pos.Cursor
	cursorOffset := pos.CursorOffset
	var changes []domain.Change
	pending := map[string]*indexEntry{}
	run := 0

	for {
		if err := ctx.Err(); err != nil {
			res.Status, res.Error = "interrupted", err.Error()
			e.noteError(round, res.Source, &res)
			return res
		}
		page, err := e.bravo.Fetch(ctx, cursor, cursorOffset)
		if err != nil {
			var expired *source.CursorExpiredError
			if errors.As(err, &expired) {
				// Cursor died: re-read the whole log from the head. The
				// revision index absorbs every already-seen old revision.
				res.CursorRestart = true
				cursor, cursorOffset = "", 0
				continue
			}
			res.Status, res.Error = classifyError(err)
			e.noteError(round, res.Source, &res)
			return res
		}
		res.PagesFetched++
		for _, raw := range page.Records {
			res.RecordsSeen++
			ch, derr := e.derive(pending, raw)
			if derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				e.noteStage(round, res.Source, journal.StageDerive)
				e.noteError(round, res.Source, &res)
				return res
			}
			if ch != nil {
				changes = append(changes, *ch)
			}
		}
		e.noteProgress(round, res.Source, res.PagesFetched, res.RecordsSeen)
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionBravo, run, page.Position)
		}
		cursor = page.Position.Cursor
		cursorOffset = page.Position.CursorOffset
		run++
		if page.Done {
			break
		}
	}
	bid, err := e.publishAndCheckpoint(round, mockdata.InstitutionBravo, e.newBatchID(mockdata.InstitutionBravo, run), changes, pending,
		state.Position{Cursor: cursor, CursorOffset: cursorOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		e.noteError(round, res.Source, &res)
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	e.noteDone(round, res.Source, &res)
	return res
}

// ---------- charlie: upload-time windows ----------

func (e *Engine) syncCharlie(ctx context.Context, round int64) (res SourceResult) {
	res.Source = mockdata.InstitutionCharlie
	pos := e.states.Get(mockdata.InstitutionCharlie)

	windows, err := e.charlie.FetchWindows(ctx)
	if err != nil {
		res.Status, res.Error = classifyError(err)
		e.noteError(round, res.Source, &res)
		return res
	}
	windowIndex := pos.WindowIndex
	if windowIndex > len(windows) {
		windowIndex = len(windows)
	}
	windowOffset := pos.WindowOffset
	var changes []domain.Change
	pending := map[string]*indexEntry{}
	run := 0

	for windowIndex < len(windows) {
		window := windows[windowIndex]
		for {
			if err := ctx.Err(); err != nil {
				res.Status, res.Error = "interrupted", err.Error()
				e.noteError(round, res.Source, &res)
				return res
			}
			page, err := e.charlie.FetchWindowPage(ctx, window, windowOffset)
			if err != nil {
				res.Status, res.Error = classifyError(err)
				e.noteError(round, res.Source, &res)
				return res
			}
			res.PagesFetched++
			for _, raw := range page.Records {
				res.RecordsSeen++
				ch, derr := e.derive(pending, raw)
				if derr != nil {
					res.Status, res.Error = "failed", derr.Error()
					e.noteStage(round, res.Source, journal.StageDerive)
					e.noteError(round, res.Source, &res)
					return res
				}
				if ch != nil {
					changes = append(changes, *ch)
				}
			}
			e.noteProgress(round, res.Source, res.PagesFetched, res.RecordsSeen)
			if e.hookAfterPage != nil {
				e.hookAfterPage(mockdata.InstitutionCharlie, run, page.Position)
			}
			windowOffset = page.Position.WindowOffset
			run++
			if page.Done {
				windowIndex++
				windowOffset = 0
				break
			}
		}
	}
	bid, err := e.publishAndCheckpoint(round, mockdata.InstitutionCharlie, e.newBatchID(mockdata.InstitutionCharlie, run), changes, pending,
		state.Position{WindowIndex: windowIndex, WindowOffset: windowOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		e.noteError(round, res.Source, &res)
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	e.noteDone(round, res.Source, &res)
	return res
}
