// Package collector orchestrates synchronization runs: it pages each source
// independently, normalizes every record, derives monotonic changes against
// the already-published history, and appends crash-safe batches.
//
// Durability and visibility model
//
//	Batches (JSON Lines) are the authoritative record. The read-position
//	state file only records where the next run starts. On startup the
//	per-record index is rebuilt by replaying every published batch, so:
//	  - an interrupted run simply re-reads from the last saved position;
//	  - replayed revisions never produce changes twice (revisions are
//	    monotonic per sample);
//	  - an old revision of a withdrawn record can never resurrect it.
//
// A source run is buffered, not applied page by page. Fetched records are
// derived against a private staging overlay; the shared record index is only
// advanced after the run's batch has been durably published. Consequently:
//   - a 429/failure on page N discards the whole partial run and leaves the
//     saved position untouched, so the next run re-reads page 1 and still
//     emits its changes (nothing was swallowed by a half-advanced index);
//   - /v1/records never exposes a record before its batch is on disk, and
//     /v1/batches never exposes a half-appended line;
//   - publish, index update and checkpoint happen in that order, so a crash
//     between publish and checkpoint resumes with an empty delta.
//
// A reschedule arrives as adjacent revisions (withdrawal under the old day,
// insert under the new day). All changes of one source run are published in
// a single batch, so the withdrawal and the insert become visible together.
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
	Status        string `json:"status"` // ok, rate_limited, failed, interrupted
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
	alpha   *source.AlphaClient
	bravo   *source.BravoClient
	charlie *source.CharlieClient

	idxMu sync.RWMutex
	index map[string]*indexEntry

	syncing atomic.Bool

	// Hook used by tests to inject failures mid-run. It receives the source
	// name, the zero-based page number just processed within that source and
	// the next read position returned by the page. A panic here aborts the
	// source run exactly like an interrupted process.
	hookAfterPage func(source string, pageNo int, next source.Next)

	// Hook used by tests to model a crash after the batch is durably
	// published (and the in-memory index advanced) but before the read
	// position is saved. A panic here leaves the batch authoritative.
	hookAfterPublish func(source string)
}

// New builds an engine and rebuilds the record index from published batches.
func New(st *store.Store, stt *state.Store, a *source.AlphaClient, b *source.BravoClient, c *source.CharlieClient) (*Engine, error) {
	e := &Engine{
		store:   st,
		states:  stt,
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

// SetPublishHook installs a test hook invoked after a batch is published and
// the index updated, but before the read position is checkpointed.
func (e *Engine) SetPublishHook(h func(source string)) {
	e.hookAfterPublish = h
}

// Syncing reports whether a run is currently in progress.
func (e *Engine) Syncing() bool { return e.syncing.Load() }

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
// by archive day and sample id. Only published changes are represented.
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
// limit, transport error, ...) never blocks the other two.
func (e *Engine) Sync(ctx context.Context) Report {
	if !e.syncing.CompareAndSwap(false, true) {
		return Report{StartedAt: time.Now().UTC().Format(time.RFC3339Nano), EndedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	defer e.syncing.Store(false)

	report := Report{
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Sources:   make([]SourceResult, 3),
	}
	var wg sync.WaitGroup
	run := func(idx int, name string, fn func(context.Context) SourceResult) {
		defer wg.Done()
		res := SourceResult{Source: name, Status: "ok"}
		func() {
			defer func() {
				if r := recover(); r != nil {
					res.Status = "failed"
					res.Error = fmt.Sprintf("panic: %v", r)
				}
			}()
			res = fn(ctx)
		}()
		report.Sources[idx] = res
	}
	wg.Add(3)
	go run(0, mockdata.InstitutionAlpha, e.syncAlpha)
	go run(1, mockdata.InstitutionBravo, e.syncBravo)
	go run(2, mockdata.InstitutionCharlie, e.syncCharlie)
	wg.Wait()
	report.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return report
}

// stager buffers one source run. Derivation consults the shared index plus a
// private overlay of changes this run has already produced, but never mutates
// the shared index; that only happens on commit, after durable publication.
type stager struct {
	e       *Engine
	staged  map[string]*indexEntry
	changes []domain.Change
}

func newStager(e *Engine) *stager {
	return &stager{e: e, staged: map[string]*indexEntry{}}
}

// derive normalizes one raw record and appends the change it would cause, or
// does nothing when the revision is stale or a no-op.
func (s *stager) derive(raw map[string]any) error {
	rec, err := normalize.Record(raw)
	if err != nil {
		return err
	}
	key := rec.Key()

	if ent, ok := s.staged[key]; ok && rec.Revision <= ent.revision {
		return nil
	}
	s.e.idxMu.RLock()
	existing, known := s.e.index[key]
	s.e.idxMu.RUnlock()
	if known && rec.Revision <= existing.revision {
		// Stale revision. In particular, an old active revision re-read after
		// a cursor restart must not resurrect a withdrawn record.
		return nil
	}

	if rec.Withdrawn {
		s.staged[key] = &indexEntry{revision: rec.Revision, withdrawn: true}
		s.changes = append(s.changes, domain.Change{
			Op:          domain.OpWithdraw,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
		})
		return nil
	}
	cp := rec
	s.staged[key] = &indexEntry{revision: rec.Revision, record: &cp}
	s.changes = append(s.changes, domain.Change{
		Op:          domain.OpUpsert,
		Institution: rec.Institution,
		SampleID:    rec.SampleID,
		SampledOn:   rec.SampledOn,
		Revision:    rec.Revision,
		Record:      &cp,
	})
	return nil
}

// commit publishes the run's batch, then advances the shared index, then
// checkpoints the next read position. Holding idxMu across publish+index means
// /v1/records can never show a record whose batch is not yet durable, while
// /v1/batches (which reads the store) and /v1/records flip together.
//
// A failure before the position is saved is safe: the next run re-reads from
// the old position, but the published revisions are already in the index and
// derive to nothing, so it just advances the checkpoint.
func (e *Engine) commit(source, batchID string, st *stager, pos state.Position) (string, error) {
	e.idxMu.Lock()
	if len(st.changes) > 0 {
		env, err := e.store.Publish(batchID, st.changes)
		if err != nil {
			e.idxMu.Unlock()
			return "", fmt.Errorf("publish batch: %w", err)
		}
		batchID = env.BatchID
	}
	for key, ent := range st.staged {
		e.index[key] = ent
	}
	e.idxMu.Unlock()

	if e.hookAfterPublish != nil {
		e.hookAfterPublish(source)
	}
	if err := e.states.Update(source, pos); err != nil {
		return batchID, fmt.Errorf("save position: %w", err)
	}
	return batchID, nil
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

func (e *Engine) syncAlpha(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionAlpha
	pos := e.states.Get(mockdata.InstitutionAlpha)
	offset := pos.Offset
	st := newStager(e)
	run := 0
	last := source.Next{Offset: offset}

	for {
		if err := ctx.Err(); err != nil {
			res.Status, res.Error = "interrupted", err.Error()
			return res
		}
		page, err := e.alpha.Fetch(ctx, offset)
		if err != nil {
			res.Status, res.Error = classifyError(err)
			return res
		}
		res.PagesFetched++
		for _, raw := range page.Records {
			res.RecordsSeen++
			if derr := st.derive(raw); derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
		}
		last = page.Position
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionAlpha, run, page.Position)
		}
		offset = page.Position.Offset
		run++
		if page.Done {
			break
		}
	}
	bid, err := e.commit(mockdata.InstitutionAlpha, e.newBatchID(mockdata.InstitutionAlpha, run), st,
		state.Position{Offset: last.Offset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(st.changes), bid, "ok"
	return res
}

// ---------- bravo: cursor ----------

func (e *Engine) syncBravo(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionBravo
	pos := e.states.Get(mockdata.InstitutionBravo)
	cursor := pos.Cursor
	cursorOffset := pos.CursorOffset
	st := newStager(e)
	run := 0
	last := source.Next{Cursor: cursor, CursorOffset: cursorOffset}

	for {
		if err := ctx.Err(); err != nil {
			res.Status, res.Error = "interrupted", err.Error()
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
			return res
		}
		res.PagesFetched++
		for _, raw := range page.Records {
			res.RecordsSeen++
			if derr := st.derive(raw); derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
		}
		last = page.Position
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
	bid, err := e.commit(mockdata.InstitutionBravo, e.newBatchID(mockdata.InstitutionBravo, run), st,
		state.Position{Cursor: last.Cursor, CursorOffset: last.CursorOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(st.changes), bid, "ok"
	return res
}

// ---------- charlie: upload-time windows ----------

func (e *Engine) syncCharlie(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionCharlie
	pos := e.states.Get(mockdata.InstitutionCharlie)

	windows, err := e.charlie.FetchWindows(ctx)
	if err != nil {
		res.Status, res.Error = classifyError(err)
		return res
	}
	windowIndex := pos.WindowIndex
	if windowIndex > len(windows) {
		windowIndex = len(windows)
	}
	windowOffset := pos.WindowOffset
	if windowIndex >= len(windows) {
		// Nothing new to read right now; keep the position pinned to the end.
		windowOffset = 0
	}
	st := newStager(e)
	run := 0

	for windowIndex < len(windows) {
		window := windows[windowIndex]
		for {
			if err := ctx.Err(); err != nil {
				res.Status, res.Error = "interrupted", err.Error()
				return res
			}
			page, err := e.charlie.FetchWindowPage(ctx, window, windowOffset)
			if err != nil {
				res.Status, res.Error = classifyError(err)
				return res
			}
			res.PagesFetched++
			for _, raw := range page.Records {
				res.RecordsSeen++
				if derr := st.derive(raw); derr != nil {
					res.Status, res.Error = "failed", derr.Error()
					return res
				}
			}
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
	bid, err := e.commit(mockdata.InstitutionCharlie, e.newBatchID(mockdata.InstitutionCharlie, run), st,
		state.Position{WindowIndex: windowIndex, WindowOffset: windowOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(st.changes), bid, "ok"
	return res
}
