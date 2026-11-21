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
// Publication model
//
//	Within one source run, changes are first derived against a private
//	staging overlay, so the live index - and therefore /v1/records - keeps
//	showing only already-published truth until the batch is durable. If a
//	later page fails (for example a 429), the staged changes are discarded
//	and the read position is not advanced: the next attempt re-fetches the
//	same pages and derives the same changes. Only after Store.Publish
//	succeeds is the staging overlay merged into the live index. If saving
//	the read position fails afterwards, the batch stays published but the
//	position stays at its previous value; the retry re-reads those pages,
//	sees the revisions are already published and emits no duplicate change.
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

// staging is one source run's private view of pending changes. It overlays
// the committed index without mutating it until publication succeeds.
type staging struct {
	// entries holds this run's newest state per archive key.
	entries map[string]*indexEntry
	// changes lists every change derived in this run in source order, so a
	// batch preserves the upstream revision sequence (withdrawal before
	// insert for a reschedule).
	changes []domain.Change
}

func newStaging() *staging {
	return &staging{entries: map[string]*indexEntry{}}
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
	// Hook used by tests to inject a state-save failure after a batch has
	// been published. It receives the source name and the position that
	// could not be persisted.
	hookBeforeCheckpoint func(source string, pos state.Position) error
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

// SetStoreWriteFault installs a test hook on the batch store to simulate disk
// write failures. It is intended for crash-safety regression tests.
func (e *Engine) SetStoreWriteFault(fn store.WriteFault) {
	e.store.SetWriteFault(fn)
}

// SetCheckpointHook installs a test hook invoked just before a source's read
// position is saved. Returning an error simulates a failed position write
// after the batch has already been published.
func (e *Engine) SetCheckpointHook(h func(source string, pos state.Position) error) {
	e.hookBeforeCheckpoint = h
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

// MaterializedKey returns one visible record by archive key.
func (e *Engine) MaterializedKey(key string) (domain.CanonicalRecord, bool) {
	e.idxMu.RLock()
	defer e.idxMu.RUnlock()
	ent, ok := e.index[key]
	if !ok || ent.withdrawn || ent.record == nil {
		return domain.CanonicalRecord{}, false
	}
	return *ent.record, true
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
	run := func(idx int, name string, fn func(context.Context, *staging) SourceResult) {
		defer wg.Done()
		res := SourceResult{Source: name, Status: "ok"}
		func() {
			defer func() {
				if r := recover(); r != nil {
					res.Status = "failed"
					res.Error = fmt.Sprintf("panic: %v", r)
				}
			}()
			// A fresh staging view per source: nothing it derives is visible
			// to readers or to another source until it is published.
			res = fn(ctx, newStaging())
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

// derive applies one raw record against the committed index plus this run's
// staging overlay. It records the resulting change in st and returns it, or
// nil when the revision is stale or a no-op. The committed index is not
// modified here: publication commits the staging view (see commit).
func (e *Engine) derive(raw map[string]any, st *staging) (*domain.Change, error) {
	rec, err := normalize.Record(raw)
	if err != nil {
		return nil, err
	}
	key := rec.Key()

	// Inside one source run the staging overlay is only touched by that
	// run's goroutine, so it needs no lock. The committed index is read
	// under a read lock; entries are replaced (never mutated in place) by
	// commit, so copying the revision out under the lock is sufficient.
	stEnt, staged := st.entries[key]
	e.idxMu.RLock()
	committed, known := e.index[key]
	e.idxMu.RUnlock()

	baseRev := int64(0)
	switch {
	case staged:
		baseRev = stEnt.revision
	case known:
		baseRev = committed.revision
	}
	if staged || known {
		if rec.Revision <= baseRev {
			// Stale revision. In particular, an old active revision re-read
			// after a cursor restart must not resurrect a withdrawn record.
			return nil, nil
		}
	}

	var ent indexEntry
	var ch domain.Change
	if rec.Withdrawn {
		ent = indexEntry{revision: rec.Revision, withdrawn: true}
		ch = domain.Change{
			Op:          domain.OpWithdraw,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
		}
	} else {
		cp := rec
		ent = indexEntry{revision: rec.Revision, record: &cp}
		ch = domain.Change{
			Op:          domain.OpUpsert,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
			Record:      &cp,
		}
	}
	st.entries[key] = &ent
	st.changes = append(st.changes, ch)
	return &ch, nil
}

// changeList returns the run's staged changes in derivation order.
func (st *staging) changeList() []domain.Change {
	return st.changes
}

// commit merges a published staging overlay into the live index. It must be
// called only after the batch is durable.
func (e *Engine) commit(st *staging) {
	e.idxMu.Lock()
	defer e.idxMu.Unlock()
	for key, ent := range st.entries {
		cp := *ent
		if ent.record != nil {
			rec := *ent.record
			cp.record = &rec
		}
		e.index[key] = &cp
	}
}

// publishAndCheckpoint writes the run's batch and then persists the next
// read position. The position is only advanced after durable publication.
// The staged changes are committed to the live index once the batch is
// durable, even if saving the position fails afterwards.
func (e *Engine) publishAndCheckpoint(source, batchID string, st *staging, pos state.Position) (string, error) {
	changes := st.changeList()
	if len(changes) > 0 {
		env, err := e.store.Publish(batchID, changes)
		if err != nil {
			return "", fmt.Errorf("publish batch: %w", err)
		}
		batchID = env.BatchID
	}
	// The batch is durable: make its changes visible and authoritative.
	e.commit(st)
	if e.hookBeforeCheckpoint != nil {
		if err := e.hookBeforeCheckpoint(source, pos); err != nil {
			return batchID, fmt.Errorf("save position: %w", err)
		}
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

func (e *Engine) syncAlpha(ctx context.Context, st *staging) (res SourceResult) {
	res.Source = mockdata.InstitutionAlpha
	pos := e.states.Get(mockdata.InstitutionAlpha)
	offset := pos.Offset
	run := 0

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
			if _, derr := e.derive(raw, st); derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
		}
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionAlpha, run, page.Position)
		}
		offset = page.Position.Offset
		run++
		if page.Done {
			break
		}
	}
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionAlpha, e.newBatchID(mockdata.InstitutionAlpha, run), st,
		state.Position{Offset: offset})
	res.BatchID = bid
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.Status = len(st.changes), "ok"
	return res
}

// ---------- bravo: cursor ----------

func (e *Engine) syncBravo(ctx context.Context, st *staging) (res SourceResult) {
	res.Source = mockdata.InstitutionBravo
	pos := e.states.Get(mockdata.InstitutionBravo)
	cursor := pos.Cursor
	cursorOffset := pos.CursorOffset
	run := 0

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
			if _, derr := e.derive(raw, st); derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
		}
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
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionBravo, e.newBatchID(mockdata.InstitutionBravo, run), st,
		state.Position{Cursor: cursor, CursorOffset: cursorOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		res.BatchID = bid
		return res
	}
	res.Changes, res.BatchID, res.Status = len(st.changes), bid, "ok"
	return res
}

// ---------- charlie: upload-time windows ----------

func (e *Engine) syncCharlie(ctx context.Context, st *staging) (res SourceResult) {
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
				if _, derr := e.derive(raw, st); derr != nil {
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
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionCharlie, e.newBatchID(mockdata.InstitutionCharlie, run), st,
		state.Position{WindowIndex: windowIndex, WindowOffset: windowOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		res.BatchID = bid
		return res
	}
	res.Changes, res.BatchID, res.Status = len(st.changes), bid, "ok"
	return res
}
