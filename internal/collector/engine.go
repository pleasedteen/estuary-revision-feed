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
// A reschedule arrives as adjacent revisions (withdrawal under the old day,
// insert under the new day). All changes of one source run are published in
// a single batch, so the withdrawal and the insert become visible together.
//
// The upload-window source (charlie) additionally checkpoints its per-window
// read offsets together with a journal of derived-but-unpublished changes
// after every page. An interrupted run resumes at the first missing page —
// never a head rescan — and the reloaded journal still leaves with the
// post-restart changes in one batch, so even a reschedule spanning a crash
// is read back from a single batch. Journaled changes found already
// published after a crash are dropped on reload, never re-emitted.
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

// derive applies one raw record to the index and returns the change it
// causes, or nil when the revision is stale or a no-op.
func (e *Engine) derive(raw map[string]any) (*domain.Change, error) {
	rec, err := normalize.Record(raw)
	if err != nil {
		return nil, err
	}
	key := rec.Key()

	e.idxMu.Lock()
	defer e.idxMu.Unlock()
	existing, known := e.index[key]

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
		e.index[key] = &indexEntry{revision: rec.Revision, withdrawn: true}
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
		e.index[key] = &indexEntry{revision: rec.Revision, record: &cp}
	}
	return ch, nil
}

// publishAndCheckpoint writes the run's batch and then persists the next
// read position. The position is only advanced after durable publication.
func (e *Engine) publishAndCheckpoint(source, batchID string, changes []domain.Change, pos state.Position) (string, error) {
	if len(changes) > 0 {
		env, err := e.store.Publish(batchID, changes)
		if err != nil {
			return "", fmt.Errorf("publish batch: %w", err)
		}
		batchID = env.BatchID
	}
	if err := e.states.Update(source, pos); err != nil {
		return "", fmt.Errorf("save position: %w", err)
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
	var changes []domain.Change
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
			ch, derr := e.derive(raw)
			if derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
			if ch != nil {
				changes = append(changes, *ch)
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
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionAlpha, e.newBatchID(mockdata.InstitutionAlpha, run), changes,
		state.Position{Offset: offset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	return res
}

// ---------- bravo: cursor ----------

func (e *Engine) syncBravo(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionBravo
	pos := e.states.Get(mockdata.InstitutionBravo)
	cursor := pos.Cursor
	cursorOffset := pos.CursorOffset
	var changes []domain.Change
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
			ch, derr := e.derive(raw)
			if derr != nil {
				res.Status, res.Error = "failed", derr.Error()
				return res
			}
			if ch != nil {
				changes = append(changes, *ch)
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
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionBravo, e.newBatchID(mockdata.InstitutionBravo, run), changes,
		state.Position{Cursor: cursor, CursorOffset: cursorOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	return res
}

// ---------- charlie: upload-time windows ----------

// syncCharlie catches up on the upload-window source. One round works from
// the directory snapshot taken at its start: for every window it requests
// only the rows between the saved per-window offset and the snapshot row
// count, so back-registered earlier windows and rows appended to
// already-consumed windows are both picked up incrementally. Rows appended
// while the round runs are left for the next round, even when a fetched page
// already straddles the snapshot boundary.
//
// After every page the per-window offsets and the pending (derived but not
// yet published) changes are checkpointed atomically, so an interrupted run
// resumes at the first missing page instead of rescanning, and the whole
// catch-up — including the part completed before an interruption — is still
// published as exactly one batch. A reschedule whose halves land on
// different pages, or on different sides of a restart, is therefore always
// read back from the same batch.
func (e *Engine) syncCharlie(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionCharlie
	pos := e.states.Get(mockdata.InstitutionCharlie)

	// Snapshot the directory: this round covers exactly the rows visible now.
	windows, err := e.charlie.FetchWindows(ctx)
	if err != nil {
		res.Status, res.Error = classifyError(err)
		return res
	}

	consumed := charlieProgress(pos, windows)
	pending := e.states.Pending(mockdata.InstitutionCharlie)
	batchID := pending.BatchID
	if batchID == "" {
		batchID = e.newBatchID(mockdata.InstitutionCharlie, 0)
	}
	changes := e.recoverPending(pending.Changes)

	checkpoint := func() error {
		return e.states.Checkpoint(mockdata.InstitutionCharlie,
			state.Position{Windows: copyWindows(consumed)},
			state.Pending{BatchID: batchID, Changes: changes})
	}

	run := 0
	for _, w := range windows {
		offset := consumed[w.Name]
		target := w.RowCount
		for offset < target {
			if err := ctx.Err(); err != nil {
				res.Status, res.Error = "interrupted", err.Error()
				return res
			}
			page, err := e.charlie.FetchWindowPage(ctx, w.Name, offset)
			if err != nil {
				res.Status, res.Error = classifyError(err)
				return res
			}
			res.PagesFetched++
			next := page.Position.WindowOffset
			if next <= offset {
				break // no progress possible (defensive; the mock is append-only)
			}
			// Never consume past the snapshot boundary: rows appended to the
			// window while this round runs belong to the next round.
			rows := page.Records
			if next > target {
				keep := target - offset
				if keep > len(rows) {
					keep = len(rows)
				}
				rows = rows[:keep]
				next = target
			}
			for _, raw := range rows {
				res.RecordsSeen++
				ch, derr := e.derive(raw)
				if derr != nil {
					res.Status, res.Error = "failed", derr.Error()
					return res
				}
				if ch != nil {
					changes = append(changes, *ch)
				}
			}
			consumed[w.Name] = next
			if err := checkpoint(); err != nil {
				res.Status, res.Error = "failed", "save checkpoint: "+err.Error()
				return res
			}
			if e.hookAfterPage != nil {
				e.hookAfterPage(mockdata.InstitutionCharlie, run, page.Position)
			}
			run++
			offset = next
		}
	}

	if len(changes) > 0 {
		env, err := e.store.Publish(batchID, changes)
		if err != nil {
			res.Status, res.Error = "failed", "publish batch: "+err.Error()
			return res
		}
		batchID = env.BatchID
	}
	// Final checkpoint: the per-page checkpoints already advanced every
	// offset, so this only clears the pending journal. If the process died
	// between publish and this write, the next run reloads the journal,
	// sees every change already reflected in the replayed index and drops
	// them — published changes are never re-emitted.
	if err := e.states.Checkpoint(mockdata.InstitutionCharlie,
		state.Position{Windows: copyWindows(consumed)}, state.Pending{}); err != nil {
		res.Status, res.Error = "failed", "save position: "+err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), batchID, "ok"
	return res
}

// charlieProgress resolves the per-window read offsets for one round. A
// position saved by the previous version (window index + inner offset, no
// per-window map) is migrated against the live directory: windows before the
// saved index are treated as consumed up to their current directory size and
// the window at the index keeps its saved inner offset. Upgrading therefore
// never re-requests — and never republishes — already-read rows, while
// windows absent from the map (newly opened or back-registered) start at 0.
func charlieProgress(pos state.Position, windows []source.WindowInfo) map[string]int {
	consumed := make(map[string]int, len(windows))
	if pos.Windows != nil {
		for w, off := range pos.Windows {
			consumed[w] = off
		}
		return consumed
	}
	for i, w := range windows {
		switch {
		case i < pos.WindowIndex:
			consumed[w.Name] = w.RowCount
		case i == pos.WindowIndex:
			consumed[w.Name] = pos.WindowOffset
		}
	}
	return consumed
}

// recoverPending reloads changes that were checkpointed by an interrupted
// run but not yet published. Changes already reflected in the replayed batch
// history (a crash after publish but before the journal was cleared) are
// dropped, so a published change is never emitted twice. Survivors are
// re-applied to the index so revisions fetched later in this run derive
// correctly against them.
func (e *Engine) recoverPending(pending []domain.Change) []domain.Change {
	if len(pending) == 0 {
		return nil
	}
	kept := make([]domain.Change, 0, len(pending))
	e.idxMu.Lock()
	defer e.idxMu.Unlock()
	for _, ch := range pending {
		key := ch.Institution + "|" + ch.SampleID + "|" + ch.SampledOn
		if ent, ok := e.index[key]; ok && ent.revision >= ch.Revision {
			continue
		}
		switch ch.Op {
		case domain.OpWithdraw:
			e.index[key] = &indexEntry{revision: ch.Revision, withdrawn: true}
		case domain.OpUpsert:
			if ch.Record != nil {
				rec := *ch.Record
				e.index[key] = &indexEntry{revision: ch.Revision, record: &rec}
			}
		}
		kept = append(kept, ch)
	}
	return kept
}

func copyWindows(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
