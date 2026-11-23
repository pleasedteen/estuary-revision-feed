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
// The cursor source (bravo) additionally keeps a per-round readback staging
// area separate from the durable index: revisions seen during a head readback
// are only committed once the tail is reached and the batch is published, so
// an interrupted or budget-capped readback can never absorb unpublished
// revisions. Its tail bookmark stores a resume cursor for the last confirmed
// page, making normal catch-up re-read at most that one page; the round is
// hard-capped at bravoDetailBudget detail requests so an always-expired
// cursor cannot loop forever and starve newly published revisions.
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

// OverridePositionForTest replaces one source's persisted read position. It is
// intended for tests that need to inject a specific bookmark (for example a
// mid-log cursor that is about to expire).
func (e *Engine) OverridePositionForTest(source string, pos state.Position) error {
	return e.states.Update(source, pos)
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

// classifyBravo evaluates one bravo revision against the durable index and the
// current round's staging area without mutating the durable index. Fresh
// revisions are returned as changes and kept in staging; stale ones (already
// published, or already superseded within this readback) are absorbed. This
// keeps an interrupted head readback from silently swallowing revisions that
// were never published.
func (e *Engine) classifyBravo(raw map[string]any, staging map[string]*indexEntry) (*domain.Change, error) {
	rec, err := normalize.Record(raw)
	if err != nil {
		return nil, err
	}
	key := rec.Key()

	e.idxMu.RLock()
	knownEntry, known := e.index[key]
	e.idxMu.RUnlock()
	if known && rec.Revision <= knownEntry.revision {
		// Older than or equal to a revision that has already been published.
		return nil, nil
	}
	if staged, ok := staging[key]; ok && rec.Revision <= staged.revision {
		// Superseded by a later revision seen earlier within this readback.
		return nil, nil
	}

	if rec.Withdrawn {
		staging[key] = &indexEntry{revision: rec.Revision, withdrawn: true}
		return &domain.Change{
			Op:          domain.OpWithdraw,
			Institution: rec.Institution,
			SampleID:    rec.SampleID,
			SampledOn:   rec.SampledOn,
			Revision:    rec.Revision,
		}, nil
	}
	cp := rec
	staging[key] = &indexEntry{revision: rec.Revision, record: &cp}
	return &domain.Change{
		Op:          domain.OpUpsert,
		Institution: rec.Institution,
		SampleID:    rec.SampleID,
		SampledOn:   rec.SampledOn,
		Revision:    rec.Revision,
		Record:      &cp,
	}, nil
}

// commitStaging promotes the round's readback staging area into the durable
// index after its batch has been prepared for publication.
func (e *Engine) commitStaging(staging map[string]*indexEntry) {
	e.idxMu.Lock()
	defer e.idxMu.Unlock()
	for key, ent := range staging {
		if ent.withdrawn || ent.record == nil {
			e.index[key] = &indexEntry{revision: ent.revision, withdrawn: true}
			continue
		}
		rec := *ent.record
		e.index[key] = &indexEntry{revision: ent.revision, record: &rec}
	}
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

// bravoDetailBudget caps the number of bravo detail requests in one round.
// It bounds both a head readback after cursor expiry and normal catch-up, so
// an always-expired cursor can no longer trap a run in an endless head loop
// that starves newly published revisions.
const bravoDetailBudget = 6

// ---------- bravo: cursor ----------

func (e *Engine) syncBravo(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionBravo
	pos := e.states.Get(mockdata.InstitutionBravo)

	// Cursor is the next page to fetch; an empty value means the head. When
	// the previous run reached the tail, Cursor is empty but ResumeCursor
	// re-enters the last confirmed page, so normal catch-up re-reads at most
	// that one page instead of the whole log.
	cursor := pos.Cursor
	if cursor == "" {
		cursor = pos.ResumeCursor
	}
	// resumeCursor is the cursor that fetches the page most recently
	// confirmed (processed and, once the tail is reached, published).
	resumeCursor := pos.ResumeCursor
	resumeOffset := pos.ResumeOffset
	cursorOffset := pos.CursorOffset

	// Readback staging: fresh revisions seen during this round are held here,
	// separate from the durable index. It cannot accumulate published old
	// records, and is only committed together with the tail batch.
	staging := map[string]*indexEntry{}
	var changes []domain.Change
	restarted := false
	reachedTail := false
	run := 0

	for res.PagesFetched < bravoDetailBudget {
		if err := ctx.Err(); err != nil {
			res.Status, res.Error = "interrupted", err.Error()
			return res
		}
		page, err := e.bravo.Fetch(ctx, cursor, cursorOffset)
		if err != nil {
			var expired *source.CursorExpiredError
			if errors.As(err, &expired) {
				// The continuation cursor is dead: read the log back from the
				// head. Old revisions are absorbed by the durable index and the
				// staging area; the request budget guarantees this converges.
				restarted = true
				cursor, cursorOffset, resumeCursor, resumeOffset = "", 0, "", 0
				continue
			}
			res.Status, res.Error = classifyError(err)
			return res
		}
		res.PagesFetched++
		// This fetch confirms the page identified by the pre-fetch cursor.
		resumeCursor, resumeOffset = cursor, cursorOffset

		for _, raw := range page.Records {
			res.RecordsSeen++
			ch, cerr := e.classifyBravo(raw, staging)
			if cerr != nil {
				res.Status, res.Error = "failed", cerr.Error()
				return res
			}
			if ch != nil {
				changes = append(changes, *ch)
			}
		}
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionBravo, run, page.Position)
		}
		run++

		cursor = page.Position.Cursor
		cursorOffset = page.Position.CursorOffset
		if page.Done {
			reachedTail = true
			break
		}
	}

	if !reachedTail {
		// Budget exhausted before the tail (e.g. a cursor that keeps expiring
		// or a log longer than one round): nothing is published and the
		// bookmark is untouched, so a later round retries without an endless
		// loop. Staged revisions stay out of the durable index.
		res.Status = "failed"
		res.Error = fmt.Sprintf("bravo detail budget of %d requests exhausted before reaching the tail", bravoDetailBudget)
		res.CursorRestart = restarted
		return res
	}

	// The whole current log has been read back: publish exactly this round's
	// new changes in one batch, then commit them to the index and checkpoint.
	e.commitStaging(staging)
	nextPos := state.Position{
		// The tail has no successor cursor; keep Cursor empty and remember the
		// last confirmed page via ResumeCursor for cross-round/restart resume.
		Cursor:       "",
		CursorOffset: cursorOffset,
		ResumeCursor: resumeCursor,
		ResumeOffset: resumeOffset,
	}
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionBravo, e.newBatchID(mockdata.InstitutionBravo, run), changes, nextPos)
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	res.CursorRestart = restarted
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
	var changes []domain.Change
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
	bid, err := e.publishAndCheckpoint(mockdata.InstitutionCharlie, e.newBatchID(mockdata.InstitutionCharlie, run), changes,
		state.Position{WindowIndex: windowIndex, WindowOffset: windowOffset})
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	res.Changes, res.BatchID, res.Status = len(changes), bid, "ok"
	return res
}
