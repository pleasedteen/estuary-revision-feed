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
// Charlie's batch unit is one complete upload-window version pass (see
// syncCharlie): the changes of a single version leave as one batch, and the
// page journal lets a crashed pass resume at its next page when the upstream
// version is still readable.
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
	// WindowRestart counts charlie page passes restarted because the saved
	// window version was retired before the pass finished.
	WindowRestart int    `json:"window_restart,omitempty"`
	Error         string `json:"error,omitempty"`
}

// Report is the full synchronization report.
type Report struct {
	StartedAt string         `json:"started_at"`
	EndedAt   string         `json:"ended_at"`
	Sources   []SourceResult `json:"sources"`
}

// Engine wires stores, state, the source clients and the in-progress page
// journal used to resume half-paged window versions.
type Engine struct {
	store   *store.Store
	states  *state.Store
	journal *journal.Store
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
func New(st *store.Store, stt *state.Store, j *journal.Store, a *source.AlphaClient, b *source.BravoClient, c *source.CharlieClient) (*Engine, error) {
	e := &Engine{
		store:   st,
		states:  stt,
		journal: j,
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
	// A pass that was interrupted mid-window leaves its partial changes in
	// the page journal, never in a published batch. The index therefore only
	// replays what downstream has actually seen; the resumed pass re-applies
	// the journal's changes when it continues.
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
//
// Charlie publishes in upload-time windows and every window is itself
// multi-page. Two anomalies make positional scanning wrong:
//   - a window can be attached late, with an upload time older than windows
//     already read (so it sorts before them);
//   - an already-read window can re-publish revisions in place (append rows,
//     bump its version), including reschedules of samples from other days.
//
// The manifest carries a version per window. A window whose version equals
// the last fully published version is skipped after the manifest alone is
// read. Every changed or newly attached window is paged in full, and the
// changes of one complete window-version pass are published as a single
// batch. A reschedule's withdrawal and insert therefore stay together, and a
// batch never mixes two versions or a partially paged window.
//
// An interrupted pass keeps its raw records and next offset in the page
// journal. If the saved version is still readable upstream, the restart
// continues from that offset and re-derives the union of journaled and new
// records — the revision index absorbs anything already published, so a crash
// between batch write and version checkpoint does not duplicate changes. If
// the saved version was retired, the pass restarts at page zero of the current
// version so one batch still holds exactly one version's complete pass.
// Downstream bookmarks are unaffected: batches keep the global monotonic
// sequence.
func (e *Engine) syncCharlie(ctx context.Context) (res SourceResult) {
	res.Source = mockdata.InstitutionCharlie
	published := e.states.WindowVersions(mockdata.InstitutionCharlie)

	windows, err := e.charlie.FetchWindows(ctx)
	if err != nil {
		res.Status, res.Error = classifyError(err)
		return res
	}

	pending, hasPending, err := e.journal.Load(mockdata.InstitutionCharlie)
	if err != nil {
		res.Status, res.Error = "failed", err.Error()
		return res
	}
	run := 0

	// Resume the interrupted window first. A journal whose version is already
	// published is stale (the crash hit after the batch and checkpoint but
	// before the journal clear) and is simply dropped.
	if hasPending {
		var current source.Window
		found := false
		for _, w := range windows {
			if w.Window == pending.Window {
				found, current = true, w
				break
			}
		}
		if !found || versionPublished(published[pending.Window], pending.Version) {
			if err := e.journal.Clear(mockdata.InstitutionCharlie); err != nil {
				res.Status, res.Error = "failed", err.Error()
				return res
			}
			hasPending = false
		} else {
			version, restarted, changes, err := e.resumeWindowPass(ctx, current, pending, &run, &res)
			if err != nil {
				res.Status, res.Error = classifyError(err)
				return res
			}
			if restarted {
				res.WindowRestart++
			}
			if err := e.publishWindow(current.Window, version, changes, &res); err != nil {
				res.Status, res.Error = classifyError(err)
				return res
			}
			published[current.Window] = version
			hasPending = false
		}
	}

	for _, w := range windows {
		if prev, ok := published[w.Window]; ok && prev == w.Version {
			// Unchanged window: only the manifest needed reading.
			continue
		}
		version, changes, err := e.runWindowPass(ctx, w, &run, &res)
		if err != nil {
			res.Status, res.Error = classifyError(err)
			return res
		}
		if err := e.publishWindow(w.Window, version, changes, &res); err != nil {
			res.Status, res.Error = classifyError(err)
			return res
		}
		published[w.Window] = version
	}

	res.Status = "ok"
	return res
}

// versionPublished reports whether a window's published version already covers
// the given journal version. A version is monotonic, so anything at or below
// the published one has already been fully paged and published.
func versionPublished(publishedVersion, journalVersion int64) bool {
	return publishedVersion >= journalVersion
}

// runWindowPass pages a window version from its first row, journaling raw
// records and progress after every page so a crash resumes at the next page.
// All pages are read before any change is derived, so the returned changes
// belong to exactly one complete version pass.
func (e *Engine) runWindowPass(ctx context.Context, w source.Window, run *int, res *SourceResult) (int64, []domain.Change, error) {
	version := w.Version
	offset := 0
	entry := journal.Entry{Source: mockdata.InstitutionCharlie, Window: w.Window, Version: version}
	var records []map[string]any
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		page, err := e.charlie.FetchWindowPage(ctx, w.Window, version, offset)
		if err != nil {
			return 0, nil, err
		}
		res.PagesFetched++
		res.RecordsSeen += len(page.Records)
		records = append(records, page.Records...)
		offset = page.Position.WindowOffset
		*run++
		// Persist page progress before anything can observe the page, so a
		// restart always continues at the next page and never publishes a
		// partial version.
		entry.Offset = offset
		entry.Records = records
		if err := e.journal.Save(entry); err != nil {
			return 0, nil, err
		}
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionCharlie, *run-1, page.Position)
		}
		if page.Done {
			break
		}
	}
	changes, err := e.deriveRecords(records)
	if err != nil {
		return 0, nil, err
	}
	return version, changes, nil
}

// resumeWindowPass continues a journaled pass. It returns the version read in
// full, whether the saved version had to be abandoned, and the changes
// derived from the complete pass.
func (e *Engine) resumeWindowPass(ctx context.Context, w source.Window, pending *journal.Entry, run *int, res *SourceResult) (int64, bool, []domain.Change, error) {
	version := pending.Version
	offset := pending.Offset
	records := append([]map[string]any(nil), pending.Records...)
	restarted := false
	entry := *pending

	// Discard a retired version's partial reading and begin the current
	// version from its first page.
	restartAt := func(newVersion int64) error {
		restarted = true
		version = newVersion
		offset = 0
		records = nil
		entry.Version = version
		entry.Offset = 0
		entry.Records = nil
		return e.journal.Save(entry)
	}

	for {
		if err := ctx.Err(); err != nil {
			return 0, restarted, nil, err
		}
		page, err := e.charlie.FetchWindowPage(ctx, w.Window, version, offset)
		if err != nil {
			var expired *source.WindowVersionExpiredError
			if !errors.As(err, &expired) {
				return 0, restarted, nil, err
			}
			if err := restartAt(w.Version); err != nil {
				return 0, restarted, nil, err
			}
			continue
		}
		// The server reports the version it actually served. If it differs
		// from the pinned version, the snapshot was retired between the
		// manifest read and this page; restart the pass at the current
		// version instead of mixing versions in one batch.
		if version != 0 && page.Position.WindowVersion != version {
			if err := restartAt(page.Position.WindowVersion); err != nil {
				return 0, restarted, nil, err
			}
			continue
		}
		if version == 0 {
			version = page.Position.WindowVersion
			entry.Version = version
		}
		res.PagesFetched++
		res.RecordsSeen += len(page.Records)
		records = append(records, page.Records...)
		offset = page.Position.WindowOffset
		*run++
		entry.Offset = offset
		entry.Records = records
		if err := e.journal.Save(entry); err != nil {
			return 0, restarted, nil, err
		}
		if e.hookAfterPage != nil {
			e.hookAfterPage(mockdata.InstitutionCharlie, *run-1, page.Position)
		}
		if page.Done {
			break
		}
	}
	changes, err := e.deriveRecords(records)
	if err != nil {
		return 0, restarted, nil, err
	}
	return version, restarted, changes, nil
}

// deriveRecords turns one complete version pass into changes against the
// revision index. Stale or already-published revisions produce nothing.
func (e *Engine) deriveRecords(records []map[string]any) ([]domain.Change, error) {
	changes := make([]domain.Change, 0, len(records))
	for _, raw := range records {
		ch, err := e.derive(raw)
		if err != nil {
			return nil, err
		}
		if ch != nil {
			changes = append(changes, *ch)
		}
	}
	return changes, nil
}

// publishWindow emits one batch for one completely paged window version and
// then durably records that version, in that order. A crash after the batch
// but before the version record only re-pages the window; the revision index
// absorbs replayed changes, so nothing is duplicated and nothing is lost.
func (e *Engine) publishWindow(window string, version int64, changes []domain.Change, res *SourceResult) error {
	if len(changes) > 0 {
		env, err := e.store.Publish(e.newBatchID(mockdata.InstitutionCharlie, 0), changes)
		if err != nil {
			return fmt.Errorf("publish charlie window %s: %w", window, err)
		}
		res.BatchID = env.BatchID
	}
	res.Changes += len(changes)
	if err := e.states.SetWindowVersion(mockdata.InstitutionCharlie, window, version); err != nil {
		return fmt.Errorf("save charlie window version: %w", err)
	}
	if err := e.journal.Clear(mockdata.InstitutionCharlie); err != nil {
		return fmt.Errorf("clear journal: %w", err)
	}
	return nil
}
