// Package journal records one entry per synchronization round so operators
// can see, per source, exactly how far the round progressed: pages fetched,
// the stage it is working in or failed at, the batch it published and
// whether the resume position was saved afterwards.
//
// The journal is persisted atomically (temp file + rename) at every
// persistence boundary — round start, batch published, position saved,
// round end — so the last round of a killed process still shows where it
// stopped after a restart, with round numbers continuing unchanged. A round
// (or source) left "running" on disk belongs to a dead process and is
// reported as "interrupted" on the next start.
//
// Querying the journal never touches the batch files: it is served from
// memory, so it answers instantly even while a source is stuck.
package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"estuary-collector/internal/state"
)

// Stage values: where a source is (or stopped) within a round. When a
// source's status is not ok, Stage is the failure stage.
const (
	StageFetch      = "fetch"      // paging the upstream source
	StageDerive     = "derive"     // normalizing / diffing records
	StagePublish    = "publish"    // writing the batch to disk
	StageCheckpoint = "checkpoint" // saving the resume position
	StageDone       = "done"       // finished
)

// Source status values.
const (
	StatusRunning     = "running"
	StatusOK          = "ok"
	StatusRateLimited = "rate_limited"
	StatusFailed      = "failed"
	StatusInterrupted = "interrupted"
)

// Run status values.
const (
	RunRunning     = "running"
	RunDone        = "done"
	RunInterrupted = "interrupted"
)

// keepRuns bounds how many rounds are retained on disk and in memory.
const keepRuns = 32

// SourceRun is the per-source progress of one round.
type SourceRun struct {
	Source       string `json:"source"`
	Status       string `json:"status"` // running, ok, rate_limited, failed, interrupted
	Stage        string `json:"stage"`  // fetch, derive, publish, checkpoint, done
	PagesFetched int    `json:"pages_fetched"`
	RecordsSeen  int    `json:"records_seen"`
	Changes      int    `json:"changes"`
	// Batch* describe the published batch once it is durable.
	BatchID      string `json:"batch_id,omitempty"`
	BatchSeq     int64  `json:"batch_seq,omitempty"`
	BatchChanges int    `json:"batch_changes,omitempty"`
	PublishedAt  string `json:"published_at,omitempty"`
	// PositionSaved reports whether the resume position was persisted after
	// the batch (or, for a change-free round, after the fetch loop). The
	// distinction batch published / position saved survives restarts, so a
	// round that stopped between the two is recognizable.
	PositionSaved bool            `json:"position_saved"`
	Position      *state.Position `json:"position,omitempty"`
	CursorRestart bool            `json:"cursor_restart,omitempty"`
	Error         string          `json:"error,omitempty"`
}

// Run is one synchronization round.
type Run struct {
	Round     int64       `json:"round"`
	Status    string      `json:"status"` // running, done, interrupted
	StartedAt string      `json:"started_at"`
	EndedAt   string      `json:"ended_at,omitempty"`
	Sources   []SourceRun `json:"sources"`
}

// file is the on-disk journal container.
type file struct {
	NextRound int64 `json:"next_round"`
	Runs      []Run `json:"runs"`
}

// Journal is a mutex-protected, atomically persisted run journal.
type Journal struct {
	path string
	mu   sync.Mutex
	f    file
}

// Open loads the journal from dir (runs.json), continuing round numbering.
// Rounds and sources left "running" by a dead process are marked
// "interrupted"; their stage/batch/position fields keep showing where they
// stopped.
func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	j := &Journal{path: filepath.Join(dir, "runs.json")}
	j.f.NextRound = 1
	b, err := os.ReadFile(j.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return j, nil
	case err != nil:
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &j.f); err != nil {
			return nil, fmt.Errorf("parse runs file: %w", err)
		}
	}
	if j.f.NextRound < 1 {
		j.f.NextRound = 1
	}
	j.mu.Lock()
	j.markInterruptedLocked()
	j.mu.Unlock()
	return j, nil
}

func (j *Journal) markInterruptedLocked() {
	for i := range j.f.Runs {
		r := &j.f.Runs[i]
		if r.Status == RunRunning {
			r.Status = RunInterrupted
		}
		for k := range r.Sources {
			if r.Sources[k].Status == StatusRunning {
				r.Sources[k].Status = StatusInterrupted
			}
		}
	}
}

// BeginRun appends a new round with every source marked running/fetch and
// persists it, so even an immediate crash leaves a visible round.
func (j *Journal) BeginRun(sources []string) Run {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := Run{
		Round:     j.f.NextRound,
		Status:    RunRunning,
		StartedAt: now(),
		Sources:   make([]SourceRun, len(sources)),
	}
	for i, s := range sources {
		r.Sources[i] = SourceRun{Source: s, Status: StatusRunning, Stage: StageFetch}
	}
	j.f.NextRound++
	j.f.Runs = append(j.f.Runs, r)
	j.trimLocked()
	j.persistLocked()
	return r
}

// UpdateSource mutates one source's entry within a round and persists the
// journal. Persistence is best-effort: the batch log stays the authoritative
// record, a journal write failure never fails the synchronization.
func (j *Journal) UpdateSource(round int64, source string, fn func(*SourceRun)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.findLocked(round)
	if r == nil {
		return
	}
	for i := range r.Sources {
		if r.Sources[i].Source == source {
			fn(&r.Sources[i])
			break
		}
	}
	j.persistLocked()
}

// EndRun marks a round done and persists it.
func (j *Journal) EndRun(round int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if r := j.findLocked(round); r != nil {
		r.Status = RunDone
		r.EndedAt = now()
	}
	j.persistLocked()
}

// Snapshot returns the current round number (0 before the first round) and a
// copy of the retained rounds, newest first. It never blocks on source
// goroutines and never scans the batch files.
func (j *Journal) Snapshot() (int64, []Run) {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Run, 0, len(j.f.Runs))
	for i := len(j.f.Runs) - 1; i >= 0; i-- {
		out = append(out, cloneRun(j.f.Runs[i]))
	}
	return j.f.NextRound - 1, out
}

// Find returns a copy of one round.
func (j *Journal) Find(round int64) (Run, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.findLocked(round)
	if r == nil {
		return Run{}, false
	}
	return cloneRun(*r), true
}

func (j *Journal) findLocked(round int64) *Run {
	for i := range j.f.Runs {
		if j.f.Runs[i].Round == round {
			return &j.f.Runs[i]
		}
	}
	return nil
}

func (j *Journal) trimLocked() {
	if len(j.f.Runs) > keepRuns {
		j.f.Runs = append([]Run(nil), j.f.Runs[len(j.f.Runs)-keepRuns:]...)
	}
}

func (j *Journal) persistLocked() {
	b, err := json.MarshalIndent(&j.f, "", "  ")
	if err != nil {
		return
	}
	tmp := j.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, j.path)
}

func cloneRun(r Run) Run {
	out := r
	out.Sources = make([]SourceRun, len(r.Sources))
	for i, s := range r.Sources {
		c := s
		if s.Position != nil {
			p := *s.Position
			c.Position = &p
		}
		out.Sources[i] = c
	}
	return out
}

func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
