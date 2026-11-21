// Package state persists per-source read positions so that an interrupted
// synchronization can resume and already-published revisions are never
// re-emitted as changes.
//
// For the upload-window source (charlie) the position is a per-window map of
// the next unread row offset, checkpointed together with the run's pending
// (derived but not yet published) changes after every page. A restart
// therefore resumes at the first missing page instead of rescanning, and the
// pending journal lets the resumed run still publish one single batch.
//
// State files written by the previous version (window_index/window_offset,
// no per-window map, no pending journal) remain readable: the engine migrates
// them on the first synchronization after the upgrade.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"estuary-collector/internal/domain"
)

// Position is a generic read position covering all three pagination styles.
// Only the fields relevant to a source are used.
type Position struct {
	// Offset source (alpha): next row offset in the change stream.
	Offset int `json:"offset,omitempty"`
	// Cursor source (bravo): opaque cursor of the next page; empty means
	// "start from head".
	Cursor string `json:"cursor,omitempty"`
	// Cursor source: last seen log offset, used for stats only.
	CursorOffset int `json:"cursor_offset,omitempty"`
	// Window source (charlie), legacy v1 fields kept only so old state
	// directories can be migrated on first read after an upgrade: index of
	// the current upload window in the sorted directory...
	WindowIndex int `json:"window_index,omitempty"`
	// ...and the next row offset inside that window.
	WindowOffset int `json:"window_offset,omitempty"`
	// Window source (charlie), v2: next unread row offset per upload window,
	// keyed by window name so back-registered earlier windows and rows
	// appended to already-consumed windows are both picked up.
	Windows map[string]int `json:"windows,omitempty"`
}

// Pending is the per-source journal of changes that were derived (and
// checkpointed) but not yet published when a run was interrupted. The batch
// id is kept so the resumed run publishes under the same identity.
type Pending struct {
	BatchID string          `json:"batch_id,omitempty"`
	Changes []domain.Change `json:"changes,omitempty"`
}

// Empty reports whether the journal holds nothing.
func (p Pending) Empty() bool {
	return p.BatchID == "" && len(p.Changes) == 0
}

// File is the on-disk state container.
type File struct {
	Positions map[string]Position `json:"positions"`
	Pending   map[string]Pending  `json:"pending,omitempty"`
}

// Store is a mutex-protected JSON file updated atomically via temp+rename.
type Store struct {
	path string
	mu   sync.Mutex
	file File
}

// Open loads an existing state file or initializes an empty one.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		path: filepath.Join(dir, "state.json"),
		file: File{Positions: map[string]Position{}, Pending: map[string]Pending{}},
	}
	b, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s.file); err != nil {
			return nil, fmt.Errorf("parse state file: %w", err)
		}
	}
	if s.file.Positions == nil {
		s.file.Positions = map[string]Position{}
	}
	if s.file.Pending == nil {
		s.file.Pending = map[string]Pending{}
	}
	return s, nil
}

// Get returns the saved position of a source.
func (s *Store) Get(source string) Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Positions[source]
}

// Pending returns the saved pending journal of a source.
func (s *Store) Pending(source string) Pending {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Pending[source]
}

// Update changes one source position and flushes the whole file atomically.
// The pending journal is left untouched.
func (s *Store) Update(source string, pos Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.file.Positions[source] = pos
	return s.flushLocked()
}

// Checkpoint atomically persists one source's read position together with
// its pending journal. Position and journal must never diverge (a page's
// derived changes have to survive exactly as long as the position says the
// page was consumed), so both live in the same atomic write. An empty
// journal clears the entry.
func (s *Store) Checkpoint(source string, pos Position, pending Pending) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.file.Positions[source] = pos
	if pending.Empty() {
		delete(s.file.Pending, source)
	} else {
		s.file.Pending[source] = pending
	}
	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	b, err := json.MarshalIndent(&s.file, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
