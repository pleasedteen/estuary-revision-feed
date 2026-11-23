// Package state persists per-source read positions so that an interrupted
// synchronization can resume and already-published revisions are never
// re-emitted as changes.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Position is a generic read position covering all three pagination styles.
// Only the fields relevant to a source are used.
type Position struct {
	// Offset source (alpha): next row offset in the change stream.
	Offset int `json:"offset,omitempty"`
	// Cursor source (bravo): cursor of the next page to fetch. Empty means
	// either "nothing confirmed yet, start from the head" or "the last run
	// reached the tail" — in the latter case ResumeCursor below carries the
	// re-entry point. The end of the log never returns a successor cursor, so
	// a tail bookmark always has an empty Cursor.
	Cursor string `json:"cursor,omitempty"`
	// Cursor source: log offset accompanying Cursor, used for stats only.
	CursorOffset int `json:"cursor_offset,omitempty"`
	// ResumeCursor re-fetches the last confirmed bravo page (the page behind
	// Cursor). It survives across rounds and restarts, so catching up after a
	// tail bookmark costs at most one re-read of the previous final page.
	// Empty means there is no confirmed page yet (head restart / fresh state).
	ResumeCursor string `json:"resume_cursor,omitempty"`
	// ResumeOffset is the log offset of the page fetched with ResumeCursor.
	ResumeOffset int `json:"resume_offset,omitempty"`
	// Window source (charlie): index of the current upload window...
	WindowIndex int `json:"window_index,omitempty"`
	// ...and the next row offset inside that window.
	WindowOffset int `json:"window_offset,omitempty"`
}

// File is the on-disk state container.
type File struct {
	Positions map[string]Position `json:"positions"`
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
	s := &Store{path: filepath.Join(dir, "state.json"), file: File{Positions: map[string]Position{}}}
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
	return s, nil
}

// Get returns the saved position of a source.
func (s *Store) Get(source string) Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Positions[source]
}

// Update changes one source position and flushes the whole file atomically.
func (s *Store) Update(source string, pos Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.file.Positions[source] = pos
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
