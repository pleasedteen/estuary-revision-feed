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
	// Cursor source (bravo): opaque cursor of the next page; empty means
	// "start from head".
	Cursor string `json:"cursor,omitempty"`
	// Cursor source: last seen log offset, used for stats only.
	CursorOffset int `json:"cursor_offset,omitempty"`
	// WindowIndex is kept for compatibility with older state files. Window
	// positions are now identity/version based (see File.Windows); positional
	// indexing cannot handle windows that are attached late or revised in
	// place.
	//
	// Deprecated: window positions live in File.Windows.
	WindowIndex int `json:"window_index,omitempty"`
}

// File is the on-disk state container.
type File struct {
	Positions map[string]Position `json:"positions"`
	// Windows records, per source, the manifest version of every window whose
	// latest complete page pass has been published. A window is skipped after
	// the manifest alone when its upstream version still matches; a newer
	// version means the window must be paged again in full.
	Windows map[string]map[string]int64 `json:"windows,omitempty"`
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
	s := &Store{path: filepath.Join(dir, "state.json"), file: File{
		Positions: map[string]Position{},
		Windows:   map[string]map[string]int64{},
	}}
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
	if s.file.Windows == nil {
		s.file.Windows = map[string]map[string]int64{}
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

// WindowVersions returns the published manifest versions of a source's
// windows. The returned map is a copy and safe to mutate.
func (s *Store) WindowVersions(source string) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.file.Windows[source]))
	for k, v := range s.file.Windows[source] {
		out[k] = v
	}
	return out
}

// SetWindowVersion records the published version of one window and flushes
// the file atomically.
func (s *Store) SetWindowVersion(source, window string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file.Windows == nil {
		s.file.Windows = map[string]map[string]int64{}
	}
	if s.file.Windows[source] == nil {
		s.file.Windows[source] = map[string]int64{}
	}
	s.file.Windows[source][window] = version
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
