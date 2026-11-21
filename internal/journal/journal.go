// Package journal persists the progress of a page pass for sources whose
// validation unit spans several pages.
//
// Charlie publishes one batch per fully paged upload-window version, but a
// crash can happen after a few pages of that version. The journal keeps the
// raw records read so far plus the next page offset, so a restart can resume
// at the next page — but only while the upstream window version is still
// readable. Raw records (rather than derived changes) are kept so the resume
// re-derives them through the revision index: a page pass that had already
// been published before the crash is absorbed and never published twice.
// Nothing in the journal is visible downstream until the version completes.
package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Entry is the outstanding page pass of one source: which window version is
// being paged, where the next fetch starts, and the records already read from
// earlier pages of that version.
type Entry struct {
	Source  string           `json:"source"`
	Window  string           `json:"window,omitempty"`
	Version int64            `json:"version,omitempty"`
	Offset  int              `json:"offset"`
	Records []map[string]any `json:"records,omitempty"`
}

// Store is a mutex-protected JSON file updated atomically via temp+rename.
type Store struct {
	path string
	mu   sync.Mutex
}

// Open loads (or initializes) the journal in dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{path: filepath.Join(dir, "journal.json")}, nil
}

// Load returns the outstanding entry for a source, if any.
func (s *Store) Load(source string) (*Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.readLocked()
	if err != nil {
		return nil, false, err
	}
	e, ok := all[source]
	if !ok {
		return nil, false, nil
	}
	return &e, true, nil
}

// Save durably records the entry.
func (s *Store) Save(entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.readLocked()
	if err != nil {
		return err
	}
	all[entry.Source] = entry
	return s.writeLocked(all)
}

// Clear removes the entry of a source.
func (s *Store) Clear(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.readLocked()
	if err != nil {
		return err
	}
	if _, ok := all[source]; !ok {
		return nil
	}
	delete(all, source)
	return s.writeLocked(all)
}

func (s *Store) readLocked() (map[string]Entry, error) {
	all := map[string]Entry{}
	b, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return all, nil
	case err != nil:
		return nil, err
	}
	if len(b) == 0 {
		return all, nil
	}
	if err := json.Unmarshal(b, &all); err != nil {
		return nil, fmt.Errorf("parse journal: %w", err)
	}
	if all == nil {
		all = map[string]Entry{}
	}
	return all, nil
}

func (s *Store) writeLocked(all map[string]Entry) error {
	b, err := json.Marshal(all)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
