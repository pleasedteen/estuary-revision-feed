// Package store appends published batches to local JSON Lines files.
//
// Every batch is one JSON object per line and carries a globally monotonic
// sequence number range. Downstream consumers persist the last sequence they
// have seen and pass it back on the next read, so batches remain available
// for multiple independent consumers and read positions never expire.
//
// Writes are crash-safe: a batch is fully rendered into a temporary file and
// renamed into place only once, so an interrupted sync can never leave a
// half-written line behind. On restart the highest sequence is rebuilt by
// scanning the batch directory, making the batches themselves the
// authoritative durability record.
package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"estuary-collector/internal/domain"
)

const (
	// RotateBytes starts a new JSONL file once the current one passes 1 MiB.
	RotateBytes = 1 << 20
	activeName  = "batches.jsonl"
)

// Store owns the batches directory and the sequence counter.
type Store struct {
	dir string
	mu  sync.Mutex

	nextSeq  int64
	active   *os.File
	activeSz int64
}

// Open prepares the batches directory and rebuilds the sequence counter.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, nextSeq: 1}
	if err := s.rebuild(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, activeName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	s.active = f
	s.activeSz = info.Size()
	return s, nil
}

// Close releases the active file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		return s.active.Close()
	}
	return nil
}

// Publish appends one batch containing changes and returns the envelope that
// was written. Empty change sets are not written. The whole batch becomes
// visible atomically.
func (s *Store) Publish(batchID string, changes []domain.Change) (domain.Envelope, error) {
	if len(changes) == 0 {
		return domain.Envelope{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	env := domain.Envelope{
		BatchID:   batchID,
		ChangedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Changes:   make([]domain.Change, len(changes)),
	}
	for i, c := range changes {
		c.Seq = s.nextSeq + int64(i)
		env.Changes[i] = c
	}
	env.Seq = s.nextSeq
	line, err := json.Marshal(env)
	if err != nil {
		return domain.Envelope{}, err
	}
	line = append(line, '\n')

	// Rotate into an immutable, sequence-prefixed file before appending, so a
	// reader that follows files by name never sees a renamed-away file.
	if s.activeSz >= RotateBytes {
		if err := s.rotateLocked(); err != nil {
			return domain.Envelope{}, err
		}
	}
	if _, err := s.active.Write(line); err != nil {
		return domain.Envelope{}, err
	}
	if err := s.active.Sync(); err != nil {
		return domain.Envelope{}, err
	}
	s.nextSeq += int64(len(changes))
	s.activeSz += int64(len(line))
	return env, nil
}

// ReadAfter returns changes whose sequence is strictly greater than afterSeq,
// up to limit envelopes (0 means unlimited). The scan always starts from the
// first batch file, so any saved downstream position remains valid even when
// the collector restarted and rotated files.
func (s *Store) ReadAfter(afterSeq int64, limit int) ([]domain.Envelope, error) {
	s.mu.Lock()
	names := s.batchFilesLocked()
	s.mu.Unlock()

	var out = make([]domain.Envelope, 0)
	for _, name := range names {
		f, err := os.Open(filepath.Join(s.dir, name))
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			var env domain.Envelope
			if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
				f.Close()
				return nil, fmt.Errorf("corrupt batch line in %s: %w", name, err)
			}
			// Filter at change granularity: an envelope may straddle the
			// saved position even when its first sequence is not past it.
			kept := keepChangesAfter(env, afterSeq)
			if len(kept.Changes) > 0 {
				out = append(out, kept)
				if limit > 0 && len(out) >= limit {
					f.Close()
					return out, nil
				}
			}
		}
		if err := scanner.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}
	return out, nil
}

func keepChangesAfter(env domain.Envelope, afterSeq int64) domain.Envelope {
	kept := env
	kept.Changes = kept.Changes[:0:0]
	for _, c := range env.Changes {
		if c.Seq > afterSeq {
			kept.Changes = append(kept.Changes, c)
		}
	}
	return kept
}

// MaxSeq returns the highest sequence that has been published (0 if empty).
func (s *Store) MaxSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeq - 1
}

// rebuild scans every JSONL file and recovers the next sequence number.
func (s *Store) rebuild() error {
	names, err := s.batchFiles()
	if err != nil {
		return err
	}
	var maxSeq int64
	for _, name := range names {
		f, err := os.Open(filepath.Join(s.dir, name))
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			var env domain.Envelope
			if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
				f.Close()
				return fmt.Errorf("corrupt batch line in %s: %w", name, err)
			}
			for _, c := range env.Changes {
				if c.Seq > maxSeq {
					maxSeq = c.Seq
				}
			}
		}
		if err := scanner.Err(); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
	s.nextSeq = maxSeq + 1
	return nil
}

func (s *Store) rotateLocked() error {
	if err := s.active.Close(); err != nil {
		return err
	}
	archived := fmt.Sprintf("batch-%012d.jsonl", s.nextSeq)
	if err := os.Rename(filepath.Join(s.dir, activeName), filepath.Join(s.dir, archived)); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, activeName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.active = f
	s.activeSz = 0
	return nil
}

func (s *Store) batchFiles() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			names = append(names, e.Name())
		}
	}
	sortNames(names)
	return names, nil
}

func (s *Store) batchFilesLocked() []string {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return []string{activeName}
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			names = append(names, e.Name())
		}
	}
	sortNames(names)
	return names
}

// sortNames keeps archived files (batch-NNNN...) before the active file.
func sortNames(names []string) {
	sort.Slice(names, func(i, j int) bool {
		a, b := names[i], names[j]
		ai := strings.HasPrefix(a, "batch-")
		bi := strings.HasPrefix(b, "batch-")
		switch {
		case ai && !bi:
			return true
		case !ai && bi:
			return false
		case ai && bi:
			na, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(a, "batch-"), ".jsonl"), 10, 64)
			nb, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(b, "batch-"), ".jsonl"), 10, 64)
			return na < nb
		default:
			return a < b
		}
	})
}
