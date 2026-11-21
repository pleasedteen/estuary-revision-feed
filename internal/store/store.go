// Package store appends published batches to local JSON Lines files.
//
// Every batch is one JSON object per line and carries a globally monotonic
// sequence number range. Downstream consumers persist the last sequence they
// have seen and pass it back on the next read, so batches remain available
// for multiple independent consumers and read positions never expire.
//
// Writes are crash-safe: a batch becomes visible only once its complete line
// (including the trailing newline) has been written and fsynced. A write that
// fails halfway is rolled back to the previous file size; if the process dies
// before that rollback, the next Open detects the trailing fragment (anything
// after the last newline) and truncates it away. Consequently an interrupted
// append never corrupts earlier batches and never exposes a partial batch.
// On restart the highest sequence is rebuilt by scanning the complete lines,
// making the batches themselves the authoritative durability record.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// WriteFault is a test hook that can shorten or fail a batch append. It
// receives the fully rendered line (including the trailing newline) and
// returns how many bytes should actually reach the file plus an optional
// error. Returning n < len(line) simulates a short write; returning a
// non-nil error makes Publish treat the append as failed and roll back the
// partial prefix. Passing nil removes the hook.
type WriteFault func(line []byte) (n int, err error)

// Store owns the batches directory and the sequence counter.
type Store struct {
	dir string
	mu  sync.RWMutex

	nextSeq  int64
	active   *os.File
	activeSz int64

	faultMu    sync.Mutex
	writeFault WriteFault
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
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
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

// SetWriteFault installs (or clears) the test hook used to exercise disk
// write failures. It is safe to call at any time.
func (s *Store) SetWriteFault(fn WriteFault) {
	s.faultMu.Lock()
	s.writeFault = fn
	s.faultMu.Unlock()
}

func (s *Store) getWriteFault() WriteFault {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	return s.writeFault
}

// Publish appends one batch containing changes and returns the envelope that
// was written. Empty change sets are not written. The whole batch becomes
// visible atomically: either the complete line is durable or nothing is.
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

	startSz := s.activeSz
	n := len(line)
	var writeErr error
	if hook := s.getWriteFault(); hook != nil {
		n, writeErr = hook(line)
		if n < 0 {
			n = 0
		}
		if n > len(line) {
			n = len(line)
		}
	}
	if n > 0 {
		if _, werr := s.active.Write(line[:n]); werr != nil && writeErr == nil {
			writeErr = werr
		}
	}
	if n != len(line) && writeErr == nil {
		// A short write would leave a fragment; treat it as a failed append.
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = s.active.Sync()
	}
	if writeErr != nil {
		// Remove any partial prefix so an in-process retry appends a clean
		// line. If this rollback fails, restart recovery drops the tail.
		if terr := s.active.Truncate(startSz); terr == nil {
			_ = s.active.Sync()
		}
		return domain.Envelope{}, writeErr
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := s.batchFilesLocked()
	out := make([]domain.Envelope, 0)
	for i, name := range names {
		last := i == len(names)-1
		err := scanLines(filepath.Join(s.dir, name), last, func(line []byte) error {
			var env domain.Envelope
			if err := json.Unmarshal(line, &env); err != nil {
				return fmt.Errorf("corrupt batch line in %s: %w", name, err)
			}
			// Filter at change granularity: an envelope may straddle the
			// saved position even when its first sequence is not past it.
			kept := keepChangesAfter(env, afterSeq)
			if len(kept.Changes) > 0 {
				out = append(out, kept)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if limit > 0 && len(out) >= limit {
			return out[:limit], nil
		}
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextSeq - 1
}

// rebuild scans every JSONL file and recovers the next sequence number. An
// incomplete trailing append in the active file (anything after the last
// newline) is truncated away: such a batch was never acknowledged, so it
// must not become visible. Complete lines remain authoritative.
func (s *Store) rebuild() error {
	names, err := s.batchFiles()
	if err != nil {
		return err
	}
	var maxSeq int64
	for i, name := range names {
		last := i == len(names)-1
		got, err := scanFileRebuilding(filepath.Join(s.dir, name), last)
		if err != nil {
			return err
		}
		if got > maxSeq {
			maxSeq = got
		}
	}
	s.nextSeq = maxSeq + 1
	return nil
}

// scanLines invokes fn for each newline-terminated line in path. A trailing
// fragment without a newline is ignored when allowIncompleteTail is true;
// otherwise it is reported as corruption (an archived file must be complete).
func scanLines(path string, allowIncompleteTail bool, fn func(line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 64*1024)
	for {
		line, rerr := br.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				if allowIncompleteTail {
					return nil
				}
				return fmt.Errorf("incomplete trailing line in %s", path)
			}
			if ferr := fn(line[:len(line)-1]); ferr != nil {
				return ferr
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
	}
}

// scanFileRebuilding returns the highest sequence in path and truncates an
// unfinished trailing append from the last (active) file.
func scanFileRebuilding(path string, allowIncompleteTail bool) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var maxSeq int64
	var offset int64
	br := bufio.NewReaderSize(f, 64*1024)
	for {
		line, rerr := br.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				if !allowIncompleteTail {
					return 0, fmt.Errorf("incomplete trailing line in %s", path)
				}
				// The batch was never fully appended; drop the fragment.
				if terr := os.Truncate(path, offset); terr != nil {
					return 0, terr
				}
				return maxSeq, nil
			}
			var env domain.Envelope
			if jerr := json.Unmarshal(line[:len(line)-1], &env); jerr != nil {
				return 0, fmt.Errorf("corrupt batch line in %s at offset %d: %w", path, offset, jerr)
			}
			for _, c := range env.Changes {
				if c.Seq > maxSeq {
					maxSeq = c.Seq
				}
			}
			offset += int64(len(line))
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return maxSeq, nil
			}
			return 0, rerr
		}
	}
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
