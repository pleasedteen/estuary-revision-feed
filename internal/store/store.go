// Package store appends published batches to local JSON Lines files.
//
// Every batch is one JSON object per line and carries a globally monotonic
// sequence number range. Downstream consumers persist the last sequence they
// have seen and pass it back on the next read, so batches remain available
// for multiple independent consumers and read positions never expire.
//
// Writes are crash-safe: a batch is a single line appended to the active file
// and fsynced before Publish returns. An interruption during the append can
// leave a partial trailing line; Open repairs that tail back to the last
// complete line, so batches themselves remain the authoritative durability
// record. On restart the highest sequence is rebuilt by scanning the batch
// directory.
//
// A batch is the atomic unit for downstream readers: ReadAfter returns whole
// envelopes only, never a subset of a batch's changes. The next_seq a caller
// saves is therefore always a batch boundary, and continuing from it can never
// split or repeat a published batch.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
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
	// RotateBytes is the default size at which a new JSONL file is started.
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
	// rotateBytes starts a new file once the active one reaches this size.
	// Zero means RotateBytes; tests override it with OpenWithRotation.
	rotateBytes int64
}

// Open prepares the batches directory and rebuilds the sequence counter.
func Open(dir string) (*Store, error) {
	return OpenWithRotation(dir, RotateBytes)
}

// OpenWithRotation is Open with an explicit rotation threshold. Sizes below
// one byte fall back to RotateBytes.
func OpenWithRotation(dir string, rotateBytes int64) (*Store, error) {
	if rotateBytes <= 0 {
		rotateBytes = RotateBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, nextSeq: 1, rotateBytes: rotateBytes}
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
	if s.activeSz >= s.rotateBytes {
		if err := s.rotateLocked(); err != nil {
			return domain.Envelope{}, err
		}
	}
	start := s.activeSz
	n, err := s.active.Write(line)
	if err == nil && n != len(line) {
		err = fmt.Errorf("short write: wrote %d of %d bytes", n, len(line))
	}
	if err == nil {
		err = s.active.Sync()
	}
	if err != nil {
		// Roll back a partial append so readers and the next Open never see
		// a torn line. O_APPEND makes the next write land at the end again.
		_ = s.active.Truncate(start)
		return domain.Envelope{}, err
	}
	s.nextSeq += int64(len(changes))
	s.activeSz += int64(len(line))
	return env, nil
}

// ReadAfter returns whole envelopes containing any sequence strictly greater
// than afterSeq, up to limit envelopes (0 means unlimited). The scan always
// starts from the first batch file, so any saved downstream position remains
// valid even when the collector restarted and rotated files.
//
// Batches are returned intact: a caller that saves the returned next_seq (the
// last sequence of the last envelope) always resumes on a batch boundary, so
// every published batch is seen completely and exactly once.
//
// Publication is atomic for readers as well: the returned slice is a stable
// snapshot taken under the store lock, so a concurrent Publish cannot inject a
// half-appended line or reorder batches.
func (s *Store) ReadAfter(afterSeq int64, limit int) ([]domain.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := s.batchFilesLocked()

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
			if len(env.Changes) == 0 {
				continue
			}
			// The batch is the atomic unit: include it whole or not at all.
			if env.Changes[len(env.Changes)-1].Seq <= afterSeq {
				continue
			}
			out = append(out, env)
			if limit > 0 && len(out) >= limit {
				f.Close()
				return out, nil
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

// MaxSeq returns the highest sequence that has been published (0 if empty).
func (s *Store) MaxSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeq - 1
}

// rebuild repairs a torn tail left by an interrupted append, then scans every
// JSONL file and recovers the next sequence number.
func (s *Store) rebuild() error {
	names, err := s.batchFiles()
	if err != nil {
		return err
	}
	var maxSeq int64
	for _, name := range names {
		path := filepath.Join(s.dir, name)
		good, scanErr := s.scanFile(path, &maxSeq)
		if scanErr == nil {
			// Drop any bytes after the last complete batch line. A torn append
			// has no trailing newline, so scanFile stopped cleanly but short.
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.Size() > good {
				if err := truncateTo(path, good); err != nil {
					return err
				}
			}
			continue
		}
		if !errors.Is(scanErr, errTornTail) || name != activeName {
			return scanErr
		}
		// Only the active file's newline-less trailing fragment is a torn
		// append from a crash. Discard it so the batch boundary is authoritative.
		if err := truncateTo(path, good); err != nil {
			return err
		}
	}
	s.nextSeq = maxSeq + 1
	return nil
}

// errTornTail marks a trailing fragment with no newline: a crash mid-append.
var errTornTail = errors.New("torn batch tail")

// scanFile reads lines from path, folding their sequence numbers into maxSeq,
// and returns the byte length of the longest valid prefix. A decode error stops
// the scan and is returned together with that prefix length.
func (s *Store) scanFile(path string, maxSeq *int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var good int64
	for scanner.Scan() {
		var env domain.Envelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
			terminated, terr := fileEndsWithNewline(path)
			if terr != nil {
				return good, terr
			}
			if !terminated {
				// A trailing line with no newline is a torn append from a crash
				// and may be discarded. A terminated corrupt line is real
				// corruption and must be surfaced.
				return good, errTornTail
			}
			return good, fmt.Errorf("corrupt batch line in %s: %w", filepath.Base(path), err)
		}
		good += int64(len(scanner.Bytes())) + 1
		for _, c := range env.Changes {
			if c.Seq > *maxSeq {
				*maxSeq = c.Seq
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return good, err
	}
	return good, nil
}

// fileEndsWithNewline reports whether the last byte of path is a newline.
func fileEndsWithNewline(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() == 0 {
		return true, nil
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], info.Size()-1); err != nil {
		return false, err
	}
	return b[0] == '\n', nil
}

// truncateTo drops everything after size and fsyncs the file.
func truncateTo(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
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
