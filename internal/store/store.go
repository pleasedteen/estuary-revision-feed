// Package store appends published batches to local JSON Lines files.
//
// Every batch is one JSON object (the same envelope shape as the historical
// format) and carries a globally monotonic, contiguous sequence range.
// Downstream consumers persist the last sequence they have seen and pass it
// back on the next read, so batches remain available for multiple
// independent consumers and read positions never expire.
//
// # Layout
//
// Each published batch is a single self-contained file:
//
//	env-<firstSeq>-<lastSeq>-<batchID>.jsonl   (sequences zero-padded to 12)
//
// The batch is streamed to a temporary name and renamed into place only once
// complete, so it becomes visible atomically and an interrupted sync never
// leaves a half-written envelope behind. The sequence range embedded in the
// file name lets sequence recovery and downstream reads skip whole files
// without parsing them.
//
// Legacy files written by earlier versions — the active batches.jsonl and
// its rotated batch-NNNN.jsonl archives, each holding one envelope per
// line — remain readable: they are scanned first, in their historical
// order, and every sequence they contain predates any env-*.jsonl file
// because the sequence counter is monotonic across restarts.
//
// # Memory model
//
// Publishing and reading are both streaming. While a source is being
// collected, its changes are spilled to a fragment file one at a time; once
// the source is fully read, the fragment is assembled into the final
// envelope with a small fixed buffer. Recovery replays envelopes change by
// change. Neither a batch nor the full history is ever materialized in
// memory, so an arbitrarily large revision backlog fits in a few MiB of
// heap.
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
	envPrefix    = "env-"
	seqWidth     = 12
	fragSuffix   = ".frag"
	tmpSuffix    = ".tmp"
	legacyActive = "batches.jsonl"
)

// Store owns the batches directory and the sequence counter.
type Store struct {
	dir string
	mu  sync.Mutex

	nextSeq int64
}

// Open prepares the batches directory, removes stale temporary files from
// interrupted runs and rebuilds the sequence counter.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, nextSeq: 1}
	if err := s.cleanStale(); err != nil {
		return nil, err
	}
	if err := s.rebuild(); err != nil {
		return nil, err
	}
	return s, nil
}

// cleanStale removes fragment and temporary files left behind by runs that
// were interrupted before their batch committed. They were never renamed
// into place, so no published position can refer to them.
func (s *Store) cleanStale() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasSuffix(n, fragSuffix) || strings.HasSuffix(n, tmpSuffix) {
			if err := os.Remove(filepath.Join(s.dir, n)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------- streaming batch writer ----------

// Writer collects one source's changes into a spill file and publishes them
// as a single envelope on Commit. A Writer is not safe for concurrent use;
// each source run owns exactly one.
type Writer struct {
	st       *Store
	batchID  string
	fragPath string
	frag     *os.File
	w        *bufio.Writer
	count    int
	closed   bool
}

// BeginBatch opens a spill file for a new batch. Nothing becomes visible
// until Commit renames the assembled envelope into place.
func (s *Store) BeginBatch(batchID string) (*Writer, error) {
	fragPath := filepath.Join(s.dir, batchID+fragSuffix)
	f, err := os.OpenFile(fragPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		st:       s,
		batchID:  batchID,
		fragPath: fragPath,
		frag:     f,
		w:        bufio.NewWriter(f),
	}, nil
}

// Add appends one change to the spill file. The sequence is assigned at
// commit time, when the batch's contiguous range is known.
func (w *Writer) Add(c domain.Change) error {
	if w.closed {
		return errors.New("batch writer already closed")
	}
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if _, err := w.w.Write(line); err != nil {
		return err
	}
	if err := w.w.WriteByte('\n'); err != nil {
		return err
	}
	w.count++
	return nil
}

// Count returns the number of changes added so far.
func (w *Writer) Count() int { return w.count }

// Commit publishes the batch: it reserves a contiguous sequence range,
// assembles the final envelope file streaming, fsyncs and renames it into
// place. An empty batch publishes nothing and returns the zero Envelope,
// mirroring the historical "empty change sets are not written" rule.
func (w *Writer) Commit() (domain.Envelope, error) {
	if err := w.closeFrag(); err != nil {
		return domain.Envelope{}, err
	}
	if w.count == 0 {
		_ = os.Remove(w.fragPath)
		return domain.Envelope{}, nil
	}

	w.st.mu.Lock()
	defer w.st.mu.Unlock()

	first := w.st.nextSeq
	last := first + int64(w.count) - 1
	changedAt := time.Now().UTC().Format(time.RFC3339Nano)
	finalName := fmt.Sprintf("%s%0*d-%0*d-%s.jsonl", envPrefix, seqWidth, first, seqWidth, last, w.batchID)
	tmpPath := filepath.Join(w.st.dir, finalName+tmpSuffix)

	if err := w.assemble(tmpPath, first, changedAt); err != nil {
		_ = os.Remove(tmpPath)
		return domain.Envelope{}, fmt.Errorf("assemble batch: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(w.st.dir, finalName)); err != nil {
		_ = os.Remove(tmpPath)
		return domain.Envelope{}, fmt.Errorf("publish batch: %w", err)
	}
	_ = os.Remove(w.fragPath)
	w.st.nextSeq = last + 1
	return domain.Envelope{Seq: first, BatchID: w.batchID, ChangedAt: changedAt}, nil
}

// Abort discards the spilled changes without publishing anything.
func (w *Writer) Abort() error {
	_ = w.closeFrag()
	err := os.Remove(w.fragPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (w *Writer) closeFrag() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.w.Flush(); err != nil {
		_ = w.frag.Close()
		return err
	}
	return w.frag.Close()
}

// assemble renders the final envelope file: the historical single-line
// envelope shape, written incrementally so the batch never resides in
// memory. Changes are re-read from the spill file one at a time and
// assigned their final sequence numbers.
func (w *Writer) assemble(tmpPath string, first int64, changedAt string) error {
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(out)

	header, err := json.Marshal(struct {
		Seq       int64  `json:"seq"`
		BatchID   string `json:"batch_id"`
		ChangedAt string `json:"changed_at"`
	}{Seq: first, BatchID: w.batchID, ChangedAt: changedAt})
	if err != nil {
		out.Close()
		return err
	}
	// {"seq":...,"batch_id":...,"changed_at":...} -> strip '}', open changes.
	if _, err := bw.Write(header[:len(header)-1]); err != nil {
		out.Close()
		return err
	}
	if _, err := bw.WriteString(`,"changes":[`); err != nil {
		out.Close()
		return err
	}

	in, err := os.Open(w.fragPath)
	if err != nil {
		out.Close()
		return err
	}
	defer in.Close()
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	seq := first
	for i := 0; scanner.Scan(); i++ {
		var c domain.Change
		if err := json.Unmarshal(scanner.Bytes(), &c); err != nil {
			out.Close()
			return fmt.Errorf("re-read spilled change: %w", err)
		}
		c.Seq = seq
		seq++
		line, err := json.Marshal(c)
		if err != nil {
			out.Close()
			return err
		}
		if i > 0 {
			if err := bw.WriteByte(','); err != nil {
				out.Close()
				return err
			}
		}
		if _, err := bw.Write(line); err != nil {
			out.Close()
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		out.Close()
		return err
	}
	if _, err := bw.WriteString("]}\n"); err != nil {
		out.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// ---------- reading ----------

// EnvelopeHeader carries the envelope fields that precede the changes array.
type EnvelopeHeader struct {
	Seq       int64
	BatchID   string
	ChangedAt string
}

// errStopWalk aborts a file walk early without being treated as a failure.
var errStopWalk = errors.New("stop envelope walk")

// ReadAfter returns changes whose sequence is strictly greater than afterSeq,
// up to limit envelopes (0 means unlimited). The scan considers every batch
// file in sequence order, so any saved downstream position remains valid
// even when the collector restarted and the layout rotated.
//
// The envelopes themselves are materialized for the caller; use a limit to
// bound that. Recovery paths that must stay streaming use StreamChanges.
func (s *Store) ReadAfter(afterSeq int64, limit int) ([]domain.Envelope, error) {
	files, err := s.orderedFiles()
	if err != nil {
		return nil, err
	}
	out := make([]domain.Envelope, 0)
	for _, bf := range files {
		if !bf.legacy && bf.lastSeq <= afterSeq {
			continue // whole file is at or before the saved position
		}
		err := walkFileEnvelopes(filepath.Join(s.dir, bf.name),
			func(hdr EnvelopeHeader, next func(*domain.Change) bool) error {
				kept := domain.Envelope{
					Seq:       hdr.Seq,
					BatchID:   hdr.BatchID,
					ChangedAt: hdr.ChangedAt,
					Changes:   make([]domain.Change, 0),
				}
				var c domain.Change
				for next(&c) {
					if c.Seq > afterSeq {
						kept.Changes = append(kept.Changes, c)
					}
				}
				if len(kept.Changes) > 0 {
					out = append(out, kept)
					if limit > 0 && len(out) >= limit {
						return errStopWalk
					}
				}
				return nil
			})
		if err != nil && !errors.Is(err, errStopWalk) {
			return nil, err
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// StreamChanges invokes fn for every published change with a sequence
// strictly greater than afterSeq, in file order, without materializing the
// history. It is the recovery path used to rebuild the record index.
func (s *Store) StreamChanges(afterSeq int64, fn func(domain.Change) error) error {
	files, err := s.orderedFiles()
	if err != nil {
		return err
	}
	for _, bf := range files {
		if !bf.legacy && bf.lastSeq <= afterSeq {
			continue
		}
		err := walkFileEnvelopes(filepath.Join(s.dir, bf.name),
			func(_ EnvelopeHeader, next func(*domain.Change) bool) error {
				var c domain.Change
				for next(&c) {
					if c.Seq <= afterSeq {
						continue
					}
					if err := fn(c); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// StreamEnvelopes invokes fn for every envelope in sequence order. The
// changes of one envelope are pulled one at a time through next, which
// returns false once the envelope is exhausted. fn may return StopWalk() to
// finish the whole walk early.
func (s *Store) StreamEnvelopes(fn func(hdr EnvelopeHeader, next func(*domain.Change) bool) error) error {
	files, err := s.orderedFiles()
	if err != nil {
		return err
	}
	for _, bf := range files {
		err := walkFileEnvelopes(filepath.Join(s.dir, bf.name), fn)
		if errors.Is(err, errStopWalk) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// StopWalk lets a StreamEnvelopes callback end the walk early, without error.
func StopWalk() error { return errStopWalk }

// MaxSeq returns the highest sequence that has been published (0 if empty).
func (s *Store) MaxSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeq - 1
}

// rebuild recovers the next sequence number. env-*.jsonl files carry their
// sequence range in the name; only legacy files need a (streaming) scan.
func (s *Store) rebuild() error {
	files, err := s.orderedFiles()
	if err != nil {
		return err
	}
	var maxSeq int64
	for _, bf := range files {
		if !bf.legacy {
			if bf.lastSeq > maxSeq {
				maxSeq = bf.lastSeq
			}
			continue
		}
		err := walkFileEnvelopes(filepath.Join(s.dir, bf.name),
			func(_ EnvelopeHeader, next func(*domain.Change) bool) error {
				var c domain.Change
				for next(&c) {
					if c.Seq > maxSeq {
						maxSeq = c.Seq
					}
				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	s.nextSeq = maxSeq + 1
	return nil
}

// walkFileEnvelopes streams the envelopes of one batch file, whether a
// legacy multi-envelope JSONL file or a single-envelope env-*.jsonl file:
// both hold consecutive top-level envelope objects of the same shape. fn is
// invoked once per envelope with its header and a pull function that decodes
// the next change, returning false when the changes array is exhausted.
//
// Both the historical and the current writer emit "changes" as the last
// envelope field, so the header is complete when fn is called.
func walkFileEnvelopes(path string, fn func(hdr EnvelopeHeader, next func(*domain.Change) bool) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("corrupt batch file %s: %w", path, err)
		}
		if d, ok := tok.(json.Delim); !ok || d != '{' {
			return fmt.Errorf("corrupt batch file %s: expected envelope object", path)
		}
		var hdr EnvelopeHeader
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return fmt.Errorf("corrupt batch file %s: %w", path, err)
			}
			key, _ := kt.(string)
			switch key {
			case "seq":
				err = dec.Decode(&hdr.Seq)
			case "batch_id":
				err = dec.Decode(&hdr.BatchID)
			case "changed_at":
				err = dec.Decode(&hdr.ChangedAt)
			case "changes":
				if err := walkChangesArray(dec, path, hdr, fn); err != nil {
					// errStopWalk propagates: the caller decides whether
					// an early stop ends just this file or the whole walk.
					return err
				}
			default:
				var skip json.RawMessage
				err = dec.Decode(&skip)
			}
			if err != nil {
				return fmt.Errorf("corrupt batch file %s: %w", path, err)
			}
		}
		if _, err := dec.Token(); err != nil { // closing '}'
			return fmt.Errorf("corrupt batch file %s: %w", path, err)
		}
	}
}

func walkChangesArray(dec *json.Decoder, path string, hdr EnvelopeHeader,
	fn func(hdr EnvelopeHeader, next func(*domain.Change) bool) error) error {
	if _, err := dec.Token(); err != nil { // opening '['
		return fmt.Errorf("corrupt batch file %s: %w", path, err)
	}
	var derr error
	next := func(c *domain.Change) bool {
		if derr != nil || !dec.More() {
			return false
		}
		if err := dec.Decode(c); err != nil {
			derr = err
			return false
		}
		return true
	}
	if err := fn(hdr, next); err != nil {
		return err // includes errStopWalk: the decoder is abandoned mid-array
	}
	if derr != nil {
		return fmt.Errorf("corrupt batch file %s: %w", path, derr)
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return fmt.Errorf("corrupt batch file %s: %w", path, err)
	}
	return nil
}

// ---------- file ordering ----------

// batchFile is one readable batch file. Legacy files (batches.jsonl and the
// batch-NNNN.jsonl archives) predate every env-*.jsonl file in sequence.
type batchFile struct {
	name              string
	legacy            bool
	firstSeq, lastSeq int64 // env files only, parsed from the name
}

// orderedFiles lists every batch file in the order their sequences were
// published: all legacy files in their historical order, then env files by
// first sequence.
func (s *Store) orderedFiles() ([]batchFile, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var legacy []string
	var envs []batchFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if !strings.HasSuffix(n, ".jsonl") {
			continue // .frag/.tmp and anything foreign
		}
		if first, last, ok := parseEnvName(n); ok {
			envs = append(envs, batchFile{name: n, firstSeq: first, lastSeq: last})
			continue
		}
		legacy = append(legacy, n)
	}
	sortLegacyNames(legacy)
	sort.Slice(envs, func(i, j int) bool { return envs[i].firstSeq < envs[j].firstSeq })

	out := make([]batchFile, 0, len(legacy)+len(envs))
	for _, n := range legacy {
		out = append(out, batchFile{name: n, legacy: true})
	}
	out = append(out, envs...)
	return out, nil
}

// parseEnvName extracts the sequence range from env-<first>-<last>-<id>.jsonl.
func parseEnvName(name string) (first, last int64, ok bool) {
	if !strings.HasPrefix(name, envPrefix) || !strings.HasSuffix(name, ".jsonl") {
		return 0, 0, false
	}
	rest := name[len(envPrefix):]
	if len(rest) < seqWidth+1+seqWidth+1 {
		return 0, 0, false
	}
	if rest[seqWidth] != '-' || rest[2*seqWidth+1] != '-' {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(rest[:seqWidth], 10, 64)
	last, err2 := strconv.ParseInt(rest[seqWidth+1:2*seqWidth+1], 10, 64)
	if err1 != nil || err2 != nil || first <= 0 || last < first {
		return 0, 0, false
	}
	return first, last, true
}

// sortLegacyNames keeps archived files (batch-NNNN...) before the historical
// active file, exactly as the rotating writer ordered them.
func sortLegacyNames(names []string) {
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
