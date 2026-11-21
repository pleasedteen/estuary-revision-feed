// Package mockdata holds the deterministic, reproducible dataset served by
// the three simulated institutions. Every institution exposes an append-only
// change stream of full record revisions; the streams differ only in the
// pagination style used to read them.
//
// Stages are advanced explicitly through the mock control API so that test
// scenarios (revision, reschedule, withdrawal, stale-revision resurrection
// attempt) are fully reproducible.
package mockdata

import (
	"fmt"
	"sort"
	"sync"
)

const (
	// InstitutionAlpha is read with offset pagination.
	InstitutionAlpha = "alpha"
	// InstitutionBravo is read with cursor pagination.
	InstitutionBravo = "bravo"
	// InstitutionCharlie is read through upload-time windows with paging.
	InstitutionCharlie = "charlie"

	PageLimitAlpha   = 3
	PageLimitBravo   = 3
	PageLimitCharlie = 2
)

// Provider is the in-memory, stage-driven source of all mock responses.
type Provider struct {
	mu sync.RWMutex

	stage int

	// alpha is an append-only change log read by offset.
	alpha []map[string]any
	// bravo is an append-only revision log read by opaque cursors.
	bravo []map[string]any
	// charlie groups rows by upload window; windows are returned in order.
	charlie map[string][]map[string]any

	// charlieLate counts generated late-arriving rows (ids C-8xx).
	charlieLate int
	// armedWindow/armedRows implement a one-shot mid-fetch append: after the
	// next charlie page is served, that many generated rows are appended to
	// the armed window, modeling an upload that lands while a round reads.
	armedWindow string
	armedRows   int
	// charlieFetches records every served charlie page (window + requested
	// offset) so tests can prove only the new range of a window is read.
	charlieFetches []CharlieFetch
}

// CharlieFetch is one served charlie page request.
type CharlieFetch struct {
	Window string
	Offset int
}

// NewProvider builds the stage-0 dataset.
func NewProvider() *Provider {
	p := &Provider{charlie: map[string][]map[string]any{}}
	p.resetLocked()
	return p
}

// Stage returns the current stage number (0-based).
func (p *Provider) Stage() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stage
}

// Reset restores the stage-0 dataset.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resetLocked()
}

func (p *Provider) resetLocked() {
	p.stage = 0
	p.alpha = stage0Alpha()
	p.bravo = stage0Bravo()
	p.charlieLate = 0
	p.armedWindow, p.armedRows = "", 0
	p.charlieFetches = nil
	p.charlie = map[string][]map[string]any{
		"2026-11-10T00:00:00Z": {
			newRow("charlie", "C-300", 1, "2026-11-10", "河口东", "悬浮物", "9.4", "mg/L", "active", "2026-11-10T01:00:00Z"),
			newRow("charlie", "C-301", 1, "2026-11-10", "河口东", "悬浮物", "11.2", "mg/L", "active", "2026-11-10T01:05:00Z"),
			newRow("charlie", "C-302", 1, "2026-11-10", "河口西", "悬浮物", "7.7", "g/m3", "active", "2026-11-10T01:10:00Z"),
		},
		"2026-11-11T00:00:00Z": {
			newRow("charlie", "C-303", 1, "2026-11-11", "河口西", "悬浮物", "6.1", "mg/L", "active", "2026-11-11T02:00:00Z"),
			newRow("charlie", "C-304", 1, "2026-11-11", "浮标1", "悬浮物", "14.8", "mg/L", "active", "2026-11-11T02:05:00Z"),
			newRow("charlie", "C-305", 1, "2026-11-11", "浮标1", "悬浮物", "10.0", "g/m3", "active", "2026-11-11T02:10:00Z"),
			newRow("charlie", "C-306", 1, "2026-11-11", "浮标2", "悬浮物", "5.5", "mg/L", "active", "2026-11-11T02:15:00Z"),
		},
	}
}

// Advance applies the next deterministic stage and returns the new number.
// After the last stage it returns the unchanged number.
func (p *Provider) Advance() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.stage {
	case 0:
		p.alpha = append(p.alpha, stage1Alpha()...)
		p.bravo = append(p.bravo, stage1Bravo()...)
		p.charlie["2026-11-12T00:00:00Z"] = []map[string]any{
			// Reschedule of C-301: old-day withdrawal and new-day insert are
			// adjacent revisions and must leave in the same batch.
			newRow("charlie", "C-301", 2, "2026-11-10", "河口东", "悬浮物", "11.2", "mg/L", "withdrawn", "2026-11-12T03:00:00Z"),
			newRow("charlie", "C-301", 3, "2026-11-14", "河口东", "悬浮物", "11.2", "mg/L", "active", "2026-11-12T03:00:01Z"),
			newRow("charlie", "C-307", 1, "2026-11-12", "浮标2", "悬浮物", "8.9", "mg/L", "active", "2026-11-12T03:05:00Z"),
		}
	case 1:
		p.alpha = append(p.alpha,
			newRow("alpha", "A-102", 2, "2026-11-11", "北滩", "悬浮物", "16.3", "g/m3", "active", "2026-11-13T09:00:00Z"),
		)
		// Stale revision of an already withdrawn record: an old active
		// revision re-read after a cursor restart must not resurrect it.
		p.bravo = append(p.bravo,
			oldRow("bravo", "B-201", 1, "2026-11-10", "南港", "悬浮物", "10.5", "g/m3", "A", "2026-11-10T10:00:00Z"),
		)
	case 2:
		p.alpha = append(p.alpha,
			newRow("alpha", "A-105", 2, "2026-11-11", "北滩", "悬浮物", "21.7", "mg/L", "active", "2026-11-15T09:00:00Z"),
		)
		p.bravo = append(p.bravo,
			oldRow("bravo", "B-200", 2, "2026-11-10", "南港", "悬浮物", "6.8", "mg/L", "A", "2026-11-15T10:00:00Z"),
		)
	case 3:
		// Charlie back-registers an upload window that sorts before every
		// window seen so far...
		p.charlie["2026-11-09T00:00:00Z"] = []map[string]any{
			newRow("charlie", "C-308", 1, "2026-11-09", "河口东", "悬浮物", "4.9", "mg/L", "active", "2026-11-09T01:00:00Z"),
			newRow("charlie", "C-309", 1, "2026-11-09", "浮标1", "悬浮物", "6.6", "g/m3", "active", "2026-11-09T01:05:00Z"),
		}
		// ...and appends detail rows to a window that was already fully read.
		// The append carries a plain revision plus a reschedule of C-302
		// whose two halves land on different pages of the window.
		p.charlie["2026-11-10T00:00:00Z"] = append(p.charlie["2026-11-10T00:00:00Z"],
			newRow("charlie", "C-300", 2, "2026-11-10", "河口东", "悬浮物", "9.8", "mg/L", "active", "2026-11-16T04:00:00Z"),
			newRow("charlie", "C-302", 2, "2026-11-10", "河口西", "悬浮物", "7.7", "mg/L", "withdrawn", "2026-11-16T04:05:00Z"),
			newRow("charlie", "C-302", 3, "2026-11-15", "河口西", "悬浮物", "7.7", "mg/L", "active", "2026-11-16T04:05:01Z"),
		)
	}
	p.stage++
	return p.stage
}

// AlphaLen returns the current length of the alpha change stream.
func (p *Provider) AlphaLen() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.alpha)
}

// AlphaPage returns one offset page and the new offset.
func (p *Provider) AlphaPage(offset, limit int) (rows []map[string]any, next int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slicePage(p.alpha, offset, limit)
}

// BravoLen returns the current length of the bravo revision log.
func (p *Provider) BravoLen() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.bravo)
}

// BravoPage returns one cursor page. The caller encodes the opaque cursor
// from the returned next offset; an offset at the end means no cursor.
func (p *Provider) BravoPage(offset, limit int) (rows []map[string]any, nextOffset int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slicePage(p.bravo, offset, limit)
}

// CharlieWindows returns the ordered upload windows and their row counts.
func (p *Provider) CharlieWindows() []map[string]any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	keys := make([]string, 0, len(p.charlie))
	for k := range p.charlie {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{
			"window":    k,
			"row_count": len(p.charlie[k]),
		})
	}
	return out
}

// CharliePage returns one page inside an upload window. Every request is
// logged. If a mid-fetch append is armed, it fires right after the page is
// sliced — the appended rows become visible to later requests but not to a
// round that already snapshotted the directory.
func (p *Provider) CharliePage(window string, offset, limit int) (rows []map[string]any, next int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	all, ok := p.charlie[window]
	if !ok {
		return nil, offset, false
	}
	p.charlieFetches = append(p.charlieFetches, CharlieFetch{Window: window, Offset: offset})
	rows, next = slicePage(all, offset, limit)
	if p.armedWindow != "" {
		p.appendLateRowsLocked(p.armedWindow, p.armedRows)
		p.armedWindow, p.armedRows = "", 0
	}
	return rows, next, true
}

// ArmCharlieAppend registers a one-shot append of n generated rows to the
// given window, fired after the next charlie page is served. It models rows
// that arrive while a synchronization round is mid-flight.
func (p *Provider) ArmCharlieAppend(window string, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armedWindow, p.armedRows = window, n
}

// CharlieFetches returns the served charlie page requests so far.
func (p *Provider) CharlieFetches() []CharlieFetch {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]CharlieFetch, len(p.charlieFetches))
	copy(out, p.charlieFetches)
	return out
}

// ResetCharlieFetches clears the served-page log.
func (p *Provider) ResetCharlieFetches() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.charlieFetches = nil
}

// appendLateRowsLocked appends n deterministically generated rows (ids
// C-800, C-801, ...) to a window. Callers must hold the lock or use the
// exported wrappers.
func (p *Provider) appendLateRowsLocked(window string, n int) {
	day := window
	if len(day) > len("2026-11-10") {
		day = day[:len("2026-11-10")]
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("C-8%02d", p.charlieLate)
		p.charlieLate++
		p.charlie[window] = append(p.charlie[window],
			newRow(InstitutionCharlie, id, 1, day, "河口东", "悬浮物", "3.3", "mg/L", "active", window))
	}
}

func slicePage(all []map[string]any, offset, limit int) ([]map[string]any, int) {
	if offset > len(all) {
		offset = len(all)
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	page := make([]map[string]any, 0, end-offset)
	for i := offset; i < end; i++ {
		page = append(page, all[i])
	}
	return page, end
}

// newRow builds a record in the "new" response format.
func newRow(inst, id string, rev int64, day, site, substance, value, unit, status, published string) map[string]any {
	return map[string]any{
		"institution":   inst,
		"sample_id":     id,
		"rev":           rev,
		"sampled_on":    day,
		"site":          site,
		"substance":     substance,
		"concentration": value,
		"unit":          unit,
		"status":        status,
		"published_at":  published,
	}
}

// oldRow builds a record in the legacy response format.
func oldRow(inst, id string, rev int64, day, station, analyte, value, unit, state, report string) map[string]any {
	return map[string]any{
		"institution":  inst,
		"sampleId":     id,
		"revisionId":   rev,
		"samplingDate": day,
		"stationName":  station,
		"analyte":      analyte,
		"value":        value,
		"unit":         unit,
		"state":        state, // A = active, D = withdrawn
		"reportTime":   report,
	}
}

func stage0Alpha() []map[string]any {
	return []map[string]any{
		newRow("alpha", "A-100", 1, "2026-11-10", "北滩", "悬浮物", "12.5", "mg/L", "active", "2026-11-10T08:00:00Z"),
		newRow("alpha", "A-101", 1, "2026-11-10", "北滩", "悬浮物", "8.2", "mg/L", "active", "2026-11-10T08:05:00Z"),
		newRow("alpha", "A-102", 1, "2026-11-11", "北滩", "悬浮物", "15.0", "g/m3", "active", "2026-11-10T08:10:00Z"),
		newRow("alpha", "A-103", 1, "2026-11-11", "南堤", "悬浮物", "19.4", "mg/L", "active", "2026-11-10T08:15:00Z"),
		newRow("alpha", "A-104", 1, "2026-11-11", "南堤", "悬浮物", "4.3", "mg/L", "active", "2026-11-10T08:20:00Z"),
		newRow("alpha", "A-105", 1, "2026-11-11", "北滩", "悬浮物", "20.1", "mg/L", "active", "2026-11-10T08:25:00Z"),
		newRow("alpha", "A-106", 1, "2026-11-12", "南堤", "悬浮物", "7.6", "mg/L", "active", "2026-11-10T08:30:00Z"),
		newRow("alpha", "A-107", 1, "2026-11-12", "南堤", "悬浮物", "9.9", "g/m3", "active", "2026-11-10T08:35:00Z"),
	}
}

func stage1Alpha() []map[string]any {
	return []map[string]any{
		// Plain revision.
		newRow("alpha", "A-100", 2, "2026-11-10", "北滩", "悬浮物", "13.1", "mg/L", "active", "2026-11-12T08:00:00Z"),
		// Reschedule of A-101 from 2026-11-10 to 2026-11-12.
		newRow("alpha", "A-101", 2, "2026-11-10", "北滩", "悬浮物", "8.2", "mg/L", "withdrawn", "2026-11-12T08:05:00Z"),
		newRow("alpha", "A-101", 3, "2026-11-12", "北滩", "悬浮物", "8.2", "mg/L", "active", "2026-11-12T08:05:01Z"),
		// Plain withdrawal.
		newRow("alpha", "A-104", 2, "2026-11-11", "南堤", "悬浮物", "4.3", "mg/L", "withdrawn", "2026-11-12T08:10:00Z"),
		// Brand-new sample.
		newRow("alpha", "A-108", 1, "2026-11-12", "南堤", "悬浮物", "6.0", "mg/L", "active", "2026-11-12T08:15:00Z"),
	}
}

func stage0Bravo() []map[string]any {
	return []map[string]any{
		oldRow("bravo", "B-200", 1, "2026-11-10", "南港", "悬浮物", "6.4", "mg/L", "A", "2026-11-10T10:00:00Z"),
		oldRow("bravo", "B-201", 1, "2026-11-10", "南港", "悬浮物", "10.5", "g/m3", "A", "2026-11-10T10:05:00Z"),
		oldRow("bravo", "B-202", 1, "2026-11-11", "东港", "悬浮物", "17.2", "mg/L", "A", "2026-11-10T10:10:00Z"),
		oldRow("bravo", "B-203", 1, "2026-11-11", "东港", "悬浮物", "3.8", "mg/L", "A", "2026-11-10T10:15:00Z"),
		oldRow("bravo", "B-204", 1, "2026-11-11", "西湾", "悬浮物", "12.0", "g/m3", "A", "2026-11-10T10:20:00Z"),
		oldRow("bravo", "B-205", 1, "2026-11-12", "西湾", "悬浮物", "5.7", "mg/L", "A", "2026-11-10T10:25:00Z"),
		oldRow("bravo", "B-206", 1, "2026-11-12", "西湾", "悬浮物", "8.8", "mg/L", "A", "2026-11-10T10:30:00Z"),
	}
}

func stage1Bravo() []map[string]any {
	return []map[string]any{
		// Plain withdrawal.
		oldRow("bravo", "B-201", 2, "2026-11-10", "南港", "悬浮物", "10.5", "g/m3", "D", "2026-11-12T10:00:00Z"),
		// Reschedule of B-203 from 2026-11-11 to 2026-11-13.
		oldRow("bravo", "B-203", 2, "2026-11-11", "东港", "悬浮物", "3.8", "mg/L", "D", "2026-11-12T10:05:00Z"),
		oldRow("bravo", "B-203", 3, "2026-11-13", "东港", "悬浮物", "3.8", "mg/L", "A", "2026-11-12T10:05:01Z"),
		// Brand-new sample.
		oldRow("bravo", "B-207", 1, "2026-11-12", "西湾", "悬浮物", "11.1", "mg/L", "A", "2026-11-12T10:10:00Z"),
	}
}
