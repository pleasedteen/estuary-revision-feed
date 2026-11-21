// Package mockdata holds the deterministic, reproducible dataset served by
// the three simulated institutions. Every institution exposes an append-only
// change stream of full record revisions; the streams differ only in the
// pagination style used to read them.
//
// Stages are advanced explicitly through the mock control API so that test
// scenarios (revision, reschedule, withdrawal, stale-revision resurrection
// attempt) are fully reproducible.
//
// Charlie is special: its upload windows are versioned. A window can be
// attached late (even with an upload time older than already-read windows),
// and an existing window can publish new or re-scheduled revisions in place
// by appending rows and bumping its version. Every version is retained as a
// readable snapshot until it is explicitly expired, which lets a collector
// that was interrupted mid-page resume at the next page of the same version.
package mockdata

import (
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

// WindowMeta is one entry of the charlie window manifest. Version changes
// whenever the window's rows are appended to, so an unchanged window can be
// skipped after reading the manifest alone.
type WindowMeta struct {
	Window   string `json:"window"`
	Version  int64  `json:"version"`
	RowCount int    `json:"row_count"`
}

// charlieWindow keeps the current rows and a readable snapshot per version.
type charlieWindow struct {
	version   int64
	rows      []map[string]any
	snapshots map[int64][]map[string]any
}

// Provider is the in-memory, stage-driven source of all mock responses.
type Provider struct {
	mu sync.RWMutex

	stage int

	// alpha is an append-only change log read by offset.
	alpha []map[string]any
	// bravo is an append-only revision log read by opaque cursors.
	bravo []map[string]any
	// charlie groups rows by upload window; windows are returned in order
	// and carry a version that changes when rows are appended.
	charlie map[string]*charlieWindow
}

// NewProvider builds the stage-0 dataset.
func NewProvider() *Provider {
	p := &Provider{charlie: map[string]*charlieWindow{}}
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
	p.charlie = map[string]*charlieWindow{}
	p.putWindowLocked("2026-11-10T00:00:00Z", 1,
		newRow("charlie", "C-300", 1, "2026-11-10", "河口东", "悬浮物", "9.4", "mg/L", "active", "2026-11-10T01:00:00Z"),
		newRow("charlie", "C-301", 1, "2026-11-10", "河口东", "悬浮物", "11.2", "mg/L", "active", "2026-11-10T01:05:00Z"),
		newRow("charlie", "C-302", 1, "2026-11-10", "河口西", "悬浮物", "7.7", "g/m3", "active", "2026-11-10T01:10:00Z"),
	)
	p.putWindowLocked("2026-11-11T00:00:00Z", 1,
		newRow("charlie", "C-303", 1, "2026-11-11", "河口西", "悬浮物", "6.1", "mg/L", "active", "2026-11-11T02:00:00Z"),
		newRow("charlie", "C-304", 1, "2026-11-11", "浮标1", "悬浮物", "14.8", "mg/L", "active", "2026-11-11T02:05:00Z"),
		newRow("charlie", "C-305", 1, "2026-11-11", "浮标1", "悬浮物", "10.0", "g/m3", "active", "2026-11-11T02:10:00Z"),
		newRow("charlie", "C-306", 1, "2026-11-11", "浮标2", "悬浮物", "5.5", "mg/L", "active", "2026-11-11T02:15:00Z"),
	)
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
		p.appendWindowLocked("2026-11-12T00:00:00Z",
			// Reschedule of C-301: old-day withdrawal and new-day insert are
			// adjacent revisions and must leave in the same batch.
			newRow("charlie", "C-301", 2, "2026-11-10", "河口东", "悬浮物", "11.2", "mg/L", "withdrawn", "2026-11-12T03:00:00Z"),
			newRow("charlie", "C-301", 3, "2026-11-14", "河口东", "悬浮物", "11.2", "mg/L", "active", "2026-11-12T03:00:01Z"),
			newRow("charlie", "C-307", 1, "2026-11-12", "浮标2", "悬浮物", "8.9", "mg/L", "active", "2026-11-12T03:05:00Z"),
		)
	case 1:
		p.alpha = append(p.alpha,
			newRow("alpha", "A-102", 2, "2026-11-11", "北滩", "悬浮物", "16.3", "g/m3", "active", "2026-11-13T09:00:00Z"),
		)
		// Stale revision of an already withdrawn record: an old active
		// revision re-read after a cursor restart must not resurrect it.
		p.bravo = append(p.bravo,
			oldRow("bravo", "B-201", 1, "2026-11-10", "南港", "悬浮物", "10.5", "g/m3", "A", "2026-11-10T10:00:00Z"),
		)
		// A window with an upload time older than every existing window is
		// attached late; it sorts before the windows already read.
		p.appendWindowLocked("2026-11-09T00:00:00Z",
			newRow("charlie", "C-290", 1, "2026-11-09", "河口东", "悬浮物", "12.0", "mg/L", "active", "2026-11-09T01:00:00Z"),
			newRow("charlie", "C-291", 1, "2026-11-09", "河口西", "悬浮物", "8.6", "mg/L", "active", "2026-11-09T01:05:00Z"),
		)
		// An already-read window re-publishes revisions in place: a plain
		// revision plus a reschedule whose withdrawal and insert must still
		// share one batch.
		p.appendWindowLocked("2026-11-10T00:00:00Z",
			newRow("charlie", "C-300", 2, "2026-11-10", "河口东", "悬浮物", "9.8", "mg/L", "active", "2026-11-13T01:00:00Z"),
			newRow("charlie", "C-302", 2, "2026-11-10", "河口西", "悬浮物", "7.7", "g/m3", "withdrawn", "2026-11-13T01:05:00Z"),
			newRow("charlie", "C-302", 3, "2026-11-13", "河口西", "悬浮物", "7.7", "g/m3", "active", "2026-11-13T01:05:01Z"),
		)
	case 2:
		p.alpha = append(p.alpha,
			newRow("alpha", "A-105", 2, "2026-11-11", "北滩", "悬浮物", "21.7", "mg/L", "active", "2026-11-15T09:00:00Z"),
		)
		p.bravo = append(p.bravo,
			oldRow("bravo", "B-200", 2, "2026-11-10", "南港", "悬浮物", "6.8", "mg/L", "A", "2026-11-15T10:00:00Z"),
		)
		// Another existing window is revised in place.
		p.appendWindowLocked("2026-11-11T00:00:00Z",
			newRow("charlie", "C-303", 2, "2026-11-11", "河口西", "悬浮物", "6.5", "mg/L", "active", "2026-11-15T02:00:00Z"),
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

// CharlieWindows returns the ordered upload windows and their current
// versions and row counts.
func (p *Provider) CharlieWindows() []WindowMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	keys := make([]string, 0, len(p.charlie))
	for k := range p.charlie {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]WindowMeta, 0, len(keys))
	for _, k := range keys {
		w := p.charlie[k]
		out = append(out, WindowMeta{Window: k, Version: w.version, RowCount: len(w.rows)})
	}
	return out
}

// CharliePage returns one page inside an upload window version. A version of
// zero means the window's current version. The returned servedVersion is the
// version actually read, so a caller pinned to an older but still readable
// version can keep paging it. readable is false when the requested version
// has been expired. ok is false when the window is unknown.
func (p *Provider) CharliePage(window string, version int64, offset, limit int) (rows []map[string]any, next int, hasMore bool, servedVersion int64, ok, readable bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, found := p.charlie[window]
	if !found {
		return nil, offset, false, 0, false, false
	}
	if version == 0 {
		version = w.version
	}
	all, readable := w.snapshots[version]
	if !readable {
		return nil, offset, false, 0, true, false
	}
	rows, next = slicePage(all, offset, limit)
	return rows, next, next < len(all), version, true, true
}

// CharlieAppend appends rows to an existing window (or creates it when
// allowCreate is set) and bumps its version. Every append produces a new
// readable snapshot, modelling a window that re-publishes revisions in place.
func (p *Provider) CharlieAppend(window string, rows []map[string]any, allowCreate bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.charlie[window]; !ok {
		if !allowCreate {
			return false
		}
		p.appendWindowLocked(window, rows...)
		return true
	}
	p.appendWindowLocked(window, rows...)
	return true
}

// CharlieExpireOldVersions drops every retained snapshot of window except its
// current version, so a collector holding an older saved version can no
// longer resume and must restart from the window's first page.
func (p *Provider) CharlieExpireOldVersions(window string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, w := range p.charlie {
		if window != "" && name != window {
			continue
		}
		for v := range w.snapshots {
			if v != w.version {
				delete(w.snapshots, v)
			}
		}
	}
}

func (p *Provider) putWindowLocked(name string, version int64, rows ...map[string]any) {
	w := &charlieWindow{version: version, snapshots: map[int64][]map[string]any{}}
	w.rows = append([]map[string]any(nil), rows...)
	w.snapshots[version] = append([]map[string]any(nil), w.rows...)
	p.charlie[name] = w
}

func (p *Provider) appendWindowLocked(name string, rows ...map[string]any) {
	w, ok := p.charlie[name]
	if !ok {
		p.putWindowLocked(name, 1, rows...)
		return
	}
	w.version++
	w.rows = append(w.rows, rows...)
	w.snapshots[w.version] = append([]map[string]any(nil), w.rows...)
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
