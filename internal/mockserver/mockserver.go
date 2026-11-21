// Package mockserver implements the three simulated institution HTTP APIs and
// the shared short-lived-token authentication endpoint. Everything is local
// and driven by the deterministic data in package mockdata.
package mockserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"estuary-collector/internal/mockdata"
)

const (
	// SharedClient is the client id used by the two institutions that share
	// short-lived access tokens (offset and upload-window sources).
	SharedClient = "estuary-collector"

	// charlieInconsistentStatus is returned when a window version expires in
	// the middle of a page pass. A collector must then restart the window
	// from its first page under the current version.
	charlieInconsistentStatus = http.StatusGone
)

type clientAuth struct {
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	generation   int64
	refreshCount int64
}

// Server bundles the mock HTTP handlers and their mutable controls.
type Server struct {
	Provider *mockdata.Provider

	mu sync.RWMutex
	// clients keyed by client id.
	clients map[string]*clientAuth
	// rateLimited marks sources currently answering 429.
	rateLimited map[string]bool
	// invalidCursors holds cursor values that must be treated as expired.
	invalidCursors map[string]bool
	requestCounts  map[string]*atomic.Int64
	// charlieManifestCalls and charliePageCalls separate the cheap window
	// manifest from the row pages, so tests can prove unchanged windows are
	// skipped without fetching rows.
	charlieManifestCalls *atomic.Int64
	charliePageCalls     *atomic.Int64
}

// New builds a mock server and its initial tokens.
func New() *Server {
	s := &Server{
		Provider:             mockdata.NewProvider(),
		clients:              map[string]*clientAuth{},
		rateLimited:          map[string]bool{},
		invalidCursors:       map[string]bool{},
		requestCounts:        map[string]*atomic.Int64{},
		charlieManifestCalls: &atomic.Int64{},
		charliePageCalls:     &atomic.Int64{},
	}
	s.clients[SharedClient] = &clientAuth{
		accessToken:  "access-gen1",
		refreshToken: "refresh-token-v1",
		generation:   1,
	}
	for _, name := range []string{mockdata.InstitutionAlpha, mockdata.InstitutionBravo, mockdata.InstitutionCharlie} {
		s.requestCounts[name] = &atomic.Int64{}
	}
	return s
}

// Handler registers every mock route on a fresh mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/auth/token", s.handleToken)
	mux.HandleFunc("/mock/control/stage", s.handleStage)
	mux.HandleFunc("/mock/control/ratelimit", s.handleRateLimit)
	mux.HandleFunc("/mock/control/cursor/expire", s.handleCursorExpire)
	mux.HandleFunc("/mock/control/charlie/append", s.handleCharlieAppend)
	mux.HandleFunc("/mock/control/charlie/expire", s.handleCharlieExpire)
	mux.HandleFunc("/mock/control/reset", s.handleReset)
	mux.HandleFunc("/mock/stats", s.handleStats)

	mux.HandleFunc("/sources/alpha/records", s.requireSharedAuth(mockdata.InstitutionAlpha, s.handleAlpha))
	mux.HandleFunc("/sources/bravo/records", s.handleBravo)
	mux.HandleFunc("/sources/charlie/windows", s.requireSharedAuth(mockdata.InstitutionCharlie, s.handleCharlieWindows))
	mux.HandleFunc("/sources/charlie/records", s.requireSharedAuth(mockdata.InstitutionCharlie, s.handleCharlieRecords))

	return mux
}

// ---------- auth ----------

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		GrantType    string `json:"grant_type"`
		ClientID     string `json:"client_id"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	c, ok := s.clients[req.ClientID]
	if !ok {
		writeError(w, http.StatusUnauthorized, "unknown client")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	switch req.GrantType {
	case "client_credentials":
		// Used by tests to read the current token.
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": c.accessToken,
			"token_type":   "Bearer",
			"generation":   c.generation,
		})
	case "refresh_token":
		if req.RefreshToken != c.refreshToken {
			writeError(w, http.StatusUnauthorized, "invalid refresh token")
			return
		}
		c.generation++
		c.refreshCount++
		c.accessToken = fmt.Sprintf("access-gen%d", c.generation)
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":  c.accessToken,
			"refresh_token": c.refreshToken,
			"token_type":    "Bearer",
			"generation":    c.generation,
		})
	default:
		writeError(w, http.StatusBadRequest, "unsupported grant type")
	}
}

// requireSharedAuth enforces the current access token for alpha/charlie and
// applies per-source rate limiting before the request reaches a handler.
func (s *Server) requireSharedAuth(source string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.requestCounts[source].Add(1)
		if s.isRateLimited(source) {
			w.Header().Set("Retry-After", "2")
			writeError(w, http.StatusTooManyRequests, source+" is rate limited")
			return
		}
		token := bearer(r)
		c := s.clients[SharedClient]
		c.mu.Lock()
		valid := token == c.accessToken
		c.mu.Unlock()
		if !valid {
			writeError(w, http.StatusUnauthorized, "access token expired or unknown")
			return
		}
		next(w, r)
	}
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

// ---------- source: alpha (offset) ----------

func (s *Server) handleAlpha(w http.ResponseWriter, r *http.Request) {
	offset := atoiDefault(r.URL.Query().Get("offset"), 0)
	limit := atoiDefault(r.URL.Query().Get("limit"), mockdata.PageLimitAlpha)
	rows, next := s.Provider.AlphaPage(offset, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"format":      "new",
		"offset":      offset,
		"limit":       limit,
		"next_offset": next,
		"has_more":    next < s.Provider.AlphaLen(),
		"records":     rows,
	})
}

// ---------- source: bravo (cursor) ----------

func (s *Server) handleBravo(w http.ResponseWriter, r *http.Request) {
	s.requestCounts[mockdata.InstitutionBravo].Add(1)
	if s.isRateLimited(mockdata.InstitutionBravo) {
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusTooManyRequests, "bravo is rate limited")
		return
	}
	cursor := r.URL.Query().Get("cursor")
	offset := 0
	if cursor != "" {
		if s.consumeCursorInvalid(cursor) {
			writeError(w, http.StatusGone, "cursor expired, restart from head")
			return
		}
		var ok bool
		offset, ok = decodeCursor(cursor)
		if !ok {
			writeError(w, http.StatusGone, "malformed cursor, restart from head")
			return
		}
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), mockdata.PageLimitBravo)
	rows, nextOffset := s.Provider.BravoPage(offset, limit)
	nextCursor := ""
	if nextOffset < s.Provider.BravoLen() {
		nextCursor = encodeCursor(nextOffset)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"format":       "legacy",
		"next_cursor":  nextCursor,
		"has_more":     nextCursor != "",
		"records":      rows,
		"_next_offset": nextOffset,
	})
}

// ---------- source: charlie (upload-time windows with paging) ----------

func (s *Server) handleCharlieWindows(w http.ResponseWriter, r *http.Request) {
	s.charlieManifestCalls.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{
		"windows": s.Provider.CharlieWindows(),
	})
}

func (s *Server) handleCharlieRecords(w http.ResponseWriter, r *http.Request) {
	s.charliePageCalls.Add(1)
	window := r.URL.Query().Get("window")
	version := int64(atoiDefault(r.URL.Query().Get("version"), 0))
	offset := atoiDefault(r.URL.Query().Get("offset"), 0)
	limit := atoiDefault(r.URL.Query().Get("limit"), mockdata.PageLimitCharlie)
	rows, next, hasMore, servedVersion, ok, readable := s.Provider.CharliePage(window, version, offset, limit)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown window: "+window)
		return
	}
	if !readable {
		// The requested snapshot has been retired. The stored version can no
		// longer be continued; the collector must restart this window from
		// page zero of the current version.
		writeError(w, charlieInconsistentStatus,
			fmt.Sprintf("window %s version %d expired; restart at current version", window, version))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"format":      "new",
		"window":      window,
		"version":     servedVersion,
		"offset":      offset,
		"limit":       limit,
		"next_offset": next,
		"has_more":    hasMore,
		"records":     rows,
	})
}

// ---------- controls ----------

func (s *Server) handleStage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"stage": s.Provider.Stage()})
	case http.MethodPost:
		stage := s.Provider.Advance()
		writeJSON(w, http.StatusOK, map[string]any{"stage": stage})
	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or POST")
	}
}

func (s *Server) handleRateLimit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	source := r.URL.Query().Get("source")
	on := r.URL.Query().Get("on") != "false"
	s.mu.Lock()
	s.rateLimited[source] = on
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"source": source, "rate_limited": on})
}

func (s *Server) handleCursorExpire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Cursor string `json:"cursor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Cursor == "" {
		writeError(w, http.StatusBadRequest, "cursor required")
		return
	}
	s.mu.Lock()
	s.invalidCursors[req.Cursor] = true
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"expired": req.Cursor})
}

// handleCharlieAppend appends raw charlie rows to a window, creating the
// window when requested. The window version is bumped, so the next manifest
// read reveals that the window must be re-paged in full.
func (s *Server) handleCharlieAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Window      string           `json:"window"`
		Create      bool             `json:"create"`
		RecordsJSON []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Window == "" {
		writeError(w, http.StatusBadRequest, "window and records required")
		return
	}
	if !s.Provider.CharlieAppend(req.Window, req.RecordsJSON, req.Create) {
		writeError(w, http.StatusNotFound, "unknown window: "+req.Window)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"appended": len(req.RecordsJSON), "window": req.Window})
}

// handleCharlieExpire retires every retained snapshot of a window except its
// current version, so an interrupted page pass can no longer resume and must
// restart the window.
func (s *Server) handleCharlieExpire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Window string `json:"window"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad json")
		return
	}
	s.Provider.CharlieExpireOldVersions(req.Window)
	writeJSON(w, http.StatusOK, map[string]any{"expired": req.Window})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	s.Provider.Reset()
	s.mu.Lock()
	s.rateLimited = map[string]bool{}
	s.invalidCursors = map[string]bool{}
	for _, c := range s.requestCounts {
		c.Store(0)
	}
	s.charlieManifestCalls.Store(0)
	s.charliePageCalls.Store(0)
	c := s.clients[SharedClient]
	c.mu.Lock()
	c.accessToken = "access-gen1"
	c.generation = 1
	c.refreshCount = 0
	c.mu.Unlock()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"reset": true})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	rl := map[string]bool{}
	for k, v := range s.rateLimited {
		rl[k] = v
	}
	c := s.clients[SharedClient]
	c.mu.Lock()
	stats := map[string]any{
		"stage":            s.Provider.Stage(),
		"rate_limited":     rl,
		"token_generation": c.generation,
		"refresh_count":    c.refreshCount,
		"charlie_windows":  s.Provider.CharlieWindows(),
	}
	c.mu.Unlock()
	s.mu.RUnlock()
	counts := map[string]int64{}
	for k, v := range s.requestCounts {
		counts[k] = v.Load()
	}
	stats["request_counts"] = counts
	stats["charlie_manifest_calls"] = s.charlieManifestCalls.Load()
	stats["charlie_page_calls"] = s.charliePageCalls.Load()
	writeJSON(w, http.StatusOK, stats)
}

// ---------- helpers ----------

func (s *Server) isRateLimited(source string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rateLimited[source]
}

// consumeCursorInvalid reports whether a cursor is expired and clears the
// mark in the same step, modeling a one-shot expiry event: after the client
// restarts from the head, a re-issued cursor with the same content works.
func (s *Server) consumeCursorInvalid(cursor string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidCursors[cursor] {
		return false
	}
	delete(s.invalidCursors, cursor)
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
