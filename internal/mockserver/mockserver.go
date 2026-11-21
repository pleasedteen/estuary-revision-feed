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
	"time"

	"estuary-collector/internal/mockdata"
)

const (
	// SharedClient is the client id used by the two institutions that share
	// short-lived access tokens (offset and upload-window sources).
	SharedClient = "estuary-collector"
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
	// bulkPageSize overrides the served page size of a source (used to keep
	// a large revision backlog on a realistically small number of pages).
	bulkPageSize map[string]int
	// bulkDelayMs adds artificial per-page latency to a source, modeling a
	// slow institution with a large backlog.
	bulkDelayMs map[string]int
}

// New builds a mock server and its initial tokens.
func New() *Server {
	s := &Server{
		Provider:       mockdata.NewProvider(),
		clients:        map[string]*clientAuth{},
		rateLimited:    map[string]bool{},
		invalidCursors: map[string]bool{},
		requestCounts:  map[string]*atomic.Int64{},
		bulkPageSize:   map[string]int{},
		bulkDelayMs:    map[string]int{},
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
	mux.HandleFunc("/mock/control/bulk", s.handleBulk)
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
	limit = s.effectivePage(mockdata.InstitutionAlpha, limit)
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

// effectivePage applies the bulk page-size override and per-page latency
// configured through /mock/control/bulk.
func (s *Server) effectivePage(source string, limit int) int {
	s.mu.RLock()
	bulk, delay := s.bulkPageSize[source], s.bulkDelayMs[source]
	s.mu.RUnlock()
	if bulk > limit {
		limit = bulk
	}
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
	return limit
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
	writeJSON(w, http.StatusOK, map[string]any{
		"windows": s.Provider.CharlieWindows(),
	})
}

func (s *Server) handleCharlieRecords(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	offset := atoiDefault(r.URL.Query().Get("offset"), 0)
	limit := atoiDefault(r.URL.Query().Get("limit"), mockdata.PageLimitCharlie)
	rows, next, ok := s.Provider.CharliePage(window, offset, limit)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown window: "+window)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"format":      "new",
		"window":      window,
		"offset":      offset,
		"limit":       limit,
		"next_offset": next,
		"has_more":    len(rows) == limit,
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

// handleBulk appends a deterministic revision backlog to a source and
// optionally enlarges its served page size and per-page latency. Used by the
// memory-bounded sync/recovery verification.
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		Source      string `json:"source"`
		Samples     int    `json:"samples"`
		Revisions   int    `json:"revisions"`
		PageSize    int    `json:"page_size"`
		PageDelayMs int    `json:"page_delay_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.Source != mockdata.InstitutionAlpha {
		writeError(w, http.StatusBadRequest, "bulk backlog is only supported for alpha")
		return
	}
	if req.Samples <= 0 || req.Revisions <= 0 {
		writeError(w, http.StatusBadRequest, "samples and revisions must be positive")
		return
	}
	rows := s.Provider.AppendBulkRevisions(req.Samples, req.Revisions)
	s.mu.Lock()
	if req.PageSize > 0 {
		s.bulkPageSize[req.Source] = req.PageSize
	}
	if req.PageDelayMs > 0 {
		s.bulkDelayMs[req.Source] = req.PageDelayMs
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"source":        req.Source,
		"rows_appended": rows,
		"stream_len":    s.Provider.AlphaLen(),
		"page_size":     req.PageSize,
		"page_delay_ms": req.PageDelayMs,
	})
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
	s.bulkPageSize = map[string]int{}
	s.bulkDelayMs = map[string]int{}
	for _, c := range s.requestCounts {
		c.Store(0)
	}
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
	}
	c.mu.Unlock()
	s.mu.RUnlock()
	counts := map[string]int64{}
	for k, v := range s.requestCounts {
		counts[k] = v.Load()
	}
	stats["request_counts"] = counts
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
