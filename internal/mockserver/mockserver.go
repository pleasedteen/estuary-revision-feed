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

	// charlieWindowPage counts inside-window page requests per window since
	// the last reset (the windows manifest call is counted separately).
	charlieWindowPage map[string]int
	// charlieArm models a whole-window retransmission arriving while a
	// collector is mid-window. armPage > 0 fires once before that page
	// request; armPage < 0 is sticky and retransmits before every page
	// request; armPage == 0 means no arm is active.
	charlieArmWindow string
	charlieArmPage   int
}

// New builds a mock server and its initial tokens.
func New() *Server {
	s := &Server{
		Provider:          mockdata.NewProvider(),
		clients:           map[string]*clientAuth{},
		rateLimited:       map[string]bool{},
		invalidCursors:    map[string]bool{},
		requestCounts:     map[string]*atomic.Int64{},
		charlieWindowPage: map[string]int{},
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
	mux.HandleFunc("/mock/control/charlie/retransmit", s.handleCharlieRetransmit)
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
	writeJSON(w, http.StatusOK, map[string]any{
		"windows": s.Provider.CharlieWindows(),
	})
}

func (s *Server) handleCharlieRecords(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")
	offset := atoiDefault(r.URL.Query().Get("offset"), 0)
	limit := atoiDefault(r.URL.Query().Get("limit"), mockdata.PageLimitCharlie)
	// A retransmission armed to happen between pages fires immediately before
	// the target page request is served. The window date and row count do not
	// change; only the per-window version (and some revisions) move on.
	s.maybeFireCharlieArm(window)
	rows, next, version, ok := s.Provider.CharliePage(window, offset, limit)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown window: "+window)
		return
	}
	s.mu.Lock()
	s.charlieWindowPage[window]++
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"format":      "new",
		"window":      window,
		"version":     version,
		"offset":      offset,
		"limit":       limit,
		"next_offset": next,
		// Use the real row count rather than len(rows)==limit so a window
		// whose size is an exact multiple of the page size still terminates
		// without an extra empty-page round trip.
		"has_more": next < s.Provider.CharlieLen(window),
		"records":  rows,
	})
}

// maybeFireCharlieArm triggers the armed retransmission. A positive arm
// fires once on the configured page request; a negative arm is sticky and
// retransmits before every page request of the window (used to exercise the
// collector's restart cap).
func (s *Server) maybeFireCharlieArm(window string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.charlieArmPage == 0 || s.charlieArmWindow != window {
		return
	}
	if s.charlieArmPage < 0 {
		// Sticky arm: the window changes before every page request.
		s.Provider.CharlieRetransmit(window)
		return
	}
	// The request about to be served is one past the number served so far.
	upcoming := s.charlieWindowPage[window] + 1
	if upcoming != s.charlieArmPage {
		return
	}
	s.charlieArmPage = 0
	s.charlieArmWindow = ""
	s.Provider.CharlieRetransmit(window)
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

// handleCharlieRetransmit controls Charlie whole-window retransmissions.
//
//	POST /mock/control/charlie/retransmit            retransmit the default
//	                                                 (first) window immediately
//	POST ...?window=2026-11-10T00:00:00Z             pick another window
//	POST ...?arm_page=2                              arm the retransmission to
//	                                                 fire before the 2nd page
//	                                                 request of that window
//	POST ...?arm_page=-1                             sticky arm: retransmit
//	                                                 before every page request
//	POST ...?arm_page=0                              disarm a pending arm
func (s *Server) handleCharlieRetransmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "2026-11-10T00:00:00Z"
	}
	query := r.URL.Query()
	if query.Has("arm_page") {
		// arm_page > 0: one-shot before that page request;
		// arm_page < 0: sticky, retransmit before every page request;
		// arm_page = 0: disarm any pending arm.
		armPage := atoiDefault(query.Get("arm_page"), 0)
		s.mu.Lock()
		if !s.Provider.CharlieWindowExists(window) {
			s.mu.Unlock()
			writeError(w, http.StatusNotFound, "unknown window: "+window)
			return
		}
		s.charlieArmWindow = window
		s.charlieArmPage = armPage
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"window":     window,
			"armed_page": armPage,
			"version":    s.Provider.CharlieVersion(window),
		})
		return
	}
	version, ok := s.Provider.CharlieRetransmit(window)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown window: "+window)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window":           window,
		"version":          version,
		"retransmit_count": s.Provider.CharlieRetransmitCount(),
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
	s.charlieWindowPage = map[string]int{}
	s.charlieArmWindow = ""
	s.charlieArmPage = 0
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
		"stage":                        s.Provider.Stage(),
		"rate_limited":                 rl,
		"token_generation":             c.generation,
		"refresh_count":                c.refreshCount,
		"charlie_retransmit_count":     s.Provider.CharlieRetransmitCount(),
		"charlie_window_page_requests": s.charlieWindowPageSnapshot(),
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

// charlieWindowPageSnapshot returns a copy of the per-window page counters.
func (s *Server) charlieWindowPageSnapshot() map[string]int {
	out := map[string]int{}
	for k, v := range s.charlieWindowPage {
		out[k] = v
	}
	return out
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
