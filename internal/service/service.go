// Package service wires the collector engine to its HTTP surface.
package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"estuary-collector/internal/auth"
	"estuary-collector/internal/collector"
	"estuary-collector/internal/memstats"
	"estuary-collector/internal/mockserver"
	"estuary-collector/internal/source"
	"estuary-collector/internal/state"
	"estuary-collector/internal/store"
)

// Config configures a Service.
type Config struct {
	DataDir      string
	MockBaseURL  string
	AuthURL      string
	ClientID     string
	RefreshToken string
	BravoToken   string
}

// Service is the running local collection service.
type Service struct {
	Engine *collector.Engine
	batch  *store.Store
	mem    *memstats.Sampler
	server *http.Server
}

// New builds stores, token managers, source clients and the engine. The heap
// sampler starts first so that index recovery — which runs inside this
// constructor — is covered by the since-start high-water mark.
func New(cfg Config) (*Service, error) {
	mem := memstats.Start(time.Millisecond)
	batch, err := store.Open(cfg.DataDir + "/batches")
	if err != nil {
		return nil, err
	}
	states, err := state.Open(cfg.DataDir + "/state")
	if err != nil {
		return nil, err
	}
	tokens := auth.NewManager(cfg.AuthURL, cfg.ClientID, cfg.RefreshToken)
	alpha := source.NewAlpha(cfg.MockBaseURL, tokens)
	bravo := source.NewBravo(cfg.MockBaseURL, cfg.BravoToken)
	charlie := source.NewCharlie(cfg.MockBaseURL, tokens)

	engine, err := collector.New(batch, states, alpha, bravo, charlie)
	if err != nil {
		return nil, err
	}
	return &Service{Engine: engine, batch: batch, mem: mem}, nil
}

// Handler returns the HTTP mux.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sync", s.handleSync)
	mux.HandleFunc("/v1/batches", s.handleBatches)
	mux.HandleFunc("/v1/records", s.handleRecords)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/debug/mem", s.handleMem)
	mux.HandleFunc("/debug/mem/reset", s.handleMemReset)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// Start binds the listener.
func (s *Service) Start(addr string) error {
	s.server = &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s.server.ListenAndServe()
}

// Shutdown stops the HTTP server.
func (s *Service) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Service) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if s.Engine.Syncing() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "synchronization already running"})
		return
	}
	report := s.Engine.Sync(r.Context())
	status := http.StatusOK
	for _, src := range report.Sources {
		if src.Status != "ok" {
			status = http.StatusPartialContent
		}
	}
	writeJSON(w, status, report)
}

func (s *Service) handleBatches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	after, err := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	if err != nil {
		after = 0
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = 100
	}
	envelopes, err := s.batch.ReadAfter(after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	nextSeq := after
	for _, env := range envelopes {
		for _, ch := range env.Changes {
			if ch.Seq > nextSeq {
				nextSeq = ch.Seq
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"after_seq": after,
		"next_seq":  nextSeq,
		"count":     len(envelopes),
		"batches":   envelopes,
	})
}

func (s *Service) handleRecords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": s.Engine.Materialized()})
}

func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"syncing":     s.Engine.Syncing(),
		"max_seq":     s.batch.MaxSeq(),
		"server_time": time.Now().UTC().Format(time.RFC3339),
	})
}

// handleMem reports the Go heap high-water marks. peak_since_start covers
// restart recovery (which runs before the HTTP surface); peak_since_reset
// covers whatever happened since the last POST /debug/mem/reset.
func (s *Service) handleMem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"heap_alloc":       s.mem.Current(),
		"peak_since_start": s.mem.Peak(),
		"peak_since_reset": s.mem.PeakSinceReset(),
	})
}

// handleMemReset settles the heap with a GC and restarts the resettable
// high-water mark, returning the steady-state baseline in bytes.
func (s *Service) handleMemReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"heap_alloc": s.mem.Reset(),
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// DefaultConfig returns the config used by the demo commands against the
// bundled mock server.
func DefaultConfig(dataDir, mockAddr string) Config {
	return Config{
		DataDir:      dataDir,
		MockBaseURL:  "http://" + mockAddr,
		AuthURL:      "http://" + mockAddr + "/auth/token",
		ClientID:     mockserver.SharedClient,
		RefreshToken: "refresh-token-v1",
		BravoToken:   "bravo-static-token",
	}
}
