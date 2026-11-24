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
	"estuary-collector/internal/journal"
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
	server *http.Server
}

// New builds stores, token managers, source clients and the engine.
func New(cfg Config) (*Service, error) {
	batch, err := store.Open(cfg.DataDir + "/batches")
	if err != nil {
		return nil, err
	}
	states, err := state.Open(cfg.DataDir + "/state")
	if err != nil {
		return nil, err
	}
	runs, err := journal.Open(cfg.DataDir + "/state")
	if err != nil {
		return nil, err
	}
	tokens := auth.NewManager(cfg.AuthURL, cfg.ClientID, cfg.RefreshToken)
	alpha := source.NewAlpha(cfg.MockBaseURL, tokens)
	bravo := source.NewBravo(cfg.MockBaseURL, cfg.BravoToken)
	charlie := source.NewCharlie(cfg.MockBaseURL, tokens)

	engine, err := collector.New(batch, states, runs, alpha, bravo, charlie)
	if err != nil {
		return nil, err
	}
	return &Service{Engine: engine, batch: batch}, nil
}

// Handler returns the HTTP mux.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sync", s.handleSync)
	mux.HandleFunc("/v1/sync/runs", s.handleSyncRuns)
	mux.HandleFunc("/v1/batches", s.handleBatches)
	mux.HandleFunc("/v1/records", s.handleRecords)
	mux.HandleFunc("/v1/status", s.handleStatus)
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

// handleSyncRuns reports per-round, per-source sync progress from the run
// journal: pages fetched, current/failure stage, published batch and resume
// position save state. It answers from the journal alone — it never scans
// the batch files and never blocks on a stuck source.
func (s *Service) handleSyncRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	if q := r.URL.Query().Get("round"); q != "" {
		n, err := strconv.ParseInt(q, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "bad round number")
			return
		}
		run, ok := s.Engine.Run(n)
		if !ok {
			writeError(w, http.StatusNotFound, "round not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"syncing": s.Engine.Syncing(),
			"run":     run,
		})
		return
	}
	current, runs := s.Engine.Runs()
	writeJSON(w, http.StatusOK, map[string]any{
		"syncing":       s.Engine.Syncing(),
		"current_round": current,
		"runs":          runs,
	})
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
