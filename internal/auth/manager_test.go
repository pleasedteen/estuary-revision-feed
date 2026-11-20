package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentInvalidateCoalescesRefresh asserts that many concurrent
// callers reporting the same expired token generation cause exactly one
// refresh request against the auth endpoint.
func TestConcurrentInvalidateCoalescesRefresh(t *testing.T) {
	var refreshes atomic.Int64
	var gen atomic.Int64
	gen.Store(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["grant_type"] == "refresh_token" {
			refreshes.Add(1)
			g := gen.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token":  tokenName(g),
				"refresh_token": "rt",
				"generation":    g,
			})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	m := NewManager(srv.URL, "client", "rt")
	m.currentToken = "gen1"
	m.generation = 1

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	tokens := make(chan string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			tok, err := m.Invalidate("gen1")
			if err != nil {
				errs <- err
				return
			}
			tokens <- tok
		}()
	}
	wg.Wait()
	close(errs)
	close(tokens)
	for err := range errs {
		t.Fatalf("invalidate: %v", err)
	}
	for tok := range tokens {
		if tok != tokenName(2) {
			t.Fatalf("all callers must receive gen2 token, got %q", tok)
		}
	}
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("expected exactly 1 refresh, got %d", got)
	}

	// A later invalidation using the now-current token is served from state
	// without any refresh.
	tok, err := m.Invalidate("gen1")
	if err != nil {
		t.Fatal(err)
	}
	if tok != tokenName(2) {
		t.Fatalf("expected cached gen2 token, got %q", tok)
	}
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("no extra refresh expected, count=%d", got)
	}
}

func tokenName(gen int64) string {
	return "gen" + string(rune('0'+gen))
}
