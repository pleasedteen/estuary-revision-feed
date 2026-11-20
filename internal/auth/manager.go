// Package auth manages the short-lived access token shared by two of the
// institutions. When a token generation expires, all concurrent callers that
// hit HTTP 401 are coalesced so that exactly one refresh request is issued
// for that generation; the other callers wait for the result and retry.
package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Manager owns one shared token lineage (refresh token stays constant).
type Manager struct {
	AuthURL      string
	ClientID     string
	HTTPClient   *http.Client
	RefreshToken string

	mu sync.Mutex
	// generation of the token held in currentToken; zero means "not loaded".
	generation int64
	// currentToken is the latest valid access token.
	currentToken string
	// refreshInFlight is non-nil while a refresh for this generation runs.
	refreshInFlight *call
}

type call struct {
	done chan struct{}
	tok  string
	err  error
}

// NewManager returns a manager that has not fetched a token yet.
func NewManager(authURL, clientID, refreshToken string) *Manager {
	return &Manager{
		AuthURL:      authURL,
		ClientID:     clientID,
		RefreshToken: refreshToken,
		HTTPClient:   &http.Client{Timeout: 10 * time.Second},
	}
}

// Token returns the current access token, fetching the first generation on
// demand if none has been loaded yet. Concurrent first loads are coalesced
// into a single refresh request.
func (m *Manager) Token() (string, error) {
	m.mu.Lock()
	if m.currentToken != "" {
		tok := m.currentToken
		m.mu.Unlock()
		return tok, nil
	}
	if m.refreshInFlight != nil {
		c := m.refreshInFlight
		m.mu.Unlock()
		<-c.done
		return c.tok, c.err
	}
	c := m.startRefreshLocked(0)
	m.mu.Unlock()
	<-c.done
	if c.err != nil {
		return "", c.err
	}
	return c.tok, nil
}

// Invalidate reports that the given (now stale) token was rejected with 401.
// Only the first report for a generation triggers an HTTP refresh; later
// callers join the in-flight refresh. The new token is returned.
func (m *Manager) Invalidate(staleToken string) (string, error) {
	m.mu.Lock()
	// A newer token is already available: the caller can simply use it.
	if m.currentToken != "" && m.currentToken != staleToken {
		tok := m.currentToken
		m.mu.Unlock()
		return tok, nil
	}
	if m.refreshInFlight != nil {
		c := m.refreshInFlight
		m.mu.Unlock()
		<-c.done
		if c.err != nil {
			return "", c.err
		}
		return c.tok, nil
	}
	c := m.startRefreshLocked(m.generation)
	m.mu.Unlock()
	<-c.done
	if c.err != nil {
		return "", c.err
	}
	return c.tok, nil
}

// startRefreshLocked must be called with mu held.
func (m *Manager) startRefreshLocked(afterGeneration int64) *call {
	c := &call{done: make(chan struct{})}
	m.refreshInFlight = c
	go func() {
		tok, gen, err := m.requestRefresh()
		m.mu.Lock()
		defer m.mu.Unlock()
		if err == nil && gen > afterGeneration {
			m.currentToken = tok
			m.generation = gen
		} else if err == nil {
			err = fmt.Errorf("refresh returned stale generation %d", gen)
		}
		c.tok = m.currentToken
		c.err = err
		m.refreshInFlight = nil
		close(c.done)
	}()
	return c
}

func (m *Manager) requestRefresh() (string, int64, error) {
	body, _ := json.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     m.ClientID,
		"refresh_token": m.RefreshToken,
	})
	req, err := http.NewRequest(http.MethodPost, m.AuthURL, bytes.NewReader(body))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.HTTPClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
		Generation  int64  `json:"generation"`
		Error       string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", 0, fmt.Errorf("decode refresh response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("refresh failed: status %d: %s", resp.StatusCode, out.Error)
	}
	return out.AccessToken, out.Generation, nil
}
