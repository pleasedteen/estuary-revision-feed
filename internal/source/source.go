// Package source implements HTTP clients for the three institutions and their
// three pagination styles:
//
//   - alpha:   offset pagination, short-lived shared token
//   - bravo:   opaque cursor pagination; an expired cursor forces a head
//     restart that may replay already-seen old revisions
//   - charlie: upload-time windows, each window itself multi-page
//
// HTTP 401 transparently refreshes the shared token (at most once per token
// generation, see package auth) and retries. HTTP 429 and bravo's 410 are
// returned as typed errors so one source failing never blocks the others.
package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"estuary-collector/internal/auth"
	"estuary-collector/internal/mockdata"
)

// Page is one fetched page: raw records plus the position the collector must
// persist once the page has been published.
type Page struct {
	Records  []map[string]any
	Position Next
	// Done is true when the source has no more pages right now.
	Done bool
}

// Next is the serialized read position after a page.
type Next struct {
	Offset       int    `json:"offset,omitempty"`
	Cursor       string `json:"cursor,omitempty"`
	CursorOffset int    `json:"cursor_offset,omitempty"`
	WindowIndex  int    `json:"window_index,omitempty"`
	WindowOffset int    `json:"window_offset,omitempty"`
}

// RateLimitedError wraps HTTP 429.
type RateLimitedError struct {
	Source     string
	RetryAfter string
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("%s rate limited (retry after %s)", e.Source, e.RetryAfter)
}

// CursorExpiredError wraps bravo's HTTP 410: the stored cursor is dead and
// the log must be re-read from the head.
type CursorExpiredError struct {
	Source string
}

func (e *CursorExpiredError) Error() string {
	return e.Source + " cursor expired, restarting from head"
}

// HTTPDoer is the minimal http client surface.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// baseClient holds common HTTP plumbing.
type baseClient struct {
	BaseURL     string
	Name        string
	HTTP        HTTPDoer
	Tokens      *auth.Manager // nil for bravo, which mocks a static bearer
	StaticToken string        // bravo uses a static bearer token
}

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
}

// do performs an authenticated GET, refreshing the shared token once on 401.
func (c *baseClient) do(ctx context.Context, path string, query url.Values) (*http.Response, []byte, error) {
	endpoint := c.BaseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	token, err := c.currentToken()
	if err != nil {
		return nil, nil, err
	}
	resp, body, err := c.sendOnce(ctx, endpoint, token)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && c.Tokens != nil {
		newToken, rerr := c.Tokens.Invalidate(token)
		if rerr != nil {
			return resp, body, rerr
		}
		resp, body, err = c.sendOnce(ctx, endpoint, newToken)
		if err != nil {
			return nil, nil, err
		}
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp, body, nil
	case http.StatusTooManyRequests:
		return resp, body, &RateLimitedError{Source: c.Name, RetryAfter: resp.Header.Get("Retry-After")}
	case http.StatusGone:
		return resp, body, &CursorExpiredError{Source: c.Name}
	default:
		return resp, body, fmt.Errorf("%s returned status %d: %s", c.Name, resp.StatusCode, truncate(body))
	}
}

func (c *baseClient) currentToken() (string, error) {
	if c.Tokens != nil {
		return c.Tokens.Token()
	}
	return c.StaticToken, nil
}

func (c *baseClient) sendOnce(ctx context.Context, endpoint, token string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%s request failed: %w", c.Name, err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, rerr := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if rerr != nil {
			break
		}
	}
	return resp, buf, nil
}

func truncate(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}

func decode(body []byte, v any) error {
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("decode response: %w (body: %s)", err, truncate(body))
	}
	return nil
}

// ---------- alpha: offset pagination ----------

// AlphaClient pages the offset-based institution.
type AlphaClient struct {
	baseClient
}

// NewAlpha creates an alpha client sharing the given token manager.
func NewAlpha(baseURL string, tokens *auth.Manager) *AlphaClient {
	return &AlphaClient{baseClient{
		BaseURL: baseURL,
		Name:    mockdata.InstitutionAlpha,
		HTTP:    newHTTPClient(),
		Tokens:  tokens,
	}}
}

type alphaResponse struct {
	Offset     int              `json:"offset"`
	NextOffset int              `json:"next_offset"`
	HasMore    bool             `json:"has_more"`
	Records    []map[string]any `json:"records"`
}

// Fetch returns the page at the given offset.
func (c *AlphaClient) Fetch(ctx context.Context, offset int) (Page, error) {
	q := url.Values{}
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("limit", fmt.Sprintf("%d", mockdata.PageLimitAlpha))
	_, body, err := c.do(ctx, "/sources/alpha/records", q)
	if err != nil {
		return Page{}, err
	}
	var resp alphaResponse
	if err := decode(body, &resp); err != nil {
		return Page{}, err
	}
	return Page{
		Records:  resp.Records,
		Position: Next{Offset: resp.NextOffset},
		Done:     !resp.HasMore,
	}, nil
}

// ---------- bravo: cursor pagination ----------

// BravoClient pages the cursor-based institution.
type BravoClient struct {
	baseClient
}

// NewBravo creates a bravo client with a static bearer token.
func NewBravo(baseURL, staticToken string) *BravoClient {
	return &BravoClient{baseClient{
		BaseURL:     baseURL,
		Name:        mockdata.InstitutionBravo,
		HTTP:        newHTTPClient(),
		StaticToken: staticToken,
	}}
}

type bravoResponse struct {
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
	Records    []map[string]any `json:"records"`
	NextOffset int              `json:"_next_offset"`
}

// Fetch returns the page at the cursor (empty cursor means the head).
func (c *BravoClient) Fetch(ctx context.Context, cursor string, cursorOffset int) (Page, error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	q.Set("limit", fmt.Sprintf("%d", mockdata.PageLimitBravo))
	_, body, err := c.do(ctx, "/sources/bravo/records", q)
	if err != nil {
		return Page{}, err
	}
	var resp bravoResponse
	if err := decode(body, &resp); err != nil {
		return Page{}, err
	}
	return Page{
		Records: resp.Records,
		Position: Next{
			Cursor:       resp.NextCursor,
			CursorOffset: resp.NextOffset,
		},
		Done: !resp.HasMore,
	}, nil
}

// ---------- charlie: upload-time windows with inner paging ----------

// CharlieClient pages the upload-window institution.
type CharlieClient struct {
	baseClient
}

// NewCharlie creates a charlie client sharing the given token manager.
func NewCharlie(baseURL string, tokens *auth.Manager) *CharlieClient {
	return &CharlieClient{baseClient{
		BaseURL: baseURL,
		Name:    mockdata.InstitutionCharlie,
		HTTP:    newHTTPClient(),
		Tokens:  tokens,
	}}
}

type windowsResponse struct {
	Windows []struct {
		Window   string `json:"window"`
		RowCount int    `json:"row_count"`
	} `json:"windows"`
}

type charlieResponse struct {
	Window     string           `json:"window"`
	NextOffset int              `json:"next_offset"`
	HasMore    bool             `json:"has_more"`
	Records    []map[string]any `json:"records"`
}

// FetchWindows returns the current ordered list of upload windows. The list
// is re-fetched every synchronization so newly opened windows are discovered.
func (c *CharlieClient) FetchWindows(ctx context.Context) ([]string, error) {
	_, body, err := c.do(ctx, "/sources/charlie/windows", nil)
	if err != nil {
		return nil, err
	}
	var resp windowsResponse
	if err := decode(body, &resp); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.Windows))
	for _, w := range resp.Windows {
		out = append(out, w.Window)
	}
	return out, nil
}

// FetchWindowPage returns one page inside an upload window.
func (c *CharlieClient) FetchWindowPage(ctx context.Context, window string, offset int) (Page, error) {
	q := url.Values{}
	q.Set("window", window)
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("limit", fmt.Sprintf("%d", mockdata.PageLimitCharlie))
	_, body, err := c.do(ctx, "/sources/charlie/records", q)
	if err != nil {
		return Page{}, err
	}
	var resp charlieResponse
	if err := decode(body, &resp); err != nil {
		return Page{}, err
	}
	return Page{
		Records:  resp.Records,
		Position: Next{WindowOffset: resp.NextOffset},
		Done:     !resp.HasMore,
	}, nil
}
