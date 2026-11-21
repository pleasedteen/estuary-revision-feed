package mockserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// TestCharlieArmFiresBetweenPages arms a retransmission on the second page
// request of the first window and verifies the first page still serves
// version 1 while the second page already serves version 2, exactly once.
func TestCharlieArmFiresBetweenPages(t *testing.T) {
	s := New()
	const window = "2026-11-10T00:00:00Z"

	arm := httptest.NewRequest(http.MethodPost,
		"/mock/control/charlie/retransmit?window="+window+"&arm_page=2", nil)
	rec := httptest.NewRecorder()
	s.handleCharlieRetransmit(rec, arm)
	if rec.Code != http.StatusOK {
		t.Fatalf("arm status = %d", rec.Code)
	}

	getPage := func(offset int) map[string]any {
		req := httptest.NewRequest(http.MethodGet,
			"/sources/charlie/records?window="+window+"&offset="+strconv.Itoa(offset)+"&limit=2", nil)
		req.Header.Set("Authorization", "Bearer access-gen1")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("page offset=%d status = %d body=%s", offset, w.Code, w.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	page1 := getPage(0)
	if page1["version"].(float64) != 1 {
		t.Fatalf("first page must still serve version 1, got %v", page1["version"])
	}
	page2 := getPage(2)
	if page2["version"].(float64) != 2 {
		t.Fatalf("second page must serve the retransmitted version 2, got %v", page2["version"])
	}
	// One-shot arm: further requests do not retransmit again.
	page3 := getPage(0)
	if page3["version"].(float64) != 2 {
		t.Fatalf("arm must fire once, got version %v", page3["version"])
	}
	if got := s.Provider.CharlieRetransmitCount(); got != 1 {
		t.Fatalf("retransmit count = %d, want 1", got)
	}

	// The manifest reflects the bumped version while row count is unchanged.
	mreq := httptest.NewRequest(http.MethodGet, "/sources/charlie/windows", nil)
	mreq.Header.Set("Authorization", "Bearer access-gen1")
	mw := httptest.NewRecorder()
	s.Handler().ServeHTTP(mw, mreq)
	var manifest struct {
		Windows []struct {
			Window   string `json:"window"`
			RowCount int    `json:"row_count"`
			Version  int64  `json:"version"`
		} `json:"windows"`
	}
	if err := json.Unmarshal(mw.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	for _, w := range manifest.Windows {
		if w.Window == window {
			if w.Version != 2 || w.RowCount != 3 {
				t.Fatalf("manifest entry after retransmit = %+v", w)
			}
			return
		}
	}
	t.Fatal("window missing from manifest")
}

// TestCharlieStickyArmRetransmitsEveryPage arms a sticky retransmission
// (negative arm_page) and checks the version bumps before every page
// request; arm_page=0 disarms it again.
func TestCharlieStickyArmRetransmitsEveryPage(t *testing.T) {
	s := New()
	const window = "2026-11-10T00:00:00Z"

	post := func(arm string) {
		req := httptest.NewRequest(http.MethodPost,
			"/mock/control/charlie/retransmit?window="+window+"&arm_page="+arm, nil)
		w := httptest.NewRecorder()
		s.handleCharlieRetransmit(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("arm %s status = %d", arm, w.Code)
		}
	}
	pageVersion := func(offset int) float64 {
		req := httptest.NewRequest(http.MethodGet,
			"/sources/charlie/records?window="+window+"&offset="+strconv.Itoa(offset)+"&limit=2", nil)
		req.Header.Set("Authorization", "Bearer access-gen1")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("page status = %d", w.Code)
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out["version"].(float64)
	}

	post("-1")
	// Every request sees a fresh version because the arm retransmits first.
	if v := pageVersion(0); v != 2 {
		t.Fatalf("first sticky page version = %v, want 2", v)
	}
	if v := pageVersion(0); v != 3 {
		t.Fatalf("second sticky page version = %v, want 3", v)
	}
	if v := pageVersion(2); v != 4 {
		t.Fatalf("third sticky page version = %v, want 4", v)
	}
	// Disarming freezes the version on subsequent requests.
	post("0")
	if v := pageVersion(0); v != 4 {
		t.Fatalf("after disarm version = %v, want 4", v)
	}
	if v := pageVersion(0); v != 4 {
		t.Fatalf("disarmed arm must not keep retransmitting, got version %v", v)
	}
}

// TestCharlieExactMultipleWindowTerminates verifies has_more terminates a
// window whose row count is an exact multiple of the page size without
// requiring an extra empty-page request.
func TestCharlieExactMultipleWindowTerminates(t *testing.T) {
	s := New()
	const window = "2026-11-11T00:00:00Z" // four rows, page limit of two

	fetch := func(offset int) map[string]any {
		req := httptest.NewRequest(http.MethodGet,
			"/sources/charlie/records?window="+window+"&offset="+strconv.Itoa(offset)+"&limit=2", nil)
		req.Header.Set("Authorization", "Bearer access-gen1")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("page offset=%d status = %d", offset, w.Code)
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := fetch(0)
	if first["has_more"].(bool) != true {
		t.Fatal("first of two pages must report has_more=true")
	}
	second := fetch(2)
	if second["has_more"].(bool) != false {
		t.Fatalf("last page of an exact-multiple window must report has_more=false, got %v", second["has_more"])
	}
}

// TestCharlieRetransmitStatsExposed verifies stats expose retransmission
// counters and per-window page request counts.
func TestCharlieRetransmitStatsExposed(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodPost,
		"/mock/control/charlie/retransmit?window=2026-11-10T00:00:00Z", nil)
	w := httptest.NewRecorder()
	s.handleCharlieRetransmit(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("retransmit status = %d", w.Code)
	}
	sw := httptest.NewRecorder()
	s.handleStats(sw, httptest.NewRequest(http.MethodGet, "/mock/stats", nil))
	var stats map[string]any
	if err := json.Unmarshal(sw.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats["charlie_retransmit_count"].(float64) != 1 {
		t.Fatalf("stats retransmit count wrong: %v", stats["charlie_retransmit_count"])
	}
	pages, ok := stats["charlie_window_page_requests"].(map[string]any)
	if !ok {
		t.Fatal("stats missing charlie_window_page_requests")
	}
	if len(pages) != 0 {
		t.Fatalf("no page requests happened yet, got %v", pages)
	}
}
