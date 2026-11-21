package mockdata

import "testing"

// TestCharlieWindowVersioning covers the deterministic whole-window
// retransmission model: window identity and row count stay fixed while the
// version and one record revision move on.
func TestCharlieWindowVersioning(t *testing.T) {
	p := NewProvider()
	const window = "2026-11-10T00:00:00Z"

	if v := p.CharlieVersion(window); v != 1 {
		t.Fatalf("initial window version = %d, want 1", v)
	}
	manifest := p.CharlieWindows()
	var first map[string]any
	for _, w := range manifest {
		if w["window"] == window {
			first = w
		}
	}
	if first == nil || first["version"].(int64) != 1 || first["row_count"].(int) != 3 {
		t.Fatalf("unexpected manifest entry: %v", first)
	}

	v, ok := p.CharlieRetransmit(window)
	if !ok || v != 2 {
		t.Fatalf("retransmit returned version %d ok=%v, want 2", v, ok)
	}
	if got := p.CharlieRetransmitCount(); got != 1 {
		t.Fatalf("retransmit count = %d, want 1", got)
	}
	// Same row count, same window date.
	rows, next, version, ok := p.CharliePage(window, 0, PageLimitCharlie)
	if !ok || version != 2 || len(rows) != PageLimitCharlie {
		t.Fatalf("retransmitted page mismatch: rows=%d next=%d version=%d ok=%v", len(rows), next, version, ok)
	}
	// C-300 now carries the higher revision; the other rows are unchanged.
	c300 := rows[0]
	if c300["sample_id"] != "C-300" || c300["rev"].(int64) != 2 {
		t.Fatalf("retransmitted C-300 should be rev2: %v", c300)
	}

	// A second retransmission bumps version and revision monotonically again.
	v, _ = p.CharlieRetransmit(window)
	if v != 3 {
		t.Fatalf("second retransmit version = %d, want 3", v)
	}
	rows, _, version, _ = p.CharliePage(window, 0, PageLimitCharlie)
	if version != 3 || rows[0]["rev"].(int64) != 3 {
		t.Fatalf("second retransmit mismatch: version=%d rev=%v", version, rows[0]["rev"])
	}

	// Unknown windows cannot be retransmitted.
	if _, ok := p.CharlieRetransmit("nope"); ok {
		t.Fatal("retransmit of an unknown window should fail")
	}
	if p.CharlieWindowExists(window) != true || p.CharlieWindowExists("nope") != false {
		t.Fatal("CharlieWindowExists mismatch")
	}

	// Reset restores version 1.
	p.Reset()
	if v := p.CharlieVersion(window); v != 1 {
		t.Fatalf("window version after reset = %d, want 1", v)
	}
	if got := p.CharlieRetransmitCount(); got != 0 {
		t.Fatalf("retransmit count after reset = %d, want 0", got)
	}
}
