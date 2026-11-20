package mockserver

import "testing"

func TestCursorRoundTrip(t *testing.T) {
	for _, offset := range []int{0, 1, 7, 1024} {
		tok := encodeCursor(offset)
		got, ok := decodeCursor(tok)
		if !ok || got != offset {
			t.Fatalf("cursor round trip failed for %d: got %d ok=%v", offset, got, ok)
		}
	}
	for _, bad := range []string{"", "!!!not-base64", "aW52YWxpZA"} {
		if _, ok := decodeCursor(bad); ok {
			t.Fatalf("malformed cursor accepted: %q", bad)
		}
	}
}
