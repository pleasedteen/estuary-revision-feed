package mockserver

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// encodeCursor turns a bravo log offset into an opaque token.
func encodeCursor(offset int) string {
	raw := "bravo-cursor:" + strconv.Itoa(offset)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor.
func decodeCursor(token string) (int, bool) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, false
	}
	raw := string(b)
	if !strings.HasPrefix(raw, "bravo-cursor:") {
		return 0, false
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(raw, "bravo-cursor:"))
	if err != nil || offset < 0 {
		return 0, false
	}
	return offset, true
}
