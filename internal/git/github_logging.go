package git

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func githubAccessTokenFingerprint(accessToken string) string {
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(accessToken))
	return hex.EncodeToString(sum[:])[:16]
}

func githubAccessTokenLength(accessToken string) int {
	return len(strings.TrimSpace(accessToken))
}
