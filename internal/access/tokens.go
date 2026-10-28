package access

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// FenceTrust is the process-level issuer allowlist. Set it once during startup.
type FenceTrust struct {
	AllowedIssuers []string
}

var fenceTrust FenceTrust

// SetFenceTrust configures the issuers that may direct Fence requests. Call
// this once during startup. An empty allowlist rejects every issuer.
func SetFenceTrust(trust FenceTrust) {
	fenceTrust = trust
}

func CleanAccessToken(raw string) string {
	token := strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("bearer "):])
	}
	return token
}

func ValidateAccessToken(raw string) (string, error) {
	token := CleanAccessToken(raw)
	if token == "" {
		return "", fmt.Errorf("git access token is required")
	}
	return token, nil
}

func ValidateAuthorizationHeader(raw string) (string, error) {
	token := strings.TrimSpace(raw)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return "", fmt.Errorf("authorization header must use bearer auth")
	}
	return token, nil
}

func FenceUserEndpoint(authorizationHeader string) (string, error) {
	baseURL, err := FenceIssuerBaseURL(authorizationHeader)
	if err != nil {
		return "", err
	}
	return baseURL + "/user", nil
}

// FenceIssuerBaseURL parses the issuer claim without verifying the signature,
// then checks that the issuer belongs to the configured Fence trust allowlist.
// The signature check stays with Fence, which receives the original token.
func FenceIssuerBaseURL(authorizationHeader string) (string, error) {
	token := CleanAccessToken(authorizationHeader)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(token, claims); err != nil {
		return "", fmt.Errorf("failed to parse authorization token: %w", err)
	}
	issuer, _ := claims["iss"].(string)
	baseURL, err := normalizeIssuerBaseURL(issuer)
	if err != nil {
		return "", fmt.Errorf("authorization token has an invalid iss: %w", err)
	}
	if len(fenceTrust.AllowedIssuers) == 0 {
		return "", fmt.Errorf("fence issuer trust is not configured")
	}
	for _, allowed := range fenceTrust.AllowedIssuers {
		normalized, err := normalizeIssuerBaseURL(allowed)
		if err == nil && normalized == baseURL {
			return baseURL, nil
		}
	}
	return "", fmt.Errorf("authorization token issuer %q is not trusted", baseURL)
}

func normalizeIssuerBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("issuer is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse issuer: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return "", fmt.Errorf("issuer must be an http or https URL without userinfo")
	}
	path := strings.TrimRight(parsed.Path, "/")
	path = strings.TrimSuffix(path, "/user")
	path = strings.TrimRight(path, "/")
	return parsed.Scheme + "://" + parsed.Host + path, nil
}
