package access

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

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
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return "", fmt.Errorf("authorization token does not include iss")
	}
	u, err := url.Parse(issuer)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return "", fmt.Errorf("authorization token has an invalid iss")
	}
	return strings.TrimRight(issuer, "/") + "/user", nil
}
