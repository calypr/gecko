package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/gecko/apierror"
	"github.com/calypr/gecko/internal/httputil"
	"github.com/golang-jwt/jwt/v5"
)

type AccessError struct {
	StatusCode int
	Message    string
}

func (e *AccessError) Error() string {
	return e.Message
}

type ResourceAccessRecord struct {
	Method  string
	Service string
}

type ResourceAccessSnapshot map[string][]ResourceAccessRecord

type FenceUserAccessHandler struct {
	client *http.Client
}

func NewFenceUserAccessHandler(client *http.Client) *FenceUserAccessHandler {
	if client == nil {
		client = http.DefaultClient
	}
	return &FenceUserAccessHandler{client: client}
}

func (h *FenceUserAccessHandler) CheckResourceServiceAccess(token, method, service, resourcePath string) (bool, error) {
	allowed, err := h.GetAllowedResources(token, method, service)
	if err != nil {
		return false, err
	}
	resources, convErr := convertAnyToStringSlice(allowed)
	if convErr != nil {
		return false, &AccessError{StatusCode: http.StatusInternalServerError, Message: "authorization snapshot returned a non-string resource"}
	}
	for _, resource := range resources {
		if resource == resourcePath {
			return true, nil
		}
	}
	return false, nil
}

func (h *FenceUserAccessHandler) GetAllowedResources(token, method, service string) ([]any, error) {
	snapshot, err := h.GetResourceAccess(token)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(snapshot))
	for resource := range snapshot {
		if ResourceAccessAllows(snapshot, resource, method, service) {
			out = append(out, resource)
		}
	}
	return out, nil
}

func (h *FenceUserAccessHandler) GetResourceAccess(token string) (ResourceAccessSnapshot, error) {
	endpoint, err := fenceUserEndpoint(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: err.Error()}
	}
	validAuthorizationHeader, err := ValidateAuthorizationHeader(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: err.Error()}
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("failed to build authorization snapshot request: %s", err)}
	}
	req.Header.Set("Authorization", validAuthorizationHeader)

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("authorization snapshot request failed: %s", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("failed to read authorization snapshot response: %s", err)}
	}
	if resp.StatusCode >= 400 {
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = fmt.Sprintf("authorization snapshot request failed with status %d", resp.StatusCode)
		}
		return nil, &AccessError{StatusCode: resp.StatusCode, Message: message}
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("invalid authorization snapshot response: %s", err)}
	}
	return parseResourceAccessSnapshot(payload)
}

func parseResourceAccessSnapshot(payload map[string]any) (ResourceAccessSnapshot, error) {
	resourceAccess, ok := payload["authz"].(map[string]any)
	if !ok || len(resourceAccess) == 0 {
		resourceAccess, ok = payload["project_access"].(map[string]any)
		if !ok {
			return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: "authorization snapshot response did not include authz/project_access"}
		}
	}

	snapshot := make(ResourceAccessSnapshot, len(resourceAccess))
	for resource, raw := range resourceAccess {
		entries, ok := raw.([]any)
		if !ok {
			continue
		}
		records := make([]ResourceAccessRecord, 0, len(entries))
		for _, entry := range entries {
			record, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			method, _ := record["method"].(string)
			service, _ := record["service"].(string)
			records = append(records, ResourceAccessRecord{
				Method:  method,
				Service: service,
			})
		}
		snapshot[resource] = records
	}
	return snapshot, nil
}

func snapshotAllows(raw any, method, service string) bool {
	entries, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, entry := range entries {
		record, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		entryMethod, _ := record["method"].(string)
		entryService, _ := record["service"].(string)
		if entryMethod != method && entryMethod != "*" {
			continue
		}
		if entryService == "*" || service == "*" || entryService == service {
			return true
		}
	}
	return false
}

func ResourceAccessAllows(snapshot ResourceAccessSnapshot, resourcePath, method, service string) bool {
	entries := snapshot[resourcePath]
	for _, entry := range entries {
		if entry.Method != method && entry.Method != "*" {
			continue
		}
		if entry.Service == "*" || service == "*" || entry.Service == service {
			return true
		}
	}
	return false
}

func fenceUserEndpoint(authorizationHeader string) (string, error) {
	token := CleanAccessToken(authorizationHeader)
	if token == "" {
		return "", fmt.Errorf("authorization header is required")
	}
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	if _, _, err := parser.ParseUnverified(token, claims); err != nil {
		return "", fmt.Errorf("failed to parse authorization token: %w", err)
	}
	iss, _ := claims["iss"].(string)
	iss = strings.TrimSpace(iss)
	if iss == "" {
		return "", fmt.Errorf("authorization token does not include iss")
	}
	return strings.TrimRight(iss, "/") + "/user", nil
}

func convertAnyToStringSlice(anySlice []any) ([]string, *httputil.ErrorResponse) {
	var stringSlice []string
	for _, v := range anySlice {
		str, ok := v.(string)
		if !ok {
			return nil, httputil.NewError(apierror.TypeInvalidAuthorizationResponse, fmt.Sprintf("Element %v is not a string", v), http.StatusInternalServerError, nil, nil)
		}
		stringSlice = append(stringSlice, str)
	}
	return stringSlice, nil
}
