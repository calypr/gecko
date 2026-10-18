package access

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/gecko/internal/httpclient"
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

type ResourceAccessHandler interface {
	GetAllowedResources(ctx context.Context, token, method, service string) ([]any, error)
	CheckResourceServiceAccess(ctx context.Context, token, method, service, resourcePath string) (bool, error)
}

type FenceUserAccessHandler struct {
	client *http.Client
}

func NewFenceUserAccessHandler(client *http.Client) *FenceUserAccessHandler {
	if client == nil {
		client = httpclient.NewServiceClient(30 * time.Second)
	}
	return &FenceUserAccessHandler{client: client}
}

func (h *FenceUserAccessHandler) CheckResourceServiceAccess(ctx context.Context, token, method, service, resourcePath string) (bool, error) {
	allowed, err := h.GetAllowedResources(ctx, token, method, service)
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

func (h *FenceUserAccessHandler) GetAllowedResources(ctx context.Context, token, method, service string) ([]any, error) {
	snapshot, err := h.GetResourceAccess(ctx, token)
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

func (h *FenceUserAccessHandler) GetResourceAccess(ctx context.Context, token string) (ResourceAccessSnapshot, error) {
	const unauthorizedMessage = "request is not authorized"
	endpoint, err := FenceUserEndpoint(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: unauthorizedMessage}
	}
	validAuthorizationHeader, err := ValidateAuthorizationHeader(token)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusUnauthorized, Message: unauthorizedMessage}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("failed to build authorization snapshot request: %s", err)}
	}
	req.Header.Set("Authorization", validAuthorizationHeader)

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, &AccessError{StatusCode: http.StatusBadGateway, Message: fmt.Sprintf("authorization snapshot request failed: %s", err)}
	}
	defer func() { _ = resp.Body.Close() }()

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

func AllowedResources(ctx context.Context, handler ResourceAccessHandler, token, method, service string) ([]string, error) {
	allowed, err := handler.GetAllowedResources(ctx, token, method, service)
	if err != nil {
		return nil, err
	}
	return convertAnyToStringSlice(allowed)
}

func convertAnyToStringSlice(anySlice []any) ([]string, error) {
	stringSlice := make([]string, 0, len(anySlice))
	for _, value := range anySlice {
		item, ok := value.(string)
		if !ok {
			return nil, &AccessError{StatusCode: http.StatusInternalServerError, Message: fmt.Sprintf("element %v is not a string", value)}
		}
		stringSlice = append(stringSlice, item)
	}
	return stringSlice, nil
}
