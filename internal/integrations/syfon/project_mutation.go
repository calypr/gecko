package syfon

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

func (manager *Manager) BulkDeleteObjects(ctx context.Context, authorizationHeader string, objectIDs []string, deleteStorageData bool) error {
	normalized := dedupeStrings(objectIDs)
	if len(normalized) == 0 {
		return nil
	}
	resp, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return err
	}
	deleteMetadata := true
	requestBody := drsapi.BulkDeleteObjectsJSONRequestBody{
		BulkObjectIds:        normalized,
		DeleteObjectMetadata: &deleteMetadata,
		DeleteStorageData:    &deleteStorageData,
	}
	response, err := resp.BulkDeleteObjectsWithResponse(ctx, requestBody)
	if err != nil {
		return fmt.Errorf("bulk delete syfon objects: %w", err)
	}
	if response.StatusCode() != http.StatusOK && response.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("bulk delete syfon objects failed with status %d", response.StatusCode())
	}
	return nil
}

func (manager *Manager) RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []ProjectObjectRegistration) ([]ProjectObjectRegistrationResult, error) {
	if len(candidates) == 0 {
		return []ProjectObjectRegistrationResult{}, nil
	}
	client, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return nil, err
	}
	request := drsapi.RegisterObjectsJSONRequestBody{Candidates: make([]drsapi.DrsObjectCandidate, 0, len(candidates))}
	for _, candidate := range candidates {
		name := strings.TrimSpace(candidate.Name)
		checksum := normalizeSHA256(candidate.Checksum)
		accessURLs := uniqueNonEmptyStrings(candidate.AccessURLs)
		if name == "" || checksum == "" || candidate.Size < 0 || len(accessURLs) == 0 {
			return nil, fmt.Errorf("register Syfon object: candidate is missing a name, SHA-256, size, or access URL")
		}
		accessMethods := make([]drsapi.AccessMethod, 0, len(accessURLs))
		for index, accessURL := range accessURLs {
			accessID := fmt.Sprintf("s3-%d", index+1)
			accessURLCopy := accessURL
			accessMethods = append(accessMethods, drsapi.AccessMethod{
				AccessId: &accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: accessURLCopy},
				Type: drsapi.AccessMethodType("s3"),
			})
		}
		controlledAccess := uniqueNonEmptyStrings(candidate.ControlledAccess)
		request.Candidates = append(request.Candidates, drsapi.DrsObjectCandidate{
			Name:             &name,
			Size:             candidate.Size,
			Checksums:        []drsapi.Checksum{{Type: "sha256", Checksum: checksum}},
			ControlledAccess: &controlledAccess,
			AccessMethods:    &accessMethods,
		})
	}
	response, err := client.RegisterObjectsWithResponse(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("register Syfon objects: %w", err)
	}
	if response.StatusCode() != http.StatusCreated || response.JSON201 == nil {
		return nil, fmt.Errorf("register Syfon objects failed with status %d: %s", response.StatusCode(), strings.TrimSpace(string(response.Body)))
	}
	if len(response.JSON201.Objects) != len(candidates) {
		return nil, fmt.Errorf("register Syfon objects: expected %d objects, received %d", len(candidates), len(response.JSON201.Objects))
	}
	results := make([]ProjectObjectRegistrationResult, len(response.JSON201.Objects))
	for index, object := range response.JSON201.Objects {
		results[index] = ProjectObjectRegistrationResult{ObjectID: strings.TrimSpace(object.Id)}
	}
	return results, nil
}

func (manager *Manager) DeleteProjectBucketObjects(ctx context.Context, authorizationHeader string, organization string, project string, objectURLs []string) ([]ProjectBucketDeleteResult, error) {
	requestBody := struct {
		Organization string   `json:"organization,omitempty"`
		Project      string   `json:"project,omitempty"`
		ObjectURLs   []string `json:"object_urls"`
	}{
		Organization: strings.TrimSpace(organization),
		Project:      strings.TrimSpace(project),
		ObjectURLs:   dedupeStrings(objectURLs),
	}
	if len(requestBody.ObjectURLs) == 0 {
		return []ProjectBucketDeleteResult{}, nil
	}
	var response struct {
		Items []struct {
			ObjectURL string `json:"object_url"`
			Status    string `json:"status"`
			Error     string `json:"error,omitempty"`
		} `json:"items"`
	}
	if err := manager.requestJSON(ctx, authorizationHeader, http.MethodPost, "/data/inspect/project-bucket/delete", nil, requestBody, &response); err != nil {
		return nil, fmt.Errorf("delete syfon project bucket objects: %w", err)
	}
	out := make([]ProjectBucketDeleteResult, 0, len(response.Items))
	for _, item := range response.Items {
		out = append(out, ProjectBucketDeleteResult{
			ObjectURL: strings.TrimSpace(item.ObjectURL),
			Status:    strings.TrimSpace(item.Status),
			Error:     strings.TrimSpace(item.Error),
		})
	}
	return out, nil
}

func (manager *Manager) BulkUpdateAccessMethods(ctx context.Context, authorizationHeader string, updates map[string][]ProjectAccessMethod) error {
	if len(updates) == 0 {
		return nil
	}
	resp, err := manager.drsClient(authorizationHeader)
	if err != nil {
		return err
	}
	const batchSize = 200
	objectIDs := make([]string, 0, len(updates))
	for objectID := range updates {
		if trimmed := strings.TrimSpace(objectID); trimmed != "" {
			objectIDs = append(objectIDs, trimmed)
		}
	}
	sort.Strings(objectIDs)
	for start := 0; start < len(objectIDs); start += batchSize {
		end := start + batchSize
		if end > len(objectIDs) {
			end = len(objectIDs)
		}
		body := drsapi.BulkUpdateAccessMethodsJSONRequestBody{
			Updates: make([]struct {
				AccessMethods []drsapi.AccessMethod `json:"access_methods"`
				ObjectId      string                `json:"object_id"`
			}, 0, end-start),
		}
		for _, objectID := range objectIDs[start:end] {
			methods := projectAccessMethodsToDRS(updates[objectID])
			if len(methods) == 0 {
				continue
			}
			body.Updates = append(body.Updates, struct {
				AccessMethods []drsapi.AccessMethod `json:"access_methods"`
				ObjectId      string                `json:"object_id"`
			}{
				ObjectId:      objectID,
				AccessMethods: methods,
			})
		}
		if len(body.Updates) == 0 {
			continue
		}
		response, err := resp.BulkUpdateAccessMethodsWithResponse(ctx, body)
		if err != nil {
			return fmt.Errorf("bulk update syfon access methods: %w", err)
		}
		if response.StatusCode() != http.StatusOK {
			return fmt.Errorf("bulk update syfon access methods failed with status %d", response.StatusCode())
		}
	}
	return nil
}
