package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Column struct {
	SemanticPath   string `json:"semanticPath"`
	Name           string `json:"name"`
	LogicalType    string `json:"logicalType"`
	ClickhouseType string `json:"clickhouseType"`
	Nullable       bool   `json:"nullable"`
	Repeated       bool   `json:"repeated"`
	Filterable     bool   `json:"filterable"`
	Sortable       bool   `json:"sortable"`
	Aggregatable   bool   `json:"aggregatable"`
}

// ActivateGeneration asks Loom to make the exact candidate execution visible.
// Gecko calls this only from the coordinated revision publish operation.
func (c Client) ActivateGeneration(ctx context.Context, projectID, generation, executionID, bearer string) error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return fmt.Errorf("loom base URL is not configured")
	}
	authResourcePath, err := projectAuthResourcePath(projectID)
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("dataframe_execution_id", executionID)
	query.Set("auth_resource_path", authResourcePath)
	u := strings.TrimRight(c.BaseURL, "/") + "/api/v1/datasets/" + url.PathEscape(projectID) + "/generations/" + url.PathEscape(generation) + "/activate?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	h := c.HTTPClient
	if h == nil {
		h = http.DefaultClient
	}
	resp, err := h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("loom generation activation returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func projectAuthResourcePath(projectID string) (string, error) {
	parts := strings.Split(projectID, "-")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("project ID %q must have format <program>-<project>", projectID)
	}
	return fmt.Sprintf("/programs/%s/projects/%s", parts[0], parts[1]), nil
}

type Output struct {
	Name     string   `json:"name"`
	DataType string   `json:"dataType"`
	Columns  []Column `json:"columns"`
}
type Execution struct {
	ID           string   `json:"id"`
	ProjectID    string   `json:"projectId"`
	Generation   string   `json:"datasetGeneration"`
	SchemaDigest string   `json:"schemaDigest"`
	State        string   `json:"state"`
	Outputs      []Output `json:"outputs"`
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// ActiveDataset is the project-scoped publication view returned by Loom's
// GraphQL API. Revision is the recipe/materialization execution identity; ID
// is retained because older Loom deployments expose that identity there.
type ActiveDataset struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	Revision          string   `json:"revision"`
	ProjectID         string   `json:"projectId"`
	DatasetGeneration string   `json:"datasetGeneration"`
	State             string   `json:"state"`
	Columns           []Column `json:"columns"`
}

type graphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}
type graphQLError struct {
	Message string `json:"message"`
}
type graphQLResponse struct {
	Data struct {
		ProjectDataframeDatasets []ActiveDataset `json:"projectDataframeDatasets"`
	} `json:"data"`
	Errors []graphQLError `json:"errors"`
}

func (c Client) GetProjectDatasets(ctx context.Context, projectID, bearer string) ([]ActiveDataset, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return nil, fmt.Errorf("loom base URL is not configured")
	}
	payload := graphQLRequest{Query: `query($projectId:String!){projectDataframeDatasets(projectId:$projectId){id name revision projectId datasetGeneration state columns{semanticPath name clickhouseType logicalType nullable repeated filterable sortable aggregatable}}}`, Variables: map[string]any{"projectId": projectID}}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/graphql/graph", strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	h := c.HTTPClient
	if h == nil {
		h = http.DefaultClient
	}
	resp, err := h.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("loom active schema returned HTTP %d", resp.StatusCode)
	}
	var out graphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Errors) > 0 {
		return nil, fmt.Errorf("loom active schema query failed: %s", out.Errors[0].Message)
	}
	return out.Data.ProjectDataframeDatasets, nil
}

func (c Client) GetExecution(ctx context.Context, id, bearer string) (Execution, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return Execution{}, fmt.Errorf("loom base URL is not configured")
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/api/v1/dataframe/recipe-executions/" + id
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Execution{}, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	h := c.HTTPClient
	if h == nil {
		h = http.DefaultClient
	}
	resp, err := h.Do(req)
	if err != nil {
		return Execution{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Execution{}, fmt.Errorf("loom returned HTTP %d", resp.StatusCode)
	}
	var out Execution
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}
