package loom

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientGetExecutionForwardsBearerAndSchema(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Errorf("authorization was not forwarded")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"exec-1","datasetGeneration":"gen-2","outputs":[{"dataType":"Patient","columns":[{"semanticPath":"Patient.id","name":"patient_id"}]}]}`))
	}))
	defer srv.Close()
	e, err := (Client{BaseURL: srv.URL}).GetExecution(context.Background(), "exec-1", "Bearer test")
	if err != nil || e.Generation != "gen-2" || e.Outputs[0].Columns[0].Name != "patient_id" {
		t.Fatalf("unexpected execution: %#v, %v", e, err)
	}
}

func TestClientActivateGenerationUsesCandidateExecution(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/datasets/project-1/generations/gen-2/activate" {
			t.Errorf("unexpected activation request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("dataframe_execution_id") != "exec-1" {
			t.Errorf("missing execution id")
		}
		if r.URL.Query().Get("auth_resource_path") != "/programs/project/projects/1" {
			t.Errorf("missing authorization resource path: %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Errorf("authorization was not forwarded")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := (Client{BaseURL: srv.URL}).ActivateGeneration(context.Background(), "project-1", "gen-2", "exec-1", "Bearer test"); err != nil {
		t.Fatal(err)
	}
}

func TestClientActivateGenerationRejectsMalformedProjectID(t *testing.T) {
	err := (Client{BaseURL: "https://loom.example"}).ActivateGeneration(context.Background(), "invalid", "gen-2", "exec-1", "Bearer test")
	if err == nil {
		t.Fatal("ActivateGeneration() unexpectedly accepted malformed project ID")
	}
}

func TestClientGetProjectDatasetsUsesGraphQLActiveSchema(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql/graph" || r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Errorf("authorization was not forwarded")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"projectDataframeDatasets":[{"id":"exec-1:Patient","name":"Patient","revision":"exec-1","projectId":"project-1","datasetGeneration":"gen-2","state":"READY","columns":[]}]}}`))
	}))
	defer srv.Close()
	values, err := (Client{BaseURL: srv.URL}).GetProjectDatasets(context.Background(), "project-1", "Bearer test")
	if err != nil || len(values) != 1 || values[0].Revision != "exec-1" || values[0].DatasetGeneration != "gen-2" {
		t.Fatalf("unexpected active datasets: %#v, %v", values, err)
	}
}
