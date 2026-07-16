package github

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/gecko/internal/git/domain"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestFetchRepositoryMetadataUsesInstallationToken(t *testing.T) {
	attempts := 0
	client := NewClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if req.Header.Get("Authorization") != "Bearer install-token" {
				t.Fatalf("expected installation token auth header, got %q", req.Header.Get("Authorization"))
			}
			return githubJSONResponse(http.StatusOK, `{"default_branch":"main","html_url":"https://github.com/BForePC/BForePC"}`), nil
		}),
	}, Config{})

	metadata, err := client.FetchRepositoryMetadata(context.Background(), "install-token", domain.GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	})
	if err != nil {
		t.Fatalf("fetch repository metadata: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one metadata request, got %d attempts", attempts)
	}
	if metadata.DefaultBranch != "main" || metadata.HTMLURL != "https://github.com/BForePC/BForePC" {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}
}

func TestFetchRepositoryMetadataDoesNotRetryEOF(t *testing.T) {
	attempts := 0
	client := NewClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			return nil, io.EOF
		}),
	}, Config{DisableTransportDiagnostics: true})

	_, err := client.FetchRepositoryMetadata(context.Background(), "install-token", domain.GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	})
	if err == nil {
		t.Fatal("expected EOF error")
	}
	if attempts != 1 {
		t.Fatalf("expected no retry after EOF, got %d attempts", attempts)
	}
}

func TestFetchRepositoryMetadataDoesNotRetryUnauthorized(t *testing.T) {
	attempts := 0
	client := NewClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			return githubJSONResponse(http.StatusUnauthorized, `{"message":"Bad credentials"}`), nil
		}),
	}, Config{})

	_, err := client.FetchRepositoryMetadata(context.Background(), "bad-token", domain.GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	})
	if err == nil {
		t.Fatal("expected unauthorized error")
	}
	if attempts != 1 {
		t.Fatalf("expected no retry for unauthorized response, got %d attempts", attempts)
	}
}

func githubJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}
