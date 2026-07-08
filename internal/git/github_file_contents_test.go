package git

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-github/v87/github"
)

type gitRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn gitRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestGetGitHubFileContentsUsesInstallationToken(t *testing.T) {
	attempts := 0
	client, err := github.NewClient(
		github.WithHTTPClient(&http.Client{
			Transport: gitRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				attempts++
				if req.Header.Get("Authorization") != "Bearer install-token" {
					t.Fatalf("expected installation token auth header, got %q", req.Header.Get("Authorization"))
				}
				return gitHubJSONResponse(http.StatusOK, `{"type":"file","name":"Practitioner.ndjson","path":"META/Practitioner.ndjson","sha":"abc","size":43,"content":"dmVyc2lvbg==","encoding":"base64"}`), nil
			}),
		}),
		github.WithAuthToken("install-token"),
	)
	if err != nil {
		t.Fatalf("create github client: %v", err)
	}

	metadata, _, err := getGitHubFileContents(context.Background(), client, GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	}, "META/Practitioner.ndjson", &github.RepositoryContentGetOptions{Ref: "main"}, "https://api.github.com", true, githubAccessTokenFingerprint("install-token"), githubAccessTokenLength("install-token"))
	if err != nil {
		t.Fatalf("get github file contents: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one github file contents request, got %d attempts", attempts)
	}
	if metadata.GetPath() != "META/Practitioner.ndjson" {
		t.Fatalf("unexpected metadata path %q", metadata.GetPath())
	}
}

func TestGetGitHubFileContentsDoesNotRetryEOF(t *testing.T) {
	attempts := 0
	client, err := github.NewClient(
		github.WithHTTPClient(&http.Client{
			Transport: gitRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				attempts++
				return nil, io.EOF
			}),
		}),
		github.WithAuthToken("install-token"),
	)
	if err != nil {
		t.Fatalf("create github client: %v", err)
	}

	_, _, err = getGitHubFileContents(context.Background(), client, GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	}, "META/Practitioner.ndjson", &github.RepositoryContentGetOptions{Ref: "main"}, "https://api.github.com", true, githubAccessTokenFingerprint("install-token"), githubAccessTokenLength("install-token"))
	if err == nil {
		t.Fatal("expected EOF error")
	}
	if attempts != 1 {
		t.Fatalf("expected no retry after EOF, got %d attempts", attempts)
	}
}

func TestGetGitHubFileContentsDoesNotRetryUnauthorized(t *testing.T) {
	attempts := 0
	client, err := github.NewClient(
		github.WithHTTPClient(&http.Client{
			Transport: gitRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				attempts++
				return gitHubJSONResponse(http.StatusUnauthorized, `{"message":"Bad credentials"}`), nil
			}),
		}),
		github.WithAuthToken("bad-token"),
	)
	if err != nil {
		t.Fatalf("create github client: %v", err)
	}

	_, _, err = getGitHubFileContents(context.Background(), client, GitRepositoryIdentity{
		Owner: "BForePC",
		Repo:  "BForePC",
	}, "META/Practitioner.ndjson", &github.RepositoryContentGetOptions{Ref: "main"}, "https://api.github.com", true, githubAccessTokenFingerprint("bad-token"), githubAccessTokenLength("bad-token"))
	if err == nil {
		t.Fatal("expected unauthorized error")
	}
	if attempts != 1 {
		t.Fatalf("expected no retry for unauthorized response, got %d attempts", attempts)
	}
}

func gitHubJSONResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}
