package apiserver

import (
	"context"
	"net/http"
	"testing"
)

// fakeAuthz backs the authorization policy tests below. CheckResourceServiceAccess
// matches an exact resource path; GetAllowedResources returns the configured list.
type fakeAuthz struct {
	resources []any
}

func (f fakeAuthz) GetAllowedResources(_ context.Context, _, _, _ string) ([]any, error) {
	return f.resources, nil
}

func (f fakeAuthz) CheckResourceServiceAccess(_ context.Context, _, _, _, resourcePath string) (bool, error) {
	for _, resource := range f.resources {
		if value, ok := resource.(string); ok && value == resourcePath {
			return true, nil
		}
	}
	return false, nil
}

func tokenCtx(token string) context.Context {
	return context.WithValue(context.Background(), authzTokenKey{}, token)
}

func TestRequireToken(t *testing.T) {
	s := &Server{Authz: fakeAuthz{}}
	if err := s.requireToken(tokenCtx("")); err == nil {
		t.Fatal("expected 401 for missing token")
	} else if err.status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", err.status)
	}
	if err := s.requireToken(tokenCtx("Bearer test")); err != nil {
		t.Fatalf("expected allow for present token, got %v", err)
	}
}

func TestProjectReadAuthz(t *testing.T) {
	s := &Server{Authz: fakeAuthz{resources: []any{"/programs/org-a/projects/proj-a"}}}
	if err := s.projectReadAuthz(tokenCtx("Bearer test"), "org-a", "proj-a"); err != nil {
		t.Fatalf("expected allow for exact project resource, got %v", err)
	}

	s = &Server{Authz: fakeAuthz{resources: []any{"/organization/org-a/project/proj-a"}}}
	if err := s.projectReadAuthz(tokenCtx("Bearer test"), "org-a", "proj-a"); err == nil {
		t.Fatal("expected 403 for legacy organization resource")
	} else if err.status != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", err.status)
	} else if err.details["request_access"] != true {
		t.Fatalf("expected request_access=true, got %#v", err.details["request_access"])
	} else if err.details["request_access_resource_path"] != "/programs/org-a/projects/proj-a" {
		t.Fatalf("unexpected request_access_resource_path: %#v", err.details["request_access_resource_path"])
	}
}

func TestOrgReadAuthz(t *testing.T) {
	s := &Server{Authz: fakeAuthz{resources: []any{"/programs/org-a"}}}
	if err := s.orgReadAuthz(tokenCtx("Bearer test"), "org-a"); err != nil {
		t.Fatalf("expected allow for program organization resource, got %v", err)
	}

	s = &Server{Authz: fakeAuthz{resources: []any{"/organization/org-a/project/proj-a"}}}
	if err := s.orgReadAuthz(tokenCtx("Bearer test"), "org-a"); err == nil {
		t.Fatal("expected 403 for legacy organization resource path")
	} else if err.status != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", err.status)
	}
}

func TestProjectConfigAuthz(t *testing.T) {
	cases := []struct {
		name      string
		resources []any
		want      int
	}{
		{"exact project", []any{"/programs/org-a/projects/proj-a"}, 0},
		{"different project", []any{"/programs/org-a/projects/other"}, http.StatusForbidden},
		{"admin wildcard", []any{"*"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{Authz: fakeAuthz{resources: tc.resources}}
			err := s.projectConfigAuthz(tokenCtx("Bearer test"), "org-a", "proj-a", "update")
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("expected allow, got %v", err)
				}
			} else if err == nil || err.status != tc.want {
				t.Fatalf("expected %d, got %v", tc.want, err)
			}
		})
	}
}

func TestProjectSetupAuthz(t *testing.T) {
	cases := []struct {
		name      string
		resources []any
		want      int
	}{
		{"org project create-descendant", []any{"/programs/org-a/projects"}, 0},
		{"org manage-owners", []any{"/programs/org-a"}, 0},
		{"no setup access", []any{"/programs/other"}, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{Authz: fakeAuthz{resources: tc.resources}}
			err := s.projectSetupAuthz(tokenCtx("Bearer test"), "org-a", "proj-a")
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("expected allow, got %v", err)
				}
			} else if err == nil || err.status != tc.want {
				t.Fatalf("expected %d, got %v", tc.want, err)
			}
		})
	}
}
