package gitanalytics

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/calypr/gecko/internal/domain"
)

func TestWrapErrorClassifiesCleanupApplyValidation(t *testing.T) {
	err := WrapError(fmt.Errorf("cleanup apply requires findings from a prior audit; refusing to rebuild audit during apply"), &Target{ProjectID: "org/proj", RefName: "main"}, "data")
	var appErr *domain.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if appErr.StatusCode != http.StatusBadRequest || string(appErr.Type) != "invalid_request" {
		t.Fatalf("unexpected error mapping: %+v", appErr)
	}
	if appErr.Details["project_id"] != "org/proj" || appErr.Details["ref"] != "main" || appErr.Details["git_subpath"] != "data" {
		t.Fatalf("unexpected error details: %+v", appErr.Details)
	}
}

func TestWrapErrorClassifiesStaleCleanupAuditAsConflict(t *testing.T) {
	err := WrapError(fmt.Errorf("cleanup audit is stale because the Git revision changed; refresh audit before applying cleanup"), &Target{ProjectID: "org/proj", RefName: "main"}, "data")
	var appErr *domain.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("expected domain error, got %v", err)
	}
	if appErr.StatusCode != http.StatusConflict || string(appErr.Type) != "conflict" {
		t.Fatalf("unexpected stale audit error mapping: %+v", appErr)
	}
}

func TestWrapErrorPreservesDomainErrors(t *testing.T) {
	original := domain.NewTypedError(domain.ErrorKindNotFound, "not_found", http.StatusNotFound, "missing", nil)
	if err := WrapError(original, nil, ""); err != original {
		t.Fatalf("expected original error, got %v", err)
	}
}
