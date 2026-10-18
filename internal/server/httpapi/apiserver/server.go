// Package apiserver implements the generated strict OpenAPI server interface.
//
// The OpenAPI document under apgen/openapi is the source of truth for the HTTP
// surface. oapi-codegen generates a StrictServerInterface from it (see
// apgen/server/gecko). This package provides the concrete implementation that
// the generated router dispatches to. Each method receives a context and a
// typed request object, calls the application services directly, and returns a
// generated response object.
package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/uc-cdis/arborist/arborist"
	yaml "go.yaml.in/yaml/v3"

	"github.com/calypr/gecko/apgen"
	"github.com/calypr/gecko/apgen/server/gecko"
	appconfig "github.com/calypr/gecko/config"
	"github.com/calypr/gecko/internal/access"
	"github.com/calypr/gecko/internal/configuration"
	"github.com/calypr/gecko/internal/gitanalytics"
	"github.com/calypr/gecko/internal/presentation"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/thumbnail"
	"github.com/calypr/gecko/internal/upload"
)

// Server holds the application services the strict handlers dispatch to. It is
// the union of the config and git handler dependencies.
type Server struct {
	Logger              arborist.Logger
	DB                  *sqlx.DB
	ConfigService       *configuration.Service
	DeleteService       *project.DeleteService
	RepoService         *repo.Service
	ProjectSetup        *project.SetupService
	ProjectSync         *project.ReconcileService
	ProjectRead         *project.ReadService
	ProjectInstallation *project.InstallationService
	UploadService       *upload.Service
	StorageAnalytics    *storageaudit.StorageAnalyticsService
	Analytics           *gitanalytics.Service
	ThumbnailStore      thumbnail.Manager
	PresentationStore   presentation.Manager
	Authz               access.ResourceAccessHandler
}

var _ gecko.StrictServerInterface = (*Server)(nil)

type authzTokenKey struct{}

// InjectAuthorization is a strict middleware that copies the Authorization
// header into the operation context so handlers can run resource authorization
// without access to the raw http.Request.
func InjectAuthorization(next gecko.StrictHandlerFunc, operationID string) gecko.StrictHandlerFunc {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
		ctx = context.WithValue(ctx, authzTokenKey{}, r.Header.Get("Authorization"))
		return next(ctx, w, r, request)
	}
}

// AuthorizationFromContext returns the bearer token captured by
// InjectAuthorization, or an empty string when the header was absent.
func AuthorizationFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(authzTokenKey{}).(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

// ListConfigTypes implements GET /config/types.
func (s *Server) ListConfigTypes(ctx context.Context, request gecko.ListConfigTypesRequestObject) (gecko.ListConfigTypesResponseObject, error) {
	return gecko.ListConfigTypes200JSONResponse(appconfig.KnownTypes()), nil
}

// NewRouter builds the generated HTTP handler for the server, with the
// authorization-injection middleware and the package's error mapping applied.
// It also serves the embedded OpenAPI spec at /swagger/doc.json so the served
// contract is the same document the router is generated from.
func NewRouter(server *Server) http.Handler {
	strict := gecko.NewStrictHandlerWithOptions(server, []gecko.StrictMiddlewareFunc{InjectAuthorization}, gecko.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  server.requestErrorHandler,
		ResponseErrorHandlerFunc: server.responseErrorHandler,
	})
	mux := &wildcardMux{inner: http.NewServeMux()}
	specBytes, contentType := renderSpecJSON()
	mux.inner.HandleFunc("GET /swagger/doc.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(specBytes)
	})
	return gecko.HandlerWithOptions(strict, gecko.StdHTTPServerOptions{
		BaseRouter: mux,
		// Route std-http-layer failures (e.g. a non-integer limit query param)
		// through the JSON error envelope instead of the default text/plain 400.
		ErrorHandlerFunc: server.requestErrorHandler,
	})
}

// renderSpecJSON converts the embedded YAML spec to JSON once so the doc
// endpoint keeps the application/json contract consumers expect. It falls back
// to the raw YAML if conversion is impossible.
func renderSpecJSON() ([]byte, string) {
	raw, err := apgen.SpecFS.ReadFile("openapi/gecko.yaml")
	if err != nil {
		return []byte("{}"), "application/json"
	}
	var value any
	if err := yaml.Unmarshal(raw, &value); err != nil {
		return raw, "application/yaml"
	}
	converted, err := json.Marshal(value)
	if err != nil {
		return raw, "application/yaml"
	}
	return converted, "application/json"
}

// wildcardMux is the BaseRouter the generated handler registers its routes on.
// It upgrades the four single-segment {path} subpath routes (tree, manifest,
// file, download) to Go ServeMux's multi-segment {path...} form so nested paths
// such as /file/src/main.go are captured, matching the legacy fiber "*" wildcard
// that oapi-codegen's std-http generator cannot express.
type wildcardMux struct {
	inner *http.ServeMux
}

func (w *wildcardMux) HandleFunc(pattern string, h func(http.ResponseWriter, *http.Request)) {
	w.inner.HandleFunc(upgradeWildcardPattern(pattern), h)
}

func (w *wildcardMux) ServeHTTP(resp http.ResponseWriter, r *http.Request) {
	w.inner.ServeHTTP(resp, r)
}

// upgradeWildcardPattern rewrites a trailing {path} to {path...}. Only the four
// subpath routes use a {path} parameter, so a suffix match is unambiguous.
func upgradeWildcardPattern(pattern string) string {
	if strings.HasSuffix(pattern, "{path}") {
		return strings.TrimSuffix(pattern, "{path}") + "{path...}"
	}
	return pattern
}
