package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bmeg/grip-graphql/middleware"
	"github.com/calypr/gecko/gecko"
	"github.com/kataras/iris/v12"
	"github.com/stretchr/testify/assert"
)

type MockJWTHandler struct {
	AllowedResources []string
	Err              error
}

func (m *MockJWTHandler) GetAllowedResources(token string, method, service string) ([]any, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	out := make([]any, len(m.AllowedResources))
	for i, s := range m.AllowedResources {
		out[i] = s
	}
	return out, nil
}

func (m *MockJWTHandler) CheckResourceServiceAccess(token, resource, service, method string) (bool, error) {
	if m.Err != nil {
		return false, m.Err
	}
	for _, r := range m.AllowedResources {
		if r == resource {
			return true, nil
		}
	}
	return false, nil
}

func setupServer() *gecko.Server {
	return &gecko.Server{
		Logger: &gecko.LogHandler{Logger: log.New(os.Stdout, "", log.Ldate|log.Ltime)},
	}
}

func TestGeneralAuthMware_NoAuthorization(t *testing.T) {
	mockJWT := &MockJWTHandler{}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu-test")

	mware(ctx)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Authorization token not provided")
}

func TestGeneralAuthMware_BadProjectID(t *testing.T) {
	mockJWT := &MockJWTHandler{AllowedResources: []string{"/programs/ohsu/projects/test"}}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu") // missing '-'

	mware(ctx)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "Failed to parse request body")
}

func TestGeneralAuthMware_GetAllowedResourcesNonServerError(t *testing.T) {
	mockJWT := &MockJWTHandler{
		Err: fmt.Errorf("generic error"),
	}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu-test")

	mware(ctx)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "expecting error to be serverError type")
}

func TestGeneralAuthMware_GetAllowedResourcesServerError(t *testing.T) {
	mockJWT := &MockJWTHandler{
		Err: &middleware.ServerError{
			Message:    "token expired",
			StatusCode: http.StatusUnauthorized,
		},
	}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu-test")

	mware(ctx)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), "token expired")
}

type MockJWTHandlerBadAny struct{}

func (m *MockJWTHandlerBadAny) GetAllowedResources(token string, method, service string) ([]any, error) {
	return []any{123}, nil // triggers convertAnyToStringSlice error
}

func (m *MockJWTHandlerBadAny) CheckResourceServiceAccess(token, resource, service, method string) (bool, error) {
	return true, nil
}

// Then in your test:
func TestGeneralAuthMware_ConvertAnyToStringSliceError(t *testing.T) {
	mockJWT := &MockJWTHandlerBadAny{}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu-test")

	mware(ctx)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "Element 123 is not a string")
}

func TestGeneralAuthMware_ParseAccessDenied(t *testing.T) {
	mockJWT := &MockJWTHandler{
		AllowedResources: []string{"/programs/other/projects/test"},
	}
	srv := setupServer()
	mware := srv.GeneralAuthMware(mockJWT, "read", "*")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("projectId", "ohsu-test")

	mware(ctx)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "User is not allowed to read on resource path")
}

func TestConfigAuthMiddleware_MethodNotAllowed(t *testing.T) {
	mockJWT := &MockJWTHandler{}
	srv := setupServer()
	cfgMware := srv.ConfigAuthMiddleware(mockJWT)

	req := httptest.NewRequest(http.MethodPatch, "/configs/cbds-XYZ?configType=explorer", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)
	ctx.Params().Set("configId", "ohsu-test")
	ctx.Params().Set("configType", "explorer")

	cfgMware(ctx)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Contains(t, rec.Body.String(), "Unsupported HTTP method")
}

func TestBaseConfigsAuthMiddleware_NoAuthorization(t *testing.T) {
	mockJWT := &MockJWTHandler{}
	srv := setupServer()
	mware := srv.BaseConfigsAuthMiddleware(mockJWT, "read", "*", "/programs")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)

	mware(ctx)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Authorization token not provided")
}

func TestBaseConfigsAuthMiddleware_InvalidJWTHandler(t *testing.T) {
	mockJWT := &MockJWTHandler{}
	srv := setupServer()
	mware := srv.BaseConfigsAuthMiddleware(mockJWT, "read", "*", "/programs")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer dummy")
	rec := httptest.NewRecorder()
	app := iris.New()
	ctx := app.ContextPool.Acquire(rec, req)

	mware(ctx)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "Invalid JWT handler configuration")
}
