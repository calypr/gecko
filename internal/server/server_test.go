package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/server/httpapi/apiserver"
	servermw "github.com/calypr/gecko/internal/server/middleware"
	"github.com/calypr/gecko/internal/storageaudit"
)

func TestInitPropagatesMissingStorageAuditRedisConfiguration(t *testing.T) {
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED", "")
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL", "")
	t.Setenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("SYFON_DATA_API_BASE_URL", "")
	server := NewServer().
		WithLogger(log.New(io.Discard, "", 0)).
		WithRepoService(repo.NewService(repo.ServiceConfig{DataDir: t.TempDir()}))
	if _, err := server.Init(); err == nil || !strings.Contains(err.Error(), "STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL or REDIS_URL is required") {
		t.Fatalf("expected server initialization to report missing Redis, got %v", err)
	}
}

func TestMakeRouterEnforcesLargeRequestBodyLimit(t *testing.T) {
	handler, err := NewServer().WithLogger(log.New(io.Discard, "", 0)).MakeRouter()
	if err != nil {
		t.Fatalf("create router: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/config/types", nil)
	req.ContentLength = servermw.MaxRequestBodyBytes + 1
	resp := serveHTTP(handler, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for body larger than %d bytes, got %d", servermw.MaxRequestBodyBytes, resp.StatusCode)
	}
	var body struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body limit error: %v", err)
	}
	if body.Error.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("body error code = %d, want %d", body.Error.Code, http.StatusRequestEntityTooLarge)
	}
}

// TestMakeRouterRoutesThroughGeneratedOpenAPI proves the direct native handler
// serves a generated route end to end.
func TestMakeRouterRoutesThroughGeneratedOpenAPI(t *testing.T) {
	handler, err := NewServer().WithLogger(log.New(io.Discard, "", 0)).MakeRouter()
	if err != nil {
		t.Fatalf("create router: %v", err)
	}

	resp := serveHTTP(handler, httptest.NewRequest(http.MethodGet, "/config/types", nil))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for /config/types, got %d", resp.StatusCode)
	}
	var types []string
	if err := json.NewDecoder(resp.Body).Decode(&types); err != nil {
		t.Fatalf("decode config types: %v", err)
	}
	if len(types) == 0 {
		t.Fatalf("expected a non-empty list of config types")
	}
}

func serveHTTP(handler http.Handler, req *http.Request) *http.Response {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}

func TestBodyLimitMiddlewareRejectsChunkedOversize(t *testing.T) {
	const bodyLimit = int64(3)
	handler := servermw.LimitRequestBody(bodyLimit, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("four"))
	request.ContentLength = -1
	response := serveHTTP(handler, request)
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for chunked body over %d bytes, got %d", bodyLimit, response.StatusCode)
	}
}

func TestMakeHTTPServerConfiguresNativeTransportBudgets(t *testing.T) {
	serviceContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewServer().WithLogger(log.New(io.Discard, "", 0))
	httpServer, err := server.MakeHTTPServer(":0", serviceContext)
	if err != nil {
		t.Fatalf("create HTTP server: %v", err)
	}
	if httpServer.ReadTimeout != 30*time.Second {
		t.Fatalf("ReadTimeout = %s, want 30s", httpServer.ReadTimeout)
	}
	if httpServer.ReadHeaderTimeout != 30*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 30s", httpServer.ReadHeaderTimeout)
	}
	if want := storageaudit.StorageChainAuditWorkBudget + time.Minute; httpServer.WriteTimeout != want {
		t.Fatalf("WriteTimeout = %s, want audit budget plus one minute (%s)", httpServer.WriteTimeout, want)
	}
	if httpServer.IdleTimeout != 2*time.Minute {
		t.Fatalf("IdleTimeout = %s, want 2m", httpServer.IdleTimeout)
	}
	if httpServer.MaxHeaderBytes != 32*1024 {
		t.Fatalf("MaxHeaderBytes = %d, want %d", httpServer.MaxHeaderBytes, 32*1024)
	}
	if got := httpServer.BaseContext(nil); got.Done() != serviceContext.Done() {
		t.Fatal("BaseContext did not return the service context")
	}
}

func TestNativeServerDisconnectCancelsGeneratedRouteDependency(t *testing.T) {
	blockingAuthz := &blockingAccess{
		started:  make(chan struct{}),
		canceled: make(chan error, 1),
	}
	appServer := NewServer().WithLogger(log.New(io.Discard, "", 0))
	appServer.apiServer = &apiserver.Server{
		Logger: &geckologging.Handler{Logger: log.New(io.Discard, "", 0)},
		Authz:  blockingAuthz,
	}
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	httpServer, err := appServer.MakeHTTPServer("127.0.0.1:0", serviceContext)
	if err != nil {
		t.Fatalf("create HTTP server: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for transport test: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()
	t.Cleanup(func() {
		cancelService()
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
		select {
		case <-serveErr:
		case <-time.After(time.Second):
			t.Error("HTTP server did not stop")
		}
	})

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("connect to native server: %v", err)
	}
	defer conn.Close()
	request := "POST /git/projects/org/proj/repair/storage-chain/audit HTTP/1.1\r\n" +
		"Host: gecko.test\r\nAuthorization: Bearer test-token\r\n" +
		"Content-Length: 0\r\nConnection: keep-alive\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		_ = conn.Close()
		t.Fatalf("write audit request: %v", err)
	}
	select {
	case <-blockingAuthz.started:
	case <-time.After(5 * time.Second):
		_ = conn.Close()
		t.Fatal("generated audit route did not enter the blocking authorization dependency")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
	select {
	case err := <-blockingAuthz.canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("authorization dependency context error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("generated route dependency context remained active after TCP client close")
	}
}

type blockingAccess struct {
	started  chan struct{}
	canceled chan error
}

func (access *blockingAccess) GetAllowedResources(ctx context.Context, _, _, _ string) ([]any, error) {
	close(access.started)
	<-ctx.Done()
	access.canceled <- ctx.Err()
	return nil, ctx.Err()
}

func (*blockingAccess) CheckResourceServiceAccess(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}

func TestRecoveryDoesNotAppend500AfterResponseCommit(t *testing.T) {
	handler := servermw.Recover(geckologgingTestLogger(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "response started")
		panic("late failure")
	}))
	response := serveHTTP(handler, httptest.NewRequest(http.MethodGet, "/", nil))
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status after committed response = %d, want 200", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != "response started" {
		t.Fatalf("body after recovery = %q, want only the committed body", body)
	}
}

func TestRecoveryReturns500BeforeResponseCommit(t *testing.T) {
	handler := servermw.Recover(geckologgingTestLogger(), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("early failure")
	}))
	response := serveHTTP(handler, httptest.NewRequest(http.MethodGet, "/", nil))
	defer response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status after pre-commit panic = %d, want 500", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != "Internal Server Error\n" {
		t.Fatalf("panic response body = %q, want generic internal error", body)
	}
}

func geckologgingTestLogger() *geckologging.Handler {
	return &geckologging.Handler{Logger: log.New(io.Discard, "", 0)}
}
