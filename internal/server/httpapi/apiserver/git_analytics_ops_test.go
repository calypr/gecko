package apiserver

import (
	"context"
	"database/sql"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/gecko/apgen/server/gecko"
	geckodb "github.com/calypr/gecko/internal/db"
	"github.com/calypr/gecko/internal/domain"
	"github.com/calypr/gecko/internal/gitanalytics"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	geckologging "github.com/calypr/gecko/internal/logging"
	"github.com/calypr/gecko/internal/project"
	"github.com/calypr/gecko/internal/repo"
	"github.com/calypr/gecko/internal/storageaudit"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/jmoiron/sqlx"
)

func TestPostStorageChainAuditKeepsProjectReadAuthorizationBeforeRefresh(t *testing.T) {
	server := &Server{Authz: fakeAuthz{}}
	request := gecko.PostGitProjectStorageChainAuditRequestObject{
		OrgTitle:     "org",
		ProjectTitle: "proj",
		Body: &gecko.PostGitProjectStorageChainAuditJSONRequestBody{
			"force_audit_refresh": true,
		},
	}
	_, err := server.PostGitProjectStorageChainAudit(tokenCtx("Bearer test"), request)
	if err == nil {
		t.Fatal("expected project read authorization to reject the request")
	}
	opErr, ok := err.(*opError)
	if !ok || opErr.status != 403 {
		t.Fatalf("expected project read authorization to return 403 before resolving the mirror, got %T %v", err, err)
	}
}

func TestPostStorageChainAuditRefreshesMirrorBeforeMissHitAndForce(t *testing.T) {
	mirrorPath := t.TempDir()
	mirror, err := git.PlainInit(mirrorPath, false)
	if err != nil {
		t.Fatalf("initialize Git mirror: %v", err)
	}
	initialHash := commitMirrorFile(t, mirrorPath, mirror, "initial")
	currentHashes := []plumbing.Hash{
		commitMirrorFile(t, mirrorPath, mirror, "first refresh"),
		commitMirrorFile(t, mirrorPath, mirror, "saved hit refresh"),
		commitMirrorFile(t, mirrorPath, mirror, "forced refresh"),
	}
	branchName := plumbing.NewBranchReferenceName("master")
	if err := mirror.Storer.SetReference(plumbing.NewHashReference(branchName, initialHash)); err != nil {
		t.Fatalf("set initial stale mirror ref: %v", err)
	}
	if err := mirror.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branchName)); err != nil {
		t.Fatalf("set mirror HEAD: %v", err)
	}

	db, sqlMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()
	database := sqlx.NewDb(db, "sqlmock")
	for range 3 {
		sqlMock.ExpectQuery("SELECT name, content FROM config_schema.projects WHERE name=\\$1").
			WithArgs("org/proj").
			WillReturnRows(sqlmock.NewRows([]string{"name", "content"}).AddRow("org/proj", []byte(`{"src_repo":"https://github.com/owner/repo"}`)))
		sqlMock.ExpectExec("INSERT INTO config_schema.git_project_state").WillReturnResult(sqlmock.NewResult(0, 1))
	}

	state := &geckodb.GitProjectState{
		ProjectID:      "org/proj",
		RepoHost:       "github.com",
		RepoOwner:      "owner",
		RepoName:       "repo",
		InstallationID: sql.NullInt64{Int64: 1, Valid: true},
		MirrorPath:     mirrorPath,
		SyncState:      "ready",
		DefaultBranch:  sql.NullString{String: "master", Valid: true},
	}
	projectReader := project.NewReadService(database, &refreshingGitRepository{state: state, mirror: mirror, hashes: currentHashes})
	responseCache := newTestResponseCache()
	analytics, err := storageaudit.NewStorageAnalyticsServiceWithCaches(emptyStorageBackend{}, storageaudit.StorageAuditCaches{
		Response:        responseCache,
		ExactJoin:       testExactJoinCache{},
		BucketInventory: testBucketInventoryCache{},
		Observations:    testStorageObservationCache{},
		Coordinator:     testProjectAuditCoordinator{response: responseCache, bucket: testBucketInventoryCache{}},
	})
	if err != nil {
		t.Fatalf("create storage analytics service with test caches: %v", err)
	}
	server := &Server{
		Logger:           &geckologging.Handler{Logger: log.New(io.Discard, "", 0)},
		ProjectRead:      projectReader,
		StorageAnalytics: analytics,
		Analytics:        gitanalytics.NewService(projectReader, analytics),
		Authz:            fakeAuthz{resources: []any{"/programs/org/projects/proj"}},
	}

	makeRequest := func(force bool) gecko.PostGitProjectStorageChainAuditRequestObject {
		body := gecko.PostGitProjectStorageChainAuditJSONRequestBody{
			"bucket_inventory_mode": "validate",
			"validation_mode":       "inventory",
		}
		if force {
			body["force_audit_refresh"] = true
		}
		return gecko.PostGitProjectStorageChainAuditRequestObject{OrgTitle: "org", ProjectTitle: "proj", Body: &body}
	}
	call := func(force bool) map[string]any {
		t.Helper()
		result, err := server.PostGitProjectStorageChainAudit(tokenCtx("Bearer test"), makeRequest(force))
		if err != nil {
			t.Fatalf("post storage chain audit force=%t: %v", force, err)
		}
		response, ok := result.(gecko.PostGitProjectStorageChainAudit200JSONResponse)
		if !ok {
			t.Fatalf("unexpected storage audit response type %T", result)
		}
		return response
	}

	first := call(false)
	firstSummary := first["summary"].(map[string]any)
	if firstSummary["git_revision"] != currentHashes[0].String() || firstSummary["audit_cache_hit"] == true {
		t.Fatalf("expected cache miss to use the freshly updated mirror, got %+v", firstSummary)
	}
	second := call(false)
	secondSummary := second["summary"].(map[string]any)
	if secondSummary["git_revision"] != currentHashes[0].String() || secondSummary["audit_cache_hit"] != true {
		t.Fatalf("expected saved hit to keep snapshot revision after refreshing the mirror, got %+v", secondSummary)
	}
	third := call(true)
	thirdSummary := third["summary"].(map[string]any)
	if thirdSummary["git_revision"] != currentHashes[2].String() || thirdSummary["audit_cache_hit"] == true {
		t.Fatalf("expected explicit refresh to rebuild from current mirror, got %+v", thirdSummary)
	}
	if err := sqlMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations: %v", err)
	}
}

type testResponseCache struct {
	entries map[string]storecache.CachedResponse
}

func newTestResponseCache() *testResponseCache {
	return &testResponseCache{entries: map[string]storecache.CachedResponse{}}
}

func (cache *testResponseCache) Get(_ context.Context, key string) (storecache.CachedResponse, bool, error) {
	value, ok := cache.entries[key]
	return storecache.CloneCachedResponse(value), ok, nil
}

func (cache *testResponseCache) Set(_ context.Context, key string, value storecache.CachedResponse) error {
	cache.entries[key] = storecache.CloneCachedResponse(value)
	return nil
}

func (cache *testResponseCache) Source() string { return "test" }

type testProjectAuditCoordinator struct {
	response storecache.ResponseCache
	bucket   storecache.BucketInventoryCache
}

type testProjectAuditLease struct {
	ctx      context.Context
	response storecache.ResponseCache
	bucket   storecache.BucketInventoryCache
}

func (coordinator testProjectAuditCoordinator) AcquireProject(ctx context.Context, _, _ string) (storecache.ProjectAuditLease, error) {
	return testProjectAuditLease{ctx: ctx, response: coordinator.response, bucket: coordinator.bucket}, nil
}

func (lease testProjectAuditLease) Context() context.Context { return lease.ctx }

func (lease testProjectAuditLease) PublishResponse(ctx context.Context, key string, value storecache.CachedResponse) error {
	return lease.response.Set(ctx, key, value)
}

func (lease testProjectAuditLease) PublishBucketInventory(ctx context.Context, key string, value storecache.CachedBucketInventory, ttl time.Duration) error {
	return lease.bucket.Set(ctx, key, value, ttl)
}

func (testProjectAuditLease) Release(context.Context) error { return nil }

type testExactJoinCache struct{}

func (testExactJoinCache) Get(context.Context, string) (storecache.CachedExactJoinState, bool, error) {
	return storecache.CachedExactJoinState{}, false, nil
}

func (testExactJoinCache) Set(context.Context, string, storecache.CachedExactJoinState, time.Duration) error {
	return nil
}

func (testExactJoinCache) Source() string { return "test" }

type testBucketInventoryCache struct{}

func (testBucketInventoryCache) Get(context.Context, string) (storecache.CachedBucketInventory, bool, error) {
	return storecache.CachedBucketInventory{}, false, nil
}

func (testBucketInventoryCache) Set(context.Context, string, storecache.CachedBucketInventory, time.Duration) error {
	return nil
}

func (testBucketInventoryCache) Source() string { return "test" }

type testStorageObservationCache struct{}

func (testStorageObservationCache) GetMany(context.Context, string, string, storecache.StorageObservationVersion, []string) (map[string]storecache.CachedStorageObservation, error) {
	return map[string]storecache.CachedStorageObservation{}, nil
}

func (testStorageObservationCache) EnsureFences(context.Context, string, string, storecache.StorageObservationVersion, []string) error {
	return nil
}

func (testStorageObservationCache) SetMany(context.Context, string, string, []storecache.StorageObservationWrite) error {
	return nil
}

func (testStorageObservationCache) NextVersion(context.Context, string, string) (storecache.StorageObservationVersion, error) {
	return storecache.StorageObservationVersion{Epoch: "test", Counter: 1}, nil
}

func (testStorageObservationCache) Source() string { return "test" }

type refreshingGitRepository struct {
	state        *geckodb.GitProjectState
	mirror       *git.Repository
	hashes       []plumbing.Hash
	refreshCalls int
}

func (repository *refreshingGitRepository) LoadProjectState(context.Context, *sqlx.DB, string, repo.Identity) (*geckodb.GitProjectState, error) {
	state := *repository.state
	return &state, nil
}

func (repository *refreshingGitRepository) RequestInstallationToken(context.Context, string, string, string, repo.Identity, string) (string, error) {
	return "installation-token", nil
}

func (repository *refreshingGitRepository) RefreshProject(_ context.Context, projectID string, _ repo.Identity, state *geckodb.GitProjectState, _ string) (*repo.ProjectRefreshResponse, *geckodb.GitProjectState, error) {
	hash := repository.hashes[repository.refreshCalls]
	repository.refreshCalls++
	branch := plumbing.NewBranchReferenceName("master")
	if err := repository.mirror.Storer.SetReference(plumbing.NewHashReference(branch, hash)); err != nil {
		return nil, nil, err
	}
	updated := *state
	updated.SyncState = "ready"
	updated.LastRefreshedAt = sql.NullTime{Time: time.Now(), Valid: true}
	return &repo.ProjectRefreshResponse{Success: true, ProjectID: projectID, SyncState: "ready", DefaultBranch: "master"}, &updated, nil
}

func commitMirrorFile(t *testing.T, mirrorPath string, repository *git.Repository, contents string) plumbing.Hash {
	t.Helper()
	filePath := filepath.Join(mirrorPath, "audit.txt")
	if err := os.WriteFile(filePath, []byte(contents), 0o600); err != nil {
		t.Fatalf("write mirror file: %v", err)
	}
	worktree, err := repository.Worktree()
	if err != nil {
		t.Fatalf("open mirror worktree: %v", err)
	}
	if _, err := worktree.Add("audit.txt"); err != nil {
		t.Fatalf("stage mirror file: %v", err)
	}
	when := time.Unix(int64(len(contents)), 0).UTC()
	hash, err := worktree.Commit(contents, &git.CommitOptions{Author: &object.Signature{Name: "Test", Email: "test@example.com", When: when}})
	if err != nil {
		t.Fatalf("commit mirror file: %v", err)
	}
	return hash
}

type emptyStorageBackend struct{}

func (emptyStorageBackend) ListBuckets(context.Context, string) (map[string]domain.StorageBucket, error) {
	return map[string]domain.StorageBucket{}, nil
}
func (emptyStorageBackend) ListBucketScopes(context.Context, string, string) ([]domain.StorageBucketScope, error) {
	return []domain.StorageBucketScope{}, nil
}
func (emptyStorageBackend) ListProjectRecords(context.Context, string, string, string) ([]gintegrationsyfon.ProjectRecord, error) {
	return []gintegrationsyfon.ProjectRecord{}, nil
}
func (emptyStorageBackend) ListProjectAuditRecords(context.Context, string, string, string, string) ([]gintegrationsyfon.ProjectRecord, error) {
	return []gintegrationsyfon.ProjectRecord{}, nil
}
func (emptyStorageBackend) GetProjectMetricsSummary(context.Context, string, string, string) (*gintegrationsyfon.ProjectMetricsSummary, error) {
	return &gintegrationsyfon.ProjectMetricsSummary{}, nil
}
func (emptyStorageBackend) ListProjectScopes(context.Context, string, string, string) ([]domain.StorageBucketScope, error) {
	return []domain.StorageBucketScope{{Bucket: "bucket", Organization: "org", ProjectID: "proj", Path: "s3://bucket"}}, nil
}
func (emptyStorageBackend) BulkGetProjectRecordsByChecksum(context.Context, string, string, string, []string) (map[string][]gintegrationsyfon.ProjectRecord, error) {
	return map[string][]gintegrationsyfon.ProjectRecord{}, nil
}
func (emptyStorageBackend) ListProjectFileUsageByObjectIDs(context.Context, string, string, string, []string, int) (map[string]gintegrationsyfon.FileUsage, error) {
	return map[string]gintegrationsyfon.FileUsage{}, nil
}
func (emptyStorageBackend) ListProjectFileUsage(context.Context, string, string, string, int) (map[string]gintegrationsyfon.FileUsage, error) {
	return map[string]gintegrationsyfon.FileUsage{}, nil
}
func (emptyStorageBackend) ListProjectBucketObjects(context.Context, string, string, string, string) (gintegrationsyfon.ProjectBucketInventory, error) {
	return gintegrationsyfon.ProjectBucketInventory{}, nil
}
func (emptyStorageBackend) ListProjectBucketInventory(context.Context, string, string, string, string) (gintegrationsyfon.ProjectBucketInventory, error) {
	return gintegrationsyfon.ProjectBucketInventory{}, nil
}
func (emptyStorageBackend) ListProjectBucketSummary(context.Context, string, string, string, string) (*gintegrationsyfon.ProjectBucketSummary, error) {
	return &gintegrationsyfon.ProjectBucketSummary{}, nil
}
func (emptyStorageBackend) BulkProbeStorageObjects(context.Context, string, []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return []gintegrationsyfon.BulkStorageProbeResult{}, nil
}
func (emptyStorageBackend) BulkConfirmStorageObjects(context.Context, string, []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return []gintegrationsyfon.BulkStorageProbeResult{}, nil
}
func (emptyStorageBackend) BulkListStorageObjects(context.Context, string, []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error) {
	return []gintegrationsyfon.BulkStorageProbeResult{}, nil
}
func (emptyStorageBackend) BulkDeleteObjects(context.Context, string, []string, bool) error {
	return nil
}
func (emptyStorageBackend) DeleteProjectBucketObjects(context.Context, string, string, string, []string) ([]gintegrationsyfon.ProjectBucketDeleteResult, error) {
	return []gintegrationsyfon.ProjectBucketDeleteResult{}, nil
}
func (emptyStorageBackend) BulkUpdateAccessMethods(context.Context, string, map[string][]gintegrationsyfon.ProjectAccessMethod) error {
	return nil
}
func (emptyStorageBackend) RegisterProjectObjects(context.Context, string, []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error) {
	return []gintegrationsyfon.ProjectObjectRegistrationResult{}, nil
}
