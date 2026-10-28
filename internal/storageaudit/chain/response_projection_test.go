package chain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func TestStorageChainAuditProjectionMatchesOriginalOrder(t *testing.T) {
	base := projectionBehaviorFixture()
	cases := []struct {
		name  string
		kind  string
		limit int
	}{
		{name: "selected one", kind: "bucket_only_object", limit: 1},
		{name: "kind limit", kind: "probe_error", limit: 2},
		{name: "default per kind limit", limit: 1},
		{name: "unlimited", limit: -1},
		{name: "zero limit is unlimited", limit: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := originalProjectStorageChainAuditResponse(base, tc.kind, tc.limit)
			got := projectStorageChainAuditResponse(base, tc.kind, tc.limit)
			assertProjectionEqual(t, want, got)
		})
	}
}

func TestStorageChainAuditProjectionPreservesFilterSummaryGroupsAndOrder(t *testing.T) {
	base := projectionBehaviorFixture()
	got := projectStorageChainAuditResponse(base, "probe_error", 2)
	if got.Summary.TotalFindings != 5 || got.Summary.CountsByKind["probe_error"] != 5 {
		t.Fatalf("kind summary must include findings past the limit: %+v", got.Summary)
	}
	if got.Summary.ReturnedFindings != 2 || !got.Summary.FindingsTruncated {
		t.Fatalf("unexpected limited summary: %+v", got.Summary)
	}
	if len(got.Groups) != 1 || got.Groups[0].Kind != "probe_error" || got.Groups[0].FindingCount != 5 {
		t.Fatalf("groups must summarize all filtered findings: %+v", got.Groups)
	}
	if got.Findings[0].NormalizedPath != "src/group-01/file-000001.dat" || got.Findings[1].NormalizedPath != "src/group-05/file-000005.dat" {
		t.Fatalf("kind projection changed source order: %+v", got.Findings)
	}

	unfiltered := projectStorageChainAuditResponse(base, "", 1)
	if unfiltered.Summary.TotalFindings != len(base.Findings) || unfiltered.Summary.ReturnedFindings != 4 {
		t.Fatalf("unfiltered limit must be applied once per kind: %+v", unfiltered.Summary)
	}
	wantPaths := []string{
		"src/group-00/file-000000.dat",
		"src/group-01/file-000001.dat",
		"src/group-02/file-000002.dat",
		"src/group-03/file-000003.dat",
	}
	for i, finding := range unfiltered.Findings {
		if finding.NormalizedPath != wantPaths[i] {
			t.Fatalf("unfiltered limit changed first-per-kind order at %d: got %q want %q", i, finding.NormalizedPath, wantPaths[i])
		}
	}
}

func TestStorageChainAuditRootPathProjectionMatchesOriginalOrder(t *testing.T) {
	base := projectionBehaviorFixture()
	inventory := filterStorageChainInventory([]model.RepoInventoryFile{
		{RepoPath: "src/group-00/a.go", Name: "a.go"},
		{RepoPath: "src/group-00/b.go", Name: "b.go"},
		{RepoPath: "src/group-01/c.go", Name: "c.go"},
	}, "src/group-00")
	for _, tc := range []struct {
		name  string
		kind  string
		limit int
	}{
		{name: "path only", limit: -1},
		{name: "path and kind limit", kind: "probe_error", limit: 1},
		{name: "path and default limit", limit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := originalProjectStorageChainAuditResponseForSubpath(base, "src/group-00", inventory, tc.kind, tc.limit)
			view := storageChainAuditResponseForSubpathView(base, "src/group-00", inventory)
			got := projectStorageChainAuditResponse(view, tc.kind, tc.limit)
			assertProjectionEqual(t, want, got)
		})
	}

	view := storageChainAuditResponseForSubpathView(base, "src/group-00", inventory)
	got := projectStorageChainAuditResponse(view, "git_only_no_syfon", 1)
	if got.PathPrefix != "src/group-00" || got.Summary.GitTrackedFileCount != 2 {
		t.Fatalf("path projection lost its path or inventory count: %+v", got)
	}
	if got.Summary.CountsByKind["bucket_syfon_git_complete"] != 1 {
		t.Fatalf("path projection lost complete-bucket count: %+v", got.Summary.CountsByKind)
	}
	if got.Summary.TotalFindings != 1 || got.Summary.ReturnedFindings != 1 {
		t.Fatalf("path summary should cover all matching findings before limit: %+v", got.Summary)
	}
}

func TestStorageChainAuditRootPathProjectionMatchesNestedPathOnly(t *testing.T) {
	inventory := filterStorageChainInventory([]model.RepoInventoryFile{{RepoPath: "src/group-00/tracked.go", Name: "tracked.go"}}, "src/group-00")
	cases := []struct {
		name  string
		apply func(*model.ChainFinding)
	}{
		{name: "record normalized path", apply: func(f *model.ChainFinding) { f.Records[0].NormalizedPath = "src/group-00/record.dat" }},
		{name: "record access method URL", apply: func(f *model.ChainFinding) {
			f.Records[0].AccessMethods[0].URL = "s3://audit-bucket/src/group-00/method.dat"
		}},
		{name: "record probe path", apply: func(f *model.ChainFinding) { f.Records[0].AccessProbes[0].Path = "src/group-00/probe.dat" }},
		{name: "evidence key", apply: func(f *model.ChainFinding) { f.Evidence.Keys = []string{"src/group-00/evidence.dat"} }},
	}
	findings := make([]model.ChainFinding, 0, len(cases)+1)
	for i, tc := range cases {
		finding := projectionRecordFinding(i, "probe_error")
		setProjectionFindingPathsOutsideSubpath(&finding)
		tc.apply(&finding)
		findings = append(findings, finding)
	}
	unmatched := projectionRecordFinding(len(cases), "probe_error")
	setProjectionFindingPathsOutsideSubpath(&unmatched)
	findings = append(findings, unmatched)
	base := reportProjectionBase(findings)
	view := storageChainAuditResponseForSubpathView(base, "src/group-00", inventory)
	got := projectStorageChainAuditResponse(view, "", -1)
	want := originalProjectStorageChainAuditResponseForSubpath(base, "src/group-00", inventory, "", -1)
	assertProjectionEqual(t, want, got)
	if len(got.Findings) != len(cases) || got.Summary.TotalFindings != len(cases) {
		t.Fatalf("path projection should match only the nested path candidates: findings=%d summary=%+v", len(got.Findings), got.Summary)
	}
	for _, finding := range got.Findings {
		if strings.HasPrefix(finding.NormalizedPath, "src/group-00/") {
			t.Fatalf("test fixture unexpectedly matched by its top-level path: %q", finding.NormalizedPath)
		}
	}
}

func setProjectionFindingPathsOutsideSubpath(finding *model.ChainFinding) {
	finding.NormalizedPath = "src/outside/finding.dat"
	finding.SourcePaths = []string{"src/outside/source.dat"}
	finding.BucketObjectURL = "s3://audit-bucket/src/outside/object.dat"
	finding.ResolvedKey = "src/outside/key.dat"
	finding.AccessURLs = []string{"s3://audit-bucket/src/outside/access.dat"}
	finding.Records[0].NormalizedPath = "src/outside/record.dat"
	finding.Records[0].AccessURLs = []string{"s3://audit-bucket/src/outside/record-access.dat"}
	finding.Records[0].AccessMethods[0].URL = "s3://audit-bucket/src/outside/method.dat"
	finding.Records[0].AccessProbes[0].URL = "s3://audit-bucket/src/outside/probe.dat"
	finding.Records[0].AccessProbes[0].Path = "src/outside/probe.dat"
	finding.Records[0].AccessProbes[0].Key = "src/outside/probe.dat"
	finding.Evidence.SourcePaths = []string{"src/outside/evidence-source.dat"}
	finding.Evidence.AccessURLs = []string{"s3://audit-bucket/src/outside/evidence-access.dat"}
	finding.Evidence.BucketObjectURLs = []string{"s3://audit-bucket/src/outside/evidence-object.dat"}
	finding.Evidence.Keys = []string{"src/outside/evidence-key.dat"}
}

func TestStorageChainAuditProjectionJSONAndTimeFieldParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		base model.ChainAuditResponse
	}{
		{name: "nil collections", base: model.ChainAuditResponse{}},
		{name: "empty collections", base: model.ChainAuditResponse{Findings: []model.ChainFinding{}, Groups: []model.ChainIssueGroup{}, Summary: model.ChainSummary{CountsByKind: map[string]int{}}}},
		{name: "timestamps and nil optionals", base: projectionBehaviorFixture()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.base.Summary.ObservedAt = "2026-10-28T21:12:13.987654321Z"
			tc.base.Summary.AuditCachedAt = "2026-10-27T18:09:10Z"
			want := originalProjectStorageChainAuditResponse(tc.base, "", 3)
			got := projectStorageChainAuditResponse(tc.base, "", 3)
			assertProjectionEqual(t, want, got)
			if got.Summary.ObservedAt != tc.base.Summary.ObservedAt || got.Summary.AuditCachedAt != tc.base.Summary.AuditCachedAt {
				t.Fatalf("projection changed timestamp fields: observed=%q cached=%q", got.Summary.ObservedAt, got.Summary.AuditCachedAt)
			}
		})
	}
}

func TestStorageChainAuditProjectionMatchesJSONReplacementForGroupKeys(t *testing.T) {
	kind1 := "probe\xff\xfe_error"
	kind2 := "probe\xfe\xff_error"
	path1 := "src/file\xff\xfe.dat"
	path2 := "src/file\xfe\xff.dat"
	object1 := "object\xff\xfe"
	object2 := "object\xfe\xff"
	base := model.ChainAuditResponse{
		Findings: []model.ChainFinding{
			{Kind: kind1, NormalizedPath: path1, ObjectIDs: []string{object1}, RecordCount: 1},
			{Kind: kind2, NormalizedPath: path2, ObjectIDs: []string{object2}, RecordCount: 1},
		},
		Summary: model.ChainSummary{
			CountsByKind:  map[string]int{kind1: 1, kind2: 1},
			TotalFindings: 2,
		},
	}
	normalizedKind := "probe" + strings.Repeat("\uFFFD", 2) + "_error"
	for _, tc := range []struct {
		name  string
		kind  string
		limit int
	}{
		{name: "unlimited", limit: -1},
		{name: "kind-filtered", kind: normalizedKind, limit: -1},
		{name: "unfiltered per-kind limit", limit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := projectStorageChainAuditResponse(base, tc.kind, tc.limit)
			want := originalProjectStorageChainAuditResponse(base, tc.kind, tc.limit)
			assertProjectionEqual(t, want, got)
			wantReturned := 2
			if tc.limit == 1 {
				wantReturned = 1
			}
			if len(got.Findings) != wantReturned || len(got.Groups) != 1 {
				t.Fatalf("normalized kinds should merge into one group: findings=%d groups=%+v", len(got.Findings), got.Groups)
			}
			group := got.Groups[0]
			if group.Kind != normalizedKind || group.FindingCount != 2 || group.PathCount != 1 || group.ObjectCount != 1 {
				t.Fatalf("group keys should use JSON's per-invalid-byte replacement: %+v", group)
			}
			if got.Summary.CountsByKind[normalizedKind] != 2 {
				t.Fatalf("kind counts should use JSON-normalized kind keys: %+v", got.Summary.CountsByKind)
			}
		})
	}
}

func TestStorageChainAuditProjectionReturnsDeeplyIsolatedFindings(t *testing.T) {
	base := projectionBehaviorFixture()
	baseJSON, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	first := projectStorageChainAuditResponse(base, "", 1)
	second := projectStorageChainAuditResponse(base, "", 1)
	first.Findings[0].SourcePaths[0] = "changed/source.go"
	first.Findings[0].AccessURLs[0] = "changed/access-url"
	first.Findings[0].Records[0].AccessMethods[0].Headers[0] = "changed-header"
	first.Findings[0].Records[0].AccessProbes[0].ValidationMismatches[0] = "changed-mismatch"
	*first.Findings[0].Records[0].AccessProbes[0].Exists = false
	first.Findings[0].Evidence.SourcePaths[0] = "changed/evidence.go"
	first.Summary.CountsByKind["bucket_only_object"] = -1
	first.Groups[0].FindingCount = -1

	baseAfter, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseJSON, baseAfter) {
		t.Fatal("mutating a returned response changed the cache snapshot")
	}
	if second.Findings[0].SourcePaths[0] == "changed/source.go" || second.Findings[0].Records[0].AccessMethods[0].Headers[0] == "changed-header" {
		t.Fatal("mutating one returned response changed another caller's response")
	}
}

func projectionBehaviorFixture() model.ChainAuditResponse {
	findings := make([]model.ChainFinding, 0, 20)
	kinds := []string{"bucket_only_object", "probe_error", "git_only_no_syfon", "syfon_git_no_bucket"}
	for i := 0; i < 20; i++ {
		findings = append(findings, projectionRecordFinding(i, kinds[i%len(kinds)]))
	}
	findings[16] = projectionRecordFinding(16, "git_only_no_syfon")
	counts := map[string]int{"bucket_syfon_git_complete": 2}
	for _, finding := range findings {
		counts[finding.Kind]++
	}
	return model.ChainAuditResponse{
		Findings: findings,
		Groups:   summarizeChainIssueGroups(findings),
		Summary: model.ChainSummary{
			CountsByKind:        counts,
			TotalFindings:       len(findings),
			GitTrackedFileCount: 3,
			SyfonRecordCount:    len(findings) + 2,
			BucketObjectCount:   len(findings) + 2,
			ObservedAt:          "2026-10-28T21:12:13.987654321Z",
			AuditCachedAt:       "2026-10-27T18:09:10Z",
		},
		PathPrefix:       "",
		BucketPathPrefix: "",
	}
}

func projectionRecordFinding(i int, kind string) model.ChainFinding {
	group := fmt.Sprintf("group-%02d", i%16)
	path := fmt.Sprintf("src/%s/file-%06d.dat", group, i)
	url := "s3://audit-bucket/" + path
	objectID := fmt.Sprintf("object-%06d", i)
	checksum := fmt.Sprintf("sha256:%064x", i+1)
	exists := true
	size := int64(1024 + i)
	record := model.CleanupRecordAudit{
		ObjectID:       objectID,
		Checksum:       checksum,
		NormalizedPath: path,
		CleanupScope:   "access_url",
		AccessURLs:     []string{url, url + "?version=2"},
		AccessMethods: []model.CleanupAccessMethod{{
			AccessID: "download",
			Type:     "signed_url",
			URL:      url,
			Headers:  []string{"Authorization: Bearer token", "x-audit-mode: projection"},
		}},
		AccessProbes: []model.CleanupAccessProbe{{
			URL:                  url,
			Operation:            "GetObject",
			Provider:             "s3",
			Bucket:               "audit-bucket",
			Key:                  path,
			Path:                 path,
			Exists:               &exists,
			Status:               "present",
			SizeBytes:            &size,
			ValidationStatus:     "validated",
			ValidationMismatches: []string{"size_match", "name_match"},
			ObservedAt:           "2026-10-28T21:12:13.987654321Z",
		}},
		Status:        "present",
		SizeBytes:     size,
		LastUpdated:   "2026-10-28T21:12:13.987654321Z",
		DownloadCount: int64(i % 101),
		LastDownload:  "2026-10-27T18:09:10Z",
	}
	return model.ChainFinding{
		Kind:              kind,
		EvidenceStatus:    "verified",
		NormalizedPath:    path,
		Checksum:          checksum,
		SourcePaths:       []string{path},
		ObjectIDs:         []string{objectID},
		Records:           []model.CleanupRecordAudit{record},
		AccessURLs:        []string{url, url + "?version=2"},
		BucketObjectURL:   url,
		ResolvedBucket:    "audit-bucket",
		ResolvedKey:       path,
		RecordCount:       1,
		SizeBytes:         size,
		BucketSizeBytes:   size,
		RecommendedAction: "Inspect storage evidence before cleanup.",
		AvailableActions:  []string{"inspect", "repair"},
		Evidence: &model.Evidence{
			Checksum:          checksum,
			SourcePaths:       []string{path},
			ObjectIDs:         []string{objectID},
			AccessURLs:        []string{url},
			BucketObjectURLs:  []string{url},
			Buckets:           []string{"audit-bucket"},
			Keys:              []string{path},
			StorageOperations: []string{"HeadObject", "GetObject"},
			ProbeStatuses:     []string{"present"},
			ValidationStates:  []string{"validated"},
			ErrorKinds:        []string{},
			Errors:            []string{},
			BucketEvaluation:  "enumerated and validated",
		},
	}
}

func originalProjectStorageChainAuditResponse(base model.ChainAuditResponse, findingKind string, findingLimit int) *model.ChainAuditResponse {
	response := cloneStorageChainAuditResponse(base)
	filteredFindings := filterStorageChainFindingsByKind(response.Findings, findingKind)
	summary := filterStorageChainSummary(response.Summary, filteredFindings)
	findings := limitStorageChainFindings(filteredFindings, findingLimit, findingKind)
	summary.ReturnedFindings = len(findings)
	summary.FindingLimit = findingLimit
	summary.FindingsTruncated = len(findings) < len(filteredFindings)
	response.Findings = findings
	response.Groups = summarizeChainIssueGroups(filteredFindings)
	response.Summary = summary
	return &response
}

func originalProjectStorageChainAuditResponseForSubpath(base model.ChainAuditResponse, gitSubpath string, inventory []model.RepoInventoryFile, findingKind string, findingLimit int) *model.ChainAuditResponse {
	pathPrefix := sarecord.NormalizeRepoSubpath(gitSubpath)
	filteredFindings := make([]model.ChainFinding, 0, len(base.Findings))
	for _, finding := range base.Findings {
		if storageChainFindingMatchesSubpath(finding, pathPrefix) {
			filteredFindings = append(filteredFindings, finding)
		}
	}

	summary := base.Summary
	countsByKind := make(map[string]int, len(summary.CountsByKind))
	for kind := range summary.CountsByKind {
		countsByKind[kind] = 0
	}
	issueGitPaths := make(map[string]struct{})
	objectIDs := make(map[string]struct{})
	bucketObjects := make(map[string]struct{})
	for _, finding := range filteredFindings {
		countsByKind[finding.Kind]++
		if storageChainFindingHasGitPath(finding.Kind) {
			for _, sourcePath := range finding.SourcePaths {
				if storagePathMatchesRepoSubpath(sourcePath, pathPrefix) && !strings.Contains(sourcePath, "://") {
					issueGitPaths[sarecord.NormalizeRepoSubpath(sourcePath)] = struct{}{}
				}
			}
			if len(finding.SourcePaths) == 0 && storagePathMatchesRepoSubpath(finding.NormalizedPath, pathPrefix) && !strings.Contains(finding.NormalizedPath, "://") {
				issueGitPaths[sarecord.NormalizeRepoSubpath(finding.NormalizedPath)] = struct{}{}
			}
		}
		for _, objectID := range finding.ObjectIDs {
			if trimmed := strings.TrimSpace(objectID); trimmed != "" {
				objectIDs[trimmed] = struct{}{}
			}
		}
		if trimmed := strings.TrimSpace(finding.BucketObjectURL); trimmed != "" {
			bucketObjects[trimmed] = struct{}{}
		}
		for _, accessURL := range finding.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				bucketObjects[trimmed] = struct{}{}
			}
		}
	}
	gitTrackedFileCount := len(inventory)
	completeCount := gitTrackedFileCount - len(issueGitPaths)
	if completeCount < 0 {
		completeCount = 0
	}
	countsByKind["bucket_syfon_git_complete"] = completeCount
	summary.CountsByKind = countsByKind
	summary.TotalFindings = len(filteredFindings)
	summary.GitTrackedFileCount = gitTrackedFileCount
	summary.SyfonRecordCount = completeCount + len(objectIDs)
	summary.BucketObjectCount = completeCount + len(bucketObjects)

	projected := base
	projected.Findings = filteredFindings
	projected.Groups = summarizeChainIssueGroups(filteredFindings)
	projected.Summary = summary
	projected.PathPrefix = pathPrefix
	projected.BucketPathPrefix = ""
	return originalProjectStorageChainAuditResponse(projected, findingKind, findingLimit)
}

func assertProjectionEqual(t testing.TB, want *model.ChainAuditResponse, got *model.ChainAuditResponse) {
	t.Helper()
	wantJSON, wantErr := json.Marshal(want)
	gotJSON, gotErr := json.Marshal(got)
	if wantErr != nil || gotErr != nil || !bytes.Equal(wantJSON, gotJSON) {
		t.Fatalf("projection JSON changed\nwant err=%v json=%s\n got err=%v json=%s", wantErr, wantJSON, gotErr, gotJSON)
	}
}

func TestStorageChainAuditProjectionTimestampNormalization(t *testing.T) {
	got := projectStorageChainAuditResponse(projectionBehaviorFixture(), "", 1)
	if _, err := time.Parse(time.RFC3339Nano, got.Summary.ObservedAt); err != nil {
		t.Fatalf("ObservedAt no longer parses as RFC3339Nano: %v", err)
	}
}
