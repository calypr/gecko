package registration

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

type Store interface {
	LoadStorageChainInventory(ctx context.Context, ref string, gitSubpath string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash) ([]model.RepoInventoryFile, error)
	LoadProjectChainScopeMappings(ctx context.Context, authorizationHeader string, organization string, project string) ([]domain.StorageBucketScope, error)
	LoadCachedProjectBucketValidationInventory(ctx context.Context, authorizationHeader string, organization string, project string, bucketPathPrefix string, forceRefresh bool) ([]gintegrationsyfon.ProjectBucketObject, map[string]gintegrationsyfon.ProjectBucketObject, record.BucketInventoryMetadata, error)
	BulkGetProjectRecordsByChecksum(ctx context.Context, authorizationHeader string, organization string, project string, checksums []string) (map[string][]gintegrationsyfon.ProjectRecord, error)
	BulkProbeStorageObjects(ctx context.Context, authorizationHeader string, items []gintegrationsyfon.BulkStorageProbeItem) ([]gintegrationsyfon.BulkStorageProbeResult, error)
	RegisterProjectObjects(ctx context.Context, authorizationHeader string, candidates []gintegrationsyfon.ProjectObjectRegistration) ([]gintegrationsyfon.ProjectObjectRegistrationResult, error)
	EvictProjectJoinCache(organization string, project string)
	EvictProjectAuditRecordCache(organization string, project string)
}

type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}

type gitOnlyRegistrationCandidate struct {
	checksum      string
	name          string
	size          int64
	accessURLs    []string
	resultIndexes []int
}

type gitOnlyBucketCandidate struct {
	url string
}

// RegisterGitOnlySyfonRecords restores only missing Syfon records. It always
// re-reads the Git LFS pointer, confirms the current scoped bucket object, and
// rejects size drift before sending a DRS registration request to Syfon.
func (service *Service) RegisterGitOnlySyfonRecords(ctx context.Context, authorizationHeader string, organization string, project string, ref string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, request model.GitOnlySyfonRegistrationRequest) (*model.GitOnlySyfonRegistrationResponse, error) {
	expectedRevision := strings.TrimSpace(request.ExpectedGitRevision)
	if expectedRevision == "" {
		return nil, fmt.Errorf("git-only Syfon registration requires expected_git_revision")
	}
	if expectedRevision != hash.String() {
		return nil, fmt.Errorf("git-only Syfon registration refused because Git revision changed from %s to %s; refresh the audit and try again", expectedRevision, hash.String())
	}
	paths := normalizedGitOnlyRegistrationPaths(request.RepoPaths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("git-only Syfon registration requires at least one repo path")
	}
	inventory, err := service.store.LoadStorageChainInventory(ctx, ref, "", mirrorPath, repo, hash)
	if err != nil {
		return nil, fmt.Errorf("load Git inventory for Syfon registration: %w", err)
	}
	byPath := make(map[string]model.RepoInventoryFile, len(inventory))
	for _, item := range inventory {
		byPath[record.NormalizeRepoSubpath(item.RepoPath)] = item
	}
	results := make([]model.GitOnlySyfonRegistrationResult, len(paths))
	checksums := make([]string, 0, len(paths))
	for index, repoPath := range paths {
		results[index].NormalizedPath = repoPath
		item, ok := byPath[repoPath]
		if !ok {
			results[index].Status = "skipped"
			results[index].Reason = "path is no longer a Git LFS pointer at the audited revision"
			continue
		}
		checksum := record.NormalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" || item.Size < 0 {
			results[index].Status = "skipped"
			results[index].Reason = "Git LFS pointer does not contain a valid SHA-256 and size"
			continue
		}
		results[index].Checksum = checksum
		results[index].GitSizeBytes = item.Size
		checksums = append(checksums, checksum)
	}
	existing, err := service.store.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, record.UniqueStrings(checksums))
	if err != nil {
		return nil, fmt.Errorf("recheck Syfon records before registration: %w", err)
	}
	scopes, err := service.store.LoadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("load project storage scopes before registration: %w", err)
	}
	bucketObjects, bucketObjectsByURL, _, err := service.store.LoadCachedProjectBucketValidationInventory(ctx, authorizationHeader, organization, project, "", false)
	if err != nil {
		return nil, fmt.Errorf("load project bucket inventory before registration: %w", err)
	}
	probes := make([]gintegrationsyfon.BulkStorageProbeItem, 0, len(results))
	probeResultIndexes := make(map[string]int, len(results))
	for index := range results {
		result := &results[index]
		if result.Status != "" {
			continue
		}
		if len(existing[result.Checksum]) > 0 {
			result.Status = "skipped"
			result.Reason = "a Syfon record for this SHA-256 already exists"
			continue
		}
		candidates := gitOnlyBucketCandidates(result.NormalizedPath, result.Checksum, scopes, organization, project, bucketObjects, bucketObjectsByURL)
		if len(candidates) == 0 {
			result.Status = "skipped"
			result.Reason = "no bucket object matched the audited Git path or SHA-256 in the configured project mappings"
			continue
		}
		size := result.GitSizeBytes
		for candidateIndex, candidate := range candidates {
			probeID := fmt.Sprintf("git-only-register-%d-%d", index, candidateIndex)
			probes = append(probes, gintegrationsyfon.BulkStorageProbeItem{
				ID:                probeID,
				ObjectURL:         candidate.url,
				ExpectedSizeBytes: &size,
			})
			probeResultIndexes[probeID] = index
		}
	}
	if len(probes) > 0 {
		observed, err := service.store.BulkProbeStorageObjects(ctx, authorizationHeader, probes)
		if err != nil {
			return nil, fmt.Errorf("recheck bucket objects before registration: %w", err)
		}
		byProbeID := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(observed))
		for _, probe := range observed {
			byProbeID[strings.TrimSpace(probe.ID)] = probe
		}
		for _, item := range probes {
			index := probeResultIndexes[item.ID]
			result := &results[index]
			if result.Status == "eligible" {
				continue
			}
			probe, ok := byProbeID[item.ID]
			if !ok {
				result.Status = "skipped"
				result.Reason = "bucket verification did not return a result"
				continue
			}
			result.BucketObjectURL = record.CanonicalStorageURL(probe.Bucket, probe.Key, item.ObjectURL)
			if result.BucketObjectURL == "" {
				result.BucketObjectURL = item.ObjectURL
			}
			result.BucketSizeBytes = derefInt64(probe.SizeBytes)
			if !probe.Exists || strings.TrimSpace(probe.Status) != "present" {
				continue
			}
			if probe.SizeBytes == nil || *probe.SizeBytes != result.GitSizeBytes {
				continue
			}
			result.Status = "eligible"
			result.Reason = "bucket object matched the audited Git path or SHA-256 and passed size verification"
		}
		for index := range results {
			if results[index].Status == "" {
				results[index].Status = "skipped"
				results[index].Reason = fmt.Sprintf("Git LFS size is %d B but no matched bucket object is present at that size", results[index].GitSizeBytes)
			}
		}
	}
	candidates := buildGitOnlyRegistrationCandidates(results)
	if len(candidates) > 0 {
		registrations := make([]gintegrationsyfon.ProjectObjectRegistration, 0, len(candidates))
		controlledAccess := []string{"/organization/" + strings.TrimSpace(organization) + "/project/" + strings.TrimSpace(project)}
		for _, candidate := range candidates {
			registrations = append(registrations, gintegrationsyfon.ProjectObjectRegistration{
				Name:             candidate.name,
				Checksum:         candidate.checksum,
				Size:             candidate.size,
				ControlledAccess: controlledAccess,
				AccessURLs:       candidate.accessURLs,
			})
		}
		registrationResults, err := service.store.RegisterProjectObjects(ctx, authorizationHeader, registrations)
		if err != nil {
			return nil, fmt.Errorf("register missing Syfon records: %w", err)
		}
		for candidateIndex, candidate := range candidates {
			objectID := registrationResults[candidateIndex].ObjectID
			for _, resultIndex := range candidate.resultIndexes {
				results[resultIndex].Status = "created"
				results[resultIndex].ObjectID = objectID
				results[resultIndex].Reason = "Syfon record created after Git pointer and bucket object verification"
			}
		}
		service.store.EvictProjectJoinCache(organization, project)
		service.store.EvictProjectAuditRecordCache(organization, project)
	}
	return &model.GitOnlySyfonRegistrationResponse{GitRevision: hash.String(), Results: results}, nil
}

func gitOnlyBucketCandidates(repoPath string, checksum string, scopes []domain.StorageBucketScope, organization string, project string, bucketObjects []gintegrationsyfon.ProjectBucketObject, bucketObjectsByURL map[string]gintegrationsyfon.ProjectBucketObject) []gitOnlyBucketCandidate {
	pathURLs := record.ProjectScopeRepoPathObjectURLs([]string{repoPath}, scopes, organization, project)
	checksumURLs := record.ProjectScopeRepoPathObjectURLs([]string{checksum}, scopes, organization, project)
	auditCandidates := make([]gitOnlyBucketCandidate, 0)
	seen := make(map[string]struct{})
	addAuditCandidate := func(objectURL string) {
		canonical := record.CanonicalStorageURL("", "", objectURL)
		if canonical == "" {
			return
		}
		if _, ok := bucketObjectsByURL[canonical]; !ok {
			return
		}
		if _, ok := seen[canonical]; ok {
			return
		}
		seen[canonical] = struct{}{}
		auditCandidates = append(auditCandidates, gitOnlyBucketCandidate{url: canonical})
	}
	for _, objectURL := range pathURLs {
		addAuditCandidate(objectURL)
	}
	for _, objectURL := range checksumURLs {
		addAuditCandidate(objectURL)
	}
	for _, object := range bucketObjects {
		objectURL := record.CanonicalStorageURL(object.Bucket, object.Key, object.ObjectURL)
		if objectURL == "" || record.NormalizeAnalyticsChecksum(object.MetaSHA256) != checksum || !objectBelongsToProjectScope(objectURL, scopes, organization, project) {
			continue
		}
		addAuditCandidate(objectURL)
	}
	if len(auditCandidates) > 0 {
		return auditCandidates
	}

	// Inventory may be unavailable for a newly uploaded object. Keep the
	// recheck conservative by probing both supported storage layouts rather
	// than assuming only the Git-path layout.
	generated := make([]gitOnlyBucketCandidate, 0, len(pathURLs)+len(checksumURLs))
	for _, objectURL := range append(pathURLs, checksumURLs...) {
		canonical := record.CanonicalStorageURL("", "", objectURL)
		if canonical == "" {
			continue
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		generated = append(generated, gitOnlyBucketCandidate{url: canonical})
	}
	return generated
}

func objectBelongsToProjectScope(objectURL string, scopes []domain.StorageBucketScope, organization string, project string) bool {
	bucket, _, ok := record.ParseStorageURL(objectURL)
	if !ok {
		return false
	}
	for _, scope := range scopes {
		if record.StorageScopeApplies(scope, organization, project) && strings.EqualFold(strings.TrimSpace(scope.Bucket), bucket) {
			return true
		}
	}
	return false
}

func normalizedGitOnlyRegistrationPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, repoPath := range paths {
		normalized := record.NormalizeRepoSubpath(repoPath)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func buildGitOnlyRegistrationCandidates(results []model.GitOnlySyfonRegistrationResult) []gitOnlyRegistrationCandidate {
	byChecksum := make(map[string]*gitOnlyRegistrationCandidate)
	for index := range results {
		result := &results[index]
		if result.Status != "eligible" {
			continue
		}
		candidate := byChecksum[result.Checksum]
		if candidate == nil {
			candidate = &gitOnlyRegistrationCandidate{
				checksum: result.Checksum,
				name:     path.Base(result.NormalizedPath),
				size:     result.GitSizeBytes,
			}
			byChecksum[result.Checksum] = candidate
		}
		if candidate.size != result.GitSizeBytes {
			result.Status = "skipped"
			result.Reason = "matching Git SHA-256 has inconsistent pointer sizes"
			continue
		}
		candidate.accessURLs = append(candidate.accessURLs, result.BucketObjectURL)
		candidate.resultIndexes = append(candidate.resultIndexes, index)
	}
	checksums := make([]string, 0, len(byChecksum))
	for checksum := range byChecksum {
		checksums = append(checksums, checksum)
	}
	sort.Strings(checksums)
	out := make([]gitOnlyRegistrationCandidate, 0, len(checksums))
	for _, checksum := range checksums {
		candidate := byChecksum[checksum]
		candidate.accessURLs = record.UniqueStrings(candidate.accessURLs)
		if len(candidate.resultIndexes) == 0 {
			continue
		}
		out = append(out, *candidate)
	}
	return out
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
