package git

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/calypr/gecko/internal/git/domain"
	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

type gitOnlyRegistrationCandidate struct {
	checksum      string
	name          string
	size          int64
	accessURLs    []string
	resultIndexes []int
}

// RegisterGitOnlySyfonRecords restores only missing Syfon records. It always
// re-reads the Git LFS pointer, confirms the current scoped bucket object, and
// rejects size drift before sending a DRS registration request to Syfon.
func (service *StorageAnalyticsService) RegisterGitOnlySyfonRecords(ctx context.Context, authorizationHeader string, organization string, project string, ref string, mirrorPath string, repo *gogit.Repository, hash plumbing.Hash, request GitOnlySyfonRegistrationRequest) (*GitOnlySyfonRegistrationResponse, error) {
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
	inventory, err := service.loadStorageChainInventory(ctx, ref, "", mirrorPath, repo, hash)
	if err != nil {
		return nil, fmt.Errorf("load Git inventory for Syfon registration: %w", err)
	}
	byPath := make(map[string]RepoInventoryFile, len(inventory))
	for _, item := range inventory {
		byPath[normalizeRepoSubpath(item.RepoPath)] = item
	}
	results := make([]GitOnlySyfonRegistrationResult, len(paths))
	checksums := make([]string, 0, len(paths))
	for index, repoPath := range paths {
		results[index].NormalizedPath = repoPath
		item, ok := byPath[repoPath]
		if !ok {
			results[index].Status = "skipped"
			results[index].Reason = "path is no longer a Git LFS pointer at the audited revision"
			continue
		}
		checksum := normalizeAnalyticsChecksum(item.Checksum)
		if checksum == "" || item.Size < 0 {
			results[index].Status = "skipped"
			results[index].Reason = "Git LFS pointer does not contain a valid SHA-256 and size"
			continue
		}
		results[index].Checksum = checksum
		results[index].GitSizeBytes = item.Size
		checksums = append(checksums, checksum)
	}
	existing, err := service.storage.BulkGetProjectRecordsByChecksum(ctx, authorizationHeader, organization, project, uniqueStrings(checksums))
	if err != nil {
		return nil, fmt.Errorf("recheck Syfon records before registration: %w", err)
	}
	scopes, err := service.loadProjectChainScopeMappings(ctx, authorizationHeader, organization, project)
	if err != nil {
		return nil, fmt.Errorf("load project storage scopes before registration: %w", err)
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
		urls := projectScopeRepoPathObjectURLs([]string{result.NormalizedPath}, scopes, organization, project)
		if len(urls) != 1 {
			result.Status = "skipped"
			result.Reason = gitOnlyRegistrationScopeReason(urls, scopes, organization, project)
			continue
		}
		probeID := fmt.Sprintf("git-only-register-%d", index)
		size := result.GitSizeBytes
		probes = append(probes, gintegrationsyfon.BulkStorageProbeItem{
			ID:                probeID,
			ObjectURL:         urls[0],
			ExpectedSizeBytes: &size,
		})
		probeResultIndexes[probeID] = index
	}
	if len(probes) > 0 {
		observed, err := service.storage.BulkProbeStorageObjects(ctx, authorizationHeader, probes)
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
			probe, ok := byProbeID[item.ID]
			if !ok {
				result.Status = "skipped"
				result.Reason = "bucket verification did not return a result"
				continue
			}
			result.BucketObjectURL = canonicalStorageURL(probe.Bucket, probe.Key, item.ObjectURL)
			if result.BucketObjectURL == "" {
				result.BucketObjectURL = item.ObjectURL
			}
			result.BucketSizeBytes = derefInt64(probe.SizeBytes)
			if !probe.Exists || strings.TrimSpace(probe.Status) != "present" {
				result.Status = "skipped"
				result.Reason = "mapped bucket object is not currently present"
				continue
			}
			if probe.SizeBytes == nil || *probe.SizeBytes != result.GitSizeBytes {
				result.Status = "skipped"
				result.Reason = fmt.Sprintf("Git LFS size is %d B but bucket size is %d B", result.GitSizeBytes, result.BucketSizeBytes)
				continue
			}
			result.Status = "eligible"
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
		registrationResults, err := service.storage.RegisterProjectObjects(ctx, authorizationHeader, registrations)
		if err != nil {
			return nil, fmt.Errorf("register missing Syfon records: %w", err)
		}
		for candidateIndex, candidate := range candidates {
			objectID := registrationResults[candidateIndex].ObjectID
			for _, resultIndex := range candidate.resultIndexes {
				results[resultIndex].Status = "created"
				results[resultIndex].ObjectID = objectID
				results[resultIndex].Reason = "Syfon record created after Git pointer and bucket size verification; bucket SHA-256 was not available for verification"
			}
		}
		service.evictProjectJoinCache(organization, project)
		service.evictProjectAuditRecordCache(organization, project)
	}
	return &GitOnlySyfonRegistrationResponse{GitRevision: hash.String(), Results: results}, nil
}

func normalizedGitOnlyRegistrationPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, repoPath := range paths {
		normalized := normalizeRepoSubpath(repoPath)
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

func gitOnlyRegistrationScopeReason(urls []string, scopes []domain.StorageBucketScope, organization string, project string) string {
	if len(urls) == 0 {
		return fmt.Sprintf("no configured bucket mapping applies to project %s/%s", organization, project)
	}
	return fmt.Sprintf("multiple configured bucket mappings apply (%d); choose a single storage mapping before creating a Syfon record", len(urls))
}

func buildGitOnlyRegistrationCandidates(results []GitOnlySyfonRegistrationResult) []gitOnlyRegistrationCandidate {
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
		candidate.accessURLs = uniqueStrings(candidate.accessURLs)
		if len(candidate.resultIndexes) == 0 {
			continue
		}
		out = append(out, *candidate)
	}
	return out
}
