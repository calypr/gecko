package cleanup

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	sarecord "github.com/calypr/gecko/internal/storageaudit/record"
)

func RemainingAccessMethodsAfterBrokenRemoval(record model.CleanupRecordAudit) []gintegrationsyfon.ProjectAccessMethod {
	methods := sarecord.ProjectAccessMethodsFromCleanupMethods(record.AccessMethods)
	if len(methods) == 0 {
		for _, accessURL := range record.AccessURLs {
			if trimmed := strings.TrimSpace(accessURL); trimmed != "" {
				methods = append(methods, gintegrationsyfon.ProjectAccessMethod{URL: trimmed})
			}
		}
	}
	brokenURLs := sarecord.BrokenAccessURLsForRecord(record)
	remaining := make([]gintegrationsyfon.ProjectAccessMethod, 0, len(methods))
	for _, method := range methods {
		url := strings.TrimSpace(method.URL)
		if url == "" {
			continue
		}
		if _, broken := brokenURLs[sarecord.NormalizeCleanupSelectionKey(url)]; broken {
			continue
		}
		remaining = append(remaining, method)
	}
	return sarecord.AppendReplacementAccessMethods(remaining, record.AccessProbes, brokenURLs)
}

func coalesceAccessMethodChanges(changes []model.AccessMethodChange) ([]model.AccessMethodChange, error) {
	out := make([]model.AccessMethodChange, 0, len(changes))
	byID := make(map[string]model.AccessMethodChange, len(changes))
	for _, change := range changes {
		previous, exists := byID[change.ObjectID]
		if exists {
			if !reflect.DeepEqual(previous.Before, change.Before) || !reflect.DeepEqual(previous.After, change.After) {
				return nil, fmt.Errorf("conflicting access method changes for Syfon record %q", change.ObjectID)
			}
			continue
		}
		byID[change.ObjectID] = change
		out = append(out, change)
	}
	return out, nil
}

func accessMethodChangeForRecord(record model.CleanupRecordAudit, after []gintegrationsyfon.ProjectAccessMethod) model.AccessMethodChange {
	before := sarecord.ProjectAccessMethodsFromCleanupMethods(record.AccessMethods)
	if len(before) == 0 {
		for _, url := range record.AccessURLs {
			if strings.TrimSpace(url) != "" {
				before = append(before, gintegrationsyfon.ProjectAccessMethod{URL: url, Type: "s3"})
			}
		}
	}
	change := model.AccessMethodChange{
		ObjectID: record.ObjectID,
		Before:   publicAccessMethods(before),
		After:    publicAccessMethods(after),
	}
	beforeURLs := accessMethodURLSet(before)
	afterURLs := accessMethodURLSet(after)
	for key, url := range beforeURLs {
		if _, kept := afterURLs[key]; !kept {
			change.Removed = append(change.Removed, url)
		}
	}
	for key, url := range afterURLs {
		if _, existed := beforeURLs[key]; existed {
			change.Retained = append(change.Retained, url)
		} else {
			change.Added = append(change.Added, url)
		}
	}
	sort.Strings(change.Removed)
	sort.Strings(change.Retained)
	sort.Strings(change.Added)
	return change
}

func publicAccessMethods(methods []gintegrationsyfon.ProjectAccessMethod) []model.CleanupAccessMethod {
	out := make([]model.CleanupAccessMethod, 0, len(methods))
	for _, method := range methods {
		out = append(out, model.CleanupAccessMethod{
			AccessID: method.AccessID,
			Type:     method.Type,
			URL:      method.URL,
		})
	}
	return out
}

func accessMethodURLSet(methods []gintegrationsyfon.ProjectAccessMethod) map[string]string {
	out := make(map[string]string, len(methods))
	for _, method := range methods {
		if key := sarecord.NormalizeCleanupSelectionKey(method.URL); key != "" {
			out[key] = strings.TrimSpace(method.URL)
		}
	}
	return out
}

func (service *Service) verifyAccessMethodChanges(ctx context.Context, authorizationHeader string, changes []model.AccessMethodChange) error {
	if len(changes) == 0 {
		return nil
	}
	items := make([]gintegrationsyfon.BulkStorageProbeItem, 0)
	itemByURL := make(map[string]string)
	for _, change := range changes {
		for _, method := range append(append([]model.CleanupAccessMethod(nil), change.Before...), change.After...) {
			url := strings.TrimSpace(method.URL)
			if url == "" || itemByURL[url] != "" {
				continue
			}
			id := fmt.Sprintf("access-repair-%d", len(items))
			itemByURL[url] = id
			items = append(items, gintegrationsyfon.BulkStorageProbeItem{ID: id, ObjectURL: url})
		}
	}
	results, err := service.store.BulkConfirmStorageObjects(ctx, authorizationHeader, items)
	if err != nil {
		return fmt.Errorf("revalidate Syfon access methods: %w", err)
	}
	byID := make(map[string]gintegrationsyfon.BulkStorageProbeResult, len(results))
	for _, result := range results {
		if _, duplicate := byID[result.ID]; duplicate {
			return fmt.Errorf("revalidate Syfon access methods: duplicate probe result %q", result.ID)
		}
		result.Operation = model.StorageChainValidationModeMetadata
		byID[result.ID] = result
	}
	if len(byID) != len(items) {
		return fmt.Errorf("revalidate Syfon access methods: expected %d probe results, got %d", len(items), len(byID))
	}
	for index := range changes {
		change := &changes[index]
		if err := validateAccessMethodChange(change, itemByURL, byID); err != nil {
			return err
		}
	}
	return nil
}

func validateAccessMethodChange(change *model.AccessMethodChange, itemByURL map[string]string, byID map[string]gintegrationsyfon.BulkStorageProbeResult) error {
	if len(change.Removed) == 0 {
		return fmt.Errorf("revalidate Syfon record %q: repair does not remove an access URL", change.ObjectID)
	}
	removed := make(map[string]struct{}, len(change.Removed))
	for _, url := range change.Removed {
		removed[sarecord.NormalizeCleanupSelectionKey(url)] = struct{}{}
	}
	change.FreshProbes = make([]model.AccessMethodProbe, 0, len(change.Before)+len(change.Added))
	seen := make(map[string]struct{})
	hasPresentAfter := false
	for _, method := range append(append([]model.CleanupAccessMethod(nil), change.Before...), change.After...) {
		url := strings.TrimSpace(method.URL)
		if _, done := seen[url]; done {
			continue
		}
		seen[url] = struct{}{}
		result, ok := byID[itemByURL[url]]
		if !ok {
			return fmt.Errorf("revalidate Syfon record %q access URL %q: missing probe result", change.ObjectID, url)
		}
		change.FreshProbes = append(change.FreshProbes, model.AccessMethodProbe{
			URL: url, Status: result.Status, ErrorKind: result.ErrorKind, VerificationMethod: result.VerificationMethod, ValidationStatus: result.ValidationStatus,
		})
		if _, isRemoved := removed[sarecord.NormalizeCleanupSelectionKey(url)]; isRemoved {
			if !sarecord.SyfonProbeIsBrokenAccess(result) || (result.Status == "not_found" && result.VerificationMethod != "ranged_get") {
				return fmt.Errorf("revalidate Syfon record %q: access URL %q is not confirmed broken by an exact storage check (status=%q, error_kind=%q, verification_method=%q)", change.ObjectID, url, result.Status, result.ErrorKind, result.VerificationMethod)
			}
		}
		for _, after := range change.After {
			if after.URL == url && result.Status == "present" && result.Exists && sarecord.ProbeValidationMatched(result.ValidationStatus) {
				hasPresentAfter = true
			}
		}
	}
	if len(change.After) > 0 && !hasPresentAfter {
		return fmt.Errorf("revalidate Syfon record %q: no retained or replacement access URL was confirmed present", change.ObjectID)
	}
	return nil
}
