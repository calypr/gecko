package storageaudit

import "testing"

func TestNormalizeCleanupApplyRequest(t *testing.T) {
	request := CleanupApplyRequest{
		GitSubpath:        " /data/ ",
		SelectedRepoPaths: []string{" /data ", "data/", "", "/other/"},
		Actions: []CleanupApplyAction{{
			Action:         " delete ",
			Kind:           " repo_orphan ",
			NormalizedPath: "/data/file/",
		}},
		Findings: []CleanupApplyFinding{{
			Kind:             " repo_orphan ",
			NormalizedPath:   "/data/file/",
			ObjectIDs:        []string{" id-1 ", "id-1", ""},
			BucketObjectURLs: []string{" /bucket/a ", "bucket/a/", ""},
			Records: []CleanupRecordAudit{{
				ObjectID:       " id-1 ",
				NormalizedPath: " /data/file/ ",
				AccessURLs:     []string{" /access/a ", "access/a/"},
			}},
		}},
	}

	NormalizeCleanupApplyRequest(&request)

	if request.GitSubpath != "data" {
		t.Fatalf("expected normalized subpath, got %q", request.GitSubpath)
	}
	if len(request.SelectedRepoPaths) != 2 || request.SelectedRepoPaths[0] != "data" || request.SelectedRepoPaths[1] != "other" {
		t.Fatalf("unexpected selected paths: %+v", request.SelectedRepoPaths)
	}
	if request.Actions[0].Action != "delete" || request.Actions[0].NormalizedPath != "data/file" {
		t.Fatalf("unexpected action: %+v", request.Actions[0])
	}
	finding := request.Findings[0]
	if finding.NormalizedPath != "data/file" || finding.ObjectIDs[0] != "id-1" || len(finding.ObjectIDs) != 1 {
		t.Fatalf("unexpected finding: %+v", finding)
	}
	if len(finding.BucketObjectURLs) != 1 || finding.BucketObjectURLs[0] != "bucket/a" {
		t.Fatalf("unexpected bucket urls: %+v", finding.BucketObjectURLs)
	}
	if finding.Records[0].ObjectID != "id-1" || len(finding.Records[0].AccessURLs) != 1 || finding.Records[0].AccessURLs[0] != "access/a" {
		t.Fatalf("unexpected record: %+v", finding.Records[0])
	}
}

func TestNormalizeStorageChainAuditRequest(t *testing.T) {
	normalized, err := NormalizeStorageChainAuditRequest(ChainAuditRequest{
		GitSubpath:          " /data/ ",
		ValidationMode:      "inventory",
		BucketPathPrefix:    "/prefix/",
		FindingKind:         " missing ",
		FindingLimit:        25,
		ForceAuditRefresh:   true,
		BucketInventoryMode: "items",
	})
	if err != nil {
		t.Fatalf("normalize request: %v", err)
	}
	if normalized.GitSubpath != "data" || normalized.BucketPathPrefix != "prefix" || normalized.FindingKind != "missing" {
		t.Fatalf("unexpected normalized request: %+v", normalized)
	}
	if normalized.Options.ValidationMode != StorageChainValidationModeInventory || normalized.Options.BucketInventoryMode != StorageChainBucketModeItems {
		t.Fatalf("unexpected normalized options: %+v", normalized.Options)
	}
	if !normalized.Options.ForceAuditRefresh || normalized.Options.FindingLimit != 25 {
		t.Fatalf("unexpected refresh options: %+v", normalized.Options)
	}
}

func TestNormalizeStorageChainAuditRequestRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		request ChainAuditRequest
	}{
		{name: "probe mode", request: ChainAuditRequest{ProbeMode: "invalid"}},
		{name: "validation mode", request: ChainAuditRequest{ValidationMode: "invalid"}},
		{name: "bucket mode", request: ChainAuditRequest{BucketInventoryMode: "invalid"}},
		{name: "finding limit", request: ChainAuditRequest{FindingLimit: -2}},
		{name: "path prefix without items", request: ChainAuditRequest{BucketPathPrefix: "prefix"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NormalizeStorageChainAuditRequest(test.request); err == nil {
				t.Fatalf("expected validation error for %+v", test.request)
			}
		})
	}
}
