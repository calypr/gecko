package record

import (
	"strings"

	"github.com/calypr/gecko/internal/storageaudit/model"
)

type EvidenceStatus string

const (
	EvidenceVerified EvidenceStatus = "verified"
	EvidenceUnknown  EvidenceStatus = "unknown"
)

const (
	OperationInventory = "inventory"
	OperationList      = "list"
	OperationMetadata  = "metadata"
	OperationDownload  = "download"
)

type Probe struct {
	Operation        string
	Status           string
	ErrorKind        string
	ValidationStatus string
}

type Assessment struct {
	Status           EvidenceStatus
	Present          bool
	Missing          bool
	MappingBroken    bool
	MetadataMismatch bool
	HasInventoryMiss bool
	HasExactEvidence bool
}

func Assess(probes []Probe, bucketObserved bool) Assessment {
	assessment := Assessment{Status: EvidenceUnknown}
	if bucketObserved {
		assessment.Present = true
		assessment.Status = EvidenceVerified
	}
	for _, probe := range probes {
		operation := strings.TrimSpace(probe.Operation)
		status := strings.TrimSpace(probe.Status)
		errorKind := strings.TrimSpace(probe.ErrorKind)
		validation := strings.TrimSpace(probe.ValidationStatus)

		if operation == OperationInventory && (status == "unknown" || errorKind == "inventory_miss") {
			assessment.HasInventoryMiss = true
			continue
		}
		isExact := operation == OperationList || operation == OperationMetadata
		if isExact {
			assessment.HasExactEvidence = true
		}
		if status == "present" {
			assessment.Present = true
			assessment.Status = EvidenceVerified
		}
		// S3-compatible backends can report a false miss for LIST or HEAD while a
		// download-compatible GET still succeeds. Only an explicit GET-style probe
		// may prove absence and unlock destructive repair.
		if operation == OperationDownload && status == "not_found" && (errorKind == "" || errorKind == "object_not_found") {
			assessment.Missing = true
			assessment.Status = EvidenceVerified
		}
		switch errorKind {
		case "missing_access_url", "scope_not_found", "credential_missing":
			assessment.MappingBroken = true
			assessment.Status = EvidenceVerified
		}
		if validation == "mismatched" {
			assessment.MetadataMismatch = true
			assessment.Status = EvidenceVerified
		}
	}
	if assessment.Present {
		assessment.Missing = false
	}
	return assessment
}

func AssessStorageRecordEvidence(record State, bucketObserved bool) Assessment {
	probes := make([]Probe, 0, len(record.AccessProbes))
	for _, probe := range record.AccessProbes {
		probes = append(probes, Probe{
			Operation:        strings.TrimSpace(probe.Operation),
			Status:           strings.TrimSpace(probe.Status),
			ErrorKind:        strings.TrimSpace(probe.ErrorKind),
			ValidationStatus: strings.TrimSpace(probe.ValidationStatus),
		})
	}
	return Assess(probes, bucketObserved)
}

func BuildFindingEvidence(checksum string, sourcePaths []string, matches []State, bucketEvaluation string) *model.Evidence {
	evidence := &model.Evidence{
		Checksum:          strings.TrimSpace(checksum),
		SourcePaths:       UniqueStrings(sourcePaths),
		ObjectIDs:         []string{},
		AccessURLs:        []string{},
		BucketObjectURLs:  []string{},
		Buckets:           []string{},
		Keys:              []string{},
		StorageOperations: []string{},
		ProbeStatuses:     []string{},
		ValidationStates:  []string{},
		ErrorKinds:        []string{},
		Errors:            []string{},
		BucketEvaluation:  strings.TrimSpace(bucketEvaluation),
	}
	for _, match := range matches {
		if objectID := strings.TrimSpace(match.ObjectID); objectID != "" {
			evidence.ObjectIDs = append(evidence.ObjectIDs, objectID)
		}
		evidence.AccessURLs = append(evidence.AccessURLs, match.AccessURLs...)
		if len(match.AccessProbes) == 0 {
			continue
		}
		for _, probe := range match.AccessProbes {
			if objectURL := CanonicalStorageURL(probe.Bucket, probe.Key, probe.ObjectURL); objectURL != "" {
				evidence.BucketObjectURLs = append(evidence.BucketObjectURLs, objectURL)
			}
			if bucket := strings.TrimSpace(probe.Bucket); bucket != "" {
				evidence.Buckets = append(evidence.Buckets, bucket)
			}
			if key := strings.TrimSpace(probe.Key); key != "" {
				evidence.Keys = append(evidence.Keys, key)
			}
			if operation := strings.TrimSpace(probe.Operation); operation != "" {
				evidence.StorageOperations = append(evidence.StorageOperations, operation)
			}
			if status := strings.TrimSpace(probe.Status); status != "" {
				evidence.ProbeStatuses = append(evidence.ProbeStatuses, status)
			}
			if validation := strings.TrimSpace(probe.ValidationStatus); validation != "" {
				evidence.ValidationStates = append(evidence.ValidationStates, validation)
			}
			if kind := strings.TrimSpace(probe.ErrorKind); kind != "" {
				evidence.ErrorKinds = append(evidence.ErrorKinds, kind)
			}
			if err := strings.TrimSpace(probe.Error); err != "" {
				evidence.Errors = append(evidence.Errors, err)
			}
		}
	}
	evidence.ObjectIDs = UniqueStrings(evidence.ObjectIDs)
	evidence.AccessURLs = UniqueStrings(evidence.AccessURLs)
	evidence.BucketObjectURLs = UniqueStrings(evidence.BucketObjectURLs)
	evidence.Buckets = UniqueStrings(evidence.Buckets)
	evidence.Keys = UniqueStrings(evidence.Keys)
	evidence.StorageOperations = UniqueStrings(evidence.StorageOperations)
	evidence.ProbeStatuses = UniqueStrings(evidence.ProbeStatuses)
	evidence.ValidationStates = UniqueStrings(evidence.ValidationStates)
	evidence.ErrorKinds = UniqueStrings(evidence.ErrorKinds)
	evidence.Errors = UniqueStrings(evidence.Errors)
	if evidence.Checksum == "" &&
		len(evidence.SourcePaths) == 0 &&
		len(evidence.ObjectIDs) == 0 &&
		len(evidence.AccessURLs) == 0 &&
		len(evidence.BucketObjectURLs) == 0 &&
		len(evidence.Buckets) == 0 &&
		len(evidence.Keys) == 0 &&
		len(evidence.StorageOperations) == 0 &&
		len(evidence.ProbeStatuses) == 0 &&
		len(evidence.ValidationStates) == 0 &&
		len(evidence.ErrorKinds) == 0 &&
		len(evidence.Errors) == 0 &&
		evidence.BucketEvaluation == "" {
		return nil
	}
	return evidence
}
