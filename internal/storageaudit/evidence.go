package storageaudit

import "strings"

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
