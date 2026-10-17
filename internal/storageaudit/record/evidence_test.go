package record

import "testing"

func TestAssessTreatsInventoryMissAsUnknown(t *testing.T) {
	assessment := Assess([]Probe{{
		Operation:        OperationInventory,
		Status:           "unknown",
		ErrorKind:        "inventory_miss",
		ValidationStatus: "unverifiable",
	}}, false)
	if assessment.Status != EvidenceUnknown || assessment.Missing || !assessment.HasInventoryMiss {
		t.Fatalf("expected an unverified inventory candidate, got %+v", assessment)
	}
}

func TestAssessTreatsExactListMissAsUnconfirmed(t *testing.T) {
	assessment := Assess([]Probe{{
		Operation:        OperationList,
		Status:           "not_found",
		ErrorKind:        "object_not_found",
		ValidationStatus: "unverifiable",
	}}, false)
	if assessment.Status != EvidenceUnknown || assessment.Missing || !assessment.HasExactEvidence {
		t.Fatalf("expected exact LIST absence to require metadata confirmation, got %+v", assessment)
	}
}

func TestAssessTreatsMetadataMissAsUnconfirmed(t *testing.T) {
	assessment := Assess([]Probe{{
		Operation:        OperationMetadata,
		Status:           "not_found",
		ErrorKind:        "object_not_found",
		ValidationStatus: "unverifiable",
	}}, false)
	if assessment.Status != EvidenceUnknown || assessment.Missing || !assessment.HasExactEvidence {
		t.Fatalf("expected metadata absence to require a download-compatible probe, got %+v", assessment)
	}
}

func TestAssessTreatsDownloadMissAsVerified(t *testing.T) {
	assessment := Assess([]Probe{{
		Operation:        OperationDownload,
		Status:           "not_found",
		ErrorKind:        "object_not_found",
		ValidationStatus: "unverifiable",
	}}, false)
	if assessment.Status != EvidenceVerified || !assessment.Missing {
		t.Fatalf("expected a download miss to verify absence, got %+v", assessment)
	}
}

func TestAssessLetsPresentLocatorWinOverMissingAlias(t *testing.T) {
	assessment := Assess([]Probe{
		{Operation: OperationList, Status: "not_found", ErrorKind: "object_not_found"},
		{Operation: OperationList, Status: "present", ValidationStatus: "matched"},
	}, false)
	if assessment.Status != EvidenceVerified || !assessment.Present || assessment.Missing {
		t.Fatalf("expected any present canonical locator to connect the record, got %+v", assessment)
	}
}
