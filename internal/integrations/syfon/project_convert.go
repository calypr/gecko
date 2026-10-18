package syfon

import (
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
)

func projectRecordFromInternal(record internalapi.InternalRecord) (ProjectRecord, bool) {
	checksum := ""
	if record.Hashes != nil {
		checksum = normalizeSHA256((*record.Hashes)["sha256"])
	}
	if checksum == "" {
		return ProjectRecord{}, false
	}
	accessURLs := make([]string, 0)
	accessMethods := make([]ProjectAccessMethod, 0)
	if record.AccessMethods != nil {
		for _, method := range *record.AccessMethods {
			projectMethod := ProjectAccessMethod{
				Type:    strings.TrimSpace(string(method.Type)),
				Headers: []string{},
			}
			if method.AccessId != nil {
				projectMethod.AccessID = strings.TrimSpace(*method.AccessId)
			}
			if method.AccessUrl != nil {
				projectMethod.URL = strings.TrimSpace(method.AccessUrl.Url)
				accessURLs = append(accessURLs, projectMethod.URL)
				if method.AccessUrl.Headers != nil {
					projectMethod.Headers = append([]string(nil), (*method.AccessUrl.Headers)...)
				}
			}
			accessMethods = append(accessMethods, projectMethod)
		}
	}
	return ProjectRecord{
		ObjectID:      strings.TrimSpace(record.Did),
		Checksum:      checksum,
		Organization:  stringValue(record.Organization),
		Project:       stringValue(record.Project),
		Size:          int64Value(record.Size),
		UpdatedAt:     parseOptionalTime(record.UpdatedTime),
		CreatedAt:     parseOptionalTime(record.CreatedTime),
		AccessURLs:    accessURLs,
		AccessMethods: accessMethods,
	}, true
}

func projectAccessMethodsToDRS(methods []ProjectAccessMethod) []drsapi.AccessMethod {
	out := make([]drsapi.AccessMethod, 0, len(methods))
	for _, method := range methods {
		accessMethod := drsapi.AccessMethod{}
		if trimmed := strings.TrimSpace(method.AccessID); trimmed != "" {
			accessMethod.AccessId = &trimmed
		}
		if trimmed := strings.TrimSpace(method.Type); trimmed != "" {
			accessMethod.Type = drsapi.AccessMethodType(trimmed)
		}
		if trimmed := strings.TrimSpace(method.URL); trimmed != "" {
			accessURL := struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: trimmed}
			if len(method.Headers) > 0 {
				headers := append([]string(nil), method.Headers...)
				accessURL.Headers = &headers
			}
			accessMethod.AccessUrl = &accessURL
		}
		out = append(out, accessMethod)
	}
	return out
}

func recordMatchesScope(record ProjectRecord, organization string, project string) bool {
	recordOrg := strings.TrimSpace(record.Organization)
	recordProject := strings.TrimSpace(record.Project)
	if recordOrg == "" && recordProject == "" {
		return true
	}
	return strings.EqualFold(recordOrg, organization) && strings.EqualFold(recordProject, project)
}
