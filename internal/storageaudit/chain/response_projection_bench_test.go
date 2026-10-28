package chain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
)

var reportProjectionBenchmarkSink *model.ChainAuditResponse

type reportProjectionWorkload struct {
	name       string
	base       model.ChainAuditResponse
	inventory  []model.RepoInventoryFile
	inputJSON  []byte
	pathPrefix string
}

type reportProjectionCase struct {
	name  string
	kind  string
	limit int
	root  bool
}

var reportProjectionCases = []reportProjectionCase{
	{name: "selected1", kind: "bucket_only_object", limit: 1},
	{name: "default5000", limit: 5000},
	{name: "rootpath_smalllimit", limit: 1, root: true},
}

func BenchmarkReportProjection(b *testing.B) {
	implementation := strings.TrimSpace(os.Getenv("PROJECTION_IMPL"))
	if implementation == "" {
		implementation = "new"
	}
	if implementation != "old" && implementation != "new" {
		b.Fatalf("PROJECTION_IMPL must be old or new, got %q", implementation)
	}
	for _, benchmarkCase := range reportProjectionBenchmarks() {
		workload := benchmarkCase.workload
		projectionCase := benchmarkCase.projection
		b.Run(workload.name+"/"+projectionCase.name+"/projection", func(b *testing.B) {
			benchReportProjection(b, workload, projectionCase, implementation, false)
		})
		b.Run(workload.name+"/"+projectionCase.name+"/json_decode", func(b *testing.B) {
			benchReportProjection(b, workload, projectionCase, implementation, true)
		})
	}
}

type reportProjectionBenchmarkCase struct {
	workload   reportProjectionWorkload
	projection reportProjectionCase
}

func benchReportProjection(b *testing.B, workload reportProjectionWorkload, projectionCase reportProjectionCase, implementation string, decode bool) {
	b.Helper()
	fixtureJSON := reportProjectionJSON(workload.base)
	old := projectReportProjectionOld(workload.base, workload.inventory, workload.pathPrefix, projectionCase)
	new := projectReportProjectionNew(workload.base, workload.inventory, workload.pathPrefix, projectionCase)
	assertProjectionEqual(b, old, new)
	responseJSON, err := json.Marshal(old)
	if err != nil {
		b.Fatal(err)
	}
	newJSON, err := json.Marshal(new)
	if err != nil {
		b.Fatal(err)
	}
	if !bytes.Equal(responseJSON, newJSON) {
		b.Fatalf("old and new response bytes differ: old=%d new=%d", len(responseJSON), len(newJSON))
	}
	b.ReportAllocs()
	if decode {
		b.SetBytes(int64(len(workload.inputJSON)))
	}
	project := projectReportProjectionOld
	if implementation == "new" {
		project = projectReportProjectionNew
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := workload.base
		if decode {
			base = model.ChainAuditResponse{}
			if err := json.Unmarshal(workload.inputJSON, &base); err != nil {
				b.Fatal(err)
			}
		}
		result := project(base, workload.inventory, workload.pathPrefix, projectionCase)
		reportProjectionBenchmarkSink = result
	}
	b.StopTimer()
	if !bytes.Equal(fixtureJSON, reportProjectionJSON(workload.base)) {
		b.Fatal("benchmark projection or decode mutated the source fixture")
	}
	b.ReportMetric(float64(len(workload.inputJSON)), "source-B")
	b.ReportMetric(float64(len(responseJSON)), "response-B")
}

func projectReportProjectionOld(base model.ChainAuditResponse, inventory []model.RepoInventoryFile, pathPrefix string, projectionCase reportProjectionCase) *model.ChainAuditResponse {
	if projectionCase.root {
		filteredInventory := filterStorageChainInventory(inventory, pathPrefix)
		pathResponse := originalProjectStorageChainAuditResponseForSubpath(base, pathPrefix, filteredInventory, "", -1)
		return originalProjectStorageChainAuditResponse(*pathResponse, projectionCase.kind, projectionCase.limit)
	}
	return originalProjectStorageChainAuditResponse(base, projectionCase.kind, projectionCase.limit)
}

func projectReportProjectionNew(base model.ChainAuditResponse, inventory []model.RepoInventoryFile, pathPrefix string, projectionCase reportProjectionCase) *model.ChainAuditResponse {
	if projectionCase.root {
		filteredInventory := filterStorageChainInventory(inventory, pathPrefix)
		pathView := storageChainAuditResponseForSubpathView(base, pathPrefix, filteredInventory)
		return projectStorageChainAuditResponse(pathView, projectionCase.kind, projectionCase.limit)
	}
	return projectStorageChainAuditResponse(base, projectionCase.kind, projectionCase.limit)
}

func reportProjectionWorkloads() []reportProjectionWorkload {
	bucket := makeReportProjectionBucketWorkload(80000)
	deep := makeReportProjectionDeepWorkload(7000)
	return []reportProjectionWorkload{bucket, deep}
}

func reportProjectionBenchmarks() []reportProjectionBenchmarkCase {
	workloads := reportProjectionWorkloads()
	return []reportProjectionBenchmarkCase{
		{workload: workloads[0], projection: reportProjectionCases[0]},
		{workload: workloads[0], projection: reportProjectionCases[1]},
		{workload: workloads[1], projection: reportProjectionCases[2]},
	}
}

func makeReportProjectionBucketWorkload(count int) reportProjectionWorkload {
	findings := make([]model.ChainFinding, count)
	inventory := make([]model.RepoInventoryFile, count)
	for i := range findings {
		key := fmt.Sprintf("src/group-%02d/object-%06d.dat", i%16, i)
		item := gintegrationsyfon.ProjectBucketObject{Bucket: "audit-bucket", Key: key, Provider: "s3", SizeBytes: 1048576}
		findings[i] = buildChainBucketOnlyFinding(item)
		inventory[i] = model.RepoInventoryFile{RepoPath: fmt.Sprintf("src/group-%02d/file-%06d.go", i%16, i), Name: fmt.Sprintf("file-%06d.go", i)}
	}
	base := reportProjectionBase(findings)
	return reportProjectionWorkload{name: "bucket80k", base: base, inventory: inventory, inputJSON: reportProjectionJSON(base), pathPrefix: "src/group-00"}
}

func makeReportProjectionDeepWorkload(count int) reportProjectionWorkload {
	findings := make([]model.ChainFinding, count)
	inventory := make([]model.RepoInventoryFile, count)
	kinds := []string{"bucket_only_object", "git_only_no_syfon", "probe_error", "syfon_git_no_bucket"}
	for i := range findings {
		findings[i] = projectionRecordFinding(i, kinds[i%len(kinds)])
		path := findings[i].NormalizedPath
		inventory[i] = model.RepoInventoryFile{RepoPath: path, Name: path[strings.LastIndex(path, "/")+1:]}
	}
	base := reportProjectionBase(findings)
	return reportProjectionWorkload{name: "deep7k", base: base, inventory: inventory, inputJSON: reportProjectionJSON(base), pathPrefix: "src/group-00"}
}

func reportProjectionBase(findings []model.ChainFinding) model.ChainAuditResponse {
	counts := make(map[string]int)
	for _, finding := range findings {
		counts[finding.Kind]++
	}
	counts["bucket_syfon_git_complete"] = len(findings) / 20
	return model.ChainAuditResponse{
		Findings: findings,
		Groups:   summarizeChainIssueGroups(findings),
		Summary: model.ChainSummary{
			CountsByKind:        counts,
			TotalFindings:       len(findings),
			GitTrackedFileCount: len(findings),
			SyfonRecordCount:    len(findings),
			BucketObjectCount:   len(findings),
			ObservedAt:          "2026-10-28T21:12:13.987654321Z",
			AuditCachedAt:       "2026-10-27T18:09:10Z",
		},
	}
}

func reportProjectionJSON(response model.ChainAuditResponse) []byte {
	data, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	return data
}
