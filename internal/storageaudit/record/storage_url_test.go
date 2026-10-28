package record

import (
	"testing"

	"github.com/calypr/gecko/internal/domain"
)

func TestCanonicalizeScopedStorageURLMatchesSyfonDownloadScopeResolution(t *testing.T) {
	tests := []struct {
		name      string
		accessURL string
		scopes    []domain.StorageBucketScope
		org       string
		project   string
		want      string
	}{
		{
			name:      "project scope rewrites alias bucket to physical bucket",
			accessURL: "s3://bforepc-prod/OHSU/koei_chin/visium_hd/barcodes.tsv.gz",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			}},
			org:     "HTAN_INT",
			project: "BForePC",
			want:    "s3://bforepc/bforepc-prod/OHSU/koei_chin/visium_hd/barcodes.tsv.gz",
		},
		{
			name:      "root scope does not require slash in path",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "s3://physical",
			}},
			org:     "org",
			project: "proj",
			want:    "s3://physical/path/file.txt",
		},
		{
			name:      "explicit bucket with prefix path",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "org",
				ProjectID:    "proj",
				Path:         "root/prefix",
			}},
			org:     "org",
			project: "proj",
			want:    "s3://physical/root/prefix/path/file.txt",
		},
		{
			name:      "already scoped url is not double prefixed",
			accessURL: "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "bforepc",
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Path:         "s3://bforepc/bforepc-prod",
			}},
			org:     "HTAN_INT",
			project: "BForePC",
			want:    "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff",
		},
		{
			name:      "unrelated scope is ignored",
			accessURL: "s3://alias/path/file.txt",
			scopes: []domain.StorageBucketScope{{
				Bucket:       "physical",
				Organization: "other",
				ProjectID:    "proj",
				Path:         "s3://physical/root",
			}},
			org:     "org",
			project: "proj",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalizeScopedStorageURL(tt.accessURL, tt.scopes, tt.org, tt.project)
			if got != tt.want {
				t.Fatalf("unexpected scoped URL: got %q want %q", got, tt.want)
			}
		})
	}
}
