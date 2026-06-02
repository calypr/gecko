package api

import (
	"encoding/json"
	"testing"

	"github.com/calypr/gecko/internal/git"
)

func TestBuildSyfonPutBucketRequestIncludesScope(t *testing.T) {
	req := buildSyfonPutBucketRequest(&git.CalyprProjectStorageIntent{
		AccessKey:    " access ",
		Bucket:       " bucket ",
		Endpoint:     " endpoint ",
		Organization: " org ",
		ProjectID:    " project ",
		Provider:     " s3 ",
		Region:       " us-east-1 ",
		SecretKey:    " secret ",
	})

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	got := string(body)
	if got != `{"access_key":"access","bucket":"bucket","endpoint":"endpoint","organization":"org","project_id":"project","provider":"s3","region":"us-east-1","secret_key":"secret"}` {
		t.Fatalf("unexpected request json: %s", got)
	}
}
