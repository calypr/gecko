package storecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/klauspost/compress/zstd"
)

func TestCachedResponseCodecRoundTripsReport(t *testing.T) {
	want := richCachedResponse()
	plain, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	encoded, err := encodeCachedResponse(context.Background(), want)
	if err != nil {
		t.Fatalf("encode response: %v", err)
	}
	if !bytes.Equal(encoded[:responseCacheCodecHeaderSize], responseCacheCodecHeader[:]) {
		t.Fatalf("codec header = %q, want %q", encoded[:responseCacheCodecHeaderSize], responseCacheCodecHeader)
	}
	if len(encoded) >= len(plain) {
		t.Fatalf("compressed size = %d bytes, want less than JSON size %d", len(encoded), len(plain))
	}

	got, err := decodeCachedResponse(context.Background(), encoded)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded response differs from input:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCachedResponseCodecReadsLegacyJSON(t *testing.T) {
	want := CachedResponse{CachedAt: time.Date(2026, 10, 1, 2, 3, 4, 0, time.UTC), RefreshDurationMillis: 17}
	legacy, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal legacy response: %v", err)
	}
	got, err := decodeCachedResponse(context.Background(), legacy)
	if err != nil {
		t.Fatalf("decode legacy JSON response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy response = %#v, want %#v", got, want)
	}
}

func TestCachedResponseCodecRejectsInvalidCompressedValues(t *testing.T) {
	valid, err := encodeCachedResponse(context.Background(), richCachedResponse())
	if err != nil {
		t.Fatalf("encode valid response: %v", err)
	}

	badChecksum := append([]byte(nil), valid...)
	badChecksum[len(badChecksum)-1] ^= 0xff
	truncatedFrame := append([]byte(nil), valid[:len(valid)-1]...)
	unknownVersion := append([]byte(nil), valid...)
	unknownVersion[len(responseCacheCodecMagic)]++

	invalidJSON, err := encodeZstdFrame([]byte("not-json"))
	if err != nil {
		t.Fatalf("encode invalid JSON frame: %v", err)
	}
	invalidJSON = append(append([]byte(nil), responseCacheCodecHeader[:]...), invalidJSON...)

	for name, payload := range map[string][]byte{
		"checksum":         badChecksum,
		"truncated frame":  truncatedFrame,
		"unknown version":  unknownVersion,
		"truncated header": []byte("GCR"),
		"invalid JSON":     invalidJSON,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeCachedResponse(context.Background(), payload); err == nil {
				t.Fatal("decode error = nil, want malformed cache value error")
			}
		})
	}
}

func TestResponseCacheCodecEnforcesValueSizeLimits(t *testing.T) {
	const limit = 1 << 20
	tooLargeJSON := bytes.Repeat([]byte("x"), limit+1)
	if _, err := encodeResponseCacheJSON(context.Background(), tooLargeJSON, limit); !errors.Is(err, errResponseCacheValueTooLarge) {
		t.Fatalf("encode over-limit JSON error = %v, want size limit error", err)
	}
	if _, err := encodeResponseCacheJSON(context.Background(), []byte("12345"), 12); !errors.Is(err, errResponseCacheValueTooLarge) {
		t.Fatalf("encode value with oversized frame error = %v, want size limit error", err)
	}

	tooLargeOutput := bytes.Repeat([]byte("x"), limit+1)
	frame, err := encodeZstdFrame(tooLargeOutput)
	if err != nil {
		t.Fatalf("encode over-limit frame: %v", err)
	}
	if _, err := decodeResponseCacheFrame(frame, limit); err == nil {
		t.Fatal("decode over-limit output error = nil, want size limit error")
	}
}

func TestCachedResponseCodecHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := encodeCachedResponse(ctx, CachedResponse{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode canceled response error = %v, want context canceled", err)
	}
	if _, err := decodeCachedResponse(ctx, []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Fatalf("decode canceled response error = %v, want context canceled", err)
	}
}

func richCachedResponse() CachedResponse {
	exists := true
	size := int64(1234)
	pathExists := true
	return CachedResponse{
		CachedAt:              time.Date(2026, 10, 1, 2, 3, 4, 0, time.UTC),
		RefreshDurationMillis: 42,
		Inventory: []model.RepoInventoryFile{{
			RepoPath: "data/file.bin",
			Name:     "file.bin",
			Checksum: strings.Repeat("a", 64),
			Size:     size,
		}},
		Response: model.ChainAuditResponse{
			Findings: []model.ChainFinding{{
				Kind:              "git_syfon_metadata_mismatch",
				EvidenceStatus:    "confirmed",
				NormalizedPath:    "data/file.bin",
				Checksum:          strings.Repeat("a", 64),
				SourcePaths:       []string{"data/file.bin", "mirror/data/file.bin"},
				ObjectIDs:         []string{"object-1", "object-2"},
				AccessURLs:        []string{"s3://bucket/data/file.bin"},
				BucketObjectURL:   "s3://bucket/data/file.bin",
				ResolvedBucket:    "bucket",
				ResolvedKey:       "data/file.bin",
				ProbeStatus:       "present",
				RecordCount:       1,
				SizeBytes:         size,
				BucketSizeBytes:   size,
				RecommendedAction: "review metadata",
				SuggestedFix:      strings.Repeat("Preserve the confirmed object while correcting its metadata. ", 2048),
				SuggestedAction:   "review",
				Actionability:     "manual",
				AvailableActions:  []string{"review", "ignore"},
				DefaultAction:     "review",
				SupportsDryRun:    true,
				Evidence: &model.Evidence{
					SourcePaths:   []string{"data/file.bin"},
					ObjectIDs:     []string{"object-1"},
					AccessURLs:    []string{"s3://bucket/data/file.bin"},
					Buckets:       []string{"bucket"},
					Keys:          []string{"data/file.bin"},
					ProbeStatuses: []string{"present"},
				},
				Records: []model.CleanupRecordAudit{{
					ObjectID:       "object-1",
					Checksum:       strings.Repeat("a", 64),
					NormalizedPath: "data/file.bin",
					CleanupScope:   "project",
					AccessURLs:     []string{"s3://bucket/data/file.bin"},
					AccessMethods:  []model.CleanupAccessMethod{{AccessID: "method-1", Type: "s3", URL: "s3://bucket/data/file.bin", Headers: []string{"x-test: true"}}},
					AccessProbes: []model.CleanupAccessProbe{{
						URL:              "s3://bucket/data/file.bin",
						Operation:        "head",
						Provider:         "s3",
						Bucket:           "bucket",
						Key:              "data/file.bin",
						Exists:           &exists,
						Status:           "present",
						SizeBytes:        &size,
						ValidationStatus: "matched",
						SizeMatch:        &exists,
						ObservedAt:       "2026-10-01T02:03:04Z",
						CacheHit:         true,
					}},
					Status:        "present",
					SizeBytes:     size,
					DownloadCount: 3,
				}},
			}},
			Groups: []model.ChainIssueGroup{{Kind: "git_syfon_metadata_mismatch", FindingCount: 1, PathCount: 1, RecordCount: 1, ObjectCount: 2, TotalBytes: size}},
			Summary: model.ChainSummary{
				CountsByKind:             map[string]int{"git_syfon_metadata_mismatch": 1},
				TotalFindings:            1,
				ReturnedFindings:         1,
				FindingLimit:             10,
				FindingsTruncated:        false,
				ValidationMode:           "full",
				GitRevision:              strings.Repeat("b", 40),
				SyfonRevision:            strings.Repeat("c", 40),
				ObservedAt:               "2026-10-01T02:03:04Z",
				BucketObjectCount:        2,
				SyfonRecordCount:         1,
				GitTrackedFileCount:      1,
				BucketPathExists:         &pathExists,
				BucketInventoryAvailable: true,
				BucketInventoryComplete:  true,
				AuditCacheHit:            true,
				AuditCacheAgeSeconds:     7,
			},
			PathPrefix: "data/",
		},
	}
}

func encodeZstdFrame(value []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, err
	}
	encoded := encoder.EncodeAll(value, nil)
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return encoded, nil
}
