package storecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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
		t.Fatalf("marshal expected response: %v", err)
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
	if got := decompressResponseCacheFrame(t, encoded[responseCacheCodecHeaderSize:]); !bytes.Equal(got, plain) {
		t.Fatal("streamed frame JSON differs from encoding/json output")
	}

	var header zstd.Header
	if err := header.Decode(encoded[responseCacheCodecHeaderSize:]); err != nil {
		t.Fatalf("decode streamed zstd header: %v", err)
	}
	if header.HasFCS {
		t.Fatal("streamed zstd frame unexpectedly declares a content size")
	}

	got, err := decodeCachedResponse(context.Background(), encoded)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded response differs from input:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCachedResponseCodecPreservesLegacyJSONSemantics(t *testing.T) {
	zone := time.FixedZone("audit", 5*60*60+30*60)
	value := CachedResponse{
		CachedAt:              time.Date(2026, 10, 1, 2, 3, 4, 123456789, zone),
		RefreshDurationMillis: 17,
		Response: model.ChainAuditResponse{
			Findings:   []model.ChainFinding{},
			Groups:     nil,
			Summary:    model.ChainSummary{CountsByKind: map[string]int{}},
			PathPrefix: string([]byte{0xff}) + "<&>",
		},
		Inventory: []model.RepoInventoryFile{},
	}
	legacyJSON, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal expected response: %v", err)
	}

	encoded, err := encodeCachedResponse(context.Background(), value)
	if err != nil {
		t.Fatalf("encode response: %v", err)
	}
	if got := decompressResponseCacheFrame(t, encoded[responseCacheCodecHeaderSize:]); !bytes.Equal(got, legacyJSON) {
		t.Fatalf("streamed JSON differs from encoding/json output:\n got: %s\nwant: %s", got, legacyJSON)
	}

	var want CachedResponse
	if err := json.Unmarshal(legacyJSON, &want); err != nil {
		t.Fatalf("decode expected response: %v", err)
	}
	got, err := decodeCachedResponse(context.Background(), encoded)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded response differs from encoding/json semantics:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestCachedResponseCodecReadsLegacyJSONAndExistingFCSFrames(t *testing.T) {
	want := richCachedResponse()
	legacyJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal legacy response: %v", err)
	}

	legacyFrame := append(responseCacheCodecHeader[:], encodeResponseCacheFrame(t, legacyJSON)...)
	var header zstd.Header
	if err := header.Decode(legacyFrame[responseCacheCodecHeaderSize:]); err != nil {
		t.Fatalf("decode legacy zstd header: %v", err)
	}
	if !header.HasFCS {
		t.Fatal("legacy EncodeAll frame has no content size")
	}

	for name, encoded := range map[string][]byte{
		"legacy raw JSON":  legacyJSON,
		"legacy FCS frame": legacyFrame,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeCachedResponse(context.Background(), encoded)
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded response differs from input:\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

func TestCachedResponseCodecAcceptsTrailingWhitespace(t *testing.T) {
	plain := []byte("{\"refresh_duration_ms\":17,\"response\":{\"findings\":null,\"summary\":{\"counts_by_kind\":null},\"path_prefix\":\"\"}} \n\t")
	framed := append(responseCacheCodecHeader[:], encodeResponseCacheFrame(t, plain)...)

	for name, encoded := range map[string][]byte{"legacy JSON": plain, "compressed frame": framed} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeCachedResponse(context.Background(), encoded)
			if err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got.RefreshDurationMillis != 17 || got.Response.PathPrefix != "" {
				t.Fatalf("decoded response = %#v, want duration 17 and empty prefix", got)
			}
		})
	}
}

func TestCachedResponseCodecRejectsTrailingJSONAndReturnsZero(t *testing.T) {
	plain := []byte("{\"refresh_duration_ms\":17,\"response\":{\"findings\":null,\"summary\":{\"counts_by_kind\":null},\"path_prefix\":\"\"}} {}")
	framed := append(responseCacheCodecHeader[:], encodeResponseCacheFrame(t, plain)...)

	for name, encoded := range map[string][]byte{"legacy JSON": plain, "compressed frame": framed} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeCachedResponse(context.Background(), encoded)
			if err == nil {
				t.Fatal("decode error = nil, want trailing JSON error")
			}
			if !reflect.DeepEqual(got, CachedResponse{}) {
				t.Fatalf("failed decode returned partial value: %#v", got)
			}
		})
	}
}

func TestCachedResponseCodecDiscardsPartialValueOnLateJSONError(t *testing.T) {
	malformed := []byte(`{"refresh_duration_ms":17,"response":{"path_prefix":"already decoded","findings":null,"summary":{"counts_by_kind":null}},`)
	got, err := decodeCachedResponse(context.Background(), malformed)
	if err == nil {
		t.Fatal("decode error = nil, want malformed JSON error")
	}
	if !reflect.DeepEqual(got, CachedResponse{}) {
		t.Fatalf("failed decode returned partial value: %#v", got)
	}
	if !strings.Contains(err.Error(), "decode storage audit response cache JSON") {
		t.Fatalf("decode error = %v, want codec error context", err)
	}
}

func TestCachedResponseCodecRejectsInvalidFramesAndReturnsZero(t *testing.T) {
	valid, err := encodeCachedResponse(context.Background(), richCachedResponse())
	if err != nil {
		t.Fatalf("encode valid response: %v", err)
	}

	badChecksum := append([]byte(nil), valid...)
	badChecksum[len(badChecksum)-1] ^= 0xff
	truncatedFrame := append([]byte(nil), valid[:len(valid)-1]...)
	unknownVersion := append([]byte(nil), valid...)
	unknownVersion[len(responseCacheCodecMagic)]++
	invalidJSON := append(responseCacheCodecHeader[:], encodeResponseCacheFrame(t, []byte("not-json"))...)

	for name, payload := range map[string][]byte{
		"checksum":         badChecksum,
		"truncated frame":  truncatedFrame,
		"unknown version":  unknownVersion,
		"truncated header": []byte("GCR"),
		"partial header":   []byte("GC"),
		"invalid JSON":     invalidJSON,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeCachedResponse(context.Background(), payload)
			if err == nil {
				t.Fatal("decode error = nil, want malformed cache value error")
			}
			if !reflect.DeepEqual(got, CachedResponse{}) {
				t.Fatalf("failed decode returned partial value: %#v", got)
			}
		})
	}
}

func TestResponseCacheCodecLimitsAcceptExactAndRejectExcess(t *testing.T) {
	ctx := context.Background()
	var encoded bytes.Buffer
	writer := &contextLimitWriter{ctx: ctx, dst: &encoded, limit: 4}
	if n, err := writer.Write([]byte("1234")); n != 4 || err != nil {
		t.Fatalf("exact-limit write = (%d, %v), want (4, nil)", n, err)
	}
	if n, err := writer.Write([]byte("5")); n != 0 || !errors.Is(err, errResponseCacheValueTooLarge) {
		t.Fatalf("over-limit write = (%d, %v), want size-limit error", n, err)
	}
	if encoded.String() != "1234" {
		t.Fatalf("published writer bytes = %q, want exact-limit prefix", encoded.String())
	}

	for _, test := range []struct {
		name string
		data string
		want string
		bad  bool
	}{
		{name: "exact decoded limit", data: "1234", want: "1234"},
		{name: "over decoded limit", data: "12345", want: "1234", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &contextLimitReader{ctx: ctx, src: strings.NewReader(test.data), limit: 4}
			got, err := io.ReadAll(reader)
			if !bytes.Equal(got, []byte(test.want)) {
				t.Fatalf("read bytes = %q, want %q", got, test.want)
			}
			if test.bad && !errors.Is(err, errResponseCacheValueTooLarge) {
				t.Fatalf("over-limit read error = %v, want size-limit error", err)
			}
			if !test.bad && err != nil {
				t.Fatalf("exact-limit read error = %v, want nil", err)
			}
		})
	}
}

func TestResponseCacheCodecWrappersHonorCancellationDuringIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &contextLimitWriter{ctx: ctx, dst: cancelOnWrite{cancel: cancel}, limit: 8}
	if n, err := writer.Write([]byte("chunk")); n != len("chunk") || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write = (%d, %v), want full first chunk and context cancellation", n, err)
	}

	readCtx, cancelRead := context.WithCancel(context.Background())
	reader := &contextLimitReader{ctx: readCtx, src: cancelOnRead{cancel: cancelRead, data: []byte("chunk")}, limit: 8}
	var buf [8]byte
	if n, err := reader.Read(buf[:]); n != len("chunk") || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = (%d, %v), want full first chunk and context cancellation", n, err)
	}
}

func TestCachedResponseCodecHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := encodeCachedResponse(ctx, CachedResponse{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode canceled response error = %v, want context canceled", err)
	}
	if got, err := decodeCachedResponse(ctx, []byte("{}")); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, CachedResponse{}) {
		t.Fatalf("decode canceled response = (%#v, %v), want zero value and context canceled", got, err)
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

func encodeResponseCacheFrame(t *testing.T, value []byte) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		t.Fatalf("create test encoder: %v", err)
	}
	frame := encoder.EncodeAll(value, nil)
	if err := encoder.Close(); err != nil {
		t.Fatalf("close test encoder: %v", err)
	}
	return frame
}

func decompressResponseCacheFrame(t *testing.T, frame []byte) []byte {
	t.Helper()
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxResponseCacheValueBytes)),
	)
	if err != nil {
		t.Fatalf("create test decoder: %v", err)
	}
	defer decoder.Close()
	value, err := decoder.DecodeAll(frame, nil)
	if err != nil {
		t.Fatalf("decompress response cache frame: %v", err)
	}
	return value
}

type cancelOnWrite struct {
	cancel context.CancelFunc
}

func (w cancelOnWrite) Write(p []byte) (int, error) {
	w.cancel()
	return len(p), nil
}

type cancelOnRead struct {
	cancel context.CancelFunc
	data   []byte
}

func (r cancelOnRead) Read(p []byte) (int, error) {
	r.cancel()
	return copy(p, r.data), nil
}
