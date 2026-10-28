package storecache

import (
	"bytes"
	"context"
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/klauspost/compress/zstd"
)

const responseCodecBenchmarkFixture = "/private/tmp/audit-report-size/large-report.json"

func TestResponseCacheCodecLargeFixtureBenchmark(t *testing.T) {
	if os.Getenv("GECKO_RUN_STREAMED_CODEC_BENCHMARK") != "1" {
		t.Skip("set GECKO_RUN_STREAMED_CODEC_BENCHMARK=1 to run the large fixture benchmark")
	}
	fixtureBytes, err := os.ReadFile(responseCodecBenchmarkFixture)
	if err != nil {
		t.Skipf("large codec benchmark fixture unavailable: %v", err)
	}
	var response model.ChainAuditResponse
	if err := jsonv1.Unmarshal(fixtureBytes, &response); err != nil {
		t.Fatalf("decode large response fixture: %v", err)
	}
	want := CachedResponse{Response: response, Inventory: []model.RepoInventoryFile{}}

	plainJSON, err := jsonv1.Marshal(want)
	if err != nil {
		t.Fatalf("marshal reference JSON: %v", err)
	}
	var wantDecoded CachedResponse
	if err := jsonv1.Unmarshal(plainJSON, &wantDecoded); err != nil {
		t.Fatalf("decode reference model: %v", err)
	}
	currentEncoded, err := encodeCurrentCachedResponse(want)
	if err != nil {
		t.Fatalf("encode frozen current response: %v", err)
	}
	streamedEncoded, err := encodeCachedResponse(context.Background(), want)
	if err != nil {
		t.Fatalf("encode streamed response: %v", err)
	}
	if !bytes.Equal(decompressResponseCacheFrame(t, currentEncoded[responseCacheCodecHeaderSize:]), plainJSON) {
		t.Fatal("frozen current frame JSON differs from encoding/json output")
	}
	if !bytes.Equal(decompressResponseCacheFrame(t, streamedEncoded[responseCacheCodecHeaderSize:]), plainJSON) {
		t.Fatal("streamed frame JSON differs from encoding/json output")
	}
	if got, err := decodeCurrentCachedResponse(streamedEncoded); err != nil || !reflect.DeepEqual(got, wantDecoded) {
		t.Fatalf("frozen current decoder on streamed frame mismatch: err=%v", err)
	}
	for name, payload := range map[string][]byte{"current FCS frame": currentEncoded, "streamed unknown-FCS frame": streamedEncoded} {
		got, err := decodeCachedResponse(context.Background(), payload)
		if err != nil {
			t.Fatalf("streamed decoder on %s: %v", name, err)
		}
		if !reflect.DeepEqual(got, wantDecoded) {
			t.Fatalf("streamed decoder changed model from %s", name)
		}
	}
	if got := decompressResponseCacheFrame(t, currentEncoded[responseCacheCodecHeaderSize:]); !bytes.Equal(got, decompressResponseCacheFrame(t, streamedEncoded[responseCacheCodecHeaderSize:])) {
		t.Fatal("current and streamed frames expand to different JSON bytes")
	}

	chunkStats := recordStreamedJSONChunks(t, want)
	var writeCurrent, writeStreamed, readCurrent, readStreamed []codecSample
	var encodedBytesConsumed, decodedFindingsConsumed int64
	for pair := 0; pair < 5; pair++ {
		if pair%2 == 0 {
			payload, sample := measureCodecEncode(t, func() ([]byte, error) { return encodeCurrentCachedResponse(want) })
			encodedBytesConsumed += int64(len(payload))
			writeCurrent = append(writeCurrent, sample)
			payload, sample = measureCodecEncode(t, func() ([]byte, error) { return encodeCachedResponse(context.Background(), want) })
			encodedBytesConsumed += int64(len(payload))
			writeStreamed = append(writeStreamed, sample)
		} else {
			payload, sample := measureCodecEncode(t, func() ([]byte, error) { return encodeCachedResponse(context.Background(), want) })
			encodedBytesConsumed += int64(len(payload))
			writeStreamed = append(writeStreamed, sample)
			payload, sample = measureCodecEncode(t, func() ([]byte, error) { return encodeCurrentCachedResponse(want) })
			encodedBytesConsumed += int64(len(payload))
			writeCurrent = append(writeCurrent, sample)
		}
	}

	for pair := 0; pair < 5; pair++ {
		check := func(value CachedResponse) {
			t.Helper()
			if !reflect.DeepEqual(value, wantDecoded) {
				t.Fatal("decode benchmark returned a different response model")
			}
			decodedFindingsConsumed += int64(len(value.Response.Findings) + value.Response.Summary.TotalFindings)
			runtime.KeepAlive(value)
		}
		if pair%2 == 0 {
			value, sample := measureCodecDecode(t, func() (CachedResponse, error) { return decodeCurrentCachedResponse(currentEncoded) })
			readCurrent = append(readCurrent, sample)
			check(value)
			value, sample = measureCodecDecode(t, func() (CachedResponse, error) { return decodeCachedResponse(context.Background(), currentEncoded) })
			readStreamed = append(readStreamed, sample)
			check(value)
		} else {
			value, sample := measureCodecDecode(t, func() (CachedResponse, error) { return decodeCachedResponse(context.Background(), currentEncoded) })
			readStreamed = append(readStreamed, sample)
			check(value)
			value, sample = measureCodecDecode(t, func() (CachedResponse, error) { return decodeCurrentCachedResponse(currentEncoded) })
			readCurrent = append(readCurrent, sample)
			check(value)
		}
	}

	t.Logf("fixture go=%s os=%s arch=%s CPUs=%d findings=%d JSON=%d bytes current=%d bytes streamed=%d bytes",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), len(want.Response.Findings), len(plainJSON), len(currentEncoded), len(streamedEncoded))
	t.Logf("streamed JSON chunks=%d max_chunk=%d bytes; byte and decoded-model parity checked before measurement",
		chunkStats.writes, chunkStats.maxBytes)
	t.Logf("encode current median=%s range=%s B/op=%s allocs/op=%s",
		formatSampleStats(writeCurrent, func(s codecSample) int64 { return s.nanos }),
		formatSampleRange(writeCurrent, func(s codecSample) int64 { return s.nanos }),
		formatSampleStats(writeCurrent, func(s codecSample) int64 { return int64(s.bytes) }),
		formatSampleStats(writeCurrent, func(s codecSample) int64 { return int64(s.allocs) }))
	t.Logf("encode streamed median=%s range=%s B/op=%s allocs/op=%s",
		formatSampleStats(writeStreamed, func(s codecSample) int64 { return s.nanos }),
		formatSampleRange(writeStreamed, func(s codecSample) int64 { return s.nanos }),
		formatSampleStats(writeStreamed, func(s codecSample) int64 { return int64(s.bytes) }),
		formatSampleStats(writeStreamed, func(s codecSample) int64 { return int64(s.allocs) }))
	t.Logf("decode current median=%s range=%s B/op=%s allocs/op=%s",
		formatSampleStats(readCurrent, func(s codecSample) int64 { return s.nanos }),
		formatSampleRange(readCurrent, func(s codecSample) int64 { return s.nanos }),
		formatSampleStats(readCurrent, func(s codecSample) int64 { return int64(s.bytes) }),
		formatSampleStats(readCurrent, func(s codecSample) int64 { return int64(s.allocs) }))
	t.Logf("decode streamed median=%s range=%s B/op=%s allocs/op=%s",
		formatSampleStats(readStreamed, func(s codecSample) int64 { return s.nanos }),
		formatSampleRange(readStreamed, func(s codecSample) int64 { return s.nanos }),
		formatSampleStats(readStreamed, func(s codecSample) int64 { return int64(s.bytes) }),
		formatSampleStats(readStreamed, func(s codecSample) int64 { return int64(s.allocs) }))
	t.Logf("consumed encoded_bytes=%d decoded_finding_count=%d", encodedBytesConsumed, decodedFindingsConsumed)
}

type codecSample struct {
	nanos  int64
	bytes  uint64
	allocs uint64
}

type jsonChunkStats struct {
	writes   int
	maxBytes int
}

func measureCodecEncode(t *testing.T, encode func() ([]byte, error)) ([]byte, codecSample) {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	payload, err := encode()
	nanos := time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("encode benchmark input: %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("encode benchmark produced no bytes")
	}
	runtime.KeepAlive(payload)
	return payload, codecSample{nanos: nanos, bytes: after.TotalAlloc - before.TotalAlloc, allocs: after.Mallocs - before.Mallocs}
}

func measureCodecDecode(t *testing.T, decode func() (CachedResponse, error)) (CachedResponse, codecSample) {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	value, err := decode()
	nanos := time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("decode benchmark input: %v", err)
	}
	runtime.KeepAlive(value)
	return value, codecSample{nanos: nanos, bytes: after.TotalAlloc - before.TotalAlloc, allocs: after.Mallocs - before.Mallocs}
}

func encodeCurrentCachedResponse(value CachedResponse) ([]byte, error) {
	plain, err := jsonv1.Marshal(value)
	if err != nil {
		return nil, err
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, err
	}
	frame := encoder.EncodeAll(plain, nil)
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	encoded := make([]byte, responseCacheCodecHeaderSize+len(frame))
	copy(encoded, responseCacheCodecHeader[:])
	copy(encoded[responseCacheCodecHeaderSize:], frame)
	return encoded, nil
}

func decodeCurrentCachedResponse(encoded []byte) (CachedResponse, error) {
	plain := encoded
	if len(encoded) >= responseCacheCodecHeaderSize && bytes.Equal(encoded[:len(responseCacheCodecMagic)], responseCacheCodecMagic[:]) {
		decoder, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(maxResponseCacheValueBytes)),
		)
		if err != nil {
			return CachedResponse{}, err
		}
		plain, err = decoder.DecodeAll(encoded[responseCacheCodecHeaderSize:], nil)
		decoder.Close()
		if err != nil {
			return CachedResponse{}, err
		}
	}
	var value CachedResponse
	if err := jsonv1.Unmarshal(plain, &value); err != nil {
		return CachedResponse{}, err
	}
	return value, nil
}

func recordStreamedJSONChunks(t *testing.T, value CachedResponse) jsonChunkStats {
	t.Helper()
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		t.Fatalf("create chunk recorder encoder: %v", err)
	}
	counter := &jsonChunkWriter{dst: &contextLimitWriter{ctx: context.Background(), dst: encoder, limit: int64(maxResponseCacheValueBytes)}}
	if err := jsonv2.MarshalWrite(counter, value, jsonv1.DefaultOptionsV1()); err != nil {
		t.Fatalf("record streamed JSON chunks: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("close chunk recorder encoder: %v", err)
	}
	return counter.stats
}

type jsonChunkWriter struct {
	dst   io.Writer
	stats jsonChunkStats
}

func (w *jsonChunkWriter) Write(p []byte) (int, error) {
	w.stats.writes++
	if len(p) > w.stats.maxBytes {
		w.stats.maxBytes = len(p)
	}
	return w.dst.Write(p)
}

func formatSampleStats(samples []codecSample, metric func(codecSample) int64) string {
	values := make([]int64, len(samples))
	for i, sample := range samples {
		values[i] = metric(sample)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return fmt.Sprint(values[len(values)/2])
}

func formatSampleRange(samples []codecSample, metric func(codecSample) int64) string {
	values := make([]int64, len(samples))
	for i, sample := range samples {
		values[i] = metric(sample)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return fmt.Sprintf("%d..%d", values[0], values[len(values)-1])
}
