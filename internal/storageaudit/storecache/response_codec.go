package storecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	responseCacheCodecVersion    byte = 1
	responseCacheCodecHeaderSize      = 4
	maxResponseCacheValueBytes        = 512 << 20
)

var responseCacheCodecMagic = [3]byte{'G', 'C', 'R'}
var responseCacheCodecHeader = [responseCacheCodecHeaderSize]byte{'G', 'C', 'R', responseCacheCodecVersion}

var errResponseCacheValueTooLarge = errors.New("storage audit response cache value exceeds its size limit")

func encodeCachedResponse(ctx context.Context, value CachedResponse) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	jsonValue, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal storage audit response cache value: %w", err)
	}
	return encodeResponseCacheJSON(ctx, jsonValue, maxResponseCacheValueBytes)
}

func encodeResponseCacheJSON(ctx context.Context, jsonValue []byte, maxValueBytes int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxValueBytes <= responseCacheCodecHeaderSize || len(jsonValue) > maxValueBytes {
		return nil, errResponseCacheValueTooLarge
	}

	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, fmt.Errorf("create storage audit response cache encoder: %w", err)
	}
	compressed := encoder.EncodeAll(jsonValue, nil)
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("close storage audit response cache encoder: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(compressed)+responseCacheCodecHeaderSize > maxValueBytes {
		return nil, errResponseCacheValueTooLarge
	}

	encoded := make([]byte, responseCacheCodecHeaderSize+len(compressed))
	copy(encoded[:responseCacheCodecHeaderSize], responseCacheCodecHeader[:])
	copy(encoded[responseCacheCodecHeaderSize:], compressed)
	return encoded, nil
}

func decodeCachedResponse(ctx context.Context, encoded []byte) (CachedResponse, error) {
	if err := ctx.Err(); err != nil {
		return CachedResponse{}, err
	}
	if len(encoded) > maxResponseCacheValueBytes {
		return CachedResponse{}, errResponseCacheValueTooLarge
	}

	jsonValue := encoded
	if len(encoded) >= len(responseCacheCodecMagic) && bytes.Equal(encoded[:len(responseCacheCodecMagic)], responseCacheCodecMagic[:]) {
		if len(encoded) < responseCacheCodecHeaderSize {
			return CachedResponse{}, errors.New("storage audit response cache codec header is truncated")
		}
		if version := encoded[len(responseCacheCodecMagic)]; version != responseCacheCodecVersion {
			return CachedResponse{}, fmt.Errorf("unsupported storage audit response cache codec version %d", version)
		}
		decoded, err := decodeResponseCacheFrame(encoded[responseCacheCodecHeaderSize:], maxResponseCacheValueBytes)
		if err != nil {
			return CachedResponse{}, err
		}
		jsonValue = decoded
	} else if len(encoded) < len(responseCacheCodecMagic) && bytes.HasPrefix(responseCacheCodecMagic[:], encoded) {
		return CachedResponse{}, errors.New("storage audit response cache codec header is truncated")
	}

	var value CachedResponse
	if err := json.Unmarshal(jsonValue, &value); err != nil {
		return CachedResponse{}, fmt.Errorf("decode storage audit response cache JSON: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return CachedResponse{}, err
	}
	return value, nil
}

func decodeResponseCacheFrame(frame []byte, maxDecodedBytes int) ([]byte, error) {
	if maxDecodedBytes <= 0 {
		return nil, errors.New("storage audit response cache decoded size limit must be positive")
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxDecodedBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("create storage audit response cache decoder: %w", err)
	}
	defer decoder.Close()

	decoded, err := decoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress storage audit response cache value: %w", err)
	}
	if len(decoded) > maxDecodedBytes {
		return nil, errResponseCacheValueTooLarge
	}
	return decoded, nil
}
