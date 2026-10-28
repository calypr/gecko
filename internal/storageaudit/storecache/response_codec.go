package storecache

import (
	"bytes"
	"context"
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"

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

	var encoded bytes.Buffer
	encoded.Write(responseCacheCodecHeader[:])
	compressedOutput := &contextLimitWriter{
		ctx:   ctx,
		dst:   &encoded,
		limit: int64(maxResponseCacheValueBytes - responseCacheCodecHeaderSize),
	}
	encoder, err := zstd.NewWriter(compressedOutput,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, fmt.Errorf("create storage audit response cache encoder: %w", err)
	}

	jsonOutput := &contextLimitWriter{
		ctx:   ctx,
		dst:   encoder,
		limit: int64(maxResponseCacheValueBytes),
	}
	marshalErr := jsonv2.MarshalWrite(jsonOutput, value, jsonv1.DefaultOptionsV1())
	closeErr := encoder.Close()
	if marshalErr != nil {
		return nil, fmt.Errorf("marshal storage audit response cache value: %w", marshalErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close storage audit response cache encoder: %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if encoded.Len() > maxResponseCacheValueBytes {
		return nil, errResponseCacheValueTooLarge
	}
	return encoded.Bytes(), nil
}

func decodeCachedResponse(ctx context.Context, encoded []byte) (CachedResponse, error) {
	if err := ctx.Err(); err != nil {
		return CachedResponse{}, err
	}
	if len(encoded) > maxResponseCacheValueBytes {
		return CachedResponse{}, errResponseCacheValueTooLarge
	}

	input := io.Reader(bytes.NewReader(encoded))
	if len(encoded) >= len(responseCacheCodecMagic) && bytes.Equal(encoded[:len(responseCacheCodecMagic)], responseCacheCodecMagic[:]) {
		if len(encoded) < responseCacheCodecHeaderSize {
			return CachedResponse{}, errors.New("storage audit response cache codec header is truncated")
		}
		if version := encoded[len(responseCacheCodecMagic)]; version != responseCacheCodecVersion {
			return CachedResponse{}, fmt.Errorf("unsupported storage audit response cache codec version %d", version)
		}

		decoder, err := zstd.NewReader(bytes.NewReader(encoded[responseCacheCodecHeaderSize:]),
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(maxResponseCacheValueBytes)),
		)
		if err != nil {
			return CachedResponse{}, fmt.Errorf("create storage audit response cache decoder: %w", err)
		}
		defer decoder.Close()
		input = decoder
	} else if len(encoded) < len(responseCacheCodecMagic) && bytes.HasPrefix(responseCacheCodecMagic[:], encoded) {
		return CachedResponse{}, errors.New("storage audit response cache codec header is truncated")
	}

	boundedInput := &contextLimitReader{
		ctx:   ctx,
		src:   input,
		limit: int64(maxResponseCacheValueBytes),
	}
	var value CachedResponse
	if err := jsonv2.UnmarshalRead(boundedInput, &value,
		jsonv1.DefaultOptionsV1(),
		jsonv1.ReportErrorsWithLegacySemantics(false),
	); err != nil {
		return CachedResponse{}, fmt.Errorf("decode storage audit response cache JSON: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return CachedResponse{}, err
	}
	return value, nil
}

type contextLimitWriter struct {
	ctx     context.Context
	dst     io.Writer
	limit   int64
	written int64
}

func (w *contextLimitWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > w.limit-w.written {
		return 0, errResponseCacheValueTooLarge
	}

	n, err := w.dst.Write(p)
	w.written += int64(n)
	if ctxErr := w.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

type contextLimitReader struct {
	ctx   context.Context
	src   io.Reader
	limit int64
	read  int64
}

func (r *contextLimitReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.read == r.limit {
		var probe [1]byte
		n, err := r.src.Read(probe[:])
		if n > 0 {
			return 0, errResponseCacheValueTooLarge
		}
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, err
	}

	if int64(len(p)) > r.limit-r.read {
		p = p[:r.limit-r.read]
	}
	n, err := r.src.Read(p)
	r.read += int64(n)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}
