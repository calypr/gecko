package middleware

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

const MaxRequestBodyBytes int64 = 50 * 1024 * 1024

func Recover(logger interface{ Error(string, ...any) }, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := &statusWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered: %v", recovered)
				if response.status == 0 {
					http.Error(response, "Internal Server Error", http.StatusInternalServerError)
				}
			}
		}()
		next.ServeHTTP(response, r)
	})
}

func LimitRequestBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limitedWriter := &bodyLimitWriter{ResponseWriter: w}
		if r.ContentLength > maxBytes {
			limitedWriter.writeTooLarge(maxBytes)
			return
		}
		if r.Body != nil {
			r.Body = &bodyLimitReader{
				ReadCloser: http.MaxBytesReader(limitedWriter, r.Body, maxBytes),
				writer:     limitedWriter,
				maxBytes:   maxBytes,
			}
		}
		next.ServeHTTP(limitedWriter, r)
	})
}

type bodyLimitReader struct {
	io.ReadCloser
	writer   *bodyLimitWriter
	maxBytes int64
}

func (r *bodyLimitReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		r.writer.writeTooLarge(r.maxBytes)
	}
	return n, err
}

type bodyLimitWriter struct {
	http.ResponseWriter
	tooLarge bool
}

func (w *bodyLimitWriter) WriteHeader(status int) {
	if w.tooLarge {
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *bodyLimitWriter) Write(body []byte) (int, error) {
	if w.tooLarge {
		return len(body), nil
	}
	return w.ResponseWriter.Write(body)
}

func (w *bodyLimitWriter) ReadFrom(source io.Reader) (int64, error) {
	if w.tooLarge {
		return io.Copy(io.Discard, source)
	}
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return readerFrom.ReadFrom(source)
	}
	return io.Copy(writerOnly{Writer: w}, source)
}

func (w *bodyLimitWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *bodyLimitWriter) Flush() {
	if w.tooLarge {
		return
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *bodyLimitWriter) writeTooLarge(maxBytes int64) {
	if w.tooLarge {
		return
	}
	w.tooLarge = true
	w.Header().Set("Content-Type", "application/json")
	w.ResponseWriter.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = fmt.Fprintf(w.ResponseWriter,
		`{"error":{"type":"request_too_large","message":"request body exceeds %d bytes","code":%d}}`+"\n",
		maxBytes,
		http.StatusRequestEntityTooLarge,
	)
}
