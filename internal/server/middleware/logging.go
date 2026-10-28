package middleware

import (
	"io"
	"net"
	"net/http"
	"time"

	"github.com/uc-cdis/arborist/arborist"
)

func RequestLogger(logger arborist.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		response := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(response, r)

		status := response.status
		if status == 0 {
			status = http.StatusOK
		}
		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		logger.Info(
			"%s %s - Status: %d - Latency: %s - Host: %s - IP: %s - Path: %s - Query: %s - Route: %s - Params: %v - RequestID: %s",
			r.Method,
			r.URL.RequestURI(),
			status,
			time.Since(start),
			r.Host,
			clientIP(r.RemoteAddr),
			r.URL.Path,
			r.URL.RawQuery,
			route,
			pathParams(r),
			r.Header.Get("X-Request-Id"),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusWriter) ReadFrom(source io.Reader) (int64, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return readerFrom.ReadFrom(source)
	}
	return io.Copy(writerOnly{Writer: w}, source)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

type writerOnly struct {
	io.Writer
}

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func pathParams(r *http.Request) map[string]string {
	params := make(map[string]string)
	for _, name := range []string{"configType", "configId", "orgTitle", "projectTitle", "projectId", "collection", "id", "path"} {
		if value := r.PathValue(name); value != "" {
			params[name] = value
		}
	}
	if len(params) == 0 {
		return nil
	}
	return params
}
