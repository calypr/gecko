package httpclient

import (
	"net/http"
	"time"
)

// NewServiceClient returns a default outbound service client with normal Go
// transport behavior, including HTTP/2 when the peer supports it.
func NewServiceClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: newTransport(),
	}
}
