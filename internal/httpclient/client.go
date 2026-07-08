package httpclient

import (
	"net"
	"net/http"
	"time"
)

// NewServiceClient returns a default outbound service client with normal Go
// transport behavior, including HTTP/2 when the peer supports it.
func NewServiceClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}
