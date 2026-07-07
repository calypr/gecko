package httpclient

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// NewServiceClient returns a default outbound service client with HTTP/2
// disabled. Gecko mostly talks to in-cluster services over HTTP/1.1, and this
// avoids process-fatal panics in Go's HTTP/2 HPACK encoder under concurrent
// service traffic.
func NewServiceClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	transport.DialContext = (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}
