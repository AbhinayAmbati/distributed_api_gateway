package gateway

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// BytesBufferPool implements httputil.BufferPool to recycle buffers and avoid allocations.
type BytesBufferPool struct {
	pool sync.Pool
}

// NewBytesBufferPool initializes a buffer pool with the given buffer size.
func NewBytesBufferPool(size int) *BytesBufferPool {
	return &BytesBufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
	}
}

// Get retrieves a byte slice from the pool.
func (bp *BytesBufferPool) Get() []byte {
	return bp.pool.Get().([]byte)
}

// Put returns a byte slice to the pool.
func (bp *BytesBufferPool) Put(b []byte) {
	// Re-slice to capacity to prevent shrinking
	bp.pool.Put(b[:cap(b)])
}

// MetricsRoundTripper intercepts backend HTTP calls to record performance metrics.
type MetricsRoundTripper struct {
	Transport  http.RoundTripper
	OnComplete func(duration time.Duration, statusCode int, err error)
}

// RoundTrip executes the request, measures latency/errors, and notifies the callback.
func (m *MetricsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	transport := m.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	duration := time.Since(start)

	statusCode := 0
	if resp != nil {
		statusCode = resp.StatusCode
	}

	if m.OnComplete != nil {
		m.OnComplete(duration, statusCode, err)
	}

	return resp, err
}

// NewProxy creates a configured ReverseProxy with recycled buffers and latency-tracking transport.
func NewProxy(targetURL string, bufferSize int, onComplete func(time.Duration, int, error)) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.BufferPool = NewBytesBufferPool(bufferSize)
	
	// Inject custom round tripper
	proxy.Transport = &MetricsRoundTripper{
		Transport:  http.DefaultTransport,
		OnComplete: onComplete,
	}

	return proxy, nil
}
