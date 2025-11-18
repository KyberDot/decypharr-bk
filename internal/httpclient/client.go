package httpclient

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/imroc/req/v3"
	"go.uber.org/ratelimit"
)

var DefaultRetryableStatus = map[int]struct{}{
	http.StatusTooManyRequests:     {},
	http.StatusInternalServerError: {},
	http.StatusBadGateway:          {},
	http.StatusServiceUnavailable:  {},
	http.StatusGatewayTimeout:      {},
}

var (
	defaultClient *req.Client
	once          sync.Once
)

type Config struct {
	BaseURL         string
	MaxRetries      int
	Timeout         time.Duration
	RateLimit       ratelimit.Limiter
	Headers         map[string]string
	RetryableStatus map[int]struct{}
	Proxy           string
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxRetries:      3,
		Timeout:         30 * time.Second,
		RateLimit:       ratelimit.NewUnlimited(),
		Headers:         make(map[string]string),
		RetryableStatus: DefaultRetryableStatus,
	}
}

func New(cfg *Config) *req.Client {
	if cfg == nil {
		cfg = NewDefaultConfig()
	}
	client := req.C().
		SetTimeout(cfg.Timeout).
		SetCommonHeaders(cfg.Headers).
		EnableKeepAlives().
		EnableCompression().
		SetCommonRetryCount(cfg.MaxRetries).
		SetCommonRetryBackoffInterval(100*time.Millisecond, 2*time.Second). // AddOrUpdate exponential backoff
		SetCommonRetryCondition(func(resp *req.Response, err error) bool {
			if cfg.RateLimit != nil {
				cfg.RateLimit.Take()
			}
			if err != nil {
				return isRetryableError(err)
			}
			_, shouldRetry := cfg.RetryableStatus[resp.StatusCode]
			return shouldRetry
		}).
		OnBeforeRequest(func(c *req.Client, r *req.Request) error {
			if cfg.RateLimit != nil {
				cfg.RateLimit.Take()
			}
			return nil
		})

	if cfg.Proxy != "" {
		client.SetProxyURL(cfg.Proxy)
	} else {
		client.SetProxy(nil)
	}
	if cfg.BaseURL != "" {
		client.SetBaseURL(cfg.BaseURL)
	}
	return client
}

func isRetryableError(err error) bool {
	errString := err.Error()

	// HTTP/2 specific errors (graceful shutdown, stream errors)
	if strings.Contains(errString, "http2: server sent GOAWAY") ||
		strings.Contains(errString, "http2: Transport received Server's graceful shutdown GOAWAY") ||
		strings.Contains(errString, "http2: stream closed") ||
		strings.Contains(errString, "http2: Transport connection broken") ||
		strings.Contains(errString, "http2: connection error") {
		return true
	}

	// Connection reset and other network errors
	if strings.Contains(errString, "connection reset by peer") ||
		strings.Contains(errString, "read: connection reset") ||
		strings.Contains(errString, "connection refused") ||
		strings.Contains(errString, "network is unreachable") ||
		strings.Contains(errString, "connection timed out") ||
		strings.Contains(errString, "no such host") ||
		strings.Contains(errString, "i/o timeout") ||
		strings.Contains(errString, "unexpected EOF") ||
		strings.Contains(errString, "TLS handshake timeout") ||
		strings.Contains(errString, "EOF") {
		return true
	}

	// Check for net.Error type which can provide more information
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Retry on timeout errors and temporary errors
		return netErr.Timeout()
	}

	// Not a retryable error
	return false
}

func DefaultClient() *req.Client {
	once.Do(func() {
		defaultClient = New(nil)
	})
	return defaultClient
}
