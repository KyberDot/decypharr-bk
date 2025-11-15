package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

var (
	MaxConnections = 10
)

// Pool manages persistent HTTP connections with retry logic and download link caching
// Key features:
// - Persistent connections (NO TCP/TLS handshake per request!)
// - Automatic retry with exponential backoff
// - Connection-level error handling (EOF, reset, etc.)
// - HTTP error classification (retryable vs fatal)
// - Download link caching with error-based invalidation
// - Link refresh callback on errors
type Pool struct {
	// HTTP client with optimized transport
	client *http.Client

	// Configuration
	maxConnections int
	maxRetries     int

	link        atomic.Value // Stores types.DownloadLink
	manager     *manager.Manager
	torrentName string
	filename    string
}

// NewPool creates a new HTTP connection pool
func NewPool(manager *manager.Manager, torrentName, filename string) *Pool {
	cfg := config.Get()
	// Create optimized HTTP transport for persistent connections
	transport := &http.Transport{
		// Connection pooling settings
		MaxIdleConns:        MaxConnections,
		MaxIdleConnsPerHost: MaxConnections,
		MaxConnsPerHost:     MaxConnections,

		// Keep connections alive
		IdleConnTimeout:   90 * time.Second,
		DisableKeepAlives: false,
		ForceAttemptHTTP2: true,

		// Timeouts
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		// TLS optimization
		TLSClientConfig: &tls.Config{
			// Reuse TLS sessions
			ClientSessionCache: tls.NewLRUClientSessionCache(MaxConnections),
			MinVersion:         tls.VersionTLS12,
		},

		// Performance tuning
		WriteBufferSize: 256 * 1024, // 256KB write buffer
		ReadBufferSize:  256 * 1024, // 256KB read buffer
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   0, // No overall timeout (we handle it per-request)
	}

	return &Pool{
		client:         client,
		maxConnections: MaxConnections,
		maxRetries:     cfg.Retries,
		manager:        manager,
		torrentName:    torrentName,
		filename:       filename,
	}
}

// Get performs an HTTP GET request with Range header using pooled connections
// Implements comprehensive retry logic with link refresh on errors
func (p *Pool) Get(ctx context.Context, start, end int64) (*http.Response, error) {
	var lastErr error

	// GetReader cached or fresh download link
	downloadLink, err := p.getOrRefreshLink(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get download link: %w", err)
	}

	// Outer loop: Link retries
	for linkRetry := 0; linkRetry < p.maxRetries; linkRetry++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Inner loop: Network retries for current link
		resp, err := p.doRequest(ctx, downloadLink.DownloadLink, start, end)
		if err != nil {
			lastErr = err

			// Check if we should try a new link
			if isLinkError(err) {
				p.invalidateLink()

				// GetReader fresh link
				downloadLink, err = p.getOrRefreshLink(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to refresh download link: %w", err)
				}
				continue
			}

			// Network error - retry with backoff
			if linkRetry < p.maxRetries-1 {
				backoff := time.Duration(linkRetry+1) * time.Second
				jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
				select {
				case <-time.After(backoff + jitter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}

			return nil, fmt.Errorf("request failed after %d retries: %w", p.maxRetries, lastErr)
		}

		// Got response - check status
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// Success!
			return resp, nil
		}

		// Bad status code - classify error
		streamErr := p.handleHTTPError(resp, downloadLink)
		_ = resp.Body.Close()

		if !streamErr.Retryable {
			return nil, streamErr // Fatal error
		}

		if streamErr.LinkError {
			// Invalidate current link and get fresh one
			p.invalidateLink()
			downloadLink, err = p.getOrRefreshLink(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to refresh download link: %w", err)
			}

			lastErr = streamErr
			continue
		}

		// Retryable HTTP error (429, 503, etc.) - retry with backoff
		lastErr = streamErr
		if linkRetry < p.maxRetries-1 {
			backoff := time.Duration(linkRetry+1) * time.Second
			jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("stream failed after %d link retries: %w", p.maxRetries, lastErr)
}

// GetReader performs an HTTP GET request with Range header using pooled connections
// Implements comprehensive retry logic with link refresh on errors
func (p *Pool) GetReader(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	var lastErr error

	// GetReader cached or fresh download link
	downloadLink, err := p.getOrRefreshLink(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get download link: %w", err)
	}

	// Outer loop: Link retries
	for linkRetry := 0; linkRetry < p.maxRetries; linkRetry++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Inner loop: Network retries for current link
		resp, err := p.doRequest(ctx, downloadLink.DownloadLink, start, end)
		if err != nil {
			lastErr = err

			// Check if we should try a new link
			if isLinkError(err) {
				p.invalidateLink()

				// GetReader fresh link
				downloadLink, err = p.getOrRefreshLink(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to refresh download link: %w", err)
				}

				continue
			}

			// Network error - retry with backoff
			if linkRetry < p.maxRetries-1 {
				backoff := time.Duration(linkRetry+1) * time.Second
				jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
				select {
				case <-time.After(backoff + jitter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}

			return nil, fmt.Errorf("request failed after %d retries: %w", p.maxRetries, lastErr)
		}

		// Got response - check status
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// Success!
			return resp.Body, nil
		}

		// Bad status code - classify error
		streamErr := p.handleHTTPError(resp, downloadLink)
		_ = resp.Body.Close()

		if !streamErr.Retryable {
			return nil, streamErr // Fatal error
		}

		if streamErr.LinkError {
			// Invalidate current link and get fresh one
			p.invalidateLink()
			downloadLink, err = p.getOrRefreshLink(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to refresh download link: %w", err)
			}

			lastErr = streamErr
			continue
		}

		// Retryable HTTP error (429, 503, etc.) - retry with backoff
		lastErr = streamErr
		if linkRetry < p.maxRetries-1 {
			backoff := time.Duration(linkRetry+1) * time.Second
			jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("stream failed after %d link retries: %w", p.maxRetries, lastErr)
}

// doRequest performs HTTP request with connection-level retry
func (p *Pool) doRequest(ctx context.Context, url string, start, end int64) (*http.Response, error) {
	var lastErr error

	// Connection-level retry (for EOF, reset, etc.)
	for connRetry := 0; connRetry < 3; connRetry++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		// Set Range header
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
		req.Header.Set("Cache-Control", "no-cache")

		// Execute request (REUSES existing connection if available!)
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err

			// Check if it's a connection error that we should retry
			if isConnectionError(err) && connRetry < 2 {

				// Brief backoff before retrying with fresh connection
				time.Sleep(time.Duration(connRetry+1) * 100 * time.Millisecond)
				continue
			}

			return nil, &StreamError{Err: err, Retryable: true}
		}

		return resp, nil
	}

	return nil, &StreamError{
		Err:       fmt.Errorf("connection retry exhausted: %w", lastErr),
		Retryable: true,
	}
}

// getOrRefreshLink gets cached link or fetches fresh one
func (p *Pool) getOrRefreshLink(ctx context.Context) (types.DownloadLink, error) {
	// Try cached link first
	downloadLink, err := p.getLink()
	if err == nil {
		return downloadLink, nil
	}

	// Fetch fresh link via manager callback
	torrent, err := p.manager.GetTorrentByFileName(p.torrentName, p.filename)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("get torrent: %w", err)
	}

	downloadLink, err = p.manager.GetDownloadLink(torrent, p.filename)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("get download link: %w", err)
	}

	// Cache the new link
	p.link.Store(downloadLink)
	return downloadLink, nil
}

// handleHTTPError classifies HTTP errors
func (p *Pool) handleHTTPError(resp *http.Response, downloadLink types.DownloadLink) manager.StreamError {
	p.invalidateLink()
	return p.manager.HandleHTTPError(resp, downloadLink)
}

func (p *Pool) invalidateLink() {
	p.link.Store(nil)
}

func (p *Pool) getLink() (types.DownloadLink, error) {
	val := p.link.Load()
	if val == nil {
		return types.DownloadLink{}, fmt.Errorf("no download link in pool")
	}
	dl, ok := val.(types.DownloadLink)
	if !ok {
		return types.DownloadLink{}, fmt.Errorf("invalid download link type in pool")
	}
	if err := dl.Valid(); err != nil {
		return types.DownloadLink{}, fmt.Errorf("invalid download link in pool: %w", err)
	}
	return dl, nil
}

// isConnectionError checks if the error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common connection errors
	if strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") {
		return true
	}

	// Check for net.Error types
	var netErr net.Error
	return errors.As(err, &netErr)
}

// isLinkError checks if error suggests link is bad
func isLinkError(err error) bool {
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		return streamErr.LinkError
	}
	return false
}

// Close closes all idle connections
func (p *Pool) Close() error {
	if transport, ok := p.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}

// GetStats returns pool statistics
func (p *Pool) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

// StreamError represents a stream error with retry information
type StreamError struct {
	Err       error
	Retryable bool
	LinkError bool // true if we should try a new link
}

func (e *StreamError) Error() string {
	return e.Err.Error()
}

func (e *StreamError) Unwrap() error {
	return e.Err
}
