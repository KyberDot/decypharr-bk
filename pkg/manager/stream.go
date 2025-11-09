package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

const (
	MaxNetworkRetries = 5
)

type StreamError struct {
	Err       error
	Retryable bool
	LinkError bool // true if we should try a new link
}

func (e StreamError) Error() string {
	return e.Err.Error()
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

func (m *Manager) Stream(ctx context.Context, torrentName, filename string, start, end int64) (*http.Response, error) {
	var lastErr error
	torrent, err := m.GetTorrentByName(torrentName)
	if err != nil {
		return nil, fmt.Errorf("failed to get torrent: %w", err)
	}

	// First, get the download link
	downloadLink, err := m.GetDownloadLink(torrent, filename)
	if err != nil {
		// If we get here, it means we couldn't get an initial download link
		// Also, we've tried to reinsert the torrent to the current active debrid service, and it failed(check fetchDownloadLink)
		// This likely means the torrent is no longer available on the debrid service
		// We should trigger a torrent mover that tries to move the torrent to another debrid service
		// We should first check if user disabled this
		if m.config.SkipAutoMove {
			return nil, fmt.Errorf("failed to get download link: %w", err)
		}

		// Now we move the torrent to the next debrid service
		m.logger.Info().
			Str("torrent", torrent.GetFolder()).
			Msg("Attempting to move torrent to another debrid service due to failed download link")

		moveErr := m.MoveTorrent(ctx, torrent)
		if moveErr != nil {
			return nil, fmt.Errorf("failed to move torrent after download link failure: %w", moveErr)
		}
		// After moving, try to get the download link again
		// This will try to get the link from the new debrid service
		torrent, err = m.GetTorrentByName(torrentName)
		if err != nil {
			return nil, fmt.Errorf("failed to get torrent after moving: %w", err)
		}
		downloadLink, err = m.GetDownloadLink(torrent, filename)
		if err != nil {
			return nil, fmt.Errorf("failed to get download link after moving torrent: %w", err)
		}
	}

	// Outer loop: Link retries
	for retry := 0; retry < m.config.Retries; retry++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := m.doRequest(ctx, downloadLink.DownloadLink, start, end)
		if err != nil {
			// Network/connection error
			lastErr = err

			// Backoff and continue network retry
			if retry < m.config.Retries {
				backoff := time.Duration(retry+1) * time.Second
				jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
				select {
				case <-time.After(backoff + jitter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			} else {
				return nil, fmt.Errorf("network request failed after retries: %w", lastErr)
			}
		}

		// Got response - check status
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// Reset error counter on success
			return resp, nil
		}

		// Bad status code - handle error
		streamErr := m.handleHTTPError(resp, downloadLink)
		closeErr := resp.Body.Close()
		if closeErr != nil {
			m.logger.Warn().
				Err(closeErr).
				Str("downloadLink", downloadLink.DownloadLink).
				Msg("Failed to close response body")
		}

		if !streamErr.Retryable {
			return nil, streamErr // Fatal error
		}

		if streamErr.LinkError {
			lastErr = streamErr

			// Try new link
			downloadLink, err = m.GetDownloadLink(torrent, filename)
			if err != nil {
				return nil, fmt.Errorf("failed to get download link: %w", err)
			}
			continue
		}

		// Retryable HTTP error (429, 503, 404 etc.) - retry network
		lastErr = streamErr
		m.logger.Trace().
			Err(lastErr).
			Str("downloadLink", downloadLink.DownloadLink).
			Str("link", downloadLink.Link).
			Int("retries", retry).
			Int("statusCode", resp.StatusCode).
			Msg("HTTP error, retrying")

		if retry < MaxNetworkRetries-1 {
			backoff := time.Duration(retry+1) * time.Second
			jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("stream failed after %d link retries: %w", m.config.Retries, lastErr)
}

func (m *Manager) StreamReader(ctx context.Context, torrentName, filename string, start, end int64) (io.ReadCloser, error) {
	resp, err := m.Stream(ctx, torrentName, filename, start, end)
	if err != nil {
		return nil, err
	}

	// Validate we got the expected content
	if resp.ContentLength == 0 {
		resp.Body.Close()
		return nil, fmt.Errorf("received empty response")
	}

	return resp.Body, nil
}

func (m *Manager) doRequest(ctx context.Context, url string, start, end int64) (*http.Response, error) {
	var lastErr error
	// Retry loop specifically for connection-level failures (EOF, reset, etc.)
	for connRetry := 0; connRetry < 3; connRetry++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, StreamError{Err: err, Retryable: false}
		}

		// Set range header
		if start > 0 || end > 0 {
			rangeHeader := fmt.Sprintf("bytes=%d-", start)
			if end > 0 {
				rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
			}
			req.Header.Set("Range", rangeHeader)
		}

		// Set optimized headers for streaming
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
		req.Header.Set("Cache-Control", "no-cache")

		resp, err := m.streamClient.Do(req)
		if err != nil {
			lastErr = err

			// Check if it's a connection error that we should retry
			if isConnectionError(err) && connRetry < 2 {
				// Brief backoff before retrying with fresh connection
				time.Sleep(time.Duration(connRetry+1) * 100 * time.Millisecond)
				continue
			}

			return nil, StreamError{Err: err, Retryable: true}
		}
		return resp, nil
	}

	return nil, StreamError{Err: fmt.Errorf("connection retry exhausted: %w", lastErr), Retryable: true}
}

func (m *Manager) handleHTTPError(resp *http.Response, downloadLink types.DownloadLink) StreamError {
	switch resp.StatusCode {
	case http.StatusNotFound:
		m.MarkLinkAsInvalid(downloadLink, "link_not_found")
		return StreamError{
			Err:       errors.New("download link not found"),
			Retryable: true,
			LinkError: true,
		}

	case http.StatusServiceUnavailable:
		body, _ := io.ReadAll(resp.Body)
		bodyStr := strings.ToLower(string(body))
		if strings.Contains(bodyStr, "bandwidth") || strings.Contains(bodyStr, "traffic") {
			m.MarkLinkAsInvalid(downloadLink, "bandwidth_exceeded")
			return StreamError{
				Err:       errors.New("bandwidth limit exceeded"),
				Retryable: true,
				LinkError: true,
			}
		}
		fallthrough

	case http.StatusTooManyRequests:
		return StreamError{
			Err:       fmt.Errorf("HTTP %d: rate limited", resp.StatusCode),
			Retryable: true,
			LinkError: false,
		}

	default:
		retryable := resp.StatusCode >= 500
		body, _ := io.ReadAll(resp.Body)
		return StreamError{
			Err:       fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body)),
			Retryable: retryable,
			LinkError: false,
		}
	}
}
