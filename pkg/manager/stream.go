package manager

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type StreamError struct {
	Err       error
	Retryable bool
	LinkError bool // true if we should try a new link
}

func (e StreamError) Error() string {
	return e.Err.Error()
}

func (m *Manager) LinkRefresh(torrentName, filename string) (types.DownloadLink, string, error) {
	cacheKey := torrentName + "/" + filename
	emptyDownloadLink := types.DownloadLink{}
	torrent, err := m.GetTorrentByFileName(torrentName, filename)
	if err != nil {
		return emptyDownloadLink, cacheKey, fmt.Errorf("get torrent: %w", err)
	}

	downloadLink, err := m.GetDownloadLink(torrent, filename)
	if err != nil {
		return emptyDownloadLink, cacheKey, fmt.Errorf("get download link: %w", err)
	}
	return downloadLink, cacheKey, nil
}

func (m *Manager) HandleHTTPError(resp *http.Response, downloadLink types.DownloadLink) StreamError {
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
