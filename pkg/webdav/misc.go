package webdav

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// getName: Returns the torrent name and filename from the path
func getName(rootDir, path string) (string, string) {
	path = strings.TrimPrefix(path, rootDir)
	parts := strings.Split(strings.TrimPrefix(path, string(os.PathSeparator)), string(os.PathSeparator))
	if len(parts) < 2 {
		return "", ""
	}
	return parts[1], strings.Join(parts[2:], string(os.PathSeparator)) // Note the change from [0] to [1]
}

func acceptsGzip(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
}

func isValidURL(str string) bool {
	u, err := url.Parse(str)
	// A valid URL should parse without error, and have a non-empty scheme and host.
	return err == nil && u.Scheme != "" && u.Host != ""
}

// Determine TTL based on the requested folder:
//   - If the path is exactly the parent folder (which changes frequently),
//     use a short TTL.
//   - Otherwise, for deeper (torrent folder) paths, use a longer TTL.
func (h *Handler) getCacheTTL(urlPath string) time.Duration {
	if _, ok := h.isParentPath(urlPath); ok {
		return 30 * time.Second // Short TTL for parent folders
	}
	return 2 * time.Minute // Longer TTL for other paths
}

var pctHex = "0123456789ABCDEF"

// fastEscapePath returns a percent-encoded path, preserving '/'
// and only encoding bytes outside the unreserved set:
//
//	ALPHA / DIGIT / '-' / '_' / '.' / '~' / '/'
func fastEscapePath(p string) string {
	var b strings.Builder

	for i := 0; i < len(p); i++ {
		c := p[i]
		// unreserved (plus '/')
		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' ||
			c == '.' || c == '~' ||
			c == '/' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(pctHex[c>>4])
			b.WriteByte(pctHex[c&0xF])
		}
	}
	return b.String()
}
