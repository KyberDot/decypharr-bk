package webdav

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"path/filepath"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

func (h *Handler) handlePropfind(current *manager.FileInfo, children []manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	sb := convertToXML(cleanPath, current, children)
	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func (h *Handler) handleGet(current *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if current.IsDir() {
		http.Error(w, "Bad Request: Cannot GET a directory", http.StatusBadRequest)
		return
	}
	h.handleDownload(current, w, r)
}

func (h *Handler) handleDelete(current *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	if err := h.manager.RemoveEntry(current); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent) // 204 No Content
}

func (h *Handler) handleHead(entry *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", getContentType(entry.Name()))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", entry.Size()))
	w.Header().Set("Last-Modified", entry.ModTime().UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCopy(current *manager.FileInfo, w http.ResponseWriter, r *http.Request, delete bool) {
	destHeader := r.Header.Get("Destination")
	if destHeader == "" {
		http.Error(w, "Bad Request: Missing Destination header", http.StatusBadRequest)
		return
	}
	destPath := path.Clean(destHeader)
	err := h.manager.CopyEntry(current, destPath, delete)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated) // 201 Created
}

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND")
	w.Header().Set("DAV", "1, 2")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDownload(info *manager.FileInfo, w http.ResponseWriter, r *http.Request) {
	etag := fmt.Sprintf("\"%x-%x\"", info.ModTime().Unix(), info.Size())
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))

	ext := filepath.Ext(info.Name())
	if contentType := mime.TypeByExtension(ext); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	if !info.IsRemote() {
		// Write .Content disposition for local files
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", utils.PathUnescape(info.Name())))
		_, _ = w.Write(info.Content())
		return

	}

	if err := h.StreamResponse(info, w, r); err != nil {
		var streamErr *streamError
		if errors.As(err, &streamErr) {
			// Handle client disconnections silently (just debug log)
			if errors.Is(streamErr.Err, context.Canceled) || errors.Is(streamErr.Err, context.DeadlineExceeded) || streamErr.IsClientDisconnection {
				return
			}
			if streamErr.StatusCode > 0 {
				h.logger.Trace().Err(err).Msgf("Error streaming %s/%s", info.Parent(), info.Name())
				http.Error(w, streamErr.Error(), streamErr.StatusCode)
				return
			} else {
				// We've already written a status code, just log the error
				h.logger.Error().Err(streamErr.Err).Msg("Streaming error")
				return
			}
		} else {
			// Generic error
			h.logger.Error().Err(err).Msgf("Error streaming file %s", info.Name())
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

}
