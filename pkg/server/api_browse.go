package server

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// BrowseEntry represents a file or folder in the browse view
type BrowseEntry struct {
	Name         string `json:"name"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	ModTime      string `json:"mod_time"`
	IsDir        bool   `json:"is_dir"`
	InfoHash     string `json:"info_hash,omitempty"`  // For torrent folders
	CanDelete    bool   `json:"can_delete,omitempty"` // Whether this can be deleted
	ActiveDebrid string `json:"active_debrid"`
}

// BrowseResponse is the response for browse requests
type BrowseResponse struct {
	Entries    []BrowseEntry `json:"entries"`
	Total      int           `json:"total"`
	Page       int           `json:"page"`
	Limit      int           `json:"limit"`
	TotalPages int           `json:"total_pages"`
	CurrentDir string        `json:"current_dir"`
	ParentDir  string        `json:"parent_dir,omitempty"`
}

// handleBrowseRoot returns the root mount points
func (s *Server) handleBrowseRoot(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 50
	}

	children := s.manager.RootEntryChildren()

	// Convert to browse entries
	entries := make([]BrowseEntry, 0, len(children))
	for _, child := range children {
		entries = append(entries, BrowseEntry{
			Name:         child.Name(),
			Path:         "/" + child.Name(),
			Size:         child.Size(),
			ModTime:      child.ModTime().Format("2006-01-02 15:04:05"),
			IsDir:        child.IsDir(),
			ActiveDebrid: child.ActiveDebrid(),
		})
	}

	// Apply pagination
	total := len(entries)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	var paginatedEntries []BrowseEntry
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paginatedEntries = entries[offset:end]
	} else {
		paginatedEntries = []BrowseEntry{}
	}

	utils.JSONResponse(w, BrowseResponse{
		Entries:    paginatedEntries,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
		CurrentDir: "/",
	}, http.StatusOK)
}

// handleBrowseMount returns subdirectories under a mount (__all__, __bad__, etc.)
func (s *Server) handleBrowseMount(w http.ResponseWriter, r *http.Request) {
	mount := chi.URLParam(r, "mount")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 50
	}

	currentInfo := s.manager.GetMountInfo(mount)
	if currentInfo == nil {
		http.Error(w, "Mount not found", http.StatusNotFound)
		return
	}

	children := s.manager.GetEntries()

	// Convert to browse entries
	entries := make([]BrowseEntry, 0, len(children))
	for _, child := range children {
		entries = append(entries, BrowseEntry{
			Name:         child.Name(),
			Path:         "/" + mount + "/" + child.Name(),
			Size:         child.Size(),
			ModTime:      child.ModTime().Format("2006-01-02 15:04:05"),
			IsDir:        child.IsDir(),
			ActiveDebrid: child.ActiveDebrid(),
		})
	}

	// Apply pagination
	total := len(entries)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	var paginatedEntries []BrowseEntry
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paginatedEntries = entries[offset:end]
	} else {
		paginatedEntries = []BrowseEntry{}
	}

	utils.JSONResponse(w, BrowseResponse{
		Entries:    paginatedEntries,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
		CurrentDir: "/" + mount,
		ParentDir:  "/",
	}, http.StatusOK)
}

// handleBrowseGroup returns torrents in a group (__all__, __bad__, custom folder)
func (s *Server) handleBrowseGroup(w http.ResponseWriter, r *http.Request) {
	mount := chi.URLParam(r, "mount")
	group := utils.PathUnescape(chi.URLParam(r, "group"))

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 50
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))

	currentInfo, children := s.manager.GetEntryChildren(group)
	if currentInfo == nil {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	// Convert to browse entries
	entries := make([]BrowseEntry, 0, len(children))
	for _, child := range children {
		// Apply search filter
		if search != "" && !strings.Contains(strings.ToLower(child.Name()), search) {
			continue
		}

		// GetReader torrent info hash for deletion support
		infoHash := ""
		canDelete := false
		if child.IsDir() {
			canDelete = true
		}

		entries = append(entries, BrowseEntry{
			Name:         child.Name(),
			Path:         "/" + mount + "/" + group + "/" + child.Name(),
			Size:         child.Size(),
			ModTime:      child.ModTime().Format("2006-01-02 15:04:05"),
			IsDir:        child.IsDir(),
			InfoHash:     infoHash,
			CanDelete:    canDelete,
			ActiveDebrid: child.ActiveDebrid(),
		})
	}

	// Apply pagination
	total := len(entries)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	var paginatedEntries []BrowseEntry
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paginatedEntries = entries[offset:end]
	} else {
		paginatedEntries = []BrowseEntry{}
	}

	utils.JSONResponse(w, BrowseResponse{
		Entries:    paginatedEntries,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
		CurrentDir: "/" + mount + "/" + group,
		ParentDir:  "/" + mount,
	}, http.StatusOK)
}

// handleBrowseTorrentFiles returns files in a torrent folder
func (s *Server) handleBrowseTorrentFiles(w http.ResponseWriter, r *http.Request) {
	mount := chi.URLParam(r, "mount")
	group := utils.PathUnescape(chi.URLParam(r, "group"))
	torrent := utils.PathUnescape(chi.URLParam(r, "torrent"))

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 50
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))

	currentInfo, children := s.manager.GetTorrentChildren(torrent)
	if currentInfo == nil {
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	// Convert to browse entries
	entries := make([]BrowseEntry, 0, len(children))
	for _, child := range children {
		// Apply search filter
		if search != "" && !strings.Contains(strings.ToLower(child.Name()), search) {
			continue
		}

		pathParts := []string{"/", mount, group}
		pathParts = append(pathParts, torrent, child.Name())

		entries = append(entries, BrowseEntry{
			Name:         child.Name(),
			Path:         filepath.Join(pathParts...),
			Size:         child.Size(),
			ModTime:      child.ModTime().Format("2006-01-02 15:04:05"),
			IsDir:        child.IsDir(),
			ActiveDebrid: child.ActiveDebrid(),
		})
	}

	// Apply pagination
	total := len(entries)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	var paginatedEntries []BrowseEntry
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paginatedEntries = entries[offset:end]
	} else {
		paginatedEntries = []BrowseEntry{}
	}

	parentPath := "/" + mount + "/" + group

	currentPath := parentPath + "/" + torrent

	response := BrowseResponse{
		Entries:    paginatedEntries,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
		CurrentDir: currentPath,
		ParentDir:  parentPath,
	}

	utils.JSONResponse(w, response, http.StatusOK)
}

// handleDeleteBrowseTorrent deletes a torrent by info hash
func (s *Server) handleDeleteBrowseTorrent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Torrent ID is required", http.StatusBadRequest)
		return
	}

	if err := s.manager.DeleteTorrent(id, true); err != nil {
		s.logger.Error().Err(err).Str("id", id).Msg("Failed to delete torrent")
		http.Error(w, "Failed to delete torrent", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]interface{}{
		"success": true,
		"message": "Torrent deleted successfully",
	}, http.StatusOK)
}

// handleBatchDeleteBrowseTorrents deletes multiple torrents
func (s *Server) handleBatchDeleteBrowseTorrents(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []string `json:"ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if len(req.IDs) == 0 {
		http.Error(w, "No torrent IDs provided", http.StatusBadRequest)
		return
	}

	if err := s.manager.DeleteTorrents(req.IDs, true); err != nil {
		s.logger.Error().Err(err).Msg("Failed to delete torrents")
		http.Error(w, "Failed to delete torrents", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]interface{}{
		"success": true,
		"message": "Torrents deleted successfully",
		"count":   len(req.IDs),
	}, http.StatusOK)
}

// handleMoveTorrent initiates a torrent move operation
func (s *Server) handleMoveTorrent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Torrent ID is required", http.StatusBadRequest)
		return
	}

	var req struct {
		TargetDebrid string `json:"target_debrid"`
		KeepSource   bool   `json:"keep_source"`
		WaitComplete bool   `json:"wait_complete"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.TargetDebrid == "" {
		http.Error(w, "Target debrid is required", http.StatusBadRequest)
		return
	}

	job, err := s.manager.SwitchTorrent(r.Context(), id, req.TargetDebrid, req.KeepSource, req.WaitComplete)
	if err != nil {
		s.logger.Error().Err(err).Str("id", id).Msg("Failed to move torrent")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]interface{}{
		"success": true,
		"job":     job,
		"message": "Migration started successfully",
	}, http.StatusOK)
}

// handleGetTorrentInfo returns detailed torrent info for context menu
func (s *Server) handleGetTorrentInfo(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "Torrent ID is required", http.StatusBadRequest)
		return
	}

	torrent, err := s.manager.GetTorrent(id)
	if err != nil {
		s.logger.Error().Err(err).Str("id", id).Msg("Failed to get torrent")
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	// GetReader available debrids for move operation
	debrids := make([]string, 0)
	for debridName := range torrent.Placements {
		if debridName != torrent.ActiveDebrid {
			debrids = append(debrids, debridName)
		}
	}

	utils.JSONResponse(w, map[string]interface{}{
		"info_hash":         torrent.InfoHash,
		"name":              torrent.Name,
		"size":              torrent.Size,
		"active_debrid":     torrent.ActiveDebrid,
		"status":            torrent.Status,
		"available_debrids": debrids,
		"placements":        torrent.Placements,
	}, http.StatusOK)
}

// handleDownloadFile proxies file download
func (s *Server) handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	torrentName := utils.PathUnescape(chi.URLParam(r, "torrent"))
	fileName := utils.PathUnescape(chi.URLParam(r, "file"))

	torrent, err := s.manager.GetTorrentByFileName(torrentName, fileName)
	if err != nil || torrent == nil {
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	file, err := torrent.GetFile(fileName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	etag := fmt.Sprintf("\"%x-%x\"", torrent.AddedOn.Unix(), file.Size)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", torrent.AddedOn.UTC().Format(http.TimeFormat))

	ext := filepath.Ext(file.Name)
	if contentType := mime.TypeByExtension(ext); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	link, err := s.manager.GetDownloadLink(torrent, fileName)
	if err != nil || link.Empty() {
		s.logger.Error().Err(err).Str("torrent", torrent.Name).Str("file", file.Name).Msg("Failed to get download link")
		http.Error(w, "Could not fetch download link", http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", file.Name))
	w.Header().Set("X-Accel-Redirect", link.DownloadLink)
	w.Header().Set("X-Accel-Buffering", "no")
	http.Redirect(w, r, link.DownloadLink, http.StatusFound)
}
