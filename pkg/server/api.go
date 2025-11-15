package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
	"golang.org/x/crypto/bcrypt"
)

func (s *Server) handleGetArrs(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.Arr().GetAll(), http.StatusOK)
}

func (s *Server) handleAddContent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := make([]*manager.ImportRequest, 0)
	errs := make([]string, 0)

	arrName := r.FormValue("arr")
	action := r.FormValue("action")
	debridName := r.FormValue("debrid")
	callbackUrl := r.FormValue("callbackUrl")
	downloadFolder := r.FormValue("downloadFolder")
	if downloadFolder == "" {
		downloadFolder = config.Get().DownloadFolder
	}
	skipMultiSeason := r.FormValue("skipMultiSeason") == "true"

	downloadUncached := r.FormValue("downloadUncached") == "true"
	rmTrackerUrls := r.FormValue("rmTrackerUrls") == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	cfg := config.Get()
	if cfg.AlwaysRmTrackerUrls {
		rmTrackerUrls = true
	}

	_arr := s.manager.Arr().Get(arrName)
	if _arr == nil {
		// These are not found in the config. They are throwaway arrs.
		_arr = arr.New(arrName, "", "", false, false, &downloadUncached, "", "")
	}

	// Handle URLs
	if urls := r.FormValue("urls"); urls != "" {
		var urlList []string
		for _, u := range strings.Split(urls, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				urlList = append(urlList, trimmed)
			}
		}

		for _, url := range urlList {
			magnet, err := utils.GetMagnetFromUrl(url, rmTrackerUrls)
			if err != nil {
				errs = append(errs, fmt.Sprintf("Failed to parse URL %s: %v", url, err))
				continue
			}

			importReq := manager.NewImportRequest(debridName, downloadFolder, magnet, _arr, config.DownloadAction(action), downloadUncached, callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			if err := s.manager.AddNewTorrent(ctx, importReq); err != nil {
				s.logger.Error().Err(err).Str("url", url).Msg("Failed to add torrent")
				errs = append(errs, fmt.Sprintf("URL %s: %v", url, err))
				continue
			}
			results = append(results, importReq)
		}
	}

	// Handle torrent/magnet files
	if files := r.MultipartForm.File["files"]; len(files) > 0 {
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				errs = append(errs, fmt.Sprintf("Failed to open file %s: %v", fileHeader.Filename, err))
				continue
			}

			magnet, err := utils.GetMagnetFromFile(file, fileHeader.Filename, rmTrackerUrls)
			if err != nil {
				errs = append(errs, fmt.Sprintf("Failed to parse torrent file %s: %v", fileHeader.Filename, err))
				continue
			}

			importReq := manager.NewImportRequest(debridName, downloadFolder, magnet, _arr, config.DownloadAction(action), downloadUncached, callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			err = s.manager.AddNewTorrent(ctx, importReq)
			if err != nil {
				s.logger.Error().Err(err).Str("file", fileHeader.Filename).Msg("Failed to add torrent")
				errs = append(errs, fmt.Sprintf("File %s: %v", fileHeader.Filename, err))
				continue
			}
			results = append(results, importReq)
		}
	}

	utils.JSONResponse(w, struct {
		Results []*manager.ImportRequest `json:"results"`
		Errors  []string                 `json:"errors,omitempty"`
	}{
		Results: results,
		Errors:  errs,
	}, http.StatusOK)
}

func (s *Server) handleRepairMedia(w http.ResponseWriter, r *http.Request) {
	var req RepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var arrs []string

	if req.ArrName != "" {
		_arr := s.manager.Arr().Get(req.ArrName)
		if _arr == nil {
			http.Error(w, "No Arrs found to repair", http.StatusNotFound)
			return
		}
		arrs = append(arrs, req.ArrName)
	}

	if err := s.repair.AddJob(arrs, req.MediaIds, req.AutoProcess, false); err != nil {
		http.Error(w, fmt.Sprintf("Failed to repair: %v", err), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, "Repair completed", http.StatusOK)
}

func (s *Server) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	v := version.GetInfo()
	utils.JSONResponse(w, v, http.StatusOK)
}

func (s *Server) handleGetTorrents(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for server-side filtering, sorting, and pagination
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort_by"))
	sortOrder := strings.TrimSpace(r.URL.Query().Get("sort_order"))

	if sortBy == "" {
		sortBy = "added_on"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// GetReader all torrents
	allTorrents := s.manager.Queue().ListFilter("", "", nil, "added_on", false)

	// Apply filters
	filteredTorrents := make([]*storage.Torrent, 0)
	for _, t := range allTorrents {
		// Search filter - search in name and hash
		if search != "" {
			searchIn := strings.ToLower(t.Name + " " + t.InfoHash)
			if !strings.Contains(searchIn, search) {
				continue
			}
		}

		// Category filter
		if category != "" && t.Category != category {
			continue
		}

		// State filter
		if state != "" && t.State != storage.TorrentState(state) {
			continue
		}

		filteredTorrents = append(filteredTorrents, t)
	}

	// Apply sorting
	sortQueuedTorrents(filteredTorrents, sortBy, sortOrder)

	// Calculate pagination
	total := len(filteredTorrents)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	// Apply pagination
	var paginatedTorrents []*storage.Torrent
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paginatedTorrents = filteredTorrents[offset:end]
	} else {
		paginatedTorrents = []*storage.Torrent{}
	}

	// GetReader unique categories
	categorySet := make(map[string]bool)
	for _, t := range allTorrents {
		if t.Category != "" {
			categorySet[t.Category] = true
		}
	}

	categories := make([]string, 0, len(categorySet))
	for c := range categorySet {
		categories = append(categories, c)
	}

	utils.JSONResponse(w, map[string]interface{}{
		"torrents":    paginatedTorrents,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
		"has_prev":    page > 1,
		"has_next":    page < totalPages,
		"categories":  categories,
	}, http.StatusOK)
}

// sortQueuedTorrents sorts torrents based on the given field and order
func sortQueuedTorrents(torrents []*storage.Torrent, sortBy, sortOrder string) {
	if len(torrents) == 0 {
		return
	}

	less := func(i, j int) bool {
		var result bool
		switch sortBy {
		case "name":
			result = strings.ToLower(torrents[i].Name) < strings.ToLower(torrents[j].Name)
		case "size":
			result = torrents[i].Size < torrents[j].Size
		case "added_on":
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		case "progress":
			result = torrents[i].Progress < torrents[j].Progress
		case "category":
			result = strings.ToLower(torrents[i].Category) < strings.ToLower(torrents[j].Category)
		case "state":
			result = torrents[i].State < torrents[j].State
		default:
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		}

		if sortOrder == "desc" {
			return !result
		}
		return result
	}

	// Bubble sort (for small datasets)
	n := len(torrents)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if !less(j, j+1) {
				torrents[j], torrents[j+1] = torrents[j+1], torrents[j]
			}
		}
	}
}

func (s *Server) handleDeleteTorrent(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	var cleanup func(torrent *storage.Torrent) error
	if removeFromDebrid {
		cleanup = func(t *storage.Torrent) error {
			go s.manager.RemoveTorrentPlacements(t)
			return nil
		}
	}

	if err := s.manager.Queue().Delete(hash, cleanup); err != nil {
		s.logger.Error().Err(err).Str("hash", hash).Msg("Failed to delete torrent")
		http.Error(w, "Failed to delete torrent", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteTorrents(w http.ResponseWriter, r *http.Request) {
	hashesStr := r.URL.Query().Get("hashes")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hashesStr == "" {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	hashes := strings.Split(hashesStr, ",")
	var cleanup func(torrent *storage.Torrent) error
	if removeFromDebrid {
		cleanup = func(t *storage.Torrent) error {
			go s.manager.RemoveTorrentPlacements(t)
			return nil
		}
	}
	if err := s.manager.Queue().DeleteWhere("", "", hashes, cleanup); err != nil {
		s.logger.Error().Err(err).Msg("Failed to delete torrents")
		http.Error(w, "Failed to delete torrents", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	arrStorage := s.manager.Arr()
	cfg := config.Get()
	cfg.Arrs = arrStorage.SyncToConfig()

	// Create response with API token info
	type ConfigResponse struct {
		*config.Config
		APIToken     string `json:"api_token,omitempty"`
		AuthUsername string `json:"auth_username,omitempty"`
	}

	response := &ConfigResponse{Config: cfg}

	// AddOrUpdate API token and auth information
	auth := cfg.GetAuth()
	if auth != nil {
		if auth.APIToken != "" {
			response.APIToken = auth.APIToken
		}
		response.AuthUsername = auth.Username
	}

	utils.JSONResponse(w, response, http.StatusOK)
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	// Decode the JSON body
	var updatedConfig config.Config
	if err := json.NewDecoder(r.Body).Decode(&updatedConfig); err != nil {
		s.logger.Error().Err(err).Msg("Failed to decode config update request")
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// GetReader the current configuration
	currentConfig := config.Get()
	setupCompleted := currentConfig.SetupCompleted

	auth := currentConfig.GetAuth()
	currentConfig = &updatedConfig
	currentConfig.Auth = auth

	// Update Arrs through the service
	arrStorage := s.manager.Arr()

	newConfigArrs := make([]config.Arr, 0)
	for _, a := range updatedConfig.Arrs {
		if a.Name == "" || a.Host == "" || a.Token == "" {
			// Skip empty or auto-generated arrs
			continue
		}
		newConfigArrs = append(newConfigArrs, a)
	}
	currentConfig.Arrs = newConfigArrs

	// Sync arrStorage with the new arrs
	arrStorage.SyncFromConfig(currentConfig.Arrs)

	// Preserve setup completed status
	currentConfig.SetupCompleted = setupCompleted

	if err := currentConfig.Save(); err != nil {
		http.Error(w, "Error saving config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	go s.Restart()

	// Return success
	utils.JSONResponse(w, map[string]string{"status": "success"}, http.StatusOK)
}

func (s *Server) handleGetRepairJobs(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.repair.GetJobs(), http.StatusOK)
}

func (s *Server) handleProcessRepairJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No job ID provided", http.StatusBadRequest)
		return
	}
	if err := s.repair.ProcessJob(id); err != nil {
		s.logger.Error().Err(err).Msg("Failed to process repair job")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteRepairJob(w http.ResponseWriter, r *http.Request) {
	// Read ids from body
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.IDs) == 0 {
		http.Error(w, "No job IDs provided", http.StatusBadRequest)
		return
	}

	s.repair.DeleteJobs(req.IDs)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStopRepairJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No job ID provided", http.StatusBadRequest)
		return
	}
	if err := s.repair.StopJob(id); err != nil {
		s.logger.Error().Err(err).Msg("Failed to stop repair job")
		http.Error(w, "Failed to stop job: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRefreshAPIToken(w http.ResponseWriter, _ *http.Request) {
	token, err := s.refreshAPIToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to refresh API token")
		http.Error(w, "Failed to refresh token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]interface{}{
		"token":   token,
		"message": "API token refreshed successfully",
	}, http.StatusOK)
}

func (s *Server) handleUpdateAuth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	auth := cfg.GetAuth()
	if auth == nil {
		auth = &config.Auth{}
	}

	// Check if trying to disable authentication (both empty)
	if req.Username == "" && req.Password == "" {
		// Disable authentication
		cfg.UseAuth = false
		auth.Username = ""
		auth.Password = ""
		if err := cfg.SaveAuth(auth); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save auth config")
			http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
			return
		}
		if err := cfg.Save(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to save config")
			http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
			return
		}

		utils.JSONResponse(w, map[string]string{
			"message": "Authentication disabled successfully",
		}, http.StatusOK)
		return
	}

	// Validate required fields
	if req.Username == "" {
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}
	if req.Password == "" {
		http.Error(w, "Password is required", http.StatusBadRequest)
		return
	}
	if req.Password != req.ConfirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to hash password")
		http.Error(w, "Failed to process password", http.StatusInternalServerError)
		return
	}

	// Update auth settings
	auth.Username = req.Username
	auth.Password = string(hashedPassword)
	cfg.UseAuth = true

	// Save auth config
	if err := cfg.SaveAuth(auth); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save auth config")
		http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
		return
	}

	// Save main config
	if err := cfg.Save(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Failed to save configuration", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]string{
		"message": "Authentication settings updated successfully",
	}, http.StatusOK)
}
