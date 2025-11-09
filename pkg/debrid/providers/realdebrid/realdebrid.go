package realdebrid

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/imroc/req/v3"
	"github.com/sirrobot01/decypharr/internal/httpclient"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/common/rar"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"go.uber.org/ratelimit"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
)

type RealDebrid struct {
	Host string `json:"host"`

	APIKey                string
	accountsManager       *account.Manager
	client                *req.Client
	repairClient          *req.Client
	autoExpiresLinksAfter time.Duration
	logger                zerolog.Logger

	rarSemaphore chan struct{}
	Profile      *types.Profile
	config       config.Debrid
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*RealDebrid, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	_log := logger.New(dc.Name)

	autoExpiresLinksAfter, err := time.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}
	mainClientConfig := &httpclient.Config{
		BaseURL:    "https://api.real-debrid.com/rest/1.0",
		Headers:    headers,
		RateLimit:  ratelimits["main"],
		Proxy:      dc.Proxy,
		MaxRetries: cfg.Retries,
		RetryableStatus: map[int]struct{}{
			http.StatusTooManyRequests: {},
			http.StatusBadGateway:      {},
		},
	}
	repairClientConfig := &httpclient.Config{
		BaseURL:    "https://api.real-debrid.com/rest/1.0",
		Headers:    headers,
		RateLimit:  ratelimits["repair"],
		Proxy:      dc.Proxy,
		MaxRetries: cfg.Retries,
		RetryableStatus: map[int]struct{}{
			http.StatusTooManyRequests: {},
			http.StatusBadGateway:      {},
		},
	}

	r := &RealDebrid{
		Host:                  "https://api.real-debrid.com/rest/1.0",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                httpclient.New(mainClientConfig),
		repairClient:          httpclient.New(repairClientConfig),
		logger:                logger.New(dc.Name),
		rarSemaphore:          make(chan struct{}, 2),
		config:                dc,
	}

	go func() {
		_, err = r.GetProfile()
		if err != nil {
			r.logger.Error().Err(err).Msg("Failed to get RealDebrid profile")
		}
	}()
	return r, nil
}

func (r *RealDebrid) Logger() zerolog.Logger {
	return r.logger
}

func (r *RealDebrid) getSelectedFiles(t *types.Torrent, data torrentInfo) (map[string]types.File, error) {
	files := make(map[string]types.File)
	selectedFiles := make([]types.File, 0)

	for _, f := range data.Files {
		if f.Selected == 1 {
			selectedFiles = append(selectedFiles, types.File{
				TorrentId: t.Id,
				Name:      filepath.Base(f.Path),
				Path:      filepath.Base(f.Path),
				Size:      f.Bytes,
				Id:        strconv.Itoa(f.ID),
			})
		}
	}

	if len(selectedFiles) == 0 {
		return files, nil
	}

	// Handle RARed torrents (single link, multiple files)
	if len(data.Links) == 1 && len(selectedFiles) > 1 {
		return r.handleRarArchive(t, data, selectedFiles)
	}

	// Standard case - map files to links
	if len(selectedFiles) > len(data.Links) {
		r.logger.Warn().Msgf("More files than links available: %d files, %d links for %s", len(selectedFiles), len(data.Links), t.Name)
	}

	for i, f := range selectedFiles {
		if i < len(data.Links) {
			f.Link = data.Links[i]
			files[f.Name] = f
		} else {
			r.logger.Warn().Str("file", f.Name).Msg("No link available for file")
		}
	}

	return files, nil
}

func (r *RealDebrid) handleRarFallback(t *types.Torrent, data torrentInfo) (map[string]types.File, error) {
	files := make(map[string]types.File)
	file := types.File{
		TorrentId: t.Id,
		Id:        "0",
		Name:      t.Name + ".rar",
		Size:      data.Bytes,
		IsRar:     true,
		ByteRange: nil,
		Path:      t.Name + ".rar",
		Link:      data.Links[0],
		Generated: time.Now(),
	}
	files[file.Name] = file
	return files, nil
}

// handleRarArchive processes RAR archives with multiple files
func (r *RealDebrid) handleRarArchive(t *types.Torrent, data torrentInfo, selectedFiles []types.File) (map[string]types.File, error) {
	// This will block if 2 RAR operations are already in progress
	r.rarSemaphore <- struct{}{}
	defer func() {
		<-r.rarSemaphore
	}()

	files := make(map[string]types.File)

	if !r.config.UnpackRar {
		r.logger.Debug().Msgf("RAR file detected, but unpacking is disabled: %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	r.logger.Info().Msgf("RAR file detected, unpacking: %s", t.Name)
	linkFile := &types.File{TorrentId: t.Id, Link: data.Links[0]}
	downloadLinkObj, err := r.GetDownloadLink(t.Id, linkFile)

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error getting download link for RAR file: %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	dlLink := downloadLinkObj.DownloadLink
	reader, err := rar.NewReader(dlLink)

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error creating RAR reader for %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	rarFiles, err := reader.GetFiles()

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error reading RAR files for %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	// Create lookup map for faster matching
	fileMap := make(map[string]*types.File)
	for i := range selectedFiles {
		// RD converts special chars to '_' for RAR file paths
		// @TODO: there might be more special chars to replace
		safeName := strings.NewReplacer("|", "_", "\"", "_", "\\", "_", "?", "_", "*", "_", ":", "_", "<", "_", ">", "_").Replace(selectedFiles[i].Name)
		fileMap[safeName] = &selectedFiles[i]
	}

	now := time.Now()

	for _, rarFile := range rarFiles {
		if file, exists := fileMap[rarFile.Name()]; exists {
			file.IsRar = true
			file.ByteRange = rarFile.ByteRange()
			file.Link = data.Links[0]
			file.Generated = now
			files[file.Name] = *file
		} else if !rarFile.IsDirectory {
			r.logger.Warn().Msgf("RAR file %s not found in torrent files", rarFile.Name())
		}
	}
	if len(files) == 0 {
		r.logger.Warn().Msgf("No valid files found in RAR archive for torrent: %s", t.Name)
		return r.handleRarFallback(t, data)
	}
	r.logger.Info().Msgf("Unpacked RAR archive for torrent: %s with %d files", t.Name, len(files))
	return files, nil
}

// getTorrentFiles returns a list of torrent files from the torrent info
// validate is used to determine if the files should be validated
// if validate is false, selected files will be returned
func (r *RealDebrid) getTorrentFiles(t *types.Torrent, data torrentInfo) map[string]types.File {
	files := make(map[string]types.File)
	cfg := config.Get()
	idx := 0

	for _, f := range data.Files {
		name := filepath.Base(f.Path)
		if !r.config.AddSamples && utils.IsSampleFile(f.Path) {
			// Skip sample files
			continue
		}

		if !cfg.IsAllowedFile(name) {
			continue
		}
		if !cfg.IsSizeAllowed(f.Bytes) {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Name:      name,
			Path:      name,
			Size:      f.Bytes,
			Id:        strconv.Itoa(f.ID),
		}
		files[name] = file
		idx++
	}
	return files
}

func (r *RealDebrid) IsAvailable(hashes []string) map[string]bool {
	// Check if the infohashes are available in the local cache
	result := make(map[string]bool)

	// Divide hashes into groups of 100
	for i := 0; i < len(hashes); i += 200 {
		end := i + 200
		if end > len(hashes) {
			end = len(hashes)
		}

		// Filter out empty strings
		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash != "" {
				validHashes = append(validHashes, hash)
			}
		}

		// If no valid hashes in this batch, continue to the next batch
		if len(validHashes) == 0 {
			continue
		}

		hashStr := strings.Join(validHashes, "/")
		var data AvailabilityResponse
		resp, err := r.client.R().
			SetSuccessResult(&data).
			Get(fmt.Sprintf("/torrents/instantAvailability/%s", hashStr))

		if err != nil {
			r.logger.Error().Err(err).Msg("Error checking availability")
			continue
		}

		if resp.IsSuccessState() {
			for _, h := range hashes[i:end] {
				hosters, exists := data[strings.ToLower(h)]
				if exists && len(hosters.Rd) > 0 {
					result[h] = true
				}
			}
		}
	}
	return result
}

func (r *RealDebrid) SubmitMagnet(t *types.Torrent) (*types.Torrent, error) {
	if t.Magnet.IsTorrent() {
		return r.addTorrent(t)
	}
	return r.addMagnet(t)
}

func (r *RealDebrid) addTorrent(t *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetSchema

	resp, err := r.client.R().
		SetHeader("Content-Type", "application/x-bittorrent").
		SetBody(t.Magnet.File).
		SetSuccessResult(&data).
		Put("/torrents/addTorrent")

	if err != nil {
		return nil, err
	}

	// Handle status codes
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		// Handle multiple_downloads
		if resp.StatusCode == 509 {
			return nil, utils.TooManyActiveDownloadsError
		}
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	t.Id = data.Id
	t.Debrid = r.config.Name
	t.Added = time.Now().Format(time.RFC3339)

	return t, nil
}

func (r *RealDebrid) addMagnet(t *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetSchema
	var errorResp map[string]interface{}

	resp, err := r.client.R().
		SetFormData(map[string]string{
			"magnet": t.Magnet.Link,
		}).
		SetSuccessResult(&data).
		SetErrorResult(&errorResp).
		Post("/torrents/addMagnet")

	if err != nil {
		return nil, err
	}

	// Handle status codes
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		t.Id = data.Id
		t.Debrid = r.config.Name
		t.Added = time.Now().Format(time.RFC3339)

		return t, nil

	case 509:
		return nil, utils.TooManyActiveDownloadsError

	default:
		// Get error body (limited to 1024 bytes like original)
		errorBody := resp.String()
		if len(errorBody) > 1024 {
			errorBody = errorBody[:1024]
		}
		return nil, fmt.Errorf("realdebrid API error: Status: %d || Body: %s",
			resp.StatusCode, errorBody)
	}
}

func (r *RealDebrid) GetTorrent(torrentId string) (*types.Torrent, error) {
	var data torrentInfo
	var errorResp map[string]interface{}

	resp, err := r.client.R().
		SetPathParam("torrentId", torrentId). // Alternative: use path params
		SetSuccessResult(&data).
		SetErrorResult(&errorResp).
		Get(fmt.Sprintf("/torrents/info/%s", torrentId))

	if err != nil {
		return nil, err
	}

	// Handle status codes
	switch resp.StatusCode {
	case http.StatusOK:
		t := &types.Torrent{
			Id:               data.ID,
			Name:             data.Filename,
			Bytes:            data.Bytes,
			Progress:         data.Progress,
			Speed:            data.Speed,
			Seeders:          data.Seeders,
			Added:            data.Added,
			Status:           types.TorrentStatus(data.Status),
			Filename:         data.Filename,
			OriginalFilename: data.OriginalFilename,
			Links:            data.Links,
			Debrid:           r.config.Name,
		}

		t.Files = r.getTorrentFiles(t, data) // Get selected files
		return t, nil
	case http.StatusNotFound:
		return nil, utils.TorrentNotFoundError

	default:
		// Get error body (limited to 1024 bytes like original)
		errorBody := resp.String()
		if len(errorBody) > 1024 {
			errorBody = errorBody[:1024]
		}
		return nil, fmt.Errorf("realdebrid API error: Status: %d || Body: %s",
			resp.StatusCode, errorBody)
	}
}

func (r *RealDebrid) GetDownloadingStatus() []string {
	return []string{"downloading", "magnet_conversion", "queued", "compressing", "uploading"}
}

func getStatus(status string) types.TorrentStatus {
	switch status {
	case "downloading", "magnet_conversion", "queued", "compressing", "uploading", "waiting_files_selection":
		return types.TorrentStatusDownloading
	case "downloaded":
		return types.TorrentStatusDownloaded
	default:
		return types.TorrentStatusError
	}
}

func (r *RealDebrid) UpdateTorrent(t *types.Torrent) error {
	var data torrentInfo
	var errorResp map[string]interface{}

	resp, err := r.client.R().
		SetSuccessResult(&data).
		SetErrorResult(&errorResp).
		Get(fmt.Sprintf("/torrents/info/%s", t.Id))

	if err != nil {
		return err
	}

	// Handle status codes
	switch resp.StatusCode {
	case http.StatusOK:
		t.Name = data.Filename
		t.Bytes = data.Bytes
		t.Progress = data.Progress
		t.Status = types.TorrentStatus(data.Status)
		t.Speed = data.Speed
		t.Seeders = data.Seeders
		t.Status = getStatus(data.Status)
		t.Filename = data.Filename
		t.OriginalFilename = data.OriginalFilename
		t.Links = data.Links
		t.Debrid = r.config.Name
		t.Files, _ = r.getSelectedFiles(t, data) // Get selected files

		return nil

	case http.StatusNotFound:
		return utils.TorrentNotFoundError

	default:
		// Get error body (limited to 1024 bytes like original)
		errorBody := resp.String()
		if len(errorBody) > 1024 {
			errorBody = errorBody[:1024]
		}
		return fmt.Errorf("realdebrid API error: Status: %d || Body: %s",
			resp.StatusCode, errorBody)
	}
}

func (r *RealDebrid) CheckStatus(t *types.Torrent) (*types.Torrent, error) {
	for {
		var data torrentInfo
		var errorResp map[string]interface{}

		resp, err := r.client.R().
			SetSuccessResult(&data).
			SetErrorResult(&errorResp).
			Get(fmt.Sprintf("/torrents/info/%s", t.Id))

		if err != nil {
			r.logger.Info().Msgf("ERROR Checking file: %v", err)
			return t, err
		}

		if resp.StatusCode != http.StatusOK {
			return t, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
		}

		debridStatus := data.Status
		t.Name = data.Filename // Important because some magnet changes the name
		t.Filename = data.Filename
		t.OriginalFilename = data.OriginalFilename
		t.Bytes = data.Bytes
		t.Progress = data.Progress

		t.Speed = data.Speed
		t.Seeders = data.Seeders
		t.Links = data.Links
		t.Status = getStatus(debridStatus)
		t.Debrid = r.config.Name
		t.Added = data.Added
		if data.Hash != "" {
			t.InfoHash = data.Hash
		}
		if debridStatus == "waiting_files_selection" {
			t.Status = types.TorrentStatusDownloading
			t.Files = r.getTorrentFiles(t, data)
			if len(t.Files) == 0 {
				return t, fmt.Errorf("no valid files found")
			}
			filesId := make([]string, 0)
			for _, f := range t.Files {
				filesId = append(filesId, f.Id)
			}

			selectURL := fmt.Sprintf("/torrents/selectFiles/%s", t.Id)
			selectResp, err := r.client.R().
				SetFormData(map[string]string{
					"files": strings.Join(filesId, ","),
				}).
				Post(selectURL)

			if err != nil {
				return t, err
			}

			if selectResp.StatusCode != http.StatusNoContent {
				if selectResp.StatusCode == 509 {
					return nil, utils.TooManyActiveDownloadsError
				}
				return t, fmt.Errorf("realdebrid API error: Status: %d", selectResp.StatusCode)
			}
		} else if debridStatus == "downloaded" {
			t.Status = types.TorrentStatusDownloaded
			t.Files, err = r.getSelectedFiles(t, data) // Get selected files
			if err != nil {
				return t, err
			}

			r.logger.Info().Msgf("Torrent: %s downloaded to RD", t.Name)
			return t, nil
		} else if t.Status == types.TorrentStatusDownloading {
			if !t.DownloadUncached {
				return t, fmt.Errorf("torrent: %s not cached", t.Name)
			}
			return t, nil
		} else {
			return t, fmt.Errorf("torrent: %s has error: %s", t.Name, debridStatus)
		}

	}
}

func (r *RealDebrid) DeleteTorrent(torrentId string) error {
	resp, err := r.client.R().Delete(fmt.Sprintf("/torrents/delete/%s", torrentId))
	if err != nil {
		return err
	}
	if !resp.IsSuccessState() {
		return fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
	r.logger.Info().Msgf("Torrent: %s deleted from RD", torrentId)
	return nil
}

func (r *RealDebrid) GetFileDownloadLinks(t *types.Torrent) (map[string]types.DownloadLink, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	files := make(map[string]types.File)
	links := make(map[string]types.DownloadLink)

	_files := t.GetFiles()
	wg.Add(len(_files))

	for _, f := range _files {
		go func(file types.File) {
			defer wg.Done()

			link, err := r.GetDownloadLink(t.Id, &file)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if link.Empty() {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("realdebrid API error: download link not found for file %s", file.Name)
				}
				mu.Unlock()
				return
			}

			file.DownloadLink = link
			mu.Lock()
			files[file.Name] = file
			links[file.Name] = link
			mu.Unlock()
		}(f)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	// AddOrUpdate links to cache
	t.Files = files
	return links, nil
}

func (r *RealDebrid) CheckLink(link string) error {
	resp, err := r.repairClient.R().
		SetFormData(map[string]string{
			"link": link,
		}).
		Post("/unrestrict/check")

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return utils.HosterUnavailableError // File has been removed
	}

	return nil
}

func (r *RealDebrid) getDownloadLink(account *account.Account, file *types.File) (types.DownloadLink, error) {
	emptyLink := types.DownloadLink{}
	link := file.Link
	if strings.HasPrefix(file.Link, "https://real-debrid.com/d/") && len(file.Link) > 39 {
		link = file.Link[0:39]
	}
	payload := url.Values{}
	payload.Add("link", link)
	var errResp ErrorResponse
	var data UnrestrictResponse
	resp, err := account.Client().R().
		SetFormDataFromValues(payload).
		SetErrorResult(&errResp).
		SetSuccessResult(&data).
		Post(fmt.Sprintf("%s/unrestrict/link/", r.Host))

	if err != nil {
		return emptyLink, err
	}
	if resp.StatusCode != http.StatusOK {
		// Read the response body to get the error message
		switch errResp.ErrorCode {
		case 19, 24, 35:
			return emptyLink, utils.HosterUnavailableError // File has been removed
		case 23, 34, 36:
			return emptyLink, utils.TrafficExceededError
		default:
			return emptyLink, fmt.Errorf("realdebrid API error: Status: %d || Code: %d", resp.StatusCode, errResp.ErrorCode)
		}
	}
	if data.Download == "" {
		return emptyLink, fmt.Errorf("realdebrid API error: download link not found")
	}
	now := time.Now()
	dl := types.DownloadLink{
		Token:        account.Token,
		Filename:     data.Filename,
		Size:         data.Filesize,
		Link:         data.Link,
		DownloadLink: data.Download,
		Generated:    now,
		ExpiresAt:    now.Add(r.autoExpiresLinksAfter),
	}

	// Store the link in the account
	account.StoreDownloadLink(dl)
	return dl, nil
}

func (r *RealDebrid) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	accounts := r.accountsManager.Active()
	for _, _account := range accounts {
		downloadLink, err := r.getDownloadLink(_account, file)
		if err == nil {
			return downloadLink, nil
		}

		retries := 0
		if errors.Is(err, utils.TrafficExceededError) {
			// Retries generating
			retries = 5
		} else {
			// If the error is not traffic exceeded, return the error
			return downloadLink, err
		}
		backOff := 1 * time.Second
		for retries > 0 {
			downloadLink, err = r.getDownloadLink(_account, file)
			if err == nil {
				return downloadLink, nil
			}
			if !errors.Is(err, utils.TrafficExceededError) {
				return downloadLink, err
			}
			// AddOrUpdate a delay before retrying
			time.Sleep(backOff)
			backOff *= 2 // Exponential backoff
			retries--
		}
	}
	return types.DownloadLink{}, fmt.Errorf("realdebrid API error: used all active accounts")
}

func (r *RealDebrid) getTorrents(offset int, limit int) (int, []*types.Torrent, error) {
	torrents := make([]*types.Torrent, 0)
	request := r.client.R()

	var data []TorrentsResponse
	request.SetSuccessResult(&data)
	if offset > 0 {
		request.SetQueryParam("offset", fmt.Sprintf("%d", offset))
	}
	if limit > 0 {
		request.SetQueryParam("limit", fmt.Sprintf("%d", limit))
	}
	resp, err := request.Get("/torrents")

	if err != nil {
		return 0, torrents, err
	}

	if resp.StatusCode == http.StatusNoContent {
		return 0, torrents, nil
	}

	if resp.StatusCode != http.StatusOK {
		return 0, torrents, fmt.Errorf("realdebrid API error: %d", resp.StatusCode)
	}

	totalItems, _ := strconv.Atoi(resp.Header.Get("X-Total-Count"))
	filenames := map[string]struct{}{}
	for _, t := range data {
		if t.Status != "downloaded" {
			continue
		}
		torrents = append(torrents, &types.Torrent{
			Id:               t.Id,
			Name:             t.Filename,
			Bytes:            t.Bytes,
			Progress:         t.Progress,
			Status:           types.TorrentStatusDownloaded,
			Filename:         t.Filename,
			OriginalFilename: t.Filename,
			Links:            t.Links,
			Files:            make(map[string]types.File),
			InfoHash:         t.Hash,
			Debrid:           r.config.Name,
			Added:            t.Added.Format(time.RFC3339),
		})
		filenames[t.Filename] = struct{}{}
	}
	return totalItems, torrents, nil
}

func (r *RealDebrid) GetTorrents() ([]*types.Torrent, error) {
	limit := 5000
	if r.config.Limit != 0 {
		limit = r.config.Limit
	}
	hardLimit := r.config.Limit

	// Get first batch and total count
	allTorrents := make([]*types.Torrent, 0)
	var fetchError error
	offset := 0
	for {
		// Fetch next batch of torrents
		_, torrents, err := r.getTorrents(offset, limit)
		if err != nil {
			fetchError = err
			break
		}
		totalTorrents := len(torrents)
		if totalTorrents == 0 {
			break
		}
		allTorrents = append(allTorrents, torrents...)
		offset += totalTorrents
		if hardLimit != 0 && len(allTorrents) >= hardLimit {
			// If hard limit is set, stop fetching more torrents
			break
		}
	}

	if fetchError != nil {
		return nil, fetchError
	}

	return allTorrents, nil
}

func (r *RealDebrid) RefreshDownloadLinks() error {
	accounts := r.accountsManager.All()

	for _, _account := range accounts {
		if _account == nil || _account.Token == "" {
			continue
		}
		offset := 0
		limit := 1000
		links := make(map[string]*types.DownloadLink)
		for {
			dl, err := r.getDownloadLinks(_account, offset, limit)
			if err != nil {
				break
			}
			if len(dl) == 0 {
				break
			}

			for _, d := range dl {
				if _, exists := links[d.Link]; exists {
					// This is ordered by date, so we can skip the rest
					continue
				}
				links[d.Link] = &d
			}

			offset += len(dl)
		}
		_account.StoreDownloadLinks(links)
	}
	return nil
}

func (r *RealDebrid) getDownloadLinks(account *account.Account, offset int, limit int) ([]types.DownloadLink, error) {
	var data []DownloadsResponse
	request := account.Client().R()
	request.SetSuccessResult(&data)
	request.SetQueryParam("limit", fmt.Sprintf("%d", limit))
	if offset > 0 {
		request.SetQueryParam("offset", fmt.Sprintf("%d", offset))
	}
	resp, err := request.Get(fmt.Sprintf("%s/downloads", r.Host))
	if err != nil {
		return nil, err
	}
	if resp.IsErrorState() {
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
	links := make([]types.DownloadLink, 0)
	for _, d := range data {
		links = append(links, types.DownloadLink{
			Token:        account.Token,
			Filename:     d.Filename,
			Size:         d.Filesize,
			Link:         d.Link,
			DownloadLink: d.Download,
			Generated:    d.Generated,
			ExpiresAt:    d.Generated.Add(r.autoExpiresLinksAfter),
			Id:           d.Id,
		})

	}
	return links, nil
}

func (r *RealDebrid) Config() config.Debrid {
	return r.config
}

func (r *RealDebrid) GetProfile() (*types.Profile, error) {
	if r.Profile != nil {
		return r.Profile, nil
	}
	var data profileResponse

	resp, err := r.client.R().
		SetSuccessResult(&data).
		Get("/user")

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}

	profile := &types.Profile{
		Name:       r.config.Name,
		Id:         data.Id,
		Username:   data.Username,
		Email:      data.Email,
		Points:     data.Points,
		Premium:    data.Premium,
		Expiration: data.Expiration,
		Type:       data.Type,
	}
	r.Profile = profile
	return profile, nil
}

func (r *RealDebrid) GetAvailableSlots() (int, error) {
	var data AvailableSlotsResponse

	resp, err := r.client.R().
		SetSuccessResult(&data).
		Get("/torrents/activeCount")

	if err != nil {
		return 0, err
	}

	if !resp.IsSuccessState() {
		return 0, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}

	return data.TotalSlots - data.ActiveSlots - r.config.MinimumFreeSlot, nil // Ensure we maintain minimum active pots
}

func (r *RealDebrid) AccountManager() *account.Manager {
	return r.accountsManager
}

func (r *RealDebrid) SyncAccounts() error {
	// Sync accounts with the current configuration
	if len(r.accountsManager.Active()) == 0 {
		return nil
	}
	for _, _account := range r.accountsManager.All() {
		if err := r.syncAccount(_account); err != nil {
			r.logger.Error().Err(err).Msgf("Error syncing account %s", _account.Username)
			continue // Skip this account and continue with the next
		}
	}
	return nil
}

func (r *RealDebrid) syncAccount(account *account.Account) error {
	if account.Token == "" {
		return fmt.Errorf("account %s has no token", account.Username)
	}
	var profile profileResponse
	resp, err := account.Client().R().
		SetSuccessResult(&profile).
		Get(fmt.Sprintf("%s/user", r.Host))
	if err != nil {
		return fmt.Errorf("error checking account %s: %w", account.Username, err)
	}
	if resp.IsErrorState() {
		return fmt.Errorf("error checking account %s, status code: %d", account.Username, resp.StatusCode)
	}
	account.Username = profile.Username

	var trafficData TrafficResponse
	trafficResp, err := account.Client().R().
		SetSuccessResult(&trafficData).
		Get(fmt.Sprintf("%s/traffic/details", r.Host))
	if err != nil {
		return fmt.Errorf("error checking traffic for account %s: %w", account.Username, err)
	}
	if trafficResp.StatusCode != http.StatusOK {
		return fmt.Errorf("error checking traffic for account %s, status code: %d", account.Username, trafficResp.StatusCode)
	}

	if len(trafficData) == 0 {
		// Skip logging traffic error
		account.TrafficUsed.Store(0)
	} else {
		today := time.Now().Format(time.DateOnly)
		if todayData, exists := trafficData[today]; exists {
			account.TrafficUsed.Store(todayData.Bytes)
		}
	}
	return nil
}

func (r *RealDebrid) DeleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	resp, err := account.Client().R().
		Delete(fmt.Sprintf("%s/downloads/delete/%s", r.Host, downloadLink.Id))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("realdebrid API error: %d", resp.StatusCode)
	}
	account.DeleteDownloadLink(downloadLink.Link)
	return nil
}
