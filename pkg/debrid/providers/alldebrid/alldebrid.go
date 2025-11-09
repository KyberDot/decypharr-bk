package alldebrid

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/httpclient"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"go.uber.org/ratelimit"
)

type AllDebrid struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *req.Client
	Profile               *types.Profile `json:"profile"`
	logger                zerolog.Logger
	config                config.Debrid
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*AllDebrid, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	_log := logger.New(dc.Name)

	clientConfig := &httpclient.Config{
		BaseURL:    "http://api.alldebrid.com/v4.1",
		Headers:    headers,
		RateLimit:  ratelimits["main"],
		Proxy:      dc.Proxy,
		MaxRetries: cfg.Retries,
		RetryableStatus: map[int]struct{}{
			http.StatusTooManyRequests: {},
			http.StatusBadGateway:      {},
		},
	}

	autoExpiresLinksAfter, err := time.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}
	return &AllDebrid{
		Host:                  "http://api.alldebrid.com/v4.1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                httpclient.New(clientConfig),
		logger:                _log,
		config:                dc,
	}, nil
}

func (ad *AllDebrid) Config() config.Debrid {
	return ad.config
}

func (ad *AllDebrid) Logger() zerolog.Logger {
	return ad.logger
}

func (ad *AllDebrid) IsAvailable(hashes []string) map[string]bool {
	// Check if the infohashes are available in the local cache
	result := make(map[string]bool)

	// Divide hashes into groups of 100
	// AllDebrid does not support checking cached infohashes
	return result
}

func (ad *AllDebrid) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	url := "/magnet/upload"
	var data UploadMagnetResponse

	resp, err := ad.client.R().
		SetQueryParam("magnets[]", torrent.Magnet.Link).
		SetSuccessResult(&data).
		Get(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	magnets := data.Data.Magnets
	if len(magnets) == 0 {
		return nil, fmt.Errorf("error adding torrent. No magnets returned")
	}
	magnet := magnets[0]
	torrentId := strconv.Itoa(magnet.ID)
	torrent.Id = torrentId
	torrent.Added = time.Now().Format(time.RFC3339)

	return torrent, nil
}

func getAlldebridStatus(statusCode int) types.TorrentStatus {
	switch {
	case statusCode == 4:
		return types.TorrentStatusDownloaded
	case statusCode >= 0 && statusCode <= 3:
		return types.TorrentStatusDownloading
	default:
		return types.TorrentStatusError
	}
}

func (ad *AllDebrid) flattenFiles(torrentId string, files []MagnetFile, parentPath string, index *int) map[string]types.File {
	result := make(map[string]types.File)

	cfg := config.Get()

	for _, f := range files {
		currentPath := f.Name
		if parentPath != "" {
			currentPath = filepath.Join(parentPath, f.Name)
		}

		if f.Elements != nil {
			// This is a folder, recurse into it
			subFiles := ad.flattenFiles(torrentId, f.Elements, currentPath, index)
			for k, v := range subFiles {
				if _, ok := result[k]; ok {
					// File already exists, use path as key
					result[v.Path] = v
				} else {
					result[k] = v
				}
			}
		} else {
			// This is a file
			fileName := filepath.Base(f.Name)

			// Skip sample files
			if !ad.config.AddSamples && utils.IsSampleFile(f.Name) {
				continue
			}
			if !cfg.IsAllowedFile(fileName) {
				continue
			}

			if !cfg.IsSizeAllowed(f.Size) {
				continue
			}

			*index++
			file := types.File{
				TorrentId: torrentId,
				Id:        strconv.Itoa(*index),
				Name:      fileName,
				Size:      f.Size,
				Path:      currentPath,
				Link:      f.Link,
			}
			result[file.Name] = file
		}
	}

	return result
}

func (ad *AllDebrid) GetTorrent(torrentId string) (*types.Torrent, error) {
	url := "/magnet/status"
	var res TorrentInfoResponse

	resp, err := ad.client.R().
		SetQueryParam("id", torrentId).
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	data := res.Data.Magnets
	status := getAlldebridStatus(data.StatusCode)
	name := data.Filename
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             name,
		Status:           status,
		Filename:         name,
		OriginalFilename: name,
		Files:            make(map[string]types.File),
		InfoHash:         data.Hash,
		Debrid:           ad.config.Name,
		Added:            time.Unix(data.CompletionDate, 0).Format(time.RFC3339),
	}
	t.Bytes = data.Size
	t.Seeders = data.Seeders
	if status == "downloaded" {
		t.Progress = 100
		index := -1
		files := ad.flattenFiles(t.Id, data.Files, "", &index)
		t.Files = files
	} else {
		t.Progress = float64(data.Downloaded) / float64(data.Size) * 100
		t.Speed = data.DownloadSpeed
	}
	return t, nil
}

func (ad *AllDebrid) UpdateTorrent(t *types.Torrent) error {
	url := "/magnet/status"
	var res TorrentInfoResponse

	resp, err := ad.client.R().
		SetQueryParam("id", t.Id).
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return err
	}

	if !resp.IsSuccessState() {
		return fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	data := res.Data.Magnets
	status := getAlldebridStatus(data.StatusCode)
	name := data.Filename
	t.Name = name
	t.Status = status
	t.Filename = name
	t.OriginalFilename = name
	t.Debrid = ad.config.Name
	t.Bytes = data.Size
	t.Seeders = data.Seeders
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Added = time.Unix(data.CompletionDate, 0).Format(time.RFC3339)
	if status == "downloaded" {
		t.Progress = 100
		index := -1
		files := ad.flattenFiles(t.Id, data.Files, "", &index)
		t.Files = files
	} else {
		t.Progress = float64(data.Downloaded) / float64(data.Size) * 100
		t.Speed = data.DownloadSpeed
	}
	return nil
}

func (ad *AllDebrid) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	for {
		err := ad.UpdateTorrent(torrent)

		if err != nil || torrent == nil {
			return torrent, err
		}
		if torrent.Status == types.TorrentStatusDownloaded {
			ad.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		} else if torrent.Status == types.TorrentStatusDownloading {
			if !torrent.DownloadUncached {
				return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
			}
			// Break out of the loop if the torrent is downloading.
			// This is necessary to prevent infinite loop since we moved to sync downloading and async processing
			return torrent, nil
		} else {
			return torrent, fmt.Errorf("torrent: %s has error", torrent.Name)
		}

	}
}

func (ad *AllDebrid) DeleteTorrent(torrentId string) error {
	url := "/magnet/delete"

	resp, err := ad.client.R().
		SetQueryParam("id", torrentId).
		Get(url)

	if err != nil {
		return err
	}

	if !resp.IsSuccessState() {
		return fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	ad.logger.Info().Msgf("Torrent %s deleted from AD", torrentId)
	return nil
}

func (ad *AllDebrid) GetFileDownloadLinks(t *types.Torrent) (map[string]types.DownloadLink, error) {
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

			link, err := ad.GetDownloadLink(t.Id, &file)
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
			links[link.Link] = link
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

func (ad *AllDebrid) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	url := "/link/unlock"
	var data DownloadLink

	resp, err := ad.client.R().
		SetQueryParam("link", file.Link).
		SetSuccessResult(&data).
		Get(url)

	if err != nil {
		return types.DownloadLink{}, err
	}

	if !resp.IsSuccessState() {
		return types.DownloadLink{}, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	if data.Error != nil {
		return types.DownloadLink{}, fmt.Errorf("error getting download link: %s", data.Error.Message)
	}
	link := data.Data.Link
	if link == "" {
		return types.DownloadLink{}, fmt.Errorf("download link is empty")
	}
	now := time.Now()
	dl := types.DownloadLink{
		Token:        ad.APIKey,
		Link:         file.Link,
		DownloadLink: link,
		Id:           data.Data.Id,
		Size:         file.Size,
		Filename:     file.Name,
		Generated:    now,
		ExpiresAt:    now.Add(ad.autoExpiresLinksAfter),
	}
	// Set the download link in the account
	ad.accountsManager.StoreDownloadLink(dl)
	return dl, nil
}

func (ad *AllDebrid) GetTorrents() ([]*types.Torrent, error) {
	url := "/magnet/status"
	torrents := make([]*types.Torrent, 0)
	var res TorrentsListResponse

	resp, err := ad.client.R().
		SetQueryParam("status", "ready").
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return torrents, err
	}

	if !resp.IsSuccessState() {
		return torrents, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	for _, magnet := range res.Data.Magnets {
		torrents = append(torrents, &types.Torrent{
			Id:               strconv.Itoa(magnet.Id),
			Name:             magnet.Filename,
			Bytes:            magnet.Size,
			Status:           getAlldebridStatus(magnet.StatusCode),
			Filename:         magnet.Filename,
			OriginalFilename: magnet.Filename,
			Files:            make(map[string]types.File),
			InfoHash:         magnet.Hash,
			Debrid:           ad.config.Name,
			Added:            time.Unix(magnet.CompletionDate, 0).Format(time.RFC3339),
		})
	}

	return torrents, nil
}

func (ad *AllDebrid) RefreshDownloadLinks() error {
	return nil
}

func (ad *AllDebrid) CheckLink(link string) error {
	return nil
}

func (ad *AllDebrid) GetAvailableSlots() (int, error) {
	// This function is a placeholder for AllDebrid
	//TODO: Implement the logic to check available slots for AllDebrid
	return 0, fmt.Errorf("GetAvailableSlots not implemented for AllDebrid")
}

func (ad *AllDebrid) GetProfile() (*types.Profile, error) {
	if ad.Profile != nil {
		return ad.Profile, nil
	}
	url := "/user"
	var res UserProfileResponse

	resp, err := ad.client.R().
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	if res.Status != "success" {
		message := "unknown error"
		if res.Error != nil {
			message = res.Error.Message
		}
		return nil, fmt.Errorf("error getting user profile: %s", message)
	}
	userData := res.Data.User
	expiration := time.Unix(userData.PremiumUntil, 0)
	profile := &types.Profile{
		Id:         1,
		Name:       ad.config.Name,
		Username:   userData.Username,
		Email:      userData.Email,
		Points:     userData.FidelityPoints,
		Premium:    userData.PremiumUntil,
		Expiration: expiration,
	}
	if userData.IsPremium {
		profile.Type = "premium"
	} else if userData.IsTrial {
		profile.Type = "trial"
	} else {
		profile.Type = "free"
	}
	ad.Profile = profile
	return profile, nil
}

func (ad *AllDebrid) AccountManager() *account.Manager {
	return ad.accountsManager
}

func (ad *AllDebrid) SyncAccounts() error {
	return nil
}

func (ad *AllDebrid) DeleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	account.DeleteDownloadLink(downloadLink.Link)
	return nil
}
