package debridlink

import (
	"fmt"
	"net/http"
	"strings"
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

type DebridLink struct {
	Host             string `json:"host"`
	APIKey           string
	accountsManager  *account.Manager
	DownloadUncached bool
	client           *req.Client

	autoExpiresLinksAfter time.Duration
	logger                zerolog.Logger
	config                config.Debrid

	Profile *types.Profile `json:"profile,omitempty"`
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*DebridLink, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
		"Content-Type":  "application/json",
	}
	_log := logger.New(dc.Name)

	clientConfig := &httpclient.Config{
		BaseURL:    "https://debrid-link.com/api/v2",
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
	return &DebridLink{
		Host:                  "https://debrid-link.com/api/v2",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		DownloadUncached:      dc.DownloadUncached,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                httpclient.New(clientConfig),
		logger:                _log,
		config:                dc,
	}, nil
}

func (dl *DebridLink) Config() config.Debrid {
	return dl.config
}

func (dl *DebridLink) Logger() zerolog.Logger {
	return dl.logger
}

func (dl *DebridLink) IsAvailable(hashes []string) map[string]bool {
	// Check if the infohashes are available in the local cache
	result := make(map[string]bool)

	// Divide hashes into groups of 100
	for i := 0; i < len(hashes); i += 100 {
		end := i + 100
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

		hashStr := strings.Join(validHashes, ",")
		url := fmt.Sprintf("/seedbox/cached/%s", hashStr)
		var data AvailableResponse

		resp, err := dl.client.R().
			SetSuccessResult(&data).
			Get(url)

		if err != nil || !resp.IsSuccessState() {
			continue
		}
		if data.Value == nil {
			return result
		}
		value := *data.Value
		for _, h := range hashes[i:end] {
			_, exists := value[h]
			if exists {
				result[h] = true
			}
		}
	}
	return result
}

func (dl *DebridLink) GetTorrent(torrentId string) (*types.Torrent, error) {
	url := fmt.Sprintf("/seedbox/%s", torrentId)
	var res torrentInfo

	resp, err := dl.client.R().
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}
	if !res.Success || res.Value == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	data := *res.Value

	if len(data) == 0 {
		return nil, fmt.Errorf("torrent not found")
	}
	t := data[0]
	name := utils.RemoveInvalidChars(t.Name)
	torrent := &types.Torrent{
		Id:               t.ID,
		Name:             name,
		Bytes:            t.TotalSize,
		Status:           "downloaded",
		Filename:         name,
		OriginalFilename: name,
		Debrid:           dl.config.Name,
		Added:            time.Unix(t.Created, 0).Format(time.RFC3339),
	}
	cfg := config.Get()
	for _, f := range t.Files {
		if !cfg.IsSizeAllowed(f.Size) {
			continue
		}
		file := types.File{
			TorrentId: t.ID,
			Id:        f.ID,
			Name:      f.Name,
			Size:      f.Size,
			Path:      f.Name,
			Link:      f.DownloadURL,
		}
		torrent.Files[file.Name] = file
	}

	return torrent, nil
}

func (dl *DebridLink) UpdateTorrent(t *types.Torrent) error {
	url := "/seedbox/list"
	var res torrentInfo

	resp, err := dl.client.R().
		SetQueryParam("ids", t.Id).
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return err
	}

	if !resp.IsSuccessState() {
		return fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}
	if !res.Success {
		return fmt.Errorf("error getting torrent")
	}
	if res.Value == nil {
		return fmt.Errorf("torrent not found")
	}
	dt := *res.Value

	if len(dt) == 0 {
		return fmt.Errorf("torrent not found")
	}
	data := dt[0]
	status := types.TorrentStatusDownloading
	if data.Status == 100 {
		status = types.TorrentStatusDownloaded
	}
	name := utils.RemoveInvalidChars(data.Name)
	t.Id = data.ID
	t.Name = name
	t.Bytes = data.TotalSize
	t.Progress = data.DownloadPercent
	t.Status = status
	t.Speed = data.DownloadSpeed
	t.Seeders = data.PeersConnected
	t.Filename = name
	t.OriginalFilename = name
	if data.HashString != "" {
		t.InfoHash = data.HashString
	}
	t.Added = time.Unix(data.Created, 0).Format(time.RFC3339)
	cfg := config.Get()
	now := time.Now()
	for _, f := range data.Files {
		if !cfg.IsSizeAllowed(f.Size) {
			continue
		}
		file := types.File{
			TorrentId: t.Id,
			Id:        f.ID,
			Name:      f.Name,
			Size:      f.Size,
			Path:      f.Name,
			Link:      f.DownloadURL,
		}
		link := types.DownloadLink{
			Token:        dl.APIKey,
			Filename:     f.Name,
			Link:         f.DownloadURL,
			DownloadLink: f.DownloadURL,
			Generated:    now,
			ExpiresAt:    now.Add(dl.autoExpiresLinksAfter),
		}
		file.DownloadLink = link
		t.Files[f.Name] = file
		dl.accountsManager.StoreDownloadLink(link)
	}

	return nil
}

func (dl *DebridLink) SubmitMagnet(t *types.Torrent) (*types.Torrent, error) {
	url := "/seedbox/add"
	payload := map[string]string{"url": t.Magnet.Link}
	var res SubmitTorrentInfo

	resp, err := dl.client.R().
		SetBodyJsonMarshal(payload).
		SetSuccessResult(&res).
		Post(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}
	if !res.Success || res.Value == nil {
		return nil, fmt.Errorf("error adding torrent")
	}
	data := *res.Value
	name := utils.RemoveInvalidChars(data.Name)
	t.Id = data.ID
	t.Name = name
	t.Bytes = data.TotalSize
	t.Progress = data.DownloadPercent
	t.Status = types.TorrentStatusDownloading
	t.Speed = data.DownloadSpeed
	t.Seeders = data.PeersConnected
	t.Filename = name
	t.OriginalFilename = name
	t.Debrid = dl.config.Name
	t.Added = time.Unix(data.Created, 0).Format(time.RFC3339)
	now := time.Now()
	for _, f := range data.Files {
		file := types.File{
			TorrentId: t.Id,
			Id:        f.ID,
			Name:      f.Name,
			Size:      f.Size,
			Path:      f.Name,
			Link:      f.DownloadURL,
			Generated: now,
		}
		link := types.DownloadLink{
			Token:        dl.APIKey,
			Filename:     f.Name,
			Link:         f.DownloadURL,
			DownloadLink: f.DownloadURL,
			Generated:    now,
			ExpiresAt:    now.Add(dl.autoExpiresLinksAfter),
		}
		file.DownloadLink = link
		t.Files[f.Name] = file
		dl.accountsManager.StoreDownloadLink(link)
	}

	return t, nil
}

func (dl *DebridLink) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	for {
		err := dl.UpdateTorrent(torrent)
		if err != nil || torrent == nil {
			return torrent, err
		}
		if torrent.Status == types.TorrentStatusDownloaded {
			dl.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
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

func (dl *DebridLink) DeleteTorrent(torrentId string) error {
	url := fmt.Sprintf("/seedbox/%s/remove", torrentId)

	resp, err := dl.client.R().Delete(url)
	if err != nil {
		return err
	}

	if !resp.IsSuccessState() {
		return fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}

	dl.logger.Info().Msgf("Torrent: %s deleted from DebridLink", torrentId)
	return nil
}

func (dl *DebridLink) GetFileDownloadLinks(t *types.Torrent) (map[string]types.DownloadLink, error) {
	links := make(map[string]types.DownloadLink)
	for _, file := range t.Files {
		link, err := dl.accountsManager.GetDownloadLink(file.Link)
		if err != nil {
			return links, err
		}
		links[file.Name] = link
	}
	return links, nil
}

func (dl *DebridLink) RefreshDownloadLinks() error {
	return nil
}

func (dl *DebridLink) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return dl.accountsManager.GetDownloadLink(file.Link)
}

func (dl *DebridLink) GetDownloadUncached() bool {
	return dl.DownloadUncached
}

func (dl *DebridLink) GetTorrents() ([]*types.Torrent, error) {
	page := 0
	perPage := 100
	torrents := make([]*types.Torrent, 0)
	for {
		t, err := dl.getTorrents(page, perPage)
		if err != nil {
			break
		}
		if len(t) == 0 {
			break
		}
		torrents = append(torrents, t...)
		page++
	}
	return torrents, nil
}

func (dl *DebridLink) getTorrents(page, perPage int) ([]*types.Torrent, error) {
	url := "/seedbox/list"
	torrents := make([]*types.Torrent, 0)
	var res torrentInfo

	resp, err := dl.client.R().
		SetQueryParam("page", fmt.Sprintf("%d", page)).
		SetQueryParam("perPage", fmt.Sprintf("%d", perPage)).
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return torrents, err
	}

	if !resp.IsSuccessState() {
		return torrents, fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}

	data := *res.Value

	if len(data) == 0 {
		return torrents, nil
	}
	for _, t := range data {
		if t.Status != 100 {
			continue
		}
		torrent := &types.Torrent{
			Id:               t.ID,
			Name:             t.Name,
			Bytes:            t.TotalSize,
			Status:           "downloaded",
			Filename:         t.Name,
			OriginalFilename: t.Name,
			InfoHash:         t.HashString,
			Files:            make(map[string]types.File),
			Debrid:           dl.config.Name,
			Added:            time.Unix(t.Created, 0).Format(time.RFC3339),
		}
		cfg := config.Get()
		now := time.Now()
		for _, f := range t.Files {
			if !cfg.IsSizeAllowed(f.Size) {
				continue
			}
			file := types.File{
				TorrentId: torrent.Id,
				Id:        f.ID,
				Name:      f.Name,
				Size:      f.Size,
				Path:      f.Name,
				Link:      f.DownloadURL,
			}
			link := types.DownloadLink{
				Token:        dl.APIKey,
				Filename:     f.Name,
				Link:         f.DownloadURL,
				DownloadLink: f.DownloadURL,
				Generated:    now,
				ExpiresAt:    now.Add(dl.autoExpiresLinksAfter),
			}
			file.DownloadLink = link
			torrent.Files[f.Name] = file
			dl.accountsManager.StoreDownloadLink(link)
		}
		torrents = append(torrents, torrent)
	}

	return torrents, nil
}

func (dl *DebridLink) CheckLink(link string) error {
	return nil
}

func (dl *DebridLink) GetAvailableSlots() (int, error) {
	//TODO: Implement the logic to check available slots for DebridLink
	return 0, fmt.Errorf("GetAvailableSlots not implemented for DebridLink")
}

func (dl *DebridLink) GetProfile() (*types.Profile, error) {
	if dl.Profile != nil {
		return dl.Profile, nil
	}
	url := "/account/infos"
	var res UserInfo

	resp, err := dl.client.R().
		SetSuccessResult(&res).
		Get(url)

	if err != nil {
		return nil, err
	}

	if !resp.IsSuccessState() {
		return nil, fmt.Errorf("debridlink API error: Status: %d", resp.StatusCode)
	}
	if !res.Success || res.Value == nil {
		return nil, fmt.Errorf("error getting user info")
	}
	data := *res.Value
	expiration := time.Unix(data.PremiumLeft, 0)
	profile := &types.Profile{
		Id:         1,
		Username:   data.Username,
		Name:       dl.config.Name,
		Email:      data.Email,
		Points:     data.Points,
		Premium:    data.PremiumLeft,
		Expiration: expiration,
	}
	if expiration.IsZero() {
		profile.Expiration = time.Now().AddDate(1, 0, 0) // Default to 1 year if no expiration
	}
	if data.PremiumLeft > 0 {
		profile.Type = "premium"
	} else {
		profile.Type = "free"
	}
	dl.Profile = profile
	return profile, nil
}

func (dl *DebridLink) AccountManager() *account.Manager {
	return dl.accountsManager
}

func (dl *DebridLink) SyncAccounts() error {
	return nil
}

func (dl *DebridLink) DeleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	account.DeleteDownloadLink(downloadLink.Link)
	return nil
}
