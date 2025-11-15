package manager

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type Downloader struct {
	manager     *Manager
	strmURL     string
	mountPath   string
	dest        string
	logger      zerolog.Logger
	downloadSem chan struct{}
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:     manager,
		strmURL:     strmURL,
		mountPath:   cfg.Mount.MountPath,
		logger:      manager.logger.With().Str("component", "downloader").Logger(),
		dest:        cfg.DownloadFolder,
		downloadSem: make(chan struct{}, cfg.MaxDownloads),
	}
}

func (d *Downloader) download(torrent *storage.Torrent) error {
	var (
		isMultiSeason bool
		seasons       []SeasonInfo
		err           error
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons, err = d.detectMultiSeason(torrent)
		if err != nil {
			d.logger.Warn().Msgf("Error detecting multi-season for %s: %v", torrent.Name, err)
			// Continue with normal processing if detection fails
			isMultiSeason = false
		}
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {

		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			// Then process the symlinks for each season torrent
			if err := d.process(result, torrentMountPath); err != nil {
				d.markAsError(result, err)
			}
		}
	}
	return d.process(torrent, torrentMountPath)
}

func (d *Downloader) process(torrent *storage.Torrent, mountPath string) error {
	timer := time.Now()
	defer func() {
		// Log how long it took to process
		d.logger.Info().Str("action", string(torrent.Action)).Str("debrid", torrent.ActiveDebrid).Msgf("Processed torrent %s in %s", torrent.Name, time.Since(timer))
	}()
	switch torrent.Action {
	case config.DownloadActionDownload:
		return d.processDownload(torrent)
	case config.DownloadActionSymlink:
		return d.processSymlink(torrent, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(torrent)
	default:
		return d.processSymlink(torrent, mountPath)
	}
}

func (d *Downloader) markAsCompleted(torrent *storage.Torrent) {
	// Mark as completed
	torrent.MarkAsCompleted(torrent.SymlinkPath())
	_ = d.manager.queue.Update(torrent)

	// Send success notification
	go func() {
		msg := fmt.Sprintf("Download completed: %s [%s] -> %s", torrent.Name, torrent.Category, torrent.SymlinkPath())
		_ = d.manager.SendDiscordMessage("download_complete", "success", msg)
	}()

	// Send callback if configured
	if torrent.CallbackURL != "" {
		go d.manager.sendCallback(torrent.CallbackURL, torrent, "completed", nil)
	}

	// Trigger arr refresh
	go func() {
		a := d.manager.arr.GetOrCreate(torrent.Category)
		a.Refresh()
	}()
}

func (d *Downloader) markAsError(torrent *storage.Torrent, err error) {
	d.logger.Error().Err(err).Str("name", torrent.Name).Msg("Failed to process action")
	torrent.MarkAsError(err)
	_ = d.manager.queue.Update(torrent)

	// Send error notification
	go func() {
		msg := fmt.Sprintf("Download failed: %s [%s] - %s", torrent.Name, torrent.Category, err.Error())
		_ = d.manager.SendDiscordMessage("download_failed", "error", msg)
	}()

	// Send callback if configured
	if torrent.CallbackURL != "" {
		go d.manager.sendCallback(torrent.CallbackURL, torrent, "failed", err)
	}
}

// processSymlink creates symlinks for torrent files
func (d *Downloader) processSymlink(torrent *storage.Torrent, mountPath string) error {
	files := torrent.GetActiveFiles()
	torrentSymlinkPath := torrent.SymlinkPath()
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	// Track pending files
	remainingFiles := make(map[string]*storage.File)
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(30 * time.Minute)
	filePaths := make([]string, 0, len(remainingFiles))

	var checkDirectory func(string) // Recursive function
	checkDirectory = func(dirPath string) {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return
		}

		for _, entry := range entries {
			entryName := entry.Name()
			fullPath := filepath.Join(dirPath, entryName)

			// Check if this matches a remaining file
			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(torrentSymlinkPath, file.Name)

				if err := os.Symlink(fullPath, fileSymlinkPath); err == nil || os.IsExist(err) {
					filePaths = append(filePaths, fileSymlinkPath)
					delete(remainingFiles, entryName)
					d.logger.Info().Msgf("File is ready: %s", file.Name)
				}
			} else if entry.IsDir() {
				// If not found and it's a directory, check inside
				checkDirectory(fullPath)
			}
		}
	}

	for len(remainingFiles) > 0 {
		select {
		case <-ticker.C:
			checkDirectory(mountPath)

		case <-timeout:
			return fmt.Errorf("timeout waiting for files: %d files still pending", len(remainingFiles))
		}
	}
	d.markAsCompleted(torrent)

	// Pre-cache files if enabled
	if !d.manager.config.SkipPreCache && len(filePaths) > 0 {
		go func() {
			d.logger.Debug().Msgf("Pre-caching %s", torrent.Name)
			if err := utils.PreCacheFile(filePaths); err != nil {
				d.logger.Error().Msgf("Failed to pre-cache file: %s", err)
			} else {
				d.logger.Debug().Msgf("Pre-cached %d files", len(filePaths))
			}
		}()
	}

	return nil
}

// processDownload downloads all files for a torrent with progress tracking
func (d *Downloader) processDownload(torrent *storage.Torrent) error {
	var wg sync.WaitGroup
	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d files...", len(files))

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	downloadedFolder := torrent.SymlinkPath()
	err := os.MkdirAll(downloadedFolder, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}
	torrent.SizeDownloaded = 0 // Reset downloaded bytes
	torrent.Progress = 0       // Reset progress

	progressCallback := func(downloaded int64, speed int64) {
		var mu sync.Mutex
		mu.Lock()
		defer mu.Unlock()

		// Update total downloaded bytes
		torrent.SizeDownloaded += downloaded
		torrent.Speed = speed

		// Calculate overall progress
		if totalSize > 0 {
			torrent.Progress = float64(torrent.SizeDownloaded) / float64(totalSize) * 100
		}

		// Update torrent progress
		torrent.Progress = torrent.Progress / 100.0
		torrent.Speed = speed
		torrent.UpdatedAt = time.Now()
		_ = d.manager.queue.Update(torrent)
	}

	errChan := make(chan error, len(files))
	for _, file := range files {
		downloadLink, err := d.manager.GetDownloadLink(torrent, file.Name)
		if err != nil {
			d.logger.Error().Msgf("Failed to get download link for %s: %v", file.Name, err)
			continue
		}
		client := &grab.Client{
			UserAgent: "Decypharr[QBitTorrent]",
			HTTPClient: &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
				},
			},
		}
		wg.Add(1)
		d.downloadSem <- struct{}{}
		go func(file *storage.File) {
			defer wg.Done()
			defer func() { <-d.downloadSem }()
			filename := file.Name

			err := d.localDownloader(
				client,
				downloadLink.DownloadLink,
				filepath.Join(downloadedFolder, filename),
				file.ByteRange,
				progressCallback,
			)

			if err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", filename, err)
				errChan <- err
			} else {
				d.logger.Info().Msgf("Downloaded %s", filename)
			}
		}(file)
	}
	wg.Wait()

	close(errChan)
	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("errors occurred during download")
	}
	d.markAsCompleted(torrent)
	d.logger.Info().Msgf("Downloaded all files for %s", torrent.Name)
	return nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(torrent *storage.Torrent) error {

	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	torrentSymlinkPath := torrent.SymlinkPath()

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	for _, file := range files {
		strmFilePath := filepath.Join(torrentSymlinkPath, file.Name+".strm")
		streamURL, err := url.JoinPath(
			d.strmURL,
			"webdav",
			"stream",
			EntryAllFolder,
			url.PathEscape(torrent.GetFolder()),
			url.PathEscape(file.Name),
		)
		if err != nil {
			continue
		}
		if err := os.WriteFile(strmFilePath, []byte(streamURL), 0644); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.markAsCompleted(torrent)
	d.logger.Info().Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) detectMultiSeason(torrent *storage.Torrent) (bool, []SeasonInfo, error) {
	torrentName := torrent.Name
	files := torrent.GetActiveFiles()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons, nil
}

// grabber downloads a file with progress callback
func (d *Downloader) localDownloader(client *grab.Client, url, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	req, err := grab.NewRequest(filename, url)
	req.NoCreateDirectories = true
	if err != nil {
		return err
	}

	// Set byte range if specified
	if byterange != nil {
		byterangeStr := fmt.Sprintf("%d-%d", byterange[0], byterange[1])
		req.HTTPRequest.Header.Set("Range", "bytes="+byterangeStr)
	}

	resp := client.Do(req)

	t := time.NewTicker(time.Second * 2)
	defer t.Stop()

	var lastReported int64
Loop:
	for {
		select {
		case <-t.C:
			current := resp.BytesComplete()
			speed := int64(resp.BytesPerSecond())
			if current != lastReported {
				if progressCallback != nil {
					progressCallback(current-lastReported, speed)
				}
				lastReported = current
			}
		case <-resp.Done:
			break Loop
		}
	}

	// Report final bytes
	if progressCallback != nil {
		progressCallback(resp.BytesComplete()-lastReported, 0)
	}

	return resp.Err()
}
