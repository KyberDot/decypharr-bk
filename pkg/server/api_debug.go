package server

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func (s *Server) handleIngests(w http.ResponseWriter, r *http.Request) {
	ingests, err := s.manager.GetIngests()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to get ingests")
		http.Error(w, "Failed to get ingests: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, ingests, 200)
}

func (s *Server) handleIngestsByDebrid(w http.ResponseWriter, r *http.Request) {
	debridName := chi.URLParam(r, "debrid")
	if debridName == "" {
		http.Error(w, "Debrid name is required", http.StatusBadRequest)
		return
	}
	ingests, err := s.manager.GetIngestsByDebrid(debridName)
	if err != nil {
		s.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to get ingests by debrid")
		http.Error(w, "Failed to get ingests: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, ingests, 200)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// GetReader uptime from manager
	uptime := s.manager.Uptime()
	startTime := s.manager.StartTime()

	stats := map[string]any{
		// Memory stats
		"heap_alloc_mb": fmt.Sprintf("%.2fMB", float64(memStats.Sys)/1024/1024),
		"memory_used":   fmt.Sprintf("%.2fMB", float64(memStats.HeapAlloc)/1024/1024),

		// GC stats
		"gc_cycles": memStats.NumGC,
		// Goroutine stats
		"goroutines": runtime.NumGoroutine(),

		// System info
		"num_cpu": runtime.NumCPU(),

		// OS info
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"go_version": runtime.Version(),

		// Uptime info
		"uptime_seconds": int64(uptime.Seconds()),
		"uptime":         uptime.String(),
		"start_time":     startTime.Format("2006-01-02 15:04:05"),
	}

	// GetReader debrid stats from manager
	debridStats := make([]debridTypes.Stats, 0)
	torrentsCounts, err := s.manager.GetTorrentsCount()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to get torrents count")
		torrentsCounts = 0
	}
	s.manager.Clients().Range(func(debridName string, client debrid.Client) bool {
		if client == nil {
			return true
		}

		debridStat := debridTypes.Stats{}
		libraryStat := debridTypes.LibraryStats{}

		profile, err := client.GetProfile()
		if err != nil {
			s.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to get debrid profile")
			profile = &debridTypes.Profile{
				Name: debridName,
			}
		}
		profile.Name = debridName
		debridStat.Profile = profile

		// GetReader torrent data from manager

		if err == nil {
			libraryStat.Total = torrentsCounts
			libraryStat.ActiveLinks = s.manager.GetTotalActiveDownloadLinks()
		}

		debridStat.Library = libraryStat
		debridStat.Accounts = client.AccountManager().Stats()
		debridStats = append(debridStats, debridStat)
		return true
	})

	// Order debrid stats by index in config
	orderedDebridStats := make([]debridTypes.Stats, 0)
	cfg := config.Get()
	for _, debridCfg := range cfg.Debrids {
		for _, ds := range debridStats {
			if ds.Profile.Name == debridCfg.Name {
				orderedDebridStats = append(orderedDebridStats, ds)
				break
			}
		}
	}

	stats["debrids"] = orderedDebridStats

	// AddOrUpdate mount stats if available (supports rclone, dfs, or external)
	mountManager := s.manager.MountManager()

	if mountManager != nil && mountManager.IsReady() {
		mountStats := mountManager.Stats()
		if mountStats == nil {
			stats["mount"] = map[string]interface{}{
				"error":   fmt.Sprintf("failed to get mount stats: %v", err),
				"type":    mountManager.Type(),
				"ready":   true,
				"enabled": cfg.Mount.Type != config.MountTypeNone,
			}
		} else {
			stats["mount"] = mountStats
		}
	} else {
		// No mount enabled or not ready
		stats["mount"] = map[string]interface{}{
			"ready":   false,
			"enabled": cfg.Mount.Type != config.MountTypeNone,
		}
	}

	utils.JSONResponse(w, stats, http.StatusOK)
}
