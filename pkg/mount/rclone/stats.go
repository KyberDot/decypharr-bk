package rclone

import (
	"encoding/json"
	"fmt"
)

type TransferringStat struct {
	Bytes    int64   `json:"bytes"`
	ETA      int64   `json:"eta"`
	Name     string  `json:"name"`
	Speed    float64 `json:"speed"`
	Size     int64   `json:"size"`
	Progress float64 `json:"progress"`
}

type VersionResponse struct {
	Arch    string `json:"arch"`
	Version string `json:"version"`
	OS      string `json:"os"`
}

type CoreStatsResponse struct {
	Bytes          int64              `json:"bytes"`
	Checks         int                `json:"checks"`
	DeletedDirs    int                `json:"deletedDirs"`
	Deletes        int                `json:"deletes"`
	ElapsedTime    float64            `json:"elapsedTime"`
	Errors         int                `json:"errors"`
	Eta            int                `json:"eta"`
	Speed          float64            `json:"speed"`
	TotalBytes     int64              `json:"totalBytes"`
	TotalChecks    int                `json:"totalChecks"`
	TotalTransfers int                `json:"totalTransfers"`
	TransferTime   float64            `json:"transferTime"`
	Transfers      int                `json:"transfers"`
	Transferring   []TransferringStat `json:"transferring,omitempty"`
}

type MemoryStats struct {
	Sys        int   `json:"Sys"`
	TotalAlloc int64 `json:"TotalAlloc"`
}

type BandwidthStats struct {
	BytesPerSecond int64  `json:"bytesPerSecond"`
	Rate           string `json:"rate"`
}

// Stats represents rclone statistics
type Stats struct {
	Type      string                `json:"type"`
	Enabled   bool                  `json:"enabled"`
	Ready     bool                  `json:"ready"`
	Core      CoreStatsResponse     `json:"core"`
	Memory    MemoryStats           `json:"memory"`
	Mounts    map[string]*MountInfo `json:"mounts"`
	Bandwidth BandwidthStats        `json:"bandwidth"`
	Version   VersionResponse       `json:"version"`
}

// GetStats retrieves statistics from the rclone RC server
func (m *Manager) GetStats() (map[string]interface{}, error) {
	stats := &Stats{}
	stats.Ready = m.IsReady()
	stats.Enabled = true
	stats.Type = m.Type()

	coreStats, err := m.GetCoreStats()
	if err == nil {
		stats.Core = *coreStats
	}

	// Get memory usage
	memStats, err := m.GetMemoryUsage()
	if err == nil {
		stats.Memory = *memStats
	}
	// Get bandwidth stats
	bwStats, err := m.GetBandwidthStats()
	if err == nil && bwStats != nil {
		stats.Bandwidth = *bwStats
	} else {
		m.logger.Error().Err(err).Msg("Failed to get rclone stats")
	}

	// Get version info
	versionResp, err := m.GetVersion()
	if err == nil {
		stats.Version = *versionResp
	}

	// Convert to map[string]interface{}
	statsMap := make(map[string]interface{})
	data, err := json.Marshal(stats)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal stats: %w", err)
	}
	if err := json.Unmarshal(data, &statsMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stats to map: %w", err)
	}

	return statsMap, nil
}

func (m *Manager) GetCoreStats() (*CoreStatsResponse, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf("rclone RC server not ready")
	}

	req := RCRequest{
		Command: "core/stats",
	}

	resp, err := makeRequest(req, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get core stats: %w", err)
	}
	defer resp.Body.Close()

	var coreStats CoreStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&coreStats); err != nil {
		return nil, fmt.Errorf("failed to decode core stats response: %w", err)
	}
	return &coreStats, nil
}

// GetMemoryUsage returns memory usage statistics
func (m *Manager) GetMemoryUsage() (*MemoryStats, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf("rclone RC server not ready")
	}

	req := RCRequest{
		Command: "core/memstats",
	}

	resp, err := makeRequest(req, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get memory stats: %w", err)
	}
	defer resp.Body.Close()
	var memStats MemoryStats

	if err := json.NewDecoder(resp.Body).Decode(&memStats); err != nil {
		return nil, fmt.Errorf("failed to decode memory stats response: %w", err)
	}
	return &memStats, nil
}

// GetBandwidthStats returns bandwidth usage for all transfers
func (m *Manager) GetBandwidthStats() (*BandwidthStats, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf("rclone RC server not ready")
	}

	req := RCRequest{
		Command: "core/bwlimit",
	}

	resp, err := makeRequest(req, false)
	if err != nil {
		// Bandwidth stats might not be available, return empty
		return nil, nil
	}
	defer resp.Body.Close()
	var bwStats BandwidthStats
	if err := json.NewDecoder(resp.Body).Decode(&bwStats); err != nil {
		return nil, fmt.Errorf("failed to decode bandwidth stats response: %w", err)
	}
	return &bwStats, nil
}

// GetVersion returns rclone version information
func (m *Manager) GetVersion() (*VersionResponse, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf("rclone RC server not ready")
	}

	req := RCRequest{
		Command: "core/version",
	}

	resp, err := makeRequest(req, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	defer resp.Body.Close()
	var versionResp VersionResponse
	if err := json.NewDecoder(resp.Body).Decode(&versionResp); err != nil {
		return nil, fmt.Errorf("failed to decode version response: %w", err)
	}
	return &versionResp, nil
}
