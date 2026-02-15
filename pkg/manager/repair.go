package manager

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type RepairManager interface {
	Run(ctx context.Context)
	AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) (string, error)
	StopJob(id string) error
	ProcessJob(id string) error
	DeleteJobs(ids []string)
	GetJobs() []*storage.Job
	Stop()
}

type FileProbeStatus string

const (
	FileProbeHealthy FileProbeStatus = "healthy"
	FileProbeBroken  FileProbeStatus = "broken"
	FileProbeUnknown FileProbeStatus = "unknown"
)

type FileProbeResult struct {
	Name     string          `json:"name"`
	InfoHash string          `json:"info_hash"`
	Protocol config.Protocol `json:"protocol"`
	Status   FileProbeStatus `json:"status"`
	Reason   string          `json:"reason,omitempty"`
}

func (m *Manager) ProbeEntryFiles(ctx context.Context, item *storage.EntryItem, filenames []string) []FileProbeResult {
	if item == nil {
		return nil
	}

	if len(item.Files) == 0 {
		results := make([]FileProbeResult, 0, len(filenames))
		for _, name := range filenames {
			results = append(results, FileProbeResult{Name: name, Status: FileProbeUnknown, Reason: "missing_entry_item"})
		}
		return results
	}

	cfg := config.Get()
	files := make(map[string]*storage.File)

	if len(filenames) > 0 {
		for _, name := range filenames {
			if f, ok := item.Files[name]; ok {
				files[name] = f
			}
		}
	} else {
		files = item.Files
	}

	if len(files) == 0 {
		return nil
	}
	results := make(map[string]FileProbeResult, len(files))

	for name, file := range files {
		select {
		case <-ctx.Done():
			return flattenProbeResults(results, files)
		default:
		}

		entry, err := m.GetEntryByName(item.Name, name)
		if err != nil {
			results[name] = FileProbeResult{
				Name:     name,
				InfoHash: file.InfoHash,
				Status:   FileProbeUnknown,
				Reason:   "entry_not_found",
			}
			continue
		}

		result := FileProbeResult{
			Name:     name,
			InfoHash: file.InfoHash,
			Protocol: entry.Protocol,
			Status:   FileProbeHealthy,
		}

		if entry.IsNZB() && cfg.Usenet.SkipRepair {
			result.Status = FileProbeUnknown
			result.Reason = "usenet_repair_disabled"
			results[name] = result
			continue
		}

		if entry.IsNZB() {
			if m.usenet == nil {
				result.Status = FileProbeUnknown
				result.Reason = "usenet_client_not_configured"
				results[name] = result
				continue
			}

			if err := m.usenet.CheckFile(ctx, entry.InfoHash, file.Name); err != nil {
				if errors.Is(err, customerror.UsenetSegmentMissingError) {
					result.Status = FileProbeBroken
					result.Reason = "usenet_segment_missing"
				} else {
					result.Status = FileProbeUnknown
					result.Reason = "usenet_probe_error"
				}
			}
			results[name] = result
			continue
		}

		client := m.ProviderClient(entry.ActiveProvider)
		if client == nil {
			result.Status = FileProbeUnknown
			result.Reason = "provider_client_not_found"
			results[name] = result
			continue
		}
		if !client.SupportsCheck() {
			result.Status = FileProbeUnknown
			result.Reason = "provider_check_unsupported"
			results[name] = result
			continue
		}

		placement := entry.GetActiveProvider()
		if placement == nil || placement.Files == nil {
			result.Status = FileProbeBroken
			result.Reason = "missing_active_placement"
			results[name] = result
			continue
		}

		placementFile, exist := placement.Files[name]
		if !exist {
			result.Status = FileProbeBroken
			result.Reason = "missing_provider_file_link"
			results[name] = result
			continue
		}

		link := cmp.Or(placementFile.Link, placementFile.Id)
		if link == "" {
			fmt.Println("Probe skipped for file", name, ": empty provider link")
			result.Status = FileProbeBroken
			result.Reason = "empty_provider_link"
			results[name] = result
			continue
		}

		if err := client.CheckFile(ctx, file.InfoHash, link); err != nil {
			if errors.Is(err, customerror.HosterUnavailableError) {
				result.Status = FileProbeBroken
				result.Reason = "hoster_unavailable"
			} else {
				result.Status = FileProbeUnknown
				result.Reason = "provider_probe_error"
			}
		}
		fmt.Println("Probe result for file", name, ":", result.Status, "-", result.Reason)
		results[name] = result
	}

	if cfg.Repair.Strategy == config.RepairStrategyPerTorrent {
		brokenInfohashes := make(map[string]struct{})
		for _, result := range results {
			if result.Status == FileProbeBroken {
				brokenInfohashes[result.InfoHash] = struct{}{}
			}
		}
		if len(brokenInfohashes) > 0 {
			for name, result := range results {
				if _, broken := brokenInfohashes[result.InfoHash]; broken {
					result.Status = FileProbeBroken
					if result.Reason == "" {
						result.Reason = "torrent_wide_failure"
					}
					results[name] = result
				}
			}
		}
	}

	return flattenProbeResults(results, files)
}

func flattenProbeResults(results map[string]FileProbeResult, files map[string]*storage.File) []FileProbeResult {
	out := make([]FileProbeResult, 0, len(files))
	for name, file := range files {
		result, ok := results[name]
		if !ok {
			result = FileProbeResult{
				Name:     name,
				InfoHash: file.InfoHash,
				Status:   FileProbeUnknown,
				Reason:   "not_probed",
			}
		}
		if result.Name == "" {
			result.Name = name
		}
		out = append(out, result)
	}
	return out
}

func (m *Manager) GetBrokenFiles(item *storage.EntryItem, filenames []string) []string {
	results := m.ProbeEntryFiles(context.Background(), item, filenames)
	brokenSet := make(map[string]struct{})
	for _, result := range results {
		if result.Status == FileProbeBroken {
			brokenSet[result.Name] = struct{}{}
		}
	}

	brokenFiles := make([]string, 0, len(brokenSet))
	for name := range brokenSet {
		brokenFiles = append(brokenFiles, name)
	}
	return brokenFiles
}

func (m *Manager) ReinsertEntry(ctx context.Context, entry *storage.Entry) error {
	if m.fixer == nil {
		return fmt.Errorf("fixer not initialized")
	}
	res, err := m.fixer.FixTorrent(ctx, entry, false)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("failed to re-insert torrent")
	}
	return nil
}
