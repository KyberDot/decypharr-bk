package repair

import (
	"os"
	"path/filepath"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

func fileIsSymlinked(file string) bool {
	info, err := os.Lstat(file)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
}

func getSymlinkTarget(file string) string {
	if fileIsSymlinked(file) {
		target, err := os.Readlink(file)
		if err != nil {
			return ""
		}
		if !filepath.IsAbs(target) {
			dir := filepath.Dir(file)
			target = filepath.Join(dir, target)
		}
		return target
	}
	return ""
}

func collectFiles(media arr.Content) map[string][]arr.ContentFile {
	uniqueParents := make(map[string][]arr.ContentFile)
	files := media.Files
	for _, file := range files {
		target := getSymlinkTarget(file.Path)
		if target != "" {
			file.IsSymlink = true
			dir, f := filepath.Split(target)
			torrentNamePath := filepath.Clean(dir)
			// Set target path folder/file.mkv
			file.TargetPath = f
			uniqueParents[torrentNamePath] = append(uniqueParents[torrentNamePath], file)
		}
	}
	return uniqueParents
}

func (r *Repair) checkTorrentFiles(torrentPath string, files []arr.ContentFile, clients map[string]common.Client, mgr *manager.Manager) []arr.ContentFile {
	brokenFiles := make([]arr.ContentFile, 0)
	emptyFiles := make([]arr.ContentFile, 0)

	r.logger.Debug().Msgf("Checking %s", torrentPath)

	// GetReader the debrid client
	dir := filepath.Dir(torrentPath)
	debridName := r.findDebridForPath(dir, clients)
	if debridName == "" {
		r.logger.Debug().Msgf("No debrid found for %s. Skipping", torrentPath)
		return emptyFiles
	}

	// Check if torrent exists in manager
	torrentName := filepath.Clean(filepath.Base(torrentPath))
	entry, err := mgr.GetEntry(torrentName)
	if err != nil {
		r.logger.Debug().Msgf("Can't find torrent %s in manager. Marking as broken", torrentName)
		// Return all files as broken
		return files
	}

	// Batch check files
	filePaths := make([]string, len(files))
	for i, file := range files {
		filePaths[i] = file.TargetPath
	}

	brokenFilePaths := mgr.GetBrokenFiles(entry, filePaths)
	if len(brokenFilePaths) > 0 {
		r.logger.Debug().Msgf("%d broken files found in %s", len(brokenFilePaths), torrentName)

		// Create a set for O(1) lookup
		brokenSet := make(map[string]bool, len(brokenFilePaths))
		for _, brokenPath := range brokenFilePaths {
			brokenSet[brokenPath] = true
		}

		// Filter broken files
		for _, contentFile := range files {
			if brokenSet[contentFile.TargetPath] {
				brokenFiles = append(brokenFiles, contentFile)
			}
		}
	}

	return brokenFiles
}

func (r *Repair) findDebridForPath(dir string, clients map[string]common.Client) string {
	// Check cache first
	if debridName, exists := r.debridPathCache.Load(dir); exists {
		return debridName.(string)
	}

	// Find debrid client
	for _, client := range clients {
		mountPath := client.Config().Folder
		if mountPath == "" {
			continue
		}

		if filepath.Clean(mountPath) == filepath.Clean(dir) {
			debridName := client.Config().Name

			// Cache the result
			r.debridPathCache.Store(dir, debridName)

			return debridName
		}
	}

	// Cache empty result to avoid repeated lookups
	r.debridPathCache.Store(dir, "")

	return ""
}
