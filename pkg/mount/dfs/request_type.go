package dfs

import (
	"fmt"
	"strings"

	"github.com/shirou/gopsutil/v3/process"
)

// RequestType represents the type of file access request
type RequestType int

const (
	RequestTypeOther RequestType = iota
	RequestTypeFFProbe
	RequestTypePlexScanner
)

// String returns the string representation of the request type
func (rt RequestType) String() string {
	switch rt {
	case RequestTypeFFProbe:
		return "ffprobe"
	case RequestTypePlexScanner:
		return "plex"
	case RequestTypeOther:
		return "other"
	default:
		return "unknown"
	}
}

// RequestInfo contains information about a file access request
type RequestInfo struct {
	Type      RequestType
	ProcessID int32
	Name      string
	CmdLine   string
}

func (ri *RequestInfo) String() string {
	return fmt.Sprintf("Type: %s, PID: %d, Name: %s, CmdLine: %s", ri.Type.String(), ri.ProcessID, ri.Name, ri.CmdLine)
}

func (ri *RequestInfo) IsScan() bool {
	return ri.Type == RequestTypeFFProbe || ri.Type == RequestTypePlexScanner
}

// DetectRequestType detects the type of request based on process information
func DetectRequestType(proc *process.Process) RequestInfo {
	info := RequestInfo{
		Type:      RequestTypeOther,
		ProcessID: proc.Pid,
	}

	name, err := proc.Name()
	if err == nil {
		info.Name = name
	}

	cmdLine, err := proc.Cmdline()
	if err == nil {
		info.CmdLine = cmdLine
	}

	// Detect ffprobe
	if strings.Contains(strings.ToLower(name), "ffprobe") {
		info.Type = RequestTypeFFProbe
		return info
	}

	// Detect Plex Media Scanner
	if strings.Contains(strings.ToLower(name), "plex") {
		if strings.Contains(strings.ToLower(cmdLine), "scanner") ||
			strings.Contains(strings.ToLower(name), "scanner") {
			info.Type = RequestTypePlexScanner
			return info
		}
	}

	// Additional detection for ffprobe via command line
	if strings.Contains(strings.ToLower(cmdLine), "ffprobe") {
		info.Type = RequestTypeFFProbe
		return info
	}

	return info
}
