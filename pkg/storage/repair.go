package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

type JobStatus string
type RepairMode string
type JobStage string
type RepairActionStatus string

const (
	JobStarted    JobStatus = "started"
	JobPending    JobStatus = "pending"
	JobFailed     JobStatus = "failed"
	JobCompleted  JobStatus = "completed"
	JobProcessing JobStatus = "processing"
	JobCancelled  JobStatus = "cancelled"
)

const (
	RepairModeDetectOnly      RepairMode = "detect_only"
	RepairModeDetectAndRepair RepairMode = "detect_and_repair"
)

const (
	JobStageQueued      JobStage = "queued"
	JobStageDiscovering JobStage = "discovering"
	JobStageProbing     JobStage = "probing"
	JobStagePlanning    JobStage = "planning"
	JobStageExecuting   JobStage = "executing"
	JobStageVerifying   JobStage = "verifying"
	JobStageCompleted   JobStage = "completed"
	JobStageFailed      JobStage = "failed"
	JobStageCancelled   JobStage = "cancelled"
)

const (
	RepairActionPlanned   RepairActionStatus = "planned"
	RepairActionRunning   RepairActionStatus = "running"
	RepairActionSucceeded RepairActionStatus = "succeeded"
	RepairActionFailed    RepairActionStatus = "failed"
	RepairActionSkipped   RepairActionStatus = "skipped"
)

type RepairStats struct {
	Discovered int `json:"discovered"`
	Probed     int `json:"probed"`
	Broken     int `json:"broken"`
	Planned    int `json:"planned"`
	Executed   int `json:"executed"`
	Fixed      int `json:"fixed"`
	Failed     int `json:"failed"`
	Unknown    int `json:"unknown"`
}

type RepairAction struct {
	ID          string             `json:"id"`
	Type        string             `json:"type"`
	EntryID     string             `json:"entry_id"`
	Protocol    config.Protocol    `json:"protocol"`
	Status      RepairActionStatus `json:"status"`
	Error       string             `json:"error,omitempty"`
	StartedAt   time.Time          `json:"started_at,omitempty"`
	CompletedAt time.Time          `json:"completed_at,omitempty"`
}

type Job struct {
	ID          string                       `json:"id"`
	Arrs        []string                     `json:"arrs"`
	MediaIDs    []string                     `json:"media_ids"`
	StartedAt   time.Time                    `json:"created_at"`
	BrokenItems map[string][]arr.ContentFile `json:"broken_items"`
	Status      JobStatus                    `json:"status"`
	CompletedAt time.Time                    `json:"finished_at"`
	FailedAt    time.Time                    `json:"failed_at"`
	AutoProcess bool                         `json:"auto_process"`
	Recurrent   bool                         `json:"recurrent"`
	Error       string                       `json:"error"`

	UpdatedAt time.Time       `json:"updated_at"`
	UniqueKey string          `json:"unique_key,omitempty"`
	Mode      RepairMode      `json:"mode,omitempty"`
	Stage     JobStage        `json:"stage,omitempty"`
	Stats     RepairStats     `json:"stats"`
	Actions   []*RepairAction `json:"actions,omitempty"`
}

func (j *Job) DiscordContext() string {
	format := `
		**ID**: %s
		**arrs**: %s
		**Media IDs**: %s
		**Status**: %s
		**Mode**: %s
		**Stage**: %s
		**Started At**: %s
		**Completed At**: %s
		**Broken**: %d
		**Fixed**: %d
`
	dateFmt := "2006-01-02 15:04:05"
	return fmt.Sprintf(
		format,
		j.ID,
		strings.Join(j.Arrs, ","),
		strings.Join(j.MediaIDs, ", "),
		j.Status,
		j.Mode,
		j.Stage,
		j.StartedAt.Format(dateFmt),
		j.CompletedAt.Format(dateFmt),
		j.Stats.Broken,
		j.Stats.Fixed,
	)
}

func RepairJobKey(arrs, mediaIDs []string, mode RepairMode) string {
	arrCopy := append([]string(nil), arrs...)
	mediaCopy := append([]string(nil), mediaIDs...)
	sort.Strings(arrCopy)
	sort.Strings(mediaCopy)
	return fmt.Sprintf("%s|%s|%s", strings.Join(arrCopy, ","), strings.Join(mediaCopy, ","), mode)
}

func (s *Storage) SaveRepairJob(key string, job *Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if key != "" {
		job.UniqueKey = key
	}
	job.UpdatedAt = time.Now()

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if err := s.repairJobs.Put(job.ID, data, nil); err != nil {
		return err
	}

	if key != "" && key != job.ID {
		_ = s.repairKeys.Put(key, []byte(job.ID), nil)
	}
	return nil
}

func (s *Storage) GetRepairJob(id string) (*Job, error) {
	data, err := s.repairJobs.Get(id)
	if err != nil {
		return nil, err
	}

	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	if job.ID == "" {
		job.ID = id
	}
	return &job, nil
}

func (s *Storage) GetRepairJobByUniqueKey(uniqueKey string) *Job {
	if uniqueKey == "" {
		return nil
	}

	jobIDData, err := s.repairKeys.Get(uniqueKey)
	if err == nil {
		job, getErr := s.GetRepairJob(string(jobIDData))
		if getErr == nil {
			return job
		}
	}

	jobs, err := s.LoadAllRepairJobs()
	if err != nil {
		return nil
	}
	for _, job := range jobs {
		if job.UniqueKey == uniqueKey {
			return job
		}
	}
	return nil
}

func (s *Storage) DeleteRepairJob(id string) error {
	if id == "" {
		return nil
	}

	if err := s.repairJobs.Delete(id); err != nil {
		return err
	}

	keysToDelete := make([]string, 0)
	_ = s.repairKeys.ForEach(func(key string, value []byte) error {
		if string(value) == id {
			keysToDelete = append(keysToDelete, key)
		}
		return nil
	})
	for _, key := range keysToDelete {
		_ = s.repairKeys.Delete(key)
	}
	return nil
}

func (s *Storage) SaveAllRepairJobs(jobs map[string]*Job) error {
	for key, job := range jobs {
		if err := s.SaveRepairJob(key, job); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) LoadAllRepairJobs() ([]*Job, error) {
	jobs := make([]*Job, 0)
	_ = s.repairJobs.ForEach(func(key string, value []byte) error {
		var job Job
		if err := json.Unmarshal(value, &job); err != nil {
			return nil
		}
		if job.ID == "" {
			job.ID = key
		}
		jobs = append(jobs, &job)
		return nil
	})
	return jobs, nil
}

func (s *Storage) CountRepairJobs() (int, error) {
	return s.repairJobs.Len(), nil
}

func (s *Storage) PrepareRepairDataV2() error {
	invalidJobIDs := make([]string, 0)
	_ = s.repairJobs.ForEach(func(key string, value []byte) error {
		var job Job
		if err := json.Unmarshal(value, &job); err != nil || job.ID == "" {
			invalidJobIDs = append(invalidJobIDs, key)
		}
		return nil
	})

	for _, id := range invalidJobIDs {
		_ = s.repairJobs.Delete(id)
	}

	keys := make([]string, 0)
	_ = s.repairKeys.ForEach(func(key string, value []byte) error {
		keys = append(keys, key)
		return nil
	})
	for _, key := range keys {
		_ = s.repairKeys.Delete(key)
	}
	return nil
}
