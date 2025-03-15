package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jupark12/go-job-queue/models"
)

// PDFJobQueue manages the queue of pdf processing jobs
type PDFJobQueue struct {
	mu             sync.RWMutex
	pendingJobs    []*models.PDFJob
	processingJobs map[string]*models.PDFJob
	completedJobs  map[string]*models.PDFJob
	failedJobs     map[string]*models.PDFJob
	jobsByID       map[string]*models.PDFJob
	dataDir        string
	jobUpdateChan  chan *models.PDFJob
}

// NewPDFJobQueue creates a new instance of PDFJobQueue
func NewPDFJobQueue(dataDir string) *PDFJobQueue {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	return &PDFJobQueue{
		pendingJobs:    make([]*models.PDFJob, 0),
		processingJobs: make(map[string]*models.PDFJob),
		completedJobs:  make(map[string]*models.PDFJob),
		failedJobs:     make(map[string]*models.PDFJob),
		jobsByID:       make(map[string]*models.PDFJob),
		dataDir:        dataDir,
		jobUpdateChan:  make(chan *models.PDFJob, 100),
	}
}

// EnqueueJob adds a new job to the queue
func (q *PDFJobQueue) EnqueueJob(sourceFile string) (*models.PDFJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Generate a unique ID for the job
	jobID := uuid.New().String()

	// Create output file path
	ext := filepath.Ext(sourceFile)
	baseName := filepath.Base(sourceFile[:len(sourceFile)-len(ext)])
	outputFile := fmt.Sprintf("%s_karaoke%s", baseName, ext)

	job := &models.PDFJob{
		ID:         jobID,
		SourceFile: sourceFile,
		OutputFile: outputFile,
		Status:     models.StatusPending,
		CreatedAt:  time.Now(),
	}

	q.pendingJobs = append(q.pendingJobs, job)
	q.jobsByID[jobID] = job

	// Persist job to disk
	if err := q.persistJob(job); err != nil {
		return nil, fmt.Errorf("failed to persist job: %v", err)
	}

	log.Printf("Job enqueued: %s for file %s", jobID, sourceFile)
	return job, nil
}

// DequeueJob gets the next pending job and marks it as processing
func (q *PDFJobQueue) DequeueJob(workerID string) (*models.PDFJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pendingJobs) == 0 {
		return nil, fmt.Errorf("no pending jobs available")
	}

	// Get the next job from the queue (FIFO)
	job := q.pendingJobs[0]
	q.pendingJobs = q.pendingJobs[1:]

	// Mark as processing
	job.Status = models.StatusProcessing
	job.StartedAt = time.Now()
	job.ProcessingNode = workerID

	// Move to processing map
	q.processingJobs[job.ID] = job

	// Persist updated job status
	if err := q.persistJob(job); err != nil {
		return nil, fmt.Errorf("failed to update job status: %v", err)
	}

	return job, nil
}

// CompleteJob marks a job as completed
func (q *PDFJobQueue) CompleteJob(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.processingJobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found in processing queue", jobID)
	}

	job.Status = models.StatusCompleted
	job.CompletedAt = time.Now()

	// Move from processing to completed
	delete(q.processingJobs, jobID)
	q.completedJobs[jobID] = job

	// Notify about job update
	q.jobUpdateChan <- job

	// Persist updated job status
	return q.persistJob(job)
}

// FailJob marks a job as failed
func (q *PDFJobQueue) FailJob(jobID string, errorMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.processingJobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found in processing queue", jobID)
	}

	job.Status = models.StatusFailed
	job.ErrorMessage = errorMsg
	job.CompletedAt = time.Now()

	// Move from processing to failed
	delete(q.processingJobs, jobID)
	q.failedJobs[jobID] = job

	// Notify about job update
	q.jobUpdateChan <- job

	// Persist updated job status
	return q.persistJob(job)
}

// GetJob retrieves a job by ID
func (q *PDFJobQueue) GetJob(jobID string) (*models.PDFJob, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, exists := q.jobsByID[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	return job, nil
}

// persistJob saves job data to disk
func (q *PDFJobQueue) persistJob(job *models.PDFJob) error {
	jobPath := filepath.Join(q.dataDir, job.ID+".json")

	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal job data: %v", err)
	}

	if err := os.WriteFile(jobPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write job file: %v", err)
	}

	return nil
}

// LoadJobs loads all persisted jobs from disk
func (q *PDFJobQueue) LoadJobs() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	files, err := os.ReadDir(q.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %v", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) != ".json" {
			continue
		}

		jobPath := filepath.Join(q.dataDir, file.Name())
		data, err := os.ReadFile(jobPath)
		if err != nil {
			log.Printf("Failed to read job file %s: %v", jobPath, err)
			continue
		}

		var job models.PDFJob
		if err := json.Unmarshal(data, &job); err != nil {
			log.Printf("Failed to unmarshal job data %s: %v", jobPath, err)
			continue
		}

		// Add job to appropriate queue based on status
		q.jobsByID[job.ID] = &job

		switch job.Status {
		case models.StatusPending:
			q.pendingJobs = append(q.pendingJobs, &job)
		case models.StatusProcessing:
			q.processingJobs[job.ID] = &job
		case models.StatusCompleted:
			q.completedJobs[job.ID] = &job
		case models.StatusFailed:
			q.failedJobs[job.ID] = &job
		}
	}

	log.Printf("Loaded %d jobs from disk", len(q.jobsByID))
	return nil
}

// GetPendingJobs returns a copy of the pending jobs slice
func (q *PDFJobQueue) GetPendingJobs() []*models.PDFJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*models.PDFJob, len(q.pendingJobs))
	copy(jobs, q.pendingJobs)
	return jobs
}

// GetProcessingJobs returns a copy of the processing jobs map
func (q *PDFJobQueue) GetProcessingJobs() []*models.PDFJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*models.PDFJob, 0, len(q.processingJobs))
	for _, job := range q.processingJobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetCompletedJobs returns a copy of the completed jobs map
func (q *PDFJobQueue) GetCompletedJobs() []*models.PDFJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*models.PDFJob, 0, len(q.completedJobs))
	for _, job := range q.completedJobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetFailedJobs returns a copy of the failed jobs map
func (q *PDFJobQueue) GetFailedJobs() []*models.PDFJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*models.PDFJob, 0, len(q.failedJobs))
	for _, job := range q.failedJobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetAllJobs returns a copy of all jobs
func (q *PDFJobQueue) GetAllJobs() []*models.PDFJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*models.PDFJob, 0, len(q.jobsByID))
	for _, job := range q.jobsByID {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetJobUpdateChannel returns the job update channel
func (q *PDFJobQueue) GetJobUpdateChannel() <-chan *models.PDFJob {
	return q.jobUpdateChan
}
