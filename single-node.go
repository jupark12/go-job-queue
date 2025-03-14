package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ggerganov/whisper.cpp/bindings/go/pkg/whisper"
	"github.com/google/uuid"
)

// JobStatus represents the current state of a job in the system
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

// AudioJob represents a job for processing an audio file
type AudioJob struct {
	ID             string    `json:"id"`
	SourceFile     string    `json:"source_file"`
	OutputFile     string    `json:"output_file"`
	Status         JobStatus `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	ProcessingNode string    `json:"processing_node,omitempty"`
}

// AudioJobQueue manages the queue of audio processing jobs
type AudioJobQueue struct {
	mu             sync.RWMutex
	pendingJobs    []*AudioJob
	processingJobs map[string]*AudioJob
	completedJobs  map[string]*AudioJob
	failedJobs     map[string]*AudioJob
	jobsByID       map[string]*AudioJob
	dataDir        string
	jobUpdateChan  chan *AudioJob
}

// NewAudioJobQueue creates a new instance of AudioJobQueue
func NewAudioJobQueue(dataDir string) *AudioJobQueue {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	return &AudioJobQueue{
		pendingJobs:    make([]*AudioJob, 0),
		processingJobs: make(map[string]*AudioJob),
		completedJobs:  make(map[string]*AudioJob),
		failedJobs:     make(map[string]*AudioJob),
		jobsByID:       make(map[string]*AudioJob),
		dataDir:        dataDir,
		jobUpdateChan:  make(chan *AudioJob, 100),
	}
}

// EnqueueJob adds a new job to the queue
func (q *AudioJobQueue) EnqueueJob(sourceFile string) (*AudioJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Generate a unique ID for the job
	jobID := uuid.New().String()

	// Create output file path
	ext := filepath.Ext(sourceFile)
	baseName := filepath.Base(sourceFile[:len(sourceFile)-len(ext)])
	outputFile := fmt.Sprintf("%s_karaoke%s", baseName, ext)

	job := &AudioJob{
		ID:         jobID,
		SourceFile: sourceFile,
		OutputFile: outputFile,
		Status:     StatusPending,
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
func (q *AudioJobQueue) DequeueJob(workerID string) (*AudioJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pendingJobs) == 0 {
		return nil, fmt.Errorf("no pending jobs available")
	}

	// Get the next job from the queue (FIFO)
	job := q.pendingJobs[0]
	q.pendingJobs = q.pendingJobs[1:]

	// Mark as processing
	job.Status = StatusProcessing
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
func (q *AudioJobQueue) CompleteJob(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.processingJobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found in processing queue", jobID)
	}

	job.Status = StatusCompleted
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
func (q *AudioJobQueue) FailJob(jobID string, errorMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.processingJobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found in processing queue", jobID)
	}

	job.Status = StatusFailed
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
func (q *AudioJobQueue) GetJob(jobID string) (*AudioJob, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, exists := q.jobsByID[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	return job, nil
}

// persistJob saves job data to disk
func (q *AudioJobQueue) persistJob(job *AudioJob) error {
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

// loadJobs loads all persisted jobs from disk
func (q *AudioJobQueue) loadJobs() error {
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

		var job AudioJob
		if err := json.Unmarshal(data, &job); err != nil {
			log.Printf("Failed to unmarshal job data %s: %v", jobPath, err)
			continue
		}

		// Add job to appropriate queue based on status
		q.jobsByID[job.ID] = &job

		switch job.Status {
		case StatusPending:
			q.pendingJobs = append(q.pendingJobs, &job)
		case StatusProcessing:
			q.processingJobs[job.ID] = &job
		case StatusCompleted:
			q.completedJobs[job.ID] = &job
		case StatusFailed:
			q.failedJobs[job.ID] = &job
		}
	}

	log.Printf("Loaded %d jobs from disk", len(q.jobsByID))
	return nil
}

// Worker represents a processing node that consumes jobs
type Worker struct {
	ID         string
	Queue      *AudioJobQueue
	Processing bool
	mu         sync.Mutex
	model      whisper.Model
}

// NewWorker creates a new worker instance
func NewWorker(id string, queue *AudioJobQueue) *Worker {
	model, err := whisper.New("user/local/share/whisper-models/ggml-base.en.bin")
	if err != nil {
		log.Fatalf("Failed to load Whisper model: %v", err)
	}

	return &Worker{
		ID:    id,
		Queue: queue,
		model: model,
	}
}

// Start begins processing jobs
func (w *Worker) Start() {
	log.Printf("Worker %s starting", w.ID)

	go func() {
		for {
			w.mu.Lock()
			w.Processing = false
			w.mu.Unlock()

			// Try to get a job from the queue
			job, err := w.Queue.DequeueJob(w.ID)
			if err != nil {
				// No jobs available, wait before trying again
				time.Sleep(5 * time.Second)
				continue
			}

			w.mu.Lock()
			w.Processing = true
			w.mu.Unlock()

			log.Printf("Worker %s processing job %s", w.ID, job.ID)

			// Process the job
			err = w.processAudioFile(job)
			if err != nil {
				log.Printf("Worker %s failed to process job %s: %v", w.ID, job.ID, err)
				w.Queue.FailJob(job.ID, err.Error())
			} else {
				log.Printf("Worker %s completed job %s", w.ID, job.ID)
				w.Queue.CompleteJob(job.ID)
			}
		}
	}()
}

// processAudioFile handles the actual audio processing
func (w *Worker) processAudioFile(job *AudioJob) error {
	context, err := w.model.NewContext()
	if err != nil {
		return fmt.Errorf("failed to create context: %v", err)
	}

	context.SetLanguage("en")
	context.SetTranslate(false)

	return nil
}

// Server handles HTTP requests for job management
type Server struct {
	queue    *AudioJobQueue
	workers  []*Worker
	httpAddr string
}

// NewServer creates a new server instance
func NewServer(queue *AudioJobQueue, httpAddr string, numWorkers int) *Server {
	server := &Server{
		queue:    queue,
		httpAddr: httpAddr,
		workers:  make([]*Worker, numWorkers),
	}

	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		server.workers[i] = NewWorker(workerID, queue)
	}

	return server
}

// Start begins the server
func (s *Server) Start() error {
	// Start HTTP server
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/jobs", s.handleJobs)
		mux.HandleFunc("/jobs/", s.handleJobDetails)

		log.Printf("HTTP server listening on %s", s.httpAddr)
		if err := http.ListenAndServe(s.httpAddr, mux); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Start workers
	for _, worker := range s.workers {
		worker.Start()
	}

	return nil
}

// handleJobs handles HTTP requests for job listing and creation
func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Create a new job
		r.ParseMultipartForm(10 << 20) // 10MB max memory
		file, header, err := r.FormFile("audioFile")
		if err != nil {
			http.Error(w, "Missing audio file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Save the file to disk
		tempDir := "uploads"
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			http.Error(w, "Failed to create upload directory", http.StatusInternalServerError)
			return
		}

		filePath := filepath.Join(tempDir, header.Filename)
		dst, err := os.Create(filePath)
		if err != nil {
			http.Error(w, "Failed to save file", http.StatusInternalServerError)
			return
		}
		defer dst.Close()

		// Copy the file data
		// In a real implementation, we'd use io.Copy to stream the file
		// For brevity, we're simplifying here

		// Enqueue the job
		job, err := s.queue.EnqueueJob(filePath)
		if err != nil {
			http.Error(w, "Failed to enqueue job", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(job)
		return
	}

	// GET - List all jobs
	w.Header().Set("Content-Type", "application/json")

	// Get jobs by status if specified
	status := r.URL.Query().Get("status")
	if status != "" {
		var jobs []*AudioJob

		s.queue.mu.RLock()
		switch JobStatus(status) {
		case StatusPending:
			jobs = make([]*AudioJob, len(s.queue.pendingJobs))
			copy(jobs, s.queue.pendingJobs)
		case StatusProcessing:
			jobs = make([]*AudioJob, 0, len(s.queue.processingJobs))
			for _, job := range s.queue.processingJobs {
				jobs = append(jobs, job)
			}
		case StatusCompleted:
			jobs = make([]*AudioJob, 0, len(s.queue.completedJobs))
			for _, job := range s.queue.completedJobs {
				jobs = append(jobs, job)
			}
		case StatusFailed:
			jobs = make([]*AudioJob, 0, len(s.queue.failedJobs))
			for _, job := range s.queue.failedJobs {
				jobs = append(jobs, job)
			}
		default:
			http.Error(w, "Invalid status parameter", http.StatusBadRequest)
			s.queue.mu.RUnlock()
			return
		}
		s.queue.mu.RUnlock()

		json.NewEncoder(w).Encode(jobs)
		return
	}

	// Return all jobs
	s.queue.mu.RLock()
	allJobs := make([]*AudioJob, 0, len(s.queue.jobsByID))
	for _, job := range s.queue.jobsByID {
		allJobs = append(allJobs, job)
	}
	s.queue.mu.RUnlock()

	json.NewEncoder(w).Encode(allJobs)
}

// handleJobDetails handles HTTP requests for specific jobs
func (s *Server) handleJobDetails(w http.ResponseWriter, r *http.Request) {
	jobID := filepath.Base(r.URL.Path)

	job, err := s.queue.GetJob(jobID)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}
