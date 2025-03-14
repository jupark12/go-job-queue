package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jupark12/karaoke-worker/models"
	"github.com/jupark12/karaoke-worker/queue"
	"github.com/jupark12/karaoke-worker/worker"
)

// Server handles HTTP requests for job management
type Server struct {
	queue    *queue.PDFJobQueue
	workers  []*worker.Worker
	httpAddr string
}

// NewServer creates a new server instance
func NewServer(queue *queue.PDFJobQueue, httpAddr string, numWorkers int, dbPool *pgxpool.Pool) *Server {
	server := &Server{
		queue:    queue,
		httpAddr: httpAddr,
		workers:  make([]*worker.Worker, numWorkers),
	}

	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		server.workers[i] = worker.NewWorker(workerID, queue, dbPool)
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
		file, header, err := r.FormFile("pdfFile")
		if err != nil {
			http.Error(w, "Missing PDF file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Save the file to disk
		tempDir := ".uploads"
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
		if _, err := io.Copy(dst, file); err != nil {
			http.Error(w, "Failed to save file data", http.StatusInternalServerError)
			return
		}

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
		var jobs []*models.PDFJob

		switch models.JobStatus(status) {
		case models.StatusPending:
			jobs = s.queue.GetPendingJobs()
		case models.StatusProcessing:
			jobs = s.queue.GetProcessingJobs()
		case models.StatusCompleted:
			jobs = s.queue.GetCompletedJobs()
		case models.StatusFailed:
			jobs = s.queue.GetFailedJobs()
		default:
			http.Error(w, "Invalid status parameter", http.StatusBadRequest)
			return
		}

		json.NewEncoder(w).Encode(jobs)
		return
	}

	// Return all jobs
	allJobs := s.queue.GetAllJobs()
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
