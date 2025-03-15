package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jupark12/go-job-queue/models"
	"github.com/jupark12/go-job-queue/queue"
	"github.com/jupark12/go-job-queue/worker"
)

// Server handles HTTP requests for job management
type Server struct {
	queue     *queue.PDFJobQueue
	workers   []*worker.Worker
	httpAddr  string
	wsManager *models.WebSocketManager
	upgrader  websocket.Upgrader
}

// NewServer creates a new server instance
func NewServer(queue *queue.PDFJobQueue, httpAddr string, numWorkers int, dbPool *pgxpool.Pool) *Server {
	wsManager := models.NewWebSocketManager()
	wsManager.Start()

	server := &Server{
		queue:     queue,
		httpAddr:  httpAddr,
		workers:   make([]*worker.Worker, numWorkers),
		wsManager: wsManager,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	// Initialize workers
	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		server.workers[i] = worker.NewWorker(workerID, queue, dbPool)

		//Set the Websocket manager in the worker
		server.workers[i].SetNotifier(server.notifyJobUpdate)
	}

	return server
}

// notifyJobUpdate is a callback function for workers to notify about job updates
func (s *Server) notifyJobUpdate(jobID string) {
	job, err := s.queue.GetJob(jobID)
	if err != nil {
		log.Printf("Failed to get job for notification: %v", err)
		return
	}
	print(job.Status)
	s.wsManager.BroadcastJobUpdate(job)
}

// Start begins the server
func (s *Server) Start() error {
	// Start HTTP server
	go func() {
		mux := http.NewServeMux()

		// Add CORS middleware
		corsMiddleware := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

				if r.Method == "OPTIONS" {
					w.WriteHeader(http.StatusOK)
					return
				}

				next.ServeHTTP(w, r)
			})
		}

		// Wrap handlers with CORS middleware
		mux.Handle("/jobs", corsMiddleware(http.HandlerFunc(s.handleJobs)))
		mux.Handle("/jobs/", corsMiddleware(http.HandlerFunc(s.handleJobDetails)))
		mux.Handle("/ws", http.HandlerFunc(s.handleWebSocket))

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

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		return
	}

	// Register the client
	s.wsManager.RegisterClient(conn)

	// Send initial job list to the client
	allJobs := s.queue.GetAllJobs()
	initialData, err := json.Marshal(map[string]interface{}{
		"type": "initial_jobs",
		"jobs": allJobs,
	})
	if err == nil {
		conn.WriteMessage(websocket.TextMessage, initialData)
	}

	// Handle disconnection
	go func() {
		for {
			// Read messages from the client (we don't really need to do anything with them)
			_, _, err := conn.ReadMessage()
			if err != nil {
				s.wsManager.UnregisterClient(conn)
				break
			}
		}
	}()
}
