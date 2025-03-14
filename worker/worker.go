package worker

import (
	"log"
	"sync"
	"time"

	"github.com/jupark12/karaoke-worker/models"
	"github.com/jupark12/karaoke-worker/queue"
)

// Worker represents a processing node that consumes jobs
type Worker struct {
	ID         string
	Queue      *queue.AudioJobQueue
	Processing bool
	mu         sync.Mutex
}

// NewWorker creates a new worker instance
func NewWorker(id string, queue *queue.AudioJobQueue) *Worker {

	return &Worker{
		ID:    id,
		Queue: queue,
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
func (w *Worker) processAudioFile(job *models.AudioJob) error {

	// TODO: Implement audio processing logic
	// For now, just simulate processing
	log.Printf("Processing audio file: %s", job.SourceFile)

	// TODO: Implement vocal removal and karaoke generation
	// For now, just simulate processing
	log.Printf("Generating karaoke version by removing vocals")
	time.Sleep(2 * time.Second)

	log.Printf("Saving karaoke file to: %s", job.OutputFile)

	return nil
}
