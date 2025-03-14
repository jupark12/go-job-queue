package models

import (
	"time"
)

// JobStatus represents the current state of a job in the system
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

// PDFJob represents a job for processing an pdf file
type PDFJob struct {
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
