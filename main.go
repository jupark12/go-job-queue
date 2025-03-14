package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jupark12/karaoke-worker/queue"
	"github.com/jupark12/karaoke-worker/server"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Configuration
	dataDir := ".data"
	httpAddr := ":8080"
	numWorkers := 4

	//Connect to database
	dbURL := "postgresql://junpark@localhost:5432/bankstatements"
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	// // Test Connection
	// var greeting string
	// err = pool.QueryRow(context.Background(), "SELECT 'Hello, PostgreSQL!'").Scan(&greeting)
	// if err != nil {
	// 	log.Fatalf("QueryRow failed: %v\n", err)
	// }
	// fmt.Println(greeting)

	// Initialize the job queue
	jobQueue := queue.NewPDFJobQueue(dataDir)

	// Load existing jobs
	if err := jobQueue.LoadJobs(); err != nil {
		log.Printf("Warning: Failed to load existing jobs: %v", err)
	}

	// Create and start the server
	srv := server.NewServer(jobQueue, httpAddr, numWorkers, pool)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("PDF processing broker started with %d workers", numWorkers)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	<-sigChan
	log.Println("Shutting down gracefully...")
}
