package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jupark12/go-job-queue/models"
	"github.com/jupark12/go-job-queue/queue"
	"github.com/ledongthuc/pdf"
)

// NotifyFunc is a function type for job notifications
type NotifyFunc func(jobID string)

// Worker represents a processing node that consumes jobs
type Worker struct {
	ID         string
	Queue      *queue.PDFJobQueue
	Processing bool
	DBPool     *pgxpool.Pool
	notifyFunc NotifyFunc
	mu         sync.Mutex
}

// NewWorker creates a new worker instance
func NewWorker(id string, queue *queue.PDFJobQueue, dbPool *pgxpool.Pool) *Worker {

	return &Worker{
		ID:     id,
		Queue:  queue,
		DBPool: dbPool,
	}
}

// SetNotifier sets the notification function
func (w *Worker) SetNotifier(notifyFunc NotifyFunc) {
	w.notifyFunc = notifyFunc
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

			// Notify about job status change to processing
			if w.notifyFunc != nil {
				w.notifyFunc(job.ID)
			}

			// Process the job
			err = w.processBankStatement(job)
			if err != nil {
				log.Printf("Worker %s failed to process job %s: %v", w.ID, job.ID, err)
				w.Queue.FailJob(job.ID, err.Error())
			} else {
				log.Printf("Worker %s completed job %s", w.ID, job.ID)
				w.Queue.CompleteJob(job.ID)
			}

			// Notify about job completion or failure
			if w.notifyFunc != nil {
				w.notifyFunc(job.ID)
			}
		}
	}()
}

// processBankStatement handles the pdf processing
func (w *Worker) processBankStatement(job *models.PDFJob) error {
	outDir := ".output"
	log.Printf("Processing PDF file: %s", job.SourceFile)

	// Check if file exists
	if _, err := os.Stat(job.SourceFile); os.IsNotExist(err) {
		return fmt.Errorf("source file does not exist: %s", job.SourceFile)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	f, r, err := pdf.Open(job.SourceFile)
	if err != nil {
		return err
	}
	defer f.Close()

	totalPages := r.NumPage()

	transactions := []models.Transaction{}

	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		p := r.Page(pageIndex)
		if p.V.IsNull() {
			continue
		}

		text, err := p.GetPlainText(nil)
		if err != nil {
			return fmt.Errorf("failed to extract text from page %d: %v", pageIndex, err)
		}

		lines := strings.Split(text, "\n")
		transactions = append(transactions, extractTransations(lines)...)
	}

	if !(len(transactions) > 0) {
		textFile := outDir + "/" + strings.TrimSuffix(filepath.Base(job.SourceFile), filepath.Ext(job.SourceFile)) + ".txt"

		cmd := exec.Command("pdftotext", "-layout", job.SourceFile, textFile)
		_, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to convert PDF with pdftotext: %v", err)
		}

		// Read the content of the generated text file
		content, err := os.ReadFile(textFile)
		if err != nil {
			return fmt.Errorf("failed to read text file: %v", err)
		}

		lines := strings.Split(string(content), "\n")

		transactions = append(transactions, extractTransations(lines)...)
	}

	// Save transactions to a file
	transactionFile := outDir + "/" + strings.TrimSuffix(filepath.Base(job.SourceFile), filepath.Ext(job.SourceFile)) + "_transactions.txt"
	file, err := os.Create(transactionFile)
	if err != nil {
		return fmt.Errorf("failed to create transaction file: %v", err)
	}

	defer file.Close()

	for _, transaction := range transactions {
		_, err := file.WriteString(fmt.Sprintf("%s,%s,%.2f,%s\n", transaction.Date, transaction.Description, transaction.Amount, transaction.Type))
		if err != nil {
			return fmt.Errorf("failed to write transaction to file: %v", err)
		}
	}

	// Save transactions to database
	if err := w.saveTransactionsToDatabase(job.ID, transactions); err != nil {
		return fmt.Errorf("failed to save transactions to database: %v", err)
	}

	log.Printf("Transactions saved to %s", transactionFile)
	log.Printf("Proccesed %d transactions", len(transactions))

	return nil
}

func extractTransations(lines []string) []models.Transaction {
	transactions := []models.Transaction{}
	// This regex will match MM/DD/YY format (e.g., 02/06/25)
	datePattern := regexp.MustCompile(`\d{2}/\d{2}/\d{2}`)
	amountPattern := regexp.MustCompile(`-?\$?(\d{1,3}(,\d{3})*\.\d{2})`)

	for _, line := range lines {
		//Skip Headers, footers, and empty lines
		if !datePattern.MatchString(line) || len(strings.TrimSpace(line)) < 10 {
			continue
		}

		dateMatch := datePattern.FindString(line)
		if dateMatch == "" {
			continue
		}

		//Parse date to time object
		parsedDate, err := time.Parse("01/02/06", dateMatch)
		if err != nil {
			continue
		}

		amountMatches := amountPattern.FindStringSubmatch(line)
		if len(amountMatches) < 2 {
			continue
		}

		isNegative := strings.Contains(amountMatches[0], "-")

		amountStr := strings.Replace(amountMatches[1], ",", "", -1)
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			continue
		}

		// Apply negative sign if needed
		if isNegative {
			amount = -amount
		}

		// Extract description as everything between date and amount
		dateIndex := strings.Index(line, dateMatch) + len(dateMatch)
		amountIndex := strings.Index(line, amountMatches[0])

		var description string
		if dateIndex < amountIndex {
			// Normal case: date comes before amount
			description = strings.TrimSpace(line[dateIndex:amountIndex])
		} else {
			// Handle case where amount might come before date (rare)
			description = strings.TrimSpace(line[0:dateIndex])
			if description == "" {
				description = "TRANSACTION"
			}
		}

		// Determine transaction type based on amount
		transactionType := "debit"
		if amount > 0 {
			transactionType = "credit"
		}

		transaction := models.Transaction{
			Date:        parsedDate,
			Amount:      amount,
			Description: description,
			Type:        transactionType,
		}

		transactions = append(transactions, transaction)
	}

	return transactions
}

// Add a function to save transactions to the database
func (w *Worker) saveTransactionsToDatabase(jobID string, transactions []models.Transaction) error {
	ctx := context.Background()

	// Begin a transaction
	tx, err := w.DBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx) // Rollback if we don't commit

	// Create transactions table if it doesn't exist
	_, err = tx.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            date DATE NOT NULL,
            description TEXT NOT NULL,
            amount FLOAT NOT NULL,
            type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to create transactions table: %v", err)
	}

	// Insert each transaction
	for _, transaction := range transactions {
		formattedDate := transaction.Date.Format("2006-01-02")

		_, err := tx.Exec(ctx, `
            INSERT INTO transactions (job_id, date, description, amount, type)
            VALUES ($1, $2, $3, $4, $5)
        `, jobID, formattedDate, transaction.Description, transaction.Amount, transaction.Type)

		if err != nil {
			return fmt.Errorf("failed to insert transaction: %v", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Printf("Saved %d transactions to database for job %s", len(transactions), jobID)
	return nil
}
