package worker

import (
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

	"github.com/jupark12/karaoke-worker/models"
	"github.com/jupark12/karaoke-worker/queue"
	"github.com/ledongthuc/pdf"
)

// Worker represents a processing node that consumes jobs
type Worker struct {
	ID         string
	Queue      *queue.PDFJobQueue
	Processing bool
	mu         sync.Mutex
}

// NewWorker creates a new worker instance
func NewWorker(id string, queue *queue.PDFJobQueue) *Worker {

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
			err = w.processBankStatement(job)
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
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to convert PDF with pdftotext: %v", err)
		}

		print(output, "OUTPUT YAY")

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

	log.Printf("Transactions saved to %s", transactionFile)
	log.Printf("Proccesed %d transactions", len(transactions))

	return nil
}

func extractTransations(lines []string) []models.Transaction {
	transactions := []models.Transaction{}
	// This regex will match MM/DD/YY format (e.g., 02/06/25)
	datePattern := regexp.MustCompile(`\d{2}/\d{2}/\d{2}`)
	amountPattern := regexp.MustCompile(`\$?(\d{1,3}(,\d{3})*\.\d{2})`)

	for _, line := range lines {
		//Skip Headers, footers, and empty lines
		if !datePattern.MatchString(line) || len(strings.TrimSpace(line)) < 10 {
			continue
		}

		dateMatch := datePattern.FindString(line)
		print(dateMatch, "DATE")
		if dateMatch == "" {
			continue
		}

		amountMatches := amountPattern.FindStringSubmatch(line)
		if len(amountMatches) < 2 {
			continue
		}

		amountStr := strings.Replace(amountMatches[1], ",", "", -1)
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			continue
		}

		description := "SOME DESCRIPTION"

		transactionType := "debit"
		if strings.Contains(strings.ToLower(line), "deposit") ||
			strings.Contains(strings.ToLower(line), "credit") ||
			strings.Contains(strings.ToLower(line), "payment received") {
			transactionType = "credit"
		}

		transaction := models.Transaction{
			Date:        dateMatch,
			Amount:      amount,
			Description: description,
			Type:        transactionType,
		}

		print(transaction.Date, transaction.Amount)

		transactions = append(transactions, transaction)
	}

	return transactions
}
