package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// TransactionManager manages transaction IDs and ensures idempotency
type TransactionManager struct {
	mu sync.RWMutex

	// Transaction tracking
	activeTransactions map[string]*TransactionInfo   // txnID -> info
	completedTxns      map[string]*TransactionResult // txnID -> result

	// Configuration
	txnTimeout   time.Duration
	maxCompleted int
}

// TransactionInfo represents an active transaction
type TransactionInfo struct {
	TxnID       string
	TableName   string
	BaseVersion uint64
	StartTime   time.Time
	Retries     int
	Status      TransactionStatus
	LastError   string
}

// TransactionResult represents a completed transaction
type TransactionResult struct {
	TxnID       string
	TableName   string
	NewVersion  uint64
	Success     bool
	Error       string
	CompletedAt time.Time
	Duration    time.Duration
}

// TransactionStatus represents the status of a transaction
type TransactionStatus int

const (
	TxnPending TransactionStatus = iota
	TxnInProgress
	TxnCompleted
	TxnFailed
	TxnTimedOut
)

func (ts TransactionStatus) String() string {
	switch ts {
	case TxnPending:
		return "pending"
	case TxnInProgress:
		return "in_progress"
	case TxnCompleted:
		return "completed"
	case TxnFailed:
		return "failed"
	case TxnTimedOut:
		return "timed_out"
	default:
		return "unknown"
	}
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		activeTransactions: make(map[string]*TransactionInfo),
		completedTxns:      make(map[string]*TransactionResult),
		txnTimeout:         10 * time.Minute,
		maxCompleted:       1000,
	}
}

// GenerateTransactionID generates a unique transaction ID
func (tm *TransactionManager) GenerateTransactionID() string {
	// Generate a random 16-byte ID
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID if random generation fails
		return fmt.Sprintf("txn_%d_%d", time.Now().UnixNano(), time.Now().Unix())
	}

	// Add timestamp prefix for better debugging
	timestamp := time.Now().Unix()
	return fmt.Sprintf("txn_%d_%s", timestamp, hex.EncodeToString(bytes))
}

// StartTransaction starts a new transaction
func (tm *TransactionManager) StartTransaction(tableName string, baseVersion uint64) string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txnID := tm.GenerateTransactionID()

	txnInfo := &TransactionInfo{
		TxnID:       txnID,
		TableName:   tableName,
		BaseVersion: baseVersion,
		StartTime:   time.Now(),
		Retries:     0,
		Status:      TxnPending,
	}

	tm.activeTransactions[txnID] = txnInfo

	log.Printf("Started transaction %s for table %s at version %d", txnID, tableName, baseVersion)
	return txnID
}

// CommitTransaction attempts to commit a transaction with retry logic
func (tm *TransactionManager) CommitTransaction(
	metadataClient MetadataClient,
	txnID string,
	adds []*gen.FileAdd,
	removes []*gen.FileRemove,
) (*gen.CommitResponse, error) {
	tm.mu.Lock()
	txnInfo, exists := tm.activeTransactions[txnID]
	if !exists {
		tm.mu.Unlock()
		return nil, fmt.Errorf("transaction %s not found", txnID)
	}

	// Check if transaction has timed out
	if time.Since(txnInfo.StartTime) > tm.txnTimeout {
		txnInfo.Status = TxnTimedOut
		tm.mu.Unlock()
		return nil, fmt.Errorf("transaction %s timed out", txnID)
	}

	// Check if already completed (idempotency)
	if result, exists := tm.completedTxns[txnID]; exists {
		tm.mu.Unlock()
		if result.Success {
			return &gen.CommitResponse{
				NewVersion: result.NewVersion,
			}, nil
		} else {
			return &gen.CommitResponse{
				Error: result.Error,
			}, fmt.Errorf("transaction previously failed: %s", result.Error)
		}
	}

	txnInfo.Status = TxnInProgress
	tm.mu.Unlock()

	startTime := time.Now()

	// Prepare commit request
	commitReq := &gen.CommitRequest{
		TableName:   txnInfo.TableName,
		BaseVersion: txnInfo.BaseVersion,
		TxnId:       txnID,
		Adds:        adds,
		Removes:     removes,
	}

	// Attempt commit with retry logic
	var lastErr error
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retrying commit for transaction %s (attempt %d/%d)", txnID, attempt+1, maxRetries)
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
		}

		resp, err := metadataClient.Commit(nil, commitReq)
		if err != nil {
			lastErr = err

			// Check if this is a retryable error (leader election, network issues)
			if tm.isRetryableError(err) {
				tm.updateTransactionRetry(txnID, err)
				continue
			} else {
				// Non-retryable error, fail immediately
				break
			}
		}

		// Check response for errors
		if resp.Error != "" {
			lastErr = fmt.Errorf("commit failed: %s", resp.Error)

			// Check if this is a duplicate transaction (idempotency)
			if tm.isDuplicateTransactionError(resp.Error) {
				log.Printf("Transaction %s was already committed (idempotency), version: %d", txnID, resp.NewVersion)
				tm.completeTransaction(txnID, resp.NewVersion, true, "", time.Since(startTime))
				return resp, nil
			}

			// Check if this is a version conflict (optimistic concurrency failure)
			if tm.isVersionConflictError(resp.Error) {
				log.Printf("Transaction %s failed due to version conflict: %s", txnID, resp.Error)
				tm.completeTransaction(txnID, 0, false, resp.Error, time.Since(startTime))
				return resp, lastErr
			}

			// Other errors might be retryable
			if tm.isRetryableError(lastErr) {
				tm.updateTransactionRetry(txnID, lastErr)
				continue
			} else {
				break
			}
		}

		// Success
		log.Printf("Transaction %s committed successfully, new version: %d", txnID, resp.NewVersion)
		tm.completeTransaction(txnID, resp.NewVersion, true, "", time.Since(startTime))
		return resp, nil
	}

	// All retries exhausted
	log.Printf("Transaction %s failed after %d attempts: %v", txnID, maxRetries, lastErr)
	tm.completeTransaction(txnID, 0, false, lastErr.Error(), time.Since(startTime))
	return nil, lastErr
}

// isRetryableError checks if an error is retryable
func (tm *TransactionManager) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Network and leadership errors are retryable
	retryablePatterns := []string{
		"not leader",
		"no leader",
		"leader election",
		"connection refused",
		"connection reset",
		"timeout",
		"context deadline exceeded",
		"unavailable",
	}

	for _, pattern := range retryablePatterns {
		if contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// isDuplicateTransactionError checks if an error indicates a duplicate transaction
func (tm *TransactionManager) isDuplicateTransactionError(errorMsg string) bool {
	return contains(errorMsg, "duplicate") || contains(errorMsg, "already exists")
}

// isVersionConflictError checks if an error indicates a version conflict
func (tm *TransactionManager) isVersionConflictError(errorMsg string) bool {
	return contains(errorMsg, "optimistic concurrency") ||
		contains(errorMsg, "version conflict") ||
		contains(errorMsg, "base version") ||
		contains(errorMsg, "does not match")
}

// updateTransactionRetry updates transaction retry information
func (tm *TransactionManager) updateTransactionRetry(txnID string, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if txnInfo, exists := tm.activeTransactions[txnID]; exists {
		txnInfo.Retries++
		txnInfo.LastError = err.Error()
	}
}

// completeTransaction marks a transaction as completed
func (tm *TransactionManager) completeTransaction(txnID string, newVersion uint64, success bool, errorMsg string, duration time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Remove from active transactions
	txnInfo, exists := tm.activeTransactions[txnID]
	if !exists {
		return
	}

	delete(tm.activeTransactions, txnID)

	// Add to completed transactions
	result := &TransactionResult{
		TxnID:       txnID,
		TableName:   txnInfo.TableName,
		NewVersion:  newVersion,
		Success:     success,
		Error:       errorMsg,
		CompletedAt: time.Now(),
		Duration:    duration,
	}

	tm.completedTxns[txnID] = result

	// Cleanup old completed transactions if we have too many
	if len(tm.completedTxns) > tm.maxCompleted {
		tm.cleanupOldTransactions()
	}

	// Update transaction status
	if success {
		txnInfo.Status = TxnCompleted
	} else {
		txnInfo.Status = TxnFailed
	}
}

// cleanupOldTransactions removes old completed transactions
func (tm *TransactionManager) cleanupOldTransactions() {
	// Remove transactions older than 1 hour
	cutoff := time.Now().Add(-1 * time.Hour)
	var oldTxns []string

	for txnID, result := range tm.completedTxns {
		if result.CompletedAt.Before(cutoff) {
			oldTxns = append(oldTxns, txnID)
		}
	}

	// Remove oldest transactions
	for _, txnID := range oldTxns {
		delete(tm.completedTxns, txnID)
	}

	log.Printf("Cleaned up %d old transactions", len(oldTxns))
}

// GetTransactionStatus returns the status of a transaction
func (tm *TransactionManager) GetTransactionStatus(txnID string) (*TransactionInfo, *TransactionResult, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// Check active transactions first
	if txnInfo, exists := tm.activeTransactions[txnID]; exists {
		return txnInfo, nil, true
	}

	// Check completed transactions
	if result, exists := tm.completedTxns[txnID]; exists {
		return nil, result, true
	}

	return nil, nil, false
}

// GetActiveTransactions returns all active transactions
func (tm *TransactionManager) GetActiveTransactions() map[string]*TransactionInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]*TransactionInfo)
	for txnID, txnInfo := range tm.activeTransactions {
		result[txnID] = txnInfo
	}

	return result
}

// GetTransactionStats returns statistics about transactions
func (tm *TransactionManager) GetTransactionStats() TransactionStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := TransactionStats{
		ActiveTransactions:    len(tm.activeTransactions),
		CompletedTransactions: len(tm.completedTxns),
	}

	// Count by status
	for _, result := range tm.completedTxns {
		if result.Success {
			stats.SuccessfulTransactions++
		} else {
			stats.FailedTransactions++
		}
	}

	// Calculate average duration
	if len(tm.completedTxns) > 0 {
		var totalDuration time.Duration
		for _, result := range tm.completedTxns {
			totalDuration += result.Duration
		}
		stats.AverageDuration = totalDuration / time.Duration(len(tm.completedTxns))
	}

	return stats
}

// TransactionStats represents statistics about transactions
type TransactionStats struct {
	ActiveTransactions     int
	CompletedTransactions  int
	SuccessfulTransactions int
	FailedTransactions     int
	AverageDuration        time.Duration
}

// CleanupTimedOutTransactions removes transactions that have timed out
func (tm *TransactionManager) CleanupTimedOutTransactions() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	var timedOutTxns []string

	for txnID, txnInfo := range tm.activeTransactions {
		if now.Sub(txnInfo.StartTime) > tm.txnTimeout {
			timedOutTxns = append(timedOutTxns, txnID)
			txnInfo.Status = TxnTimedOut
		}
	}

	for _, txnID := range timedOutTxns {
		txnInfo := tm.activeTransactions[txnID]
		delete(tm.activeTransactions, txnID)

		// Add to completed transactions as failed
		result := &TransactionResult{
			TxnID:       txnID,
			TableName:   txnInfo.TableName,
			NewVersion:  0,
			Success:     false,
			Error:       "transaction timed out",
			CompletedAt: now,
			Duration:    now.Sub(txnInfo.StartTime),
		}
		tm.completedTxns[txnID] = result

		log.Printf("Transaction %s timed out after %v", txnID, now.Sub(txnInfo.StartTime))
	}
}

// StartCleanupRoutine starts a background routine to cleanup timed out transactions
func (tm *TransactionManager) StartCleanupRoutine() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			tm.CleanupTimedOutTransactions()
		}
	}()
}
