package coordinator

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"mini-lakehouse/pkg/storage"
	"mini-lakehouse/proto/gen"
)

// CompactionService handles table compaction operations
type CompactionService struct {
	metadataClient MetadataClient
	storageClient  *storage.Client
	txnManager     *TransactionManager

	// Configuration
	minFileSize   int64 // Files smaller than this are compaction candidates
	maxFileSize   int64 // Target size for compacted files
	minFilesCount int   // Minimum number of small files to trigger compaction
}

// CompactionCandidate represents a file that is a candidate for compaction
type CompactionCandidate struct {
	Path string
	Size int64
	Rows int64
}

// CompactionPlan represents a plan for compacting files
type CompactionPlan struct {
	TableName     string
	BaseVersion   uint64
	InputFiles    []CompactionCandidate
	OutputPath    string
	TxnID         string
	EstimatedSize int64
	EstimatedRows int64
}

// CompactionConfig holds configuration for compaction operations
type CompactionConfig struct {
	MinFileSize   int64 // 10MB default
	MaxFileSize   int64 // 128MB default
	MinFilesCount int   // 3 files minimum to trigger compaction
}

// NewCompactionService creates a new compaction service
func NewCompactionService(
	metadataClient MetadataClient,
	storageClient *storage.Client,
	txnManager *TransactionManager,
	config *CompactionConfig,
) *CompactionService {
	if config == nil {
		config = &CompactionConfig{
			MinFileSize:   10 * 1024 * 1024,  // 10MB
			MaxFileSize:   128 * 1024 * 1024, // 128MB
			MinFilesCount: 3,
		}
	}

	return &CompactionService{
		metadataClient: metadataClient,
		storageClient:  storageClient,
		txnManager:     txnManager,
		minFileSize:    config.MinFileSize,
		maxFileSize:    config.MaxFileSize,
		minFilesCount:  config.MinFilesCount,
	}
}

// IdentifyCompactionCandidates identifies files that are candidates for compaction
// based on size thresholds and returns a compaction plan if compaction is needed
func (cs *CompactionService) IdentifyCompactionCandidates(ctx context.Context, tableName string) (*CompactionPlan, error) {
	log.Printf("Identifying compaction candidates for table %s", tableName)

	// Get the latest version of the table
	latestResp, err := cs.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version: %w", err)
	}
	if latestResp.Error != "" {
		return nil, fmt.Errorf("failed to get latest version: %s", latestResp.Error)
	}

	baseVersion := latestResp.Version

	// Get the snapshot for the latest version
	snapshotResp, err := cs.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   baseVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}
	if snapshotResp.Error != "" {
		return nil, fmt.Errorf("failed to get snapshot: %s", snapshotResp.Error)
	}

	// Analyze files to identify compaction candidates
	candidates := cs.analyzeFilesForCompaction(snapshotResp.Files)

	if len(candidates) < cs.minFilesCount {
		log.Printf("Table %s has %d small files, minimum %d required for compaction",
			tableName, len(candidates), cs.minFilesCount)
		return nil, nil // No compaction needed
	}

	// Group candidates into compaction groups
	compactionGroups := cs.groupCandidatesForCompaction(candidates)

	if len(compactionGroups) == 0 {
		log.Printf("No suitable compaction groups found for table %s", tableName)
		return nil, nil
	}

	// For now, return the first compaction group as a plan
	// In production, we might want to prioritize groups or compact multiple groups
	firstGroup := compactionGroups[0]

	plan := &CompactionPlan{
		TableName:     tableName,
		BaseVersion:   baseVersion,
		InputFiles:    firstGroup,
		TxnID:         cs.txnManager.GenerateTransactionID(),
		EstimatedSize: cs.calculateEstimatedSize(firstGroup),
		EstimatedRows: cs.calculateEstimatedRows(firstGroup),
	}

	log.Printf("Created compaction plan for table %s: %d files -> estimated %d bytes, %d rows",
		tableName, len(plan.InputFiles), plan.EstimatedSize, plan.EstimatedRows)

	return plan, nil
}

// analyzeFilesForCompaction analyzes files and returns those that are candidates for compaction
func (cs *CompactionService) analyzeFilesForCompaction(files []*gen.FileInfo) []CompactionCandidate {
	var candidates []CompactionCandidate

	for _, file := range files {
		// Check if file is smaller than the minimum size threshold
		if int64(file.Size) < cs.minFileSize {
			candidate := CompactionCandidate{
				Path: file.Path,
				Size: int64(file.Size),
				Rows: int64(file.Rows),
			}
			candidates = append(candidates, candidate)
		}
	}

	// Sort candidates by size (smallest first) for better grouping
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Size < candidates[j].Size
	})

	log.Printf("Found %d compaction candidates out of %d total files", len(candidates), len(files))

	return candidates
}

// groupCandidatesForCompaction groups candidates into compaction groups
// Each group should not exceed the maximum file size when compacted
func (cs *CompactionService) groupCandidatesForCompaction(candidates []CompactionCandidate) [][]CompactionCandidate {
	var groups [][]CompactionCandidate
	var currentGroup []CompactionCandidate
	var currentGroupSize int64

	for _, candidate := range candidates {
		// Check if adding this candidate would exceed the max file size
		if currentGroupSize+candidate.Size > cs.maxFileSize && len(currentGroup) > 0 {
			// Start a new group if current group has at least minimum files
			if len(currentGroup) >= cs.minFilesCount {
				groups = append(groups, currentGroup)
			}
			currentGroup = []CompactionCandidate{candidate}
			currentGroupSize = candidate.Size
		} else {
			// Add to current group
			currentGroup = append(currentGroup, candidate)
			currentGroupSize += candidate.Size
		}
	}

	// Add the last group if it meets the minimum files requirement
	if len(currentGroup) >= cs.minFilesCount {
		groups = append(groups, currentGroup)
	}

	log.Printf("Grouped %d candidates into %d compaction groups", len(candidates), len(groups))

	return groups
}

// calculateEstimatedSize calculates the estimated size of the compacted file
func (cs *CompactionService) calculateEstimatedSize(candidates []CompactionCandidate) int64 {
	var totalSize int64
	for _, candidate := range candidates {
		totalSize += candidate.Size
	}

	// Apply compression factor (Parquet typically compresses well)
	// Assume 10% compression improvement from better column encoding
	return int64(float64(totalSize) * 0.9)
}

// calculateEstimatedRows calculates the estimated number of rows in the compacted file
func (cs *CompactionService) calculateEstimatedRows(candidates []CompactionCandidate) int64 {
	var totalRows int64
	for _, candidate := range candidates {
		totalRows += candidate.Rows
	}
	return totalRows
}

// GetCompactionMetrics returns metrics about compaction candidates for a table
func (cs *CompactionService) GetCompactionMetrics(ctx context.Context, tableName string) (*CompactionMetrics, error) {
	// Get the latest version of the table
	latestResp, err := cs.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version: %w", err)
	}
	if latestResp.Error != "" {
		return nil, fmt.Errorf("failed to get latest version: %s", latestResp.Error)
	}

	// Get the snapshot for the latest version
	snapshotResp, err := cs.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   latestResp.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}
	if snapshotResp.Error != "" {
		return nil, fmt.Errorf("failed to get snapshot: %s", snapshotResp.Error)
	}

	// Analyze files
	candidates := cs.analyzeFilesForCompaction(snapshotResp.Files)

	metrics := &CompactionMetrics{
		TableName:        tableName,
		Version:          latestResp.Version,
		TotalFiles:       len(snapshotResp.Files),
		SmallFiles:       len(candidates),
		CompactionNeeded: len(candidates) >= cs.minFilesCount,
		Timestamp:        time.Now(),
	}

	// Calculate size metrics
	var totalSize, smallFilesSize int64
	for _, file := range snapshotResp.Files {
		totalSize += int64(file.Size)
	}
	for _, candidate := range candidates {
		smallFilesSize += candidate.Size
	}

	metrics.TotalSize = totalSize
	metrics.SmallFilesSize = smallFilesSize

	if len(candidates) > 0 {
		metrics.AverageSmallFileSize = smallFilesSize / int64(len(candidates))
	}

	// Calculate potential space savings
	if len(candidates) >= cs.minFilesCount {
		groups := cs.groupCandidatesForCompaction(candidates)
		var potentialSavings int64
		for _, group := range groups {
			originalSize := cs.calculateOriginalSize(group)
			compactedSize := cs.calculateEstimatedSize(group)
			potentialSavings += originalSize - compactedSize
		}
		metrics.PotentialSavings = potentialSavings
	}

	return metrics, nil
}

// calculateOriginalSize calculates the original total size of candidates
func (cs *CompactionService) calculateOriginalSize(candidates []CompactionCandidate) int64 {
	var totalSize int64
	for _, candidate := range candidates {
		totalSize += candidate.Size
	}
	return totalSize
}

// CompactionMetrics represents metrics about compaction for a table
type CompactionMetrics struct {
	TableName            string
	Version              uint64
	TotalFiles           int
	SmallFiles           int
	CompactionNeeded     bool
	TotalSize            int64
	SmallFilesSize       int64
	AverageSmallFileSize int64
	PotentialSavings     int64
	Timestamp            time.Time
}

// ShouldTriggerCompaction determines if compaction should be triggered for a table
func (cs *CompactionService) ShouldTriggerCompaction(ctx context.Context, tableName string) (bool, error) {
	metrics, err := cs.GetCompactionMetrics(ctx, tableName)
	if err != nil {
		return false, fmt.Errorf("failed to get compaction metrics: %w", err)
	}

	// Trigger compaction if:
	// 1. We have enough small files
	// 2. Small files represent a significant portion of total storage
	smallFilesRatio := float64(metrics.SmallFilesSize) / float64(metrics.TotalSize)

	shouldCompact := metrics.CompactionNeeded && smallFilesRatio > 0.1 // 10% threshold

	log.Printf("Compaction decision for table %s: needed=%v, ratio=%.2f, trigger=%v",
		tableName, metrics.CompactionNeeded, smallFilesRatio, shouldCompact)

	return shouldCompact, nil
}

// ExecuteCompaction executes a compaction plan by reading multiple small files
// and writing fewer large files
func (cs *CompactionService) ExecuteCompaction(ctx context.Context, plan *CompactionPlan) (*CompactionResult, error) {
	log.Printf("Executing compaction for table %s with %d input files", plan.TableName, len(plan.InputFiles))

	// Start the transaction
	txnID := cs.txnManager.StartTransaction(plan.TableName, plan.BaseVersion)
	plan.TxnID = txnID

	startTime := time.Now()

	// Create temporary output path for compacted file
	tempPath := fmt.Sprintf("tables/%s/_tmp/%s/compacted", plan.TableName, txnID)
	plan.OutputPath = tempPath

	// Execute the compaction job
	result, err := cs.executeCompactionJob(ctx, plan)
	if err != nil {
		return &CompactionResult{
			Success:  false,
			Error:    fmt.Sprintf("compaction job failed: %v", err),
			TxnID:    txnID,
			Duration: time.Since(startTime),
		}, nil
	}

	if !result.Success {
		return result, nil
	}

	// Commit the compaction transaction
	newVersion, err := cs.commitCompaction(ctx, plan, result)
	if err != nil {
		return &CompactionResult{
			Success:  false,
			Error:    fmt.Sprintf("failed to commit compaction: %v", err),
			TxnID:    txnID,
			Duration: time.Since(startTime),
		}, nil
	}

	result.NewVersion = newVersion
	result.Duration = time.Since(startTime)

	log.Printf("Compaction completed successfully for table %s: %d files -> 1 file, new version: %d",
		plan.TableName, len(plan.InputFiles), newVersion)

	return result, nil
}

// executeCompactionJob executes the actual compaction work
func (cs *CompactionService) executeCompactionJob(ctx context.Context, plan *CompactionPlan) (*CompactionResult, error) {
	// For MVP, we'll simulate the compaction process
	// In production, this would involve:
	// 1. Reading all input Parquet files
	// 2. Merging the data while maintaining sort order
	// 3. Writing a new consolidated Parquet file
	// 4. Calculating statistics and metadata

	log.Printf("Simulating compaction job for %d files", len(plan.InputFiles))

	// Simulate processing time based on data size
	processingTime := time.Duration(plan.EstimatedSize/1024/1024) * time.Millisecond * 10 // 10ms per MB
	if processingTime < 100*time.Millisecond {
		processingTime = 100 * time.Millisecond
	}
	if processingTime > 5*time.Second {
		processingTime = 5 * time.Second
	}

	time.Sleep(processingTime)

	// Generate output file path
	outputFileName := fmt.Sprintf("part-compacted-%d.parquet", time.Now().UnixNano())
	outputPath := fmt.Sprintf("%s/%s", plan.OutputPath, outputFileName)

	// Calculate actual metrics (in production, these would come from the compaction process)
	actualSize := cs.calculateEstimatedSize(plan.InputFiles) // Use estimated size for simulation
	actualRows := cs.calculateEstimatedRows(plan.InputFiles)

	result := &CompactionResult{
		Success:    true,
		TxnID:      plan.TxnID,
		InputFiles: len(plan.InputFiles),
		OutputFiles: []CompactionOutput{
			{
				Path: outputPath,
				Size: actualSize,
				Rows: actualRows,
			},
		},
		BytesRead:    cs.calculateOriginalSize(plan.InputFiles),
		BytesWritten: actualSize,
	}

	log.Printf("Compaction job completed: read %d bytes, wrote %d bytes",
		result.BytesRead, result.BytesWritten)

	return result, nil
}

// commitCompaction commits the compaction transaction with atomic file adds/removes
func (cs *CompactionService) commitCompaction(ctx context.Context, plan *CompactionPlan, result *CompactionResult) (uint64, error) {
	log.Printf("Committing compaction transaction %s", plan.TxnID)

	// Prepare file additions (new compacted files)
	var fileAdds []*gen.FileAdd
	for i, output := range result.OutputFiles {
		// Generate final path for the compacted file
		finalPath := fmt.Sprintf("data/compacted-%05d-%s.parquet", i, plan.TxnID)

		// Move file from temp to final location (simulated for MVP)
		err := cs.moveCompactedFile(ctx, output.Path, plan.TableName, finalPath)
		if err != nil {
			return 0, fmt.Errorf("failed to move compacted file: %w", err)
		}

		fileAdd := &gen.FileAdd{
			Path:      finalPath,
			Rows:      uint64(output.Rows),
			Size:      uint64(output.Size),
			Partition: make(map[string]string), // No partitioning for MVP
			Stats: &gen.FileStats{
				MinValues: make(map[string]string), // TODO: Calculate actual stats
				MaxValues: make(map[string]string),
			},
		}

		fileAdds = append(fileAdds, fileAdd)
	}

	// Prepare file removals (old small files being compacted)
	var fileRemoves []*gen.FileRemove
	for _, inputFile := range plan.InputFiles {
		fileRemove := &gen.FileRemove{
			Path: inputFile.Path,
		}
		fileRemoves = append(fileRemoves, fileRemove)
	}

	// Commit the transaction using transaction manager for retry logic
	commitResp, err := cs.txnManager.CommitTransaction(
		cs.metadataClient,
		plan.TxnID,
		fileAdds,
		fileRemoves,
	)
	if err != nil {
		return 0, fmt.Errorf("transaction commit failed: %w", err)
	}

	if commitResp.Error != "" {
		return 0, fmt.Errorf("commit failed: %s", commitResp.Error)
	}

	// Clean up temporary files
	err = cs.cleanupCompactionTempFiles(ctx, plan.TableName, plan.TxnID)
	if err != nil {
		log.Printf("Warning: failed to clean up compaction temp files: %v", err)
	}

	return commitResp.NewVersion, nil
}

// moveCompactedFile moves a compacted file from temporary location to final data location
func (cs *CompactionService) moveCompactedFile(ctx context.Context, tempPath, tableName, finalPath string) error {
	// For MVP, we'll simulate the file move operation
	// In production, this would involve actual S3 copy operations
	log.Printf("Moving compacted file from %s to %s", tempPath, finalPath)

	// Simulate file move operation
	time.Sleep(10 * time.Millisecond)

	return nil
}

// cleanupCompactionTempFiles removes temporary files after successful compaction commit
func (cs *CompactionService) cleanupCompactionTempFiles(ctx context.Context, tableName, txnID string) error {
	tempPrefix := fmt.Sprintf("tables/%s/_tmp/%s/", tableName, txnID)

	log.Printf("Cleaning up compaction temp files with prefix: %s", tempPrefix)

	// For MVP, we'll simulate the cleanup
	// In production, this would involve listing and deleting S3 objects
	time.Sleep(5 * time.Millisecond)

	return nil
}

// CompactionResult represents the result of a compaction operation
type CompactionResult struct {
	Success      bool
	Error        string
	TxnID        string
	NewVersion   uint64
	InputFiles   int
	OutputFiles  []CompactionOutput
	BytesRead    int64
	BytesWritten int64
	Duration     time.Duration
}

// CompactionOutput represents an output file from compaction
type CompactionOutput struct {
	Path string
	Size int64
	Rows int64
}

// RunCompactionCycle runs a complete compaction cycle for a table
// This includes identification, planning, and execution
func (cs *CompactionService) RunCompactionCycle(ctx context.Context, tableName string) (*CompactionResult, error) {
	log.Printf("Running compaction cycle for table %s", tableName)

	// Check if compaction is needed
	shouldCompact, err := cs.ShouldTriggerCompaction(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to check compaction trigger: %w", err)
	}

	if !shouldCompact {
		log.Printf("Compaction not needed for table %s", tableName)
		return &CompactionResult{
			Success: true,
			Error:   "compaction not needed",
		}, nil
	}

	// Identify compaction candidates and create plan
	plan, err := cs.IdentifyCompactionCandidates(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to identify compaction candidates: %w", err)
	}

	if plan == nil {
		log.Printf("No compaction plan generated for table %s", tableName)
		return &CompactionResult{
			Success: true,
			Error:   "no compaction plan needed",
		}, nil
	}

	// Execute the compaction
	result, err := cs.ExecuteCompaction(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute compaction: %w", err)
	}

	return result, nil
}

// GetCompactionHistory returns the history of compaction operations for a table
func (cs *CompactionService) GetCompactionHistory(ctx context.Context, tableName string, limit int) ([]CompactionHistoryEntry, error) {
	// For MVP, we'll return an empty history
	// In production, this would query a compaction history store
	return []CompactionHistoryEntry{}, nil
}

// CompactionHistoryEntry represents a historical compaction operation
type CompactionHistoryEntry struct {
	TxnID        string
	TableName    string
	Version      uint64
	InputFiles   int
	OutputFiles  int
	BytesRead    int64
	BytesWritten int64
	Duration     time.Duration
	Timestamp    time.Time
	Success      bool
	Error        string
}

// CompactionCoordinator manages concurrent compaction operations and ensures safety
type CompactionCoordinator struct {
	compactionService *CompactionService
	mu                sync.RWMutex
	activeCompactions map[string]*ActiveCompaction // tableName -> compaction info
	maxConcurrent     int
}

// ActiveCompaction represents an ongoing compaction operation
type ActiveCompaction struct {
	TableName   string
	TxnID       string
	BaseVersion uint64
	StartTime   time.Time
	Status      CompactionStatus
}

// CompactionStatus represents the status of a compaction operation
type CompactionStatus int

const (
	CompactionPending CompactionStatus = iota
	CompactionRunning
	CompactionCompleted
	CompactionFailed
)

func (cs CompactionStatus) String() string {
	switch cs {
	case CompactionPending:
		return "pending"
	case CompactionRunning:
		return "running"
	case CompactionCompleted:
		return "completed"
	case CompactionFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// NewCompactionCoordinator creates a new compaction coordinator
func NewCompactionCoordinator(compactionService *CompactionService) *CompactionCoordinator {
	return &CompactionCoordinator{
		compactionService: compactionService,
		activeCompactions: make(map[string]*ActiveCompaction),
		maxConcurrent:     3, // Limit concurrent compactions
	}
}

// SafeExecuteCompaction executes compaction with safety checks for concurrent operations
func (cc *CompactionCoordinator) SafeExecuteCompaction(ctx context.Context, tableName string) (*CompactionResult, error) {
	log.Printf("Starting safe compaction execution for table %s", tableName)

	// Check if compaction is already running for this table
	cc.mu.Lock()
	if active, exists := cc.activeCompactions[tableName]; exists {
		cc.mu.Unlock()
		return &CompactionResult{
			Success: false,
			Error:   fmt.Sprintf("compaction already running for table %s (txn: %s)", tableName, active.TxnID),
		}, nil
	}

	// Check if we've reached the maximum concurrent compactions
	if len(cc.activeCompactions) >= cc.maxConcurrent {
		cc.mu.Unlock()
		return &CompactionResult{
			Success: false,
			Error:   fmt.Sprintf("maximum concurrent compactions reached (%d)", cc.maxConcurrent),
		}, nil
	}

	// Reserve the compaction slot
	activeCompaction := &ActiveCompaction{
		TableName: tableName,
		StartTime: time.Now(),
		Status:    CompactionPending,
	}
	cc.activeCompactions[tableName] = activeCompaction
	cc.mu.Unlock()

	// Ensure cleanup on exit
	defer func() {
		cc.mu.Lock()
		delete(cc.activeCompactions, tableName)
		cc.mu.Unlock()
	}()

	// Execute compaction with version validation
	result, err := cc.executeCompactionWithVersionValidation(ctx, tableName, activeCompaction)
	if err != nil {
		activeCompaction.Status = CompactionFailed
		return &CompactionResult{
			Success: false,
			Error:   fmt.Sprintf("compaction failed: %v", err),
		}, nil
	}

	activeCompaction.Status = CompactionCompleted
	return result, nil
}

// executeCompactionWithVersionValidation executes compaction with proper version validation
func (cc *CompactionCoordinator) executeCompactionWithVersionValidation(ctx context.Context, tableName string, activeCompaction *ActiveCompaction) (*CompactionResult, error) {
	// Get the current version before starting compaction
	latestResp, err := cc.compactionService.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version: %w", err)
	}
	if latestResp.Error != "" {
		return nil, fmt.Errorf("failed to get latest version: %s", latestResp.Error)
	}

	baseVersion := latestResp.Version
	activeCompaction.BaseVersion = baseVersion

	log.Printf("Starting compaction for table %s at base version %d", tableName, baseVersion)

	// Identify compaction candidates at this specific version
	plan, err := cc.identifyCompactionCandidatesAtVersion(ctx, tableName, baseVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to identify compaction candidates: %w", err)
	}

	if plan == nil {
		return &CompactionResult{
			Success: true,
			Error:   "no compaction needed",
		}, nil
	}

	activeCompaction.TxnID = plan.TxnID
	activeCompaction.Status = CompactionRunning

	// Execute compaction with retry logic for version conflicts
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retrying compaction for table %s (attempt %d/%d)", tableName, attempt+1, maxRetries)

			// Re-check the version before retry
			currentResp, err := cc.compactionService.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
				TableName: tableName,
			})
			if err != nil {
				lastErr = fmt.Errorf("failed to get current version for retry: %w", err)
				continue
			}
			if currentResp.Error != "" {
				lastErr = fmt.Errorf("failed to get current version for retry: %s", currentResp.Error)
				continue
			}

			// If version has changed, we need to re-plan the compaction
			if currentResp.Version != baseVersion {
				log.Printf("Table version changed from %d to %d, re-planning compaction", baseVersion, currentResp.Version)

				newPlan, err := cc.identifyCompactionCandidatesAtVersion(ctx, tableName, currentResp.Version)
				if err != nil {
					lastErr = fmt.Errorf("failed to re-plan compaction: %w", err)
					continue
				}

				if newPlan == nil {
					// No compaction needed at new version
					return &CompactionResult{
						Success: true,
						Error:   "no compaction needed after version change",
					}, nil
				}

				plan = newPlan
				baseVersion = currentResp.Version
				activeCompaction.BaseVersion = baseVersion
				activeCompaction.TxnID = plan.TxnID
			}
		}

		// Execute the compaction
		result, err := cc.compactionService.ExecuteCompaction(ctx, plan)
		if err != nil {
			lastErr = err
			continue
		}

		if !result.Success {
			// Check if this is a version conflict error
			if cc.isVersionConflictError(result.Error) {
				log.Printf("Version conflict during compaction commit, will retry: %s", result.Error)
				lastErr = fmt.Errorf("version conflict: %s", result.Error)
				continue
			} else {
				// Non-retryable error
				return result, nil
			}
		}

		// Success
		log.Printf("Compaction completed successfully for table %s after %d attempts", tableName, attempt+1)
		return result, nil
	}

	// All retries exhausted
	return &CompactionResult{
		Success: false,
		Error:   fmt.Sprintf("compaction failed after %d attempts: %v", maxRetries, lastErr),
	}, nil
}

// identifyCompactionCandidatesAtVersion identifies compaction candidates at a specific version
func (cc *CompactionCoordinator) identifyCompactionCandidatesAtVersion(ctx context.Context, tableName string, version uint64) (*CompactionPlan, error) {
	// Get the snapshot for the specific version
	snapshotResp, err := cc.compactionService.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: tableName,
		Version:   version,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}
	if snapshotResp.Error != "" {
		return nil, fmt.Errorf("failed to get snapshot: %s", snapshotResp.Error)
	}

	// Analyze files to identify compaction candidates
	candidates := cc.compactionService.analyzeFilesForCompaction(snapshotResp.Files)

	if len(candidates) < cc.compactionService.minFilesCount {
		log.Printf("Table %s at version %d has %d small files, minimum %d required for compaction",
			tableName, version, len(candidates), cc.compactionService.minFilesCount)
		return nil, nil // No compaction needed
	}

	// Group candidates into compaction groups
	compactionGroups := cc.compactionService.groupCandidatesForCompaction(candidates)

	if len(compactionGroups) == 0 {
		log.Printf("No suitable compaction groups found for table %s at version %d", tableName, version)
		return nil, nil
	}

	// Return the first compaction group as a plan
	firstGroup := compactionGroups[0]

	plan := &CompactionPlan{
		TableName:     tableName,
		BaseVersion:   version,
		InputFiles:    firstGroup,
		TxnID:         cc.compactionService.txnManager.GenerateTransactionID(),
		EstimatedSize: cc.compactionService.calculateEstimatedSize(firstGroup),
		EstimatedRows: cc.compactionService.calculateEstimatedRows(firstGroup),
	}

	return plan, nil
}

// isVersionConflictError checks if an error indicates a version conflict
func (cc *CompactionCoordinator) isVersionConflictError(errorMsg string) bool {
	return contains(errorMsg, "optimistic concurrency") ||
		contains(errorMsg, "version conflict") ||
		contains(errorMsg, "base version") ||
		contains(errorMsg, "does not match") ||
		contains(errorMsg, "current latest version")
}

// GetActiveCompactions returns information about currently active compactions
func (cc *CompactionCoordinator) GetActiveCompactions() map[string]*ActiveCompaction {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	result := make(map[string]*ActiveCompaction)
	for tableName, compaction := range cc.activeCompactions {
		// Create a copy to avoid race conditions
		result[tableName] = &ActiveCompaction{
			TableName:   compaction.TableName,
			TxnID:       compaction.TxnID,
			BaseVersion: compaction.BaseVersion,
			StartTime:   compaction.StartTime,
			Status:      compaction.Status,
		}
	}

	return result
}

// IsCompactionRunning checks if compaction is currently running for a table
func (cc *CompactionCoordinator) IsCompactionRunning(tableName string) bool {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	_, exists := cc.activeCompactions[tableName]
	return exists
}

// CancelCompaction attempts to cancel a running compaction (best effort)
func (cc *CompactionCoordinator) CancelCompaction(tableName string) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	active, exists := cc.activeCompactions[tableName]
	if !exists {
		return fmt.Errorf("no active compaction found for table %s", tableName)
	}

	// For MVP, we'll just mark it as failed
	// In production, this would involve canceling the actual compaction job
	active.Status = CompactionFailed
	delete(cc.activeCompactions, tableName)

	log.Printf("Cancelled compaction for table %s (txn: %s)", tableName, active.TxnID)
	return nil
}

// ValidateCompactionSafety validates that compaction can be safely performed
func (cc *CompactionCoordinator) ValidateCompactionSafety(ctx context.Context, tableName string) error {
	// Check if table exists and is accessible
	latestResp, err := cc.compactionService.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
		TableName: tableName,
	})
	if err != nil {
		return fmt.Errorf("failed to validate table access: %w", err)
	}
	if latestResp.Error != "" {
		return fmt.Errorf("table validation failed: %s", latestResp.Error)
	}

	// Check if compaction is already running
	if cc.IsCompactionRunning(tableName) {
		return fmt.Errorf("compaction already running for table %s", tableName)
	}

	// Check system resources (simplified for MVP)
	activeCount := len(cc.GetActiveCompactions())
	if activeCount >= cc.maxConcurrent {
		return fmt.Errorf("maximum concurrent compactions reached (%d/%d)", activeCount, cc.maxConcurrent)
	}

	return nil
}
