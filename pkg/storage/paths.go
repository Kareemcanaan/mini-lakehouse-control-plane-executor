package storage

import (
	"fmt"
	"path"
	"strings"
)

// PathBuilder provides utilities for constructing object storage paths
type PathBuilder struct{}

// NewPathBuilder creates a new path builder
func NewPathBuilder() *PathBuilder {
	return &PathBuilder{}
}

// TableDataPath returns the path for table data files
// Format: tables/<table_name>/data/part-<id>-<uuid>.parquet
func (pb *PathBuilder) TableDataPath(tableName, partID, uuid string) string {
	filename := fmt.Sprintf("part-%s-%s.parquet", partID, uuid)
	return path.Join("tables", tableName, "data", filename)
}

// TableDataDir returns the directory path for table data
// Format: tables/<table_name>/data/
func (pb *PathBuilder) TableDataDir(tableName string) string {
	return path.Join("tables", tableName, "data") + "/"
}

// TransactionLogPath returns the path for a specific transaction log entry
// Format: tables/<table_name>/_log/<version>.json
func (pb *PathBuilder) TransactionLogPath(tableName string, version uint64) string {
	filename := fmt.Sprintf("%020d.json", version)
	return path.Join("tables", tableName, "_log", filename)
}

// TransactionLogDir returns the directory path for transaction logs
// Format: tables/<table_name>/_log/
func (pb *PathBuilder) TransactionLogDir(tableName string) string {
	return path.Join("tables", tableName, "_log") + "/"
}

// CheckpointPath returns the path for a checkpoint file (optional, for v2)
// Format: tables/<table_name>/_log/checkpoint_<version>.json
func (pb *PathBuilder) CheckpointPath(tableName string, version uint64) string {
	filename := fmt.Sprintf("checkpoint_%020d.json", version)
	return path.Join("tables", tableName, "_log", filename)
}

// TempDataPath returns the path for temporary data files during transactions
// Format: tables/<table_name>/_tmp/<txn_id>/<attempt>/part-<id>-<uuid>.parquet
func (pb *PathBuilder) TempDataPath(tableName, txnID string, attempt int, partID, uuid string) string {
	filename := fmt.Sprintf("part-%s-%s.parquet", partID, uuid)
	return path.Join("tables", tableName, "_tmp", txnID, fmt.Sprintf("%d", attempt), filename)
}

// TempDataDir returns the directory path for temporary data files
// Format: tables/<table_name>/_tmp/<txn_id>/<attempt>/
func (pb *PathBuilder) TempDataDir(tableName, txnID string, attempt int) string {
	return path.Join("tables", tableName, "_tmp", txnID, fmt.Sprintf("%d", attempt)) + "/"
}

// ShuffleOutputPath returns the path for shuffle output files
// Format: shuffle/<job_id>/<stage_id>/<task_id>/<attempt>/part-<partition>.parquet
func (pb *PathBuilder) ShuffleOutputPath(jobID, stageID string, taskID int, attempt int, partition int) string {
	filename := fmt.Sprintf("part-%d.parquet", partition)
	return path.Join("shuffle", jobID, stageID, fmt.Sprintf("%d", taskID), fmt.Sprintf("%d", attempt), filename)
}

// ShuffleOutputDir returns the directory path for shuffle outputs
// Format: shuffle/<job_id>/<stage_id>/<task_id>/<attempt>/
func (pb *PathBuilder) ShuffleOutputDir(jobID, stageID string, taskID int, attempt int) string {
	return path.Join("shuffle", jobID, stageID, fmt.Sprintf("%d", taskID), fmt.Sprintf("%d", attempt)) + "/"
}

// ShuffleSuccessPath returns the path for the SUCCESS manifest
// Format: shuffle/<job_id>/<stage_id>/<task_id>/SUCCESS.json
func (pb *PathBuilder) ShuffleSuccessPath(jobID, stageID string, taskID int) string {
	return path.Join("shuffle", jobID, stageID, fmt.Sprintf("%d", taskID), "SUCCESS.json")
}

// ResultPath returns the path for query result files
// Format: results/<job_id>/result.parquet
func (pb *PathBuilder) ResultPath(jobID string) string {
	return path.Join("results", jobID, "result.parquet")
}

// ResultDir returns the directory path for query results
// Format: results/<job_id>/
func (pb *PathBuilder) ResultDir(jobID string) string {
	return path.Join("results", jobID) + "/"
}

// ParseTableDataPath extracts table name, part ID, and UUID from a data path
func (pb *PathBuilder) ParseTableDataPath(path string) (tableName, partID, uuid string, err error) {
	// Expected format: tables/<table_name>/data/part-<id>-<uuid>.parquet
	parts := strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "tables" || parts[2] != "data" {
		return "", "", "", fmt.Errorf("invalid table data path format: %s", path)
	}

	tableName = parts[1]
	filename := parts[3]

	// Remove .parquet extension
	if !strings.HasSuffix(filename, ".parquet") {
		return "", "", "", fmt.Errorf("invalid parquet file extension: %s", filename)
	}
	filename = strings.TrimSuffix(filename, ".parquet")

	// Parse part-<id>-<uuid>
	if !strings.HasPrefix(filename, "part-") {
		return "", "", "", fmt.Errorf("invalid part file prefix: %s", filename)
	}
	filename = strings.TrimPrefix(filename, "part-")

	// Split by last dash to separate ID and UUID
	lastDash := strings.LastIndex(filename, "-")
	if lastDash == -1 {
		return "", "", "", fmt.Errorf("invalid part file format: %s", filename)
	}

	partID = filename[:lastDash]
	uuid = filename[lastDash+1:]

	return tableName, partID, uuid, nil
}

// ParseTransactionLogPath extracts table name and version from a transaction log path
func (pb *PathBuilder) ParseTransactionLogPath(path string) (tableName string, version uint64, err error) {
	// Expected format: tables/<table_name>/_log/<version>.json
	parts := strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "tables" || parts[2] != "_log" {
		return "", 0, fmt.Errorf("invalid transaction log path format: %s", path)
	}

	tableName = parts[1]
	filename := parts[3]

	// Remove .json extension
	if !strings.HasSuffix(filename, ".json") {
		return "", 0, fmt.Errorf("invalid json file extension: %s", filename)
	}
	filename = strings.TrimSuffix(filename, ".json")

	// Parse version number - must be exactly 20 digits
	if len(filename) != 20 {
		return "", 0, fmt.Errorf("invalid version format: %s", filename)
	}

	var v uint64
	n, err := fmt.Sscanf(filename, "%020d", &v)
	if err != nil || n != 1 {
		return "", 0, fmt.Errorf("invalid version format: %s", filename)
	}

	return tableName, v, nil
}
