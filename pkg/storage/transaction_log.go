package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Schema represents the table schema
type Schema struct {
	Fields []Field `json:"fields"`
}

// Field represents a column in the table schema
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// FileAdd represents a file being added to the table
type FileAdd struct {
	Path      string            `json:"path"`
	Rows      int64             `json:"rows"`
	Size      int64             `json:"size"`
	Partition map[string]string `json:"partition"`
	Stats     *FileStats        `json:"stats,omitempty"`
}

// FileRemove represents a file being removed from the table
type FileRemove struct {
	Path string `json:"path"`
}

// FileStats contains statistics about the file
type FileStats struct {
	MinValues map[string]interface{} `json:"min_values,omitempty"`
	MaxValues map[string]interface{} `json:"max_values,omitempty"`
}

// TransactionLogEntry represents a single entry in the transaction log
type TransactionLogEntry struct {
	Version     uint64       `json:"version"`
	TimestampMs int64        `json:"timestamp_ms"`
	TxnID       string       `json:"txn_id"`
	Schema      *Schema      `json:"schema,omitempty"`
	Adds        []FileAdd    `json:"adds,omitempty"`
	Removes     []FileRemove `json:"removes,omitempty"`
}

// TableSnapshot represents the state of a table at a specific version
type TableSnapshot struct {
	Version uint64
	Schema  *Schema
	Files   []FileAdd
}

// TransactionLog manages transaction log operations
type TransactionLog struct {
	client      *Client
	pathBuilder *PathBuilder
}

// NewTransactionLog creates a new transaction log manager
func NewTransactionLog(client *Client) *TransactionLog {
	return &TransactionLog{
		client:      client,
		pathBuilder: NewPathBuilder(),
	}
}

// WriteLogEntry writes a transaction log entry to object storage
func (tl *TransactionLog) WriteLogEntry(ctx context.Context, tableName string, entry *TransactionLogEntry) error {
	// Serialize the entry to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	// Create the object path
	objectPath := tl.pathBuilder.TransactionLogPath(tableName, entry.Version)

	// Write to object storage
	reader := strings.NewReader(string(data))
	err = tl.client.PutObject(ctx, objectPath, reader, int64(len(data)), "application/json")
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}

	return nil
}

// ReadLogEntry reads a specific transaction log entry from object storage
func (tl *TransactionLog) ReadLogEntry(ctx context.Context, tableName string, version uint64) (*TransactionLogEntry, error) {
	// Create the object path
	objectPath := tl.pathBuilder.TransactionLogPath(tableName, version)

	// Read from object storage
	obj, err := tl.client.GetObject(ctx, objectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get log entry: %w", err)
	}
	defer obj.Close()

	// Read all data
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read log entry data: %w", err)
	}

	// Deserialize the entry
	var entry TransactionLogEntry
	err = json.Unmarshal(data, &entry)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal log entry: %w", err)
	}

	return &entry, nil
}

// ListLogEntries lists all transaction log entries for a table
func (tl *TransactionLog) ListLogEntries(ctx context.Context, tableName string) ([]uint64, error) {
	// Get the log directory prefix
	prefix := tl.pathBuilder.TransactionLogDir(tableName)

	// List objects with the prefix
	objects, err := tl.client.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	var versions []uint64
	for _, object := range objects {
		// Parse the version from the object path
		_, version, err := tl.pathBuilder.ParseTransactionLogPath(object.Key)
		if err != nil {
			// Skip invalid paths (might be checkpoint files or other artifacts)
			continue
		}

		versions = append(versions, version)
	}

	// Sort versions in ascending order
	sort.Slice(versions, func(i, j int) bool {
		return versions[i] < versions[j]
	})

	return versions, nil
}

// BuildSnapshot builds a table snapshot by replaying log entries up to a specific version
func (tl *TransactionLog) BuildSnapshot(ctx context.Context, tableName string, targetVersion uint64) (*TableSnapshot, error) {
	// List all log entries
	versions, err := tl.ListLogEntries(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to list log entries: %w", err)
	}

	// Filter versions up to target version
	var applicableVersions []uint64
	for _, version := range versions {
		if version <= targetVersion {
			applicableVersions = append(applicableVersions, version)
		}
	}

	if len(applicableVersions) == 0 {
		return nil, fmt.Errorf("no log entries found for version %d", targetVersion)
	}

	// Initialize snapshot
	snapshot := &TableSnapshot{
		Version: targetVersion,
		Files:   make([]FileAdd, 0),
	}

	// Track files by path for efficient add/remove operations
	fileMap := make(map[string]FileAdd)

	// Replay log entries in order
	for _, version := range applicableVersions {
		entry, err := tl.ReadLogEntry(ctx, tableName, version)
		if err != nil {
			return nil, fmt.Errorf("failed to read log entry %d: %w", version, err)
		}

		// Update schema (latest schema wins)
		if entry.Schema != nil {
			snapshot.Schema = entry.Schema
		}

		// Apply file additions
		for _, add := range entry.Adds {
			fileMap[add.Path] = add
		}

		// Apply file removals
		for _, remove := range entry.Removes {
			delete(fileMap, remove.Path)
		}
	}

	// Convert file map to slice
	for _, file := range fileMap {
		snapshot.Files = append(snapshot.Files, file)
	}

	// Sort files by path for deterministic results
	sort.Slice(snapshot.Files, func(i, j int) bool {
		return snapshot.Files[i].Path < snapshot.Files[j].Path
	})

	return snapshot, nil
}

// GetLatestVersion returns the latest version number for a table
func (tl *TransactionLog) GetLatestVersion(ctx context.Context, tableName string) (uint64, error) {
	versions, err := tl.ListLogEntries(ctx, tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to list log entries: %w", err)
	}

	if len(versions) == 0 {
		return 0, fmt.Errorf("no log entries found for table %s", tableName)
	}

	// Return the highest version number
	return versions[len(versions)-1], nil
}

// CreateLogEntry creates a new transaction log entry with the current timestamp
func (tl *TransactionLog) CreateLogEntry(version uint64, txnID string, schema *Schema, adds []FileAdd, removes []FileRemove) *TransactionLogEntry {
	return &TransactionLogEntry{
		Version:     version,
		TimestampMs: time.Now().UnixMilli(),
		TxnID:       txnID,
		Schema:      schema,
		Adds:        adds,
		Removes:     removes,
	}
}

// ValidateLogEntry validates that a log entry is well-formed
func (tl *TransactionLog) ValidateLogEntry(entry *TransactionLogEntry) error {
	if entry.Version == 0 {
		return fmt.Errorf("version must be greater than 0")
	}

	if entry.TxnID == "" {
		return fmt.Errorf("transaction ID cannot be empty")
	}

	if entry.TimestampMs <= 0 {
		return fmt.Errorf("timestamp must be positive")
	}

	// Validate file paths
	for _, add := range entry.Adds {
		if add.Path == "" {
			return fmt.Errorf("file add path cannot be empty")
		}
		if add.Rows < 0 {
			return fmt.Errorf("file add rows cannot be negative")
		}
		if add.Size < 0 {
			return fmt.Errorf("file add size cannot be negative")
		}
	}

	for _, remove := range entry.Removes {
		if remove.Path == "" {
			return fmt.Errorf("file remove path cannot be empty")
		}
	}

	return nil
}
