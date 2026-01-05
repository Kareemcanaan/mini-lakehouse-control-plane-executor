package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	pb "mini-lakehouse/proto/gen"

	"github.com/hashicorp/raft"
)

// TableMetadata represents metadata for a single table
type TableMetadata struct {
	Name          string     `json:"name"`
	LatestVersion uint64     `json:"latest_version"`
	Schema        *pb.Schema `json:"schema"`
	CreatedAt     time.Time  `json:"created_at"`
}

// LogEntry represents a single entry in the transaction log
type LogEntry struct {
	Version     uint64           `json:"version"`
	TimestampMs int64            `json:"timestamp_ms"`
	TxnID       string           `json:"txn_id"`
	Schema      *pb.Schema       `json:"schema"`
	Adds        []*pb.FileAdd    `json:"adds"`
	Removes     []*pb.FileRemove `json:"removes"`
}

// StateMachine implements the Raft FSM interface for table metadata
type StateMachine struct {
	mu           sync.RWMutex
	tables       map[string]*TableMetadata       // tableName -> metadata
	versions     map[string]map[uint64]*LogEntry // tableName -> version -> LogEntry
	transactions map[string]map[string]uint64    // tableName -> txnID -> version (for idempotency)
}

// NewStateMachine creates a new state machine instance
func NewStateMachine() *StateMachine {
	return &StateMachine{
		tables:       make(map[string]*TableMetadata),
		versions:     make(map[string]map[uint64]*LogEntry),
		transactions: make(map[string]map[string]uint64),
	}
}

// Command represents a command that can be applied to the state machine
type Command struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// CreateTableCommand represents a create table command
type CreateTableCommand struct {
	TableName string     `json:"table_name"`
	Schema    *pb.Schema `json:"schema"`
}

// CommitCommand represents a commit command
type CommitCommand struct {
	TableName   string           `json:"table_name"`
	BaseVersion uint64           `json:"base_version"`
	TxnID       string           `json:"txn_id"`
	Adds        []*pb.FileAdd    `json:"adds"`
	Removes     []*pb.FileRemove `json:"removes"`
}

// Apply implements raft.FSM interface
func (fsm *StateMachine) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	switch cmd.Type {
	case "create_table":
		return fsm.applyCreateTable(cmd.Data)
	case "commit":
		return fsm.applyCommit(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// applyCreateTable applies a create table command
func (fsm *StateMachine) applyCreateTable(data interface{}) interface{} {
	cmdData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal create table data: %v", err)
	}

	var cmd CreateTableCommand
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal create table command: %v", err)
	}

	// Check if table already exists
	if _, exists := fsm.tables[cmd.TableName]; exists {
		return fmt.Errorf("table %s already exists", cmd.TableName)
	}

	// Create table metadata
	fsm.tables[cmd.TableName] = &TableMetadata{
		Name:          cmd.TableName,
		LatestVersion: 0,
		Schema:        cmd.Schema,
		CreatedAt:     time.Now(),
	}

	// Initialize version and transaction maps for this table
	fsm.versions[cmd.TableName] = make(map[uint64]*LogEntry)
	fsm.transactions[cmd.TableName] = make(map[string]uint64)

	return nil
}

// applyCommit applies a commit command with optimistic concurrency control
func (fsm *StateMachine) applyCommit(data interface{}) interface{} {
	cmdData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal commit data: %v", err)
	}

	var cmd CommitCommand
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal commit command: %v", err)
	}

	// Validate input
	if cmd.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if cmd.TxnID == "" {
		return fmt.Errorf("transaction ID cannot be empty")
	}

	// Check if table exists
	table, exists := fsm.tables[cmd.TableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", cmd.TableName)
	}

	// Check for duplicate transaction (idempotency)
	// Ensure transaction map exists for this table
	if fsm.transactions[cmd.TableName] == nil {
		fsm.transactions[cmd.TableName] = make(map[string]uint64)
	}
	if existingVersion, exists := fsm.transactions[cmd.TableName][cmd.TxnID]; exists {
		return map[string]interface{}{
			"new_version": existingVersion,
			"duplicate":   true,
		}
	}

	// Optimistic concurrency control: validate base version
	if cmd.BaseVersion != table.LatestVersion {
		return fmt.Errorf("optimistic concurrency failure: base version %d does not match current version %d", cmd.BaseVersion, table.LatestVersion)
	}

	// Validate file operations
	if err := fsm.validateFileOperations(cmd.TableName, cmd.Adds, cmd.Removes); err != nil {
		return fmt.Errorf("file operation validation failed: %v", err)
	}

	// Create new version
	newVersion := table.LatestVersion + 1
	logEntry := &LogEntry{
		Version:     newVersion,
		TimestampMs: time.Now().UnixMilli(),
		TxnID:       cmd.TxnID,
		Schema:      table.Schema,
		Adds:        cmd.Adds,
		Removes:     cmd.Removes,
	}

	// Atomically update state
	// Ensure version map exists for this table
	if fsm.versions[cmd.TableName] == nil {
		fsm.versions[cmd.TableName] = make(map[uint64]*LogEntry)
	}
	fsm.versions[cmd.TableName][newVersion] = logEntry
	fsm.transactions[cmd.TableName][cmd.TxnID] = newVersion
	table.LatestVersion = newVersion

	return map[string]interface{}{
		"new_version": newVersion,
		"duplicate":   false,
	}
}

// validateFileOperations validates that file operations are consistent
func (fsm *StateMachine) validateFileOperations(tableName string, adds []*pb.FileAdd, removes []*pb.FileRemove) error {
	// Get current file set
	currentFiles := make(map[string]bool)
	if versions, exists := fsm.versions[tableName]; exists {
		table := fsm.tables[tableName]
		for v := uint64(1); v <= table.LatestVersion; v++ {
			if entry, exists := versions[v]; exists {
				// Apply adds
				for _, add := range entry.Adds {
					currentFiles[add.Path] = true
				}
				// Apply removes
				for _, remove := range entry.Removes {
					delete(currentFiles, remove.Path)
				}
			}
		}
	}

	// Validate removes - can only remove files that exist
	for _, remove := range removes {
		if !currentFiles[remove.Path] {
			return fmt.Errorf("cannot remove file %s: file does not exist", remove.Path)
		}
	}

	// Validate adds - cannot add files that already exist (unless being removed in same transaction)
	removedInTxn := make(map[string]bool)
	for _, remove := range removes {
		removedInTxn[remove.Path] = true
	}

	for _, add := range adds {
		if currentFiles[add.Path] && !removedInTxn[add.Path] {
			return fmt.Errorf("cannot add file %s: file already exists", add.Path)
		}
		if add.Path == "" {
			return fmt.Errorf("file path cannot be empty")
		}
		if add.Rows == 0 && add.Size > 0 {
			return fmt.Errorf("file %s has size but no rows", add.Path)
		}
	}

	return nil
}

// Snapshot implements raft.FSM interface
func (fsm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Create a deep copy of the state
	snapshot := &StateMachineSnapshot{
		tables:       make(map[string]*TableMetadata),
		versions:     make(map[string]map[uint64]*LogEntry),
		transactions: make(map[string]map[string]uint64),
	}

	// Copy tables
	for name, table := range fsm.tables {
		snapshot.tables[name] = &TableMetadata{
			Name:          table.Name,
			LatestVersion: table.LatestVersion,
			Schema:        table.Schema,
			CreatedAt:     table.CreatedAt,
		}
	}

	// Copy versions
	for tableName, versions := range fsm.versions {
		snapshot.versions[tableName] = make(map[uint64]*LogEntry)
		for version, entry := range versions {
			snapshot.versions[tableName][version] = &LogEntry{
				Version:     entry.Version,
				TimestampMs: entry.TimestampMs,
				TxnID:       entry.TxnID,
				Schema:      entry.Schema,
				Adds:        entry.Adds,
				Removes:     entry.Removes,
			}
		}
	}

	// Copy transactions
	for tableName, txns := range fsm.transactions {
		snapshot.transactions[tableName] = make(map[string]uint64)
		for txnID, version := range txns {
			snapshot.transactions[tableName][txnID] = version
		}
	}

	return snapshot, nil
}

// Restore implements raft.FSM interface
func (fsm *StateMachine) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var snapshotData StateMachineSnapshot
	decoder := json.NewDecoder(snapshot)
	if err := decoder.Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode snapshot: %v", err)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.tables = snapshotData.tables
	fsm.versions = snapshotData.versions
	fsm.transactions = snapshotData.transactions

	return nil
}

// GetTable returns table metadata (read-only)
func (fsm *StateMachine) GetTable(tableName string) (*TableMetadata, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	table, exists := fsm.tables[tableName]
	return table, exists
}

// GetSnapshot returns files for a specific table version
func (fsm *StateMachine) GetSnapshot(tableName string, version uint64) ([]*pb.FileInfo, *pb.Schema, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	table, exists := fsm.tables[tableName]
	if !exists {
		return nil, nil, fmt.Errorf("table %s does not exist", tableName)
	}

	if version > table.LatestVersion {
		return nil, nil, fmt.Errorf("version %d does not exist for table %s", version, tableName)
	}

	// Build file list by replaying log entries up to the requested version
	files := make(map[string]*pb.FileInfo)

	for v := uint64(1); v <= version; v++ {
		entry, exists := fsm.versions[tableName][v]
		if !exists {
			continue
		}

		// Apply adds
		for _, add := range entry.Adds {
			files[add.Path] = &pb.FileInfo{
				Path:      add.Path,
				Rows:      add.Rows,
				Size:      add.Size,
				Partition: add.Partition,
				Stats:     add.Stats,
			}
		}

		// Apply removes
		for _, remove := range entry.Removes {
			delete(files, remove.Path)
		}
	}

	// Convert map to slice
	fileList := make([]*pb.FileInfo, 0, len(files))
	for _, file := range files {
		fileList = append(fileList, file)
	}

	return fileList, table.Schema, nil
}

// StateMachineSnapshot implements raft.FSMSnapshot interface
type StateMachineSnapshot struct {
	tables       map[string]*TableMetadata
	versions     map[string]map[uint64]*LogEntry
	transactions map[string]map[string]uint64
}

// Persist implements raft.FSMSnapshot interface
func (s *StateMachineSnapshot) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(s); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to encode snapshot: %v", err)
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot interface
func (s *StateMachineSnapshot) Release() {
	// Nothing to release for in-memory snapshot
}
