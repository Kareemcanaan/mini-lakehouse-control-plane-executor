package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mini-lakehouse/proto/gen"
)

// SnapshotIsolationManager manages snapshot isolation for reads
type SnapshotIsolationManager struct {
	metadataClient MetadataClient

	// Snapshot cache to avoid repeated metadata calls
	mu            sync.RWMutex
	snapshotCache map[string]*CachedSnapshot // key: "tableName:version"
	cacheTimeout  time.Duration
}

// CachedSnapshot represents a cached table snapshot
type CachedSnapshot struct {
	TableName string
	Version   uint64
	Files     []*gen.FileInfo
	Schema    *gen.Schema
	CachedAt  time.Time
}

// SnapshotReadRequest represents a request for snapshot-isolated read
type SnapshotReadRequest struct {
	TableName string
	Version   uint64 // 0 means latest version
}

// SnapshotReadResponse represents the response for snapshot-isolated read
type SnapshotReadResponse struct {
	TableName    string
	Version      uint64
	Files        []*gen.FileInfo
	Schema       *gen.Schema
	SnapshotTime time.Time
}

// NewSnapshotIsolationManager creates a new snapshot isolation manager
func NewSnapshotIsolationManager(metadataClient MetadataClient) *SnapshotIsolationManager {
	return &SnapshotIsolationManager{
		metadataClient: metadataClient,
		snapshotCache:  make(map[string]*CachedSnapshot),
		cacheTimeout:   5 * time.Minute, // Cache snapshots for 5 minutes
	}
}

// GetSnapshot returns a consistent snapshot of a table at a specific version
func (sim *SnapshotIsolationManager) GetSnapshot(ctx context.Context, req *SnapshotReadRequest) (*SnapshotReadResponse, error) {
	log.Printf("Getting snapshot for table %s at version %d", req.TableName, req.Version)

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%d", req.TableName, req.Version)
	if cached := sim.getCachedSnapshot(cacheKey); cached != nil {
		log.Printf("Using cached snapshot for %s", cacheKey)
		return &SnapshotReadResponse{
			TableName:    cached.TableName,
			Version:      cached.Version,
			Files:        cached.Files,
			Schema:       cached.Schema,
			SnapshotTime: cached.CachedAt,
		}, nil
	}

	// Resolve version if needed
	targetVersion := req.Version
	if targetVersion == 0 {
		// Get latest version
		latestResp, err := sim.metadataClient.GetLatestVersion(ctx, &gen.GetLatestVersionRequest{
			TableName: req.TableName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get latest version: %w", err)
		}
		if latestResp.Error != "" {
			return nil, fmt.Errorf("metadata service error: %s", latestResp.Error)
		}
		targetVersion = latestResp.Version
	}

	// Get snapshot from metadata service
	snapshotResp, err := sim.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: req.TableName,
		Version:   targetVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}
	if snapshotResp.Error != "" {
		return nil, fmt.Errorf("metadata service error: %s", snapshotResp.Error)
	}

	// Create response
	response := &SnapshotReadResponse{
		TableName:    req.TableName,
		Version:      targetVersion,
		Files:        snapshotResp.Files,
		Schema:       snapshotResp.Schema,
		SnapshotTime: time.Now(),
	}

	// Cache the snapshot
	sim.cacheSnapshot(cacheKey, &CachedSnapshot{
		TableName: req.TableName,
		Version:   targetVersion,
		Files:     snapshotResp.Files,
		Schema:    snapshotResp.Schema,
		CachedAt:  response.SnapshotTime,
	})

	log.Printf("Retrieved snapshot for table %s at version %d with %d files",
		req.TableName, targetVersion, len(snapshotResp.Files))

	return response, nil
}

// getCachedSnapshot retrieves a cached snapshot if it's still valid
func (sim *SnapshotIsolationManager) getCachedSnapshot(cacheKey string) *CachedSnapshot {
	sim.mu.RLock()
	defer sim.mu.RUnlock()

	cached, exists := sim.snapshotCache[cacheKey]
	if !exists {
		return nil
	}

	// Check if cache is still valid
	if time.Since(cached.CachedAt) > sim.cacheTimeout {
		// Cache expired, remove it
		delete(sim.snapshotCache, cacheKey)
		return nil
	}

	return cached
}

// cacheSnapshot stores a snapshot in the cache
func (sim *SnapshotIsolationManager) cacheSnapshot(cacheKey string, snapshot *CachedSnapshot) {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	sim.snapshotCache[cacheKey] = snapshot
}

// InvalidateCache invalidates cached snapshots for a table
func (sim *SnapshotIsolationManager) InvalidateCache(tableName string) {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	// Remove all cached snapshots for this table
	for key := range sim.snapshotCache {
		if len(key) > len(tableName) && key[:len(tableName)] == tableName && key[len(tableName)] == ':' {
			delete(sim.snapshotCache, key)
		}
	}

	log.Printf("Invalidated snapshot cache for table %s", tableName)
}

// CleanupExpiredCache removes expired entries from the cache
func (sim *SnapshotIsolationManager) CleanupExpiredCache() {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	cutoff := time.Now().Add(-sim.cacheTimeout)

	for key, cached := range sim.snapshotCache {
		if cached.CachedAt.Before(cutoff) {
			delete(sim.snapshotCache, key)
		}
	}
}

// StartCacheCleanup starts a background goroutine to clean up expired cache entries
func (sim *SnapshotIsolationManager) StartCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sim.CleanupExpiredCache()
		}
	}
}

// GetCacheStats returns statistics about the snapshot cache
func (sim *SnapshotIsolationManager) GetCacheStats() *SnapshotCacheStats {
	sim.mu.RLock()
	defer sim.mu.RUnlock()

	stats := &SnapshotCacheStats{
		TotalEntries: len(sim.snapshotCache),
		TableStats:   make(map[string]int),
	}

	for key := range sim.snapshotCache {
		// Extract table name from cache key
		if colonIndex := len(key); colonIndex > 0 {
			for i, r := range key {
				if r == ':' {
					colonIndex = i
					break
				}
			}
			if colonIndex < len(key) {
				tableName := key[:colonIndex]
				stats.TableStats[tableName]++
			}
		}
	}

	return stats
}

// SnapshotCacheStats represents statistics about the snapshot cache
type SnapshotCacheStats struct {
	TotalEntries int            `json:"total_entries"`
	TableStats   map[string]int `json:"table_stats"`
}

// ValidateSnapshotConsistency validates that a snapshot is consistent
func (sim *SnapshotIsolationManager) ValidateSnapshotConsistency(ctx context.Context, snapshot *SnapshotReadResponse) error {
	// Basic validation checks
	if snapshot.Version == 0 {
		return fmt.Errorf("invalid snapshot version: 0")
	}

	if len(snapshot.Files) == 0 {
		log.Printf("Warning: snapshot for table %s version %d has no files", snapshot.TableName, snapshot.Version)
	}

	if snapshot.Schema == nil || len(snapshot.Schema.Fields) == 0 {
		return fmt.Errorf("snapshot has invalid schema")
	}

	// Verify that the snapshot is still current by checking with metadata service
	// This helps detect cases where the metadata service state has changed
	currentResp, err := sim.metadataClient.GetSnapshot(ctx, &gen.GetSnapshotRequest{
		TableName: snapshot.TableName,
		Version:   snapshot.Version,
	})
	if err != nil {
		return fmt.Errorf("failed to validate snapshot consistency: %w", err)
	}
	if currentResp.Error != "" {
		return fmt.Errorf("snapshot validation error: %s", currentResp.Error)
	}

	// Compare file lists to ensure consistency
	if len(currentResp.Files) != len(snapshot.Files) {
		return fmt.Errorf("snapshot inconsistency: file count mismatch (cached: %d, current: %d)",
			len(snapshot.Files), len(currentResp.Files))
	}

	// Create a map of current files for quick lookup
	currentFiles := make(map[string]*gen.FileInfo)
	for _, file := range currentResp.Files {
		currentFiles[file.Path] = file
	}

	// Verify each cached file exists in current snapshot
	for _, cachedFile := range snapshot.Files {
		if currentFile, exists := currentFiles[cachedFile.Path]; !exists {
			return fmt.Errorf("snapshot inconsistency: file %s missing from current snapshot", cachedFile.Path)
		} else {
			// Verify file metadata matches
			if currentFile.Size != cachedFile.Size || currentFile.Rows != cachedFile.Rows {
				return fmt.Errorf("snapshot inconsistency: file %s metadata mismatch", cachedFile.Path)
			}
		}
	}

	return nil
}

// CreateSnapshotReadQuery creates a query with snapshot isolation
func (sim *SnapshotIsolationManager) CreateSnapshotReadQuery(
	ctx context.Context,
	tableName string,
	version uint64,
	filter string,
	groupBy []string,
	aggregates []Aggregate,
	projection []string,
) (*SimpleQuery, *SnapshotReadResponse, error) {

	// Get consistent snapshot
	snapshot, err := sim.GetSnapshot(ctx, &SnapshotReadRequest{
		TableName: tableName,
		Version:   version,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	// Validate snapshot consistency
	err = sim.ValidateSnapshotConsistency(ctx, snapshot)
	if err != nil {
		// Invalidate cache and retry once
		log.Printf("Snapshot consistency validation failed, invalidating cache: %v", err)
		sim.InvalidateCache(tableName)

		snapshot, err = sim.GetSnapshot(ctx, &SnapshotReadRequest{
			TableName: tableName,
			Version:   version,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get snapshot after cache invalidation: %w", err)
		}
	}

	// Create query with the resolved version
	query := &SimpleQuery{
		TableName:  tableName,
		Version:    snapshot.Version, // Use the resolved version
		Filter:     filter,
		GroupBy:    groupBy,
		Aggregates: aggregates,
		Projection: projection,
	}

	return query, snapshot, nil
}
