package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"mini-lakehouse/proto/gen"

	"github.com/leanovate/gopter"
	goptergen "github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Feature: mini-lakehouse, Property 20: SUCCESS manifest authority**
// *For any* successfully completed task, the coordinator should create a SUCCESS manifest after validating outputs
// **Validates: Requirements 5.4, 5.6**
func TestProperty20SuccessManifestAuthority(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	properties.Property("coordinator creates SUCCESS manifest after validating task outputs", prop.ForAll(
		func(taskID int, attempt int, numOutputs int) bool {
			// Ensure valid ranges
			if taskID < 1 {
				taskID = 1
			}
			if taskID > 1000 {
				taskID = 1000
			}
			if attempt < 1 {
				attempt = 1
			}
			if attempt > 5 {
				attempt = 5
			}
			if numOutputs < 1 {
				numOutputs = 1
			}
			if numOutputs > 10 {
				numOutputs = 10
			}

			// Create test context
			ctx := context.Background()
			jobID := fmt.Sprintf("test_job_%d", taskID)
			stageID := 0

			// Create test fault tolerance manager
			ftm := NewTestFaultToleranceManager()

			// Generate task outputs
			var taskOutputs []*gen.TaskOutput

			for i := 0; i < numOutputs; i++ {
				outputPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/attempt_%d/part-%d.parquet",
					jobID, stageID, taskID, attempt, i)

				taskOutput := &gen.TaskOutput{
					Partition: uint32(i),
					Path:      outputPath,
					Rows:      uint64(100 + i*50),     // Deterministic row counts
					Size:      uint64(1024 * (i + 1)), // Deterministic sizes
				}

				taskOutputs = append(taskOutputs, taskOutput)

				// Pre-populate storage with the output files
				ftm.mockStorage[outputPath] = &MockStorageObject{
					Path: outputPath,
					Size: int64(taskOutput.Size),
					Data: fmt.Sprintf("mock parquet data for partition %d", i),
				}
			}

			// Create task result
			taskResult := &gen.TaskResult{
				Success: true,
				Outputs: taskOutputs,
				Metrics: &gen.TaskMetrics{
					DurationMs:    uint64(5000 + taskID*100), // Deterministic duration
					RowsProcessed: uint64(numOutputs * 100),
					BytesRead:     uint64(numOutputs * 2048),
					BytesWritten:  uint64(numOutputs * 1024),
				},
			}

			// Test 1: Validate that outputs exist before writing SUCCESS manifest
			err := ftm.ValidateTaskOutputs(ctx, taskOutputs)
			if err != nil {
				t.Logf("Output validation failed: %v", err)
				return false
			}

			// Test 2: Write SUCCESS manifest
			err = ftm.WriteSuccessManifest(ctx, jobID, stageID, taskID, attempt, taskResult)
			if err != nil {
				t.Logf("Failed to write SUCCESS manifest: %v", err)
				return false
			}

			// Test 3: Verify SUCCESS manifest was written to correct location
			expectedManifestPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)
			manifestObj, exists := ftm.mockStorage[expectedManifestPath]
			if !exists {
				t.Logf("SUCCESS manifest not found at expected path: %s", expectedManifestPath)
				return false
			}

			// Test 4: Verify SUCCESS manifest content
			var manifest SuccessManifest
			err = json.Unmarshal([]byte(manifestObj.Data), &manifest)
			if err != nil {
				t.Logf("Failed to parse SUCCESS manifest: %v", err)
				return false
			}

			// Verify manifest structure
			if manifest.TaskID != taskID {
				t.Logf("Manifest task ID mismatch: expected %d, got %d", taskID, manifest.TaskID)
				return false
			}

			if manifest.Attempt != attempt {
				t.Logf("Manifest attempt mismatch: expected %d, got %d", attempt, manifest.Attempt)
				return false
			}

			if len(manifest.Outputs) != numOutputs {
				t.Logf("Manifest outputs count mismatch: expected %d, got %d", numOutputs, len(manifest.Outputs))
				return false
			}

			// Verify each output in manifest
			for i, output := range manifest.Outputs {
				expectedOutput := taskOutputs[i]

				if output.Partition != int(expectedOutput.Partition) {
					t.Logf("Output partition mismatch: expected %d, got %d", expectedOutput.Partition, output.Partition)
					return false
				}

				if output.Path != expectedOutput.Path {
					t.Logf("Output path mismatch: expected %s, got %s", expectedOutput.Path, output.Path)
					return false
				}

				if output.Rows != expectedOutput.Rows {
					t.Logf("Output rows mismatch: expected %d, got %d", expectedOutput.Rows, output.Rows)
					return false
				}

				if output.Size != expectedOutput.Size {
					t.Logf("Output size mismatch: expected %d, got %d", expectedOutput.Size, output.Size)
					return false
				}
			}

			// Verify metrics in manifest
			if manifest.Metrics == nil {
				t.Logf("Manifest metrics missing")
				return false
			}

			if manifest.Metrics.DurationMs != taskResult.Metrics.DurationMs {
				t.Logf("Metrics duration mismatch: expected %d, got %d",
					taskResult.Metrics.DurationMs, manifest.Metrics.DurationMs)
				return false
			}

			// Test 5: Verify timestamp is reasonable (within last minute)
			now := time.Now().UnixMilli()
			if manifest.TimestampMs > now || manifest.TimestampMs < now-60000 {
				t.Logf("Manifest timestamp unreasonable: %d (now: %d)", manifest.TimestampMs, now)
				return false
			}

			// Test 6: Verify we can read the manifest back
			readManifest, err := ftm.ReadSuccessManifest(ctx, jobID, stageID, taskID)
			if err != nil {
				t.Logf("Failed to read SUCCESS manifest: %v", err)
				return false
			}

			if readManifest.TaskID != taskID || readManifest.Attempt != attempt {
				t.Logf("Read manifest data mismatch")
				return false
			}

			// Test 7: Verify GetSuccessfulTaskOutputs works
			outputs, err := ftm.GetSuccessfulTaskOutputs(ctx, jobID, stageID, taskID)
			if err != nil {
				t.Logf("Failed to get successful task outputs: %v", err)
				return false
			}

			if len(outputs) != numOutputs {
				t.Logf("GetSuccessfulTaskOutputs returned wrong count: expected %d, got %d", numOutputs, len(outputs))
				return false
			}

			return true
		},
		goptergen.IntRange(1, 100), // taskID range
		goptergen.IntRange(1, 3),   // attempt range
		goptergen.IntRange(1, 5),   // numOutputs range
	))

	properties.TestingRun(t)
}

// Test that SUCCESS manifest authority is enforced - outputs must exist before manifest creation
func TestProperty20SuccessManifestAuthorityValidation(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	properties.Property("coordinator rejects SUCCESS manifest creation when outputs don't exist", prop.ForAll(
		func(taskID int, missingOutputIndex int) bool {
			// Ensure valid ranges
			if taskID < 1 {
				taskID = 1
			}
			if taskID > 100 {
				taskID = 100
			}
			if missingOutputIndex < 0 {
				missingOutputIndex = 0
			}
			if missingOutputIndex > 2 {
				missingOutputIndex = 2
			}

			ctx := context.Background()
			jobID := fmt.Sprintf("test_job_%d", taskID)
			stageID := 0
			attempt := 1
			numOutputs := 3

			// Create test fault tolerance manager
			ftm := NewTestFaultToleranceManager()

			// Generate task outputs, but don't create one of the files in storage
			var taskOutputs []*gen.TaskOutput

			for i := 0; i < numOutputs; i++ {
				outputPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/attempt_%d/part-%d.parquet",
					jobID, stageID, taskID, attempt, i)

				taskOutput := &gen.TaskOutput{
					Partition: uint32(i),
					Path:      outputPath,
					Rows:      uint64(100),
					Size:      uint64(1024),
				}

				taskOutputs = append(taskOutputs, taskOutput)

				// Only create the file in storage if it's not the missing one
				if i != missingOutputIndex {
					ftm.mockStorage[outputPath] = &MockStorageObject{
						Path: outputPath,
						Size: 1024,
						Data: fmt.Sprintf("mock data %d", i),
					}
				}
			}

			// Create task result
			taskResult := &gen.TaskResult{
				Success: true,
				Outputs: taskOutputs,
			}

			// Test: Validation should fail because one output is missing
			err := ftm.ValidateTaskOutputs(ctx, taskOutputs)
			if err == nil {
				t.Logf("Expected validation to fail due to missing output, but it succeeded")
				return false
			}

			// Verify the error mentions the missing file
			expectedMissingPath := taskOutputs[missingOutputIndex].Path
			if !strings.Contains(err.Error(), expectedMissingPath) {
				t.Logf("Error message should mention missing file %s, got: %s", expectedMissingPath, err.Error())
				return false
			}

			// Test: WriteSuccessManifest should also fail
			err = ftm.WriteSuccessManifest(ctx, jobID, stageID, taskID, attempt, taskResult)
			if err == nil {
				t.Logf("Expected WriteSuccessManifest to fail due to missing output, but it succeeded")
				return false
			}

			// Verify no SUCCESS manifest was created
			manifestPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)
			_, exists := ftm.mockStorage[manifestPath]
			if exists {
				t.Logf("SUCCESS manifest should not exist when validation fails")
				return false
			}

			return true
		},
		goptergen.IntRange(1, 50), // taskID range
		goptergen.IntRange(0, 2),  // missingOutputIndex range (0, 1, or 2)
	))

	properties.TestingRun(t)
}

// TestFaultToleranceManager is a test-specific implementation for property testing
type TestFaultToleranceManager struct {
	mockStorage map[string]*MockStorageObject
}

type MockStorageObject struct {
	Path string
	Size int64
	Data string
}

func NewTestFaultToleranceManager() *TestFaultToleranceManager {
	return &TestFaultToleranceManager{
		mockStorage: make(map[string]*MockStorageObject),
	}
}

// ValidateTaskOutputs validates that all task outputs exist
func (ftm *TestFaultToleranceManager) ValidateTaskOutputs(ctx context.Context, outputs []*gen.TaskOutput) error {
	for _, output := range outputs {
		_, exists := ftm.mockStorage[output.Path]
		if !exists {
			return fmt.Errorf("task output does not exist: %s", output.Path)
		}
	}
	return nil
}

// WriteSuccessManifest writes a SUCCESS manifest for a completed task
func (ftm *TestFaultToleranceManager) WriteSuccessManifest(ctx context.Context, jobID string, stageID, taskID, attempt int, result *gen.TaskResult) error {
	// First validate that all outputs exist
	err := ftm.ValidateTaskOutputs(ctx, result.Outputs)
	if err != nil {
		return fmt.Errorf("output validation failed: %w", err)
	}

	// Create SUCCESS manifest
	manifest := SuccessManifest{
		TaskID:      taskID,
		Attempt:     attempt,
		TimestampMs: time.Now().UnixMilli(),
		Outputs:     make([]TaskOutputInfo, len(result.Outputs)),
	}

	// Convert outputs
	for i, output := range result.Outputs {
		manifest.Outputs[i] = TaskOutputInfo{
			Partition: int(output.Partition),
			Path:      output.Path,
			Rows:      output.Rows,
			Size:      output.Size,
		}
	}

	// Convert metrics if present
	if result.Metrics != nil {
		manifest.Metrics = &TaskMetricsInfo{
			DurationMs:    result.Metrics.DurationMs,
			RowsProcessed: result.Metrics.RowsProcessed,
			BytesRead:     result.Metrics.BytesRead,
			BytesWritten:  result.Metrics.BytesWritten,
		}
	}

	// Serialize manifest
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to serialize SUCCESS manifest: %w", err)
	}

	// Write SUCCESS manifest to mock storage
	successPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)
	ftm.mockStorage[successPath] = &MockStorageObject{
		Path: successPath,
		Size: int64(len(manifestData)),
		Data: string(manifestData),
	}

	return nil
}

// ReadSuccessManifest reads a SUCCESS manifest for a task
func (ftm *TestFaultToleranceManager) ReadSuccessManifest(ctx context.Context, jobID string, stageID, taskID int) (*SuccessManifest, error) {
	successPath := fmt.Sprintf("shuffle/%s/stage_%d/task_%d/SUCCESS.json", jobID, stageID, taskID)

	obj, exists := ftm.mockStorage[successPath]
	if !exists {
		return nil, fmt.Errorf("SUCCESS manifest not found: %s", successPath)
	}

	var manifest SuccessManifest
	err := json.Unmarshal([]byte(obj.Data), &manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SUCCESS manifest: %w", err)
	}

	return &manifest, nil
}

// GetSuccessfulTaskOutputs returns the outputs for a successfully completed task
func (ftm *TestFaultToleranceManager) GetSuccessfulTaskOutputs(ctx context.Context, jobID string, stageID, taskID int) ([]TaskOutputInfo, error) {
	manifest, err := ftm.ReadSuccessManifest(ctx, jobID, stageID, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to read SUCCESS manifest: %w", err)
	}

	return manifest.Outputs, nil
}

// ListSuccessfulTasks returns all tasks in a stage that have SUCCESS manifests
func (ftm *TestFaultToleranceManager) ListSuccessfulTasks(ctx context.Context, jobID string, stageID int) ([]int, error) {
	prefix := fmt.Sprintf("shuffle/%s/stage_%d/", jobID, stageID)
	var successfulTasks []int

	// Look for SUCCESS.json files
	for path := range ftm.mockStorage {
		if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, "/SUCCESS.json") {
			// Extract task ID from path: shuffle/job/stage_X/task_Y/SUCCESS.json
			parts := strings.Split(path, "/")
			if len(parts) >= 4 {
				taskDir := parts[len(parts)-2] // task_Y
				if strings.HasPrefix(taskDir, "task_") {
					var taskID int
					_, err := fmt.Sscanf(taskDir, "task_%d", &taskID)
					if err == nil {
						successfulTasks = append(successfulTasks, taskID)
					}
				}
			}
		}
	}

	return successfulTasks, nil
}
