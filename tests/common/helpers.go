package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WaitForCoordinator waits for the coordinator to be ready
func WaitForCoordinator(coordinatorURL string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second) // Increased from 2 seconds
	defer ticker.Stop()

	fmt.Printf("Waiting for coordinator at %s to be ready (timeout: %v)...\n", coordinatorURL, timeout)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("coordinator not ready within %v", timeout)
		case <-ticker.C:
			resp, err := http.Get(coordinatorURL + "/health")
			if err != nil {
				fmt.Printf("Coordinator health check failed: %v, retrying...\n", err)
				continue
			}

			if resp.StatusCode == http.StatusOK {
				// Check if the response indicates the coordinator is fully ready
				var healthResp map[string]interface{}
				if json.NewDecoder(resp.Body).Decode(&healthResp) == nil {
					if status, ok := healthResp["status"].(string); ok && status == "healthy" {
						if metadataConnected, ok := healthResp["metadata_service_connected"].(bool); ok && metadataConnected {
							resp.Body.Close()
							fmt.Printf("Coordinator is ready and connected to metadata service!\n")
							return nil
						} else {
							fmt.Printf("Coordinator is running but not connected to metadata service, waiting...\n")
						}
					}
				}
			} else {
				fmt.Printf("Coordinator health check returned status %d, retrying...\n", resp.StatusCode)
			}

			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

// MakeRequest makes an HTTP request with the given method, URL, and body
func MakeRequest(t *testing.T, method, url string, body interface{}) *http.Response {
	var reqBody *bytes.Buffer

	if body != nil {
		jsonData, err := json.Marshal(body)
		require.NoError(t, err, "Should marshal request body")
		reqBody = bytes.NewBuffer(jsonData)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}

	req, err := http.NewRequest(method, url, reqBody)
	require.NoError(t, err, "Should create HTTP request")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 60 * time.Second} // Longer timeout for chaos tests
	resp, err := client.Do(req)
	require.NoError(t, err, "Should execute HTTP request")

	return resp
}

// CleanupTable attempts to delete a table (ignores errors)
func CleanupTable(coordinatorURL, tableName string) {
	req, _ := http.NewRequest("DELETE", coordinatorURL+"/tables/"+tableName, nil)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

// KillContainer kills a Docker container
func KillContainer(containerName string) error {
	cmd := exec.Command("docker", "kill", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to kill container %s: %v, output: %s", containerName, err, string(output))
	}
	return nil
}

// StartContainer starts a Docker container
func StartContainer(containerName string) error {
	cmd := exec.Command("docker", "start", containerName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start container %s: %v, output: %s", containerName, err, string(output))
	}
	return nil
}

// FindCurrentLeader finds the current Raft leader from a list of endpoints
func FindCurrentLeader(endpoints []MetadataEndpoint) (*MetadataEndpoint, error) {
	for _, endpoint := range endpoints {
		resp, err := http.Get(endpoint.URL + "/leader")
		if err != nil {
			continue
		}

		var leaderResp LeaderResponse
		err = json.NewDecoder(resp.Body).Decode(&leaderResp)
		resp.Body.Close()

		if err != nil {
			continue
		}

		if leaderResp.IsLeader {
			return &endpoint, nil
		}
	}

	return nil, fmt.Errorf("no leader found")
}

// AttemptCommitWithRetry attempts to commit data with retry logic
func AttemptCommitWithRetry(coordinatorURL, tableName string, data []LeaderTestRecord, txnID string) CommitResult {
	maxAttempts := 5

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		insertRequest := InsertDataRequest[LeaderTestRecord]{Data: data}

		response, err := http.Post(
			coordinatorURL+"/tables/"+tableName+"/insert",
			"application/json",
			func() *bytes.Buffer {
				jsonData, _ := json.Marshal(insertRequest)
				return bytes.NewBuffer(jsonData)
			}(),
		)

		if err != nil {
			if attempt == maxAttempts {
				return CommitResult{TxnID: txnID, Success: false, Error: err.Error(), Attempts: attempt}
			}
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		response.Body.Close()

		if response.StatusCode == http.StatusOK {
			return CommitResult{TxnID: txnID, Success: true, Attempts: attempt}
		}

		if attempt == maxAttempts {
			return CommitResult{TxnID: txnID, Success: false, Error: fmt.Sprintf("HTTP %d", response.StatusCode), Attempts: attempt}
		}

		// Exponential backoff
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return CommitResult{TxnID: txnID, Success: false, Error: "max attempts exceeded", Attempts: maxAttempts}
}

// GenerateLargeDataset generates a large dataset for chaos testing
func GenerateLargeDataset(size int) []ChaosTestRecord {
	categories := []string{"Electronics", "Furniture", "Books", "Clothing", "Sports"}
	data := make([]ChaosTestRecord, size)

	for i := 0; i < size; i++ {
		category := categories[i%len(categories)]
		value := float64(10 + (i % 200)) // Values from 10 to 209

		data[i] = ChaosTestRecord{
			ID:       int64(i + 1),
			Category: category,
			Value:    value,
			Data:     fmt.Sprintf("test_data_%d_%s", i+1, category),
		}
	}

	return data
}
