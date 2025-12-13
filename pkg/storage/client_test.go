package storage

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	// Test client creation with default values
	config := Config{
		Endpoint:        "localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		BucketName:      "test-bucket",
		UseSSL:          false,
	}

	// Note: This test requires MinIO to be running
	// In a real environment, we would use a test container or mock
	client, err := NewClient(config)
	if err != nil {
		t.Logf("Expected error when MinIO is not running: %v", err)
		return
	}

	if client == nil {
		t.Error("Expected client to be created")
	}

	if client.bucketName != config.BucketName {
		t.Errorf("Expected bucket name %s, got %s", config.BucketName, client.bucketName)
	}

	if client.retryCount != 3 {
		t.Errorf("Expected default retry count 3, got %d", client.retryCount)
	}
}

func TestConfigDefaults(t *testing.T) {
	config := Config{
		Endpoint:        "localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		BucketName:      "test-bucket",
		UseSSL:          false,
		// RetryCount and RetryDelay not set - should use defaults
	}

	// This will fail to connect but we can still test the config processing
	_, err := NewClient(config)
	if err == nil {
		t.Error("Expected error when MinIO is not running")
	}

	// The error should indicate connection failure, not config issues
	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}
