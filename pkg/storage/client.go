package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps MinIO client with retry logic and path utilities
type Client struct {
	client     *minio.Client
	bucketName string
	retryCount int
	retryDelay time.Duration
}

// Config holds configuration for the storage client
type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	UseSSL          bool
	RetryCount      int
	RetryDelay      time.Duration
}

// NewClient creates a new storage client with retry logic
func NewClient(config Config) (*Client, error) {
	// Set defaults
	if config.RetryCount == 0 {
		config.RetryCount = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}

	// Initialize MinIO client
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	client := &Client{
		client:     minioClient,
		bucketName: config.BucketName,
		retryCount: config.RetryCount,
		retryDelay: config.RetryDelay,
	}

	// Ensure bucket exists
	ctx := context.Background()
	exists, err := client.bucketExists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		err = client.createBucket(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return client, nil
}

// bucketExists checks if the bucket exists
func (c *Client) bucketExists(ctx context.Context) (bool, error) {
	return c.client.BucketExists(ctx, c.bucketName)
}

// createBucket creates the bucket
func (c *Client) createBucket(ctx context.Context) error {
	return c.client.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
}

// PutObject uploads an object with retry logic
func (c *Client) PutObject(ctx context.Context, objectPath string, reader io.Reader, objectSize int64, contentType string) error {
	var lastErr error

	for attempt := 0; attempt <= c.retryCount; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}

		_, err := c.client.PutObject(ctx, c.bucketName, objectPath, reader, objectSize, minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("failed to put object after %d attempts: %w", c.retryCount+1, lastErr)
}

// GetObject downloads an object with retry logic
func (c *Client) GetObject(ctx context.Context, objectPath string) (*minio.Object, error) {
	var lastErr error

	for attempt := 0; attempt <= c.retryCount; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}

		obj, err := c.client.GetObject(ctx, c.bucketName, objectPath, minio.GetObjectOptions{})
		if err == nil {
			return obj, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to get object after %d attempts: %w", c.retryCount+1, lastErr)
}

// ListObjectsChannel lists objects with a given prefix and returns a channel
func (c *Client) ListObjectsChannel(ctx context.Context, prefix string) <-chan minio.ObjectInfo {
	return c.client.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
}

// DeleteObject deletes an object
func (c *Client) DeleteObject(ctx context.Context, objectPath string) error {
	return c.client.RemoveObject(ctx, c.bucketName, objectPath, minio.RemoveObjectOptions{})
}

// ObjectExists checks if an object exists
func (c *Client) ObjectExists(ctx context.Context, objectPath string) (bool, error) {
	_, err := c.client.StatObject(ctx, c.bucketName, objectPath, minio.StatObjectOptions{})
	if err != nil {
		// Check if error is "object not found"
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CopyObject copies an object from source to destination
func (c *Client) CopyObject(ctx context.Context, srcPath, destPath string) error {
	var lastErr error

	for attempt := 0; attempt <= c.retryCount; attempt++ {
		if attempt > 0 {
			time.Sleep(c.retryDelay * time.Duration(attempt))
		}

		// Create copy source
		src := minio.CopySrcOptions{
			Bucket: c.bucketName,
			Object: srcPath,
		}

		// Create copy destination
		dst := minio.CopyDestOptions{
			Bucket: c.bucketName,
			Object: destPath,
		}

		_, err := c.client.CopyObject(ctx, dst, src)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("failed to copy object after %d attempts: %w", c.retryCount+1, lastErr)
}

// ObjectInfo represents information about an object
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ContentType  string
}

// ListObjects lists objects with a given prefix and returns a slice
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo

	objectCh := c.client.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}

		objects = append(objects, ObjectInfo{
			Key:          object.Key,
			Size:         object.Size,
			LastModified: object.LastModified,
			ContentType:  object.ContentType,
		})
	}

	return objects, nil
}
