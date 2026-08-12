//go:build !js

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Storage provides a zero-JVM native AWS S3 storage implementation.
type S3Storage struct {
	client *s3.Client
}

var _ Storage = (*S3Storage)(nil)

// S3Options allows configuring custom S3 endpoints, regions, or clients.
type S3Options struct {
	Region           string
	Endpoint         string
	UsePathStyle     bool
	CustomHTTPClient aws.HTTPClient
}

// NewS3Storage creates a new S3Storage instance with standard AWS credential discovery.
func NewS3Storage(ctx context.Context, optFns ...func(*S3Options)) (*S3Storage, error) {
	opts := S3Options{}
	for _, fn := range optFns {
		fn(&opts)
	}

	var configFns []func(*awsconfig.LoadOptions) error
	if opts.Region != "" {
		configFns = append(configFns, awsconfig.WithRegion(opts.Region))
	}
	if opts.CustomHTTPClient != nil {
		configFns = append(configFns, awsconfig.WithHTTPClient(opts.CustomHTTPClient))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, configFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS default configuration: %w", err)
	}

	var s3OptFns []func(*s3.Options)
	if opts.Endpoint != "" {
		s3OptFns = append(s3OptFns, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(opts.Endpoint)
			o.UsePathStyle = opts.UsePathStyle
		})
	}

	client := s3.NewFromConfig(cfg, s3OptFns...)
	return &S3Storage{client: client}, nil
}

// NewS3StorageWithClient initializes S3Storage with an existing S3 client.
func NewS3StorageWithClient(client *s3.Client) *S3Storage {
	return &S3Storage{client: client}
}

// Read fetches an object from S3.
func (s *S3Storage) Read(ctx context.Context, path string) ([]byte, error) {
	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("failed to get s3 object %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	return io.ReadAll(resp.Body)
}

// Write uploads data to S3.
func (s *S3Storage) Write(ctx context.Context, path string, data []byte) error {
	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to put s3 object %s: %w", path, err)
	}
	return nil
}

// List lists all S3 objects matching the prefix.
func (s *S3Storage) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	bucket, keyPrefix, err := ParseS3URI(prefix)
	if err != nil {
		return nil, err
	}

	var results []FileInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(keyPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list s3 objects with prefix %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)
			fullPath := fmt.Sprintf("s3://%s/%s", bucket, objKey)
			var modTime time.Time
			if obj.LastModified != nil {
				modTime = *obj.LastModified
			}
			results = append(results, FileInfo{
				Path:    fullPath,
				Size:    aws.ToInt64(obj.Size),
				ModTime: modTime,
				IsDir:   strings.HasSuffix(objKey, "/"),
			})
		}
	}

	return results, nil
}

// Exists checks if an object exists in S3.
func (s *S3Storage) Exists(ctx context.Context, path string) (bool, error) {
	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return false, err
	}

	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *s3types.NotFound
		var apiErr smithy.APIError
		if errors.As(err, &notFound) || (errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete removes an object from S3.
func (s *S3Storage) Delete(ctx context.Context, path string) error {
	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return err
	}

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete s3 object %s: %w", path, err)
	}
	return nil
}

// Close is a no-op for S3Storage.
func (s *S3Storage) Close() error {
	return nil
}

// ParseS3URI extracts bucket and key from an s3:// or s3a:// URI.
func ParseS3URI(uri string) (string, string, error) {
	cleaned := uri
	if strings.HasPrefix(cleaned, "s3://") {
		cleaned = strings.TrimPrefix(cleaned, "s3://")
	} else if strings.HasPrefix(cleaned, "s3a://") {
		cleaned = strings.TrimPrefix(cleaned, "s3a://")
	} else {
		return "", "", fmt.Errorf("%w: path must start with s3:// or s3a://", ErrInvalidPath)
	}

	parts := strings.SplitN(cleaned, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", fmt.Errorf("%w: missing bucket name in %s", ErrInvalidPath, uri)
	}

	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}
	return bucket, key, nil
}
