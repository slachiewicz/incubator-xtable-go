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
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// credentialExpiryBuffer guards against clock skew between this process and whichever catalog
// vended the credentials: without it, a credential set could pass the construction-time check and
// still expire moments into a request against S3's own clock.
const credentialExpiryBuffer = time.Minute

// expiredCredentialErrorCodes are the AWS API error codes seen when a request is made with expired
// (or otherwise invalidated) temporary credentials. A request that fails with one of these is
// translated to ErrCredentialsExpired rather than surfacing the raw AWS error, so a sync that
// outlives a vended credential's lifetime fails naming the actual cause.
var expiredCredentialErrorCodes = map[string]bool{
	"ExpiredToken":          true,
	"ExpiredTokenException": true,
	"RequestExpired":        true,
}

// asExpiredCredentialsError reports whether err is an AWS API error indicating the credentials used
// for the request had expired, wrapping it in ErrCredentialsExpired when so. Returns (nil, false)
// for every other error, including a merely invalid or unauthorized credential, since those are not
// the same failure and renaming them would be misleading.
func asExpiredCredentialsError(err error) (error, bool) {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && expiredCredentialErrorCodes[apiErr.ErrorCode()] {
		return fmt.Errorf("%w: %w", ErrCredentialsExpired, err), true
	}
	return nil, false
}

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
	// Credentials, when set, is used instead of the standard AWS credential chain
	// (awsconfig.LoadDefaultConfig's environment/profile/IMDS discovery). This is the path a
	// catalog-vended credential (pkg/catalog.StorageCredentials) takes to reach S3; nothing else in
	// this package changes when it is nil, which is the default and the common case.
	Credentials *S3StaticCredentials
}

// NewS3Storage creates a new S3Storage instance with standard AWS credential discovery, unless
// opts.Credentials overrides it with a static credential set.
func NewS3Storage(ctx context.Context, optFns ...func(*S3Options)) (*S3Storage, error) {
	opts := S3Options{}
	for _, fn := range optFns {
		fn(&opts)
	}

	if opts.Credentials.Expired(time.Now(), credentialExpiryBuffer) {
		return nil, fmt.Errorf("%w: expired at %s", ErrCredentialsExpired, opts.Credentials.Expiration)
	}

	var configFns []func(*awsconfig.LoadOptions) error
	if opts.Region != "" {
		configFns = append(configFns, awsconfig.WithRegion(opts.Region))
	}
	if opts.CustomHTTPClient != nil {
		configFns = append(configFns, awsconfig.WithHTTPClient(opts.CustomHTTPClient))
	}
	if opts.Credentials != nil {
		configFns = append(configFns, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			opts.Credentials.AccessKeyID, opts.Credentials.SecretAccessKey, opts.Credentials.SessionToken,
		)))
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
		if expiredErr, ok := asExpiredCredentialsError(err); ok {
			return nil, expiredErr
		}
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
		if expiredErr, ok := asExpiredCredentialsError(err); ok {
			return expiredErr
		}
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
			if expiredErr, ok := asExpiredCredentialsError(err); ok {
				return nil, expiredErr
			}
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
		if expiredErr, ok := asExpiredCredentialsError(err); ok {
			return false, expiredErr
		}
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
		if expiredErr, ok := asExpiredCredentialsError(err); ok {
			return expiredErr
		}
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
