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
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"
)

// Standard sentinel storage errors.
var (
	ErrNotFound         = errors.New("file not found")
	ErrAlreadyExists    = errors.New("file already exists")
	ErrInvalidPath      = errors.New("invalid storage path")
	ErrPermissionDenied = errors.New("permission denied")
)

// FileInfo represents metadata for an object or file in storage.
type FileInfo struct {
	Path    string    `json:"path"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"modTime"`
	IsDir   bool      `json:"isDir"`
}

// Storage defines the unified storage interface across Local FS, Amazon S3, Google GCS, Azure Blob, and Memory.
type Storage interface {
	// Read reads the entire content of the file at the specified path.
	Read(ctx context.Context, path string) ([]byte, error)

	// Write writes the given data to the specified path atomically.
	Write(ctx context.Context, path string, data []byte) error

	// List lists all files and directories matching the prefix.
	List(ctx context.Context, prefix string) ([]FileInfo, error)

	// Exists checks if an object or directory exists at path.
	Exists(ctx context.Context, path string) (bool, error)

	// Delete deletes the file or object at path.
	Delete(ctx context.Context, path string) error

	// Close releases any open network connections or resources.
	Close() error
}

// JoinPath safely joins path elements while preserving URI schemes (e.g. s3://, mem://, file://).
func JoinPath(base string, elem ...string) string {
	schemes := []string{"s3://", "s3a://", "gs://", "mem://", "file://"}
	for _, scheme := range schemes {
		if strings.HasPrefix(base, scheme) {
			trimmed := strings.TrimPrefix(base, scheme)
			parts := append([]string{trimmed}, elem...)
			joined := strings.Join(parts, "/")
			// Clean duplicate slashes but keep URI intact
			for strings.Contains(joined, "//") {
				joined = strings.ReplaceAll(joined, "//", "/")
			}
			return scheme + strings.TrimPrefix(joined, "/")
		}
	}
	parts := append([]string{base}, elem...)
	return filepath.Join(parts...)
}

// NewStorageForPath automatically resolves and instantiates the appropriate Storage implementation for a path URI.
func NewStorageForPath(ctx context.Context, path string) (Storage, error) {
	return NewStorageForPathWithOptions(ctx, path)
}

// NewStorageForPathWithOptions automatically resolves and instantiates Storage with optional S3 configuration.
func NewStorageForPathWithOptions(ctx context.Context, path string, optFns ...func(*S3Options)) (Storage, error) {
	if strings.HasPrefix(path, "s3://") || strings.HasPrefix(path, "s3a://") {
		return NewS3Storage(ctx, optFns...)
	}
	if strings.HasPrefix(path, "mem://") {
		return NewMemoryStorage(), nil
	}
	return NewLocalStorage(), nil
}
