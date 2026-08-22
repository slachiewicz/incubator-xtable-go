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
	"fmt"
	"path"
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
	ErrPathNotUnderBase = errors.New("path is not under the base path")
)

// uriSchemes are the URI prefixes the path helpers (JoinPath, TrimScheme, RelativizePath)
// recognize. It is deliberately broader than the schemes NewStorageForPathWithOptions can back
// with a client: gs:// parses, because foreign metadata may carry such paths, but has no storage
// backend here and is rejected at storage selection.
var uriSchemes = []string{"s3://", "s3a://", "gs://", "mem://", "file://"}

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
	for _, scheme := range uriSchemes {
		if strings.HasPrefix(base, scheme) {
			trimmed := strings.TrimPrefix(base, scheme)
			parts := append([]string{trimmed}, elem...)
			joined := strings.Join(parts, "/")
			// Clean duplicate slashes but keep URI intact
			for strings.Contains(joined, "//") {
				joined = strings.ReplaceAll(joined, "//", "/")
			}
			// file:// carries an absolute path after the scheme, and dropping its leading
			// separator would turn file:///data/events into the relative file://data/events.
			if strings.HasPrefix(trimmed, "/") {
				return scheme + joined
			}
			return scheme + strings.TrimPrefix(joined, "/")
		}
	}
	parts := append([]string{base}, elem...)
	return filepath.Join(parts...)
}

// TrimScheme removes a recognized URI scheme from a path, returning the scheme and the remainder.
// An unrecognized or absent scheme yields an empty scheme and the path unchanged.
func TrimScheme(p string) (scheme, rest string) {
	for _, s := range uriSchemes {
		if strings.HasPrefix(p, s) {
			return s, strings.TrimPrefix(p, s)
		}
	}
	return "", p
}

// RelativizePath returns physicalPath expressed relative to basePath, which is what every target
// stores in its metadata so that a table survives being copied or moved.
//
// The comparison is scheme-aware: a scheme is stripped from either side before comparing, because
// formats disagree about whether to record one. An Iceberg manifest reports
// file:///data/events/f.parquet for a table whose base path is /data/events, and a plain string
// prefix does not match — the bug this exists to prevent. Stripping the scheme from both sides also
// makes s3:// and s3a:// equivalent, which is right: they name the same object store.
//
// A path that is already relative comes back unchanged. A path outside the base path is an error
// wrapping ErrPathNotUnderBase, never a silently returned absolute path: every caller has to decide
// what such a file means for its format.
func RelativizePath(physicalPath, basePath string) (string, error) {
	_, base := TrimScheme(basePath)
	scheme, file := TrimScheme(physicalPath)

	if base == "" {
		return "", fmt.Errorf("%w: base path %q has no path component", ErrInvalidPath, basePath)
	}
	if file == "" {
		return "", fmt.Errorf("%w: file path %q has no path component", ErrInvalidPath, physicalPath)
	}

	// Clean collapses duplicate separators, drops a trailing one and resolves "..", so that a base
	// path written with or without a trailing slash compares the same.
	base = path.Clean(base)
	file = path.Clean(file)

	// model.DataFile documents PhysicalPath as a fully qualified URI or a relative path. Carrying no
	// scheme and not starting at the root is what makes it the second kind, and such a path is
	// already relative to the table — with the exception of one that climbs out of it.
	if scheme == "" && !strings.HasPrefix(file, "/") {
		if file == ".." || strings.HasPrefix(file, "../") {
			return "", fmt.Errorf("%w: %q climbs out of %q", ErrPathNotUnderBase, physicalPath, basePath)
		}
		return file, nil
	}

	if file == base {
		return "", fmt.Errorf("%w: %q is the base path itself", ErrPathNotUnderBase, physicalPath)
	}
	rest, found := strings.CutPrefix(file, base)
	// The match has to end on a separator, or /data/events would claim /data/events2/f.parquet.
	// A base path of "/" is the one case that already ends in one.
	if !found || (!strings.HasPrefix(rest, "/") && !strings.HasSuffix(base, "/")) {
		return "", fmt.Errorf("%w: %q is not under %q", ErrPathNotUnderBase, physicalPath, basePath)
	}
	return strings.TrimPrefix(rest, "/"), nil
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
	// Any other URI scheme must fail here rather than fall through: local storage would treat
	// "gs://bucket/table" as a relative directory and create a literal "gs:" directory on the
	// first write.
	if scheme, _, found := strings.Cut(path, "://"); found && scheme != "file" {
		return nil, fmt.Errorf("%w: no storage backend for scheme %q (supported: s3://, s3a://, mem://, file://, or a plain local path)",
			ErrInvalidPath, scheme+"://")
	}
	return NewLocalStorage(), nil
}
