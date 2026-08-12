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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// LocalStorage implements Storage for the local OS filesystem.
type LocalStorage struct{}

// NewLocalStorage creates a new LocalStorage instance.
func NewLocalStorage() *LocalStorage {
	return &LocalStorage{}
}

// CleanPath normalizes file paths by stripping the "file://" URI scheme if present.
func CleanPath(p string) string {
	if strings.HasPrefix(p, "file://") {
		return strings.TrimPrefix(p, "file://")
	}
	return p
}

// Read reads file contents from the local filesystem.
func (s *LocalStorage) Read(_ context.Context, path string) ([]byte, error) {
	cleanPath := CleanPath(path)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, err
	}
	return data, nil
}

// Write writes data to path atomically, creating any parent directories automatically.
func (s *LocalStorage) Write(_ context.Context, path string, data []byte) error {
	cleanPath := CleanPath(path)
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to temporary file in same directory then atomic rename
	tmpFile, err := os.CreateTemp(dir, ".xtable-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file in %s: %w", dir, err)
	}
	tmpName := tmpFile.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to write data: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpName, cleanPath); err != nil {
		return fmt.Errorf("failed to rename temp file to %s: %w", cleanPath, err)
	}
	return nil
}

// List lists all files under the given prefix or directory path.
func (s *LocalStorage) List(_ context.Context, prefix string) ([]FileInfo, error) {
	cleanPath := CleanPath(prefix)
	info, err := os.Stat(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var results []FileInfo
	if !info.IsDir() {
		return []FileInfo{{
			Path:    cleanPath,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   false,
		}}, nil
	}

	err = filepath.WalkDir(cleanPath, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == cleanPath {
			return nil
		}
		dInfo, err := d.Info()
		if err != nil {
			return err
		}
		results = append(results, FileInfo{
			Path:    p,
			Size:    dInfo.Size(),
			ModTime: dInfo.ModTime(),
			IsDir:   d.IsDir(),
		})
		return nil
	})
	return results, err
}

// Exists checks whether a file or directory exists.
func (s *LocalStorage) Exists(_ context.Context, path string) (bool, error) {
	cleanPath := CleanPath(path)
	_, err := os.Stat(cleanPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Delete removes a file or empty directory.
func (s *LocalStorage) Delete(_ context.Context, path string) error {
	cleanPath := CleanPath(path)
	err := os.RemoveAll(cleanPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Close is a no-op for LocalStorage.
func (s *LocalStorage) Close() error {
	return nil
}
