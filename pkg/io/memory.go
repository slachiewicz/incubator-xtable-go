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
	"sort"
	"strings"
	"sync"
	"time"
)

type memFile struct {
	data    []byte
	modTime time.Time
}

// MemoryStorage implements Storage in-memory for testing, benchmarking, and WASM runtime.
type MemoryStorage struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

// NewMemoryStorage creates a thread-safe MemoryStorage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		files: make(map[string]*memFile),
	}
}

// Read reads file bytes from the in-memory map.
func (s *MemoryStorage) Read(_ context.Context, path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, exists := s.files[path]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
	}
	cp := make([]byte, len(f.data))
	copy(cp, f.data)
	return cp, nil
}

// Write stores file bytes in the in-memory map.
func (s *MemoryStorage) Write(_ context.Context, path string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]byte, len(data))
	copy(cp, data)
	s.files[path] = &memFile{
		data:    cp,
		modTime: time.Now(),
	}
	return nil
}

// List lists all in-memory files matching the prefix.
func (s *MemoryStorage) List(_ context.Context, prefix string) ([]FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []FileInfo
	for path, file := range s.files {
		if strings.HasPrefix(path, prefix) {
			results = append(results, FileInfo{
				Path:    path,
				Size:    int64(len(file.data)),
				ModTime: file.modTime,
				IsDir:   false,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})
	return results, nil
}

// Exists checks if the path exists in memory.
func (s *MemoryStorage) Exists(_ context.Context, path string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.files[path]
	return exists, nil
}

// Delete removes the file from memory.
func (s *MemoryStorage) Delete(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.files, path)
	return nil
}

// Close is a no-op for MemoryStorage.
func (s *MemoryStorage) Close() error {
	return nil
}
