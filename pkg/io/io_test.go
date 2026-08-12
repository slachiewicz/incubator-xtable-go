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

package io_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/io"
)

func TestStorage_Implementations(t *testing.T) {
	t.Parallel()

	tempDir, err := os.MkdirTemp("", "xtable-io-test-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tempDir) }()

	storages := []struct {
		name    string
		storage io.Storage
		base    string
	}{
		{
			name:    "memory",
			storage: io.NewMemoryStorage(),
			base:    "mem://table",
		},
		{
			name:    "local",
			storage: io.NewLocalStorage(),
			base:    tempDir,
		},
	}

	for _, s := range storages {
		s := s
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			filePath := filepath.Join(s.base, "_delta_log", "00000000000000000000.json")

			// Check not exists
			exists, err := s.storage.Exists(ctx, filePath)
			require.NoError(t, err)
			assert.False(t, exists)

			// Read non-existent
			_, err = s.storage.Read(ctx, filePath)
			require.ErrorIs(t, err, io.ErrNotFound)

			// Write file
			testData := []byte(`{"commitInfo":{"timestamp":1700000000}}`)
			err = s.storage.Write(ctx, filePath, testData)
			require.NoError(t, err)

			// Check exists
			exists, err = s.storage.Exists(ctx, filePath)
			require.NoError(t, err)
			assert.True(t, exists)

			// Read file
			readData, err := s.storage.Read(ctx, filePath)
			require.NoError(t, err)
			assert.Equal(t, testData, readData)

			// List files
			prefix := filepath.Join(s.base, "_delta_log")
			files, err := s.storage.List(ctx, prefix)
			require.NoError(t, err)
			assert.NotEmpty(t, files)

			// Delete file
			err = s.storage.Delete(ctx, filePath)
			require.NoError(t, err)

			exists, err = s.storage.Exists(ctx, filePath)
			require.NoError(t, err)
			assert.False(t, exists)
		})
	}
}
