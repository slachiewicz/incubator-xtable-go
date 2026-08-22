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

package test_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// The delta-rs-checkpoint fixture is a table whose pre-checkpoint JSON commits were deleted by
// delta-rs's own log cleanup: the only copy of the metaData and protocol state, and of most add
// actions, lives in the Parquet checkpoint. This is the shape every long-lived production Delta
// table takes once log retention kicks in.

// TestDeltaCheckpoint_SnapshotFromCheckpoint asserts the reader reconstructs the full state from
// the checkpoint plus the surviving JSON tail.
func TestDeltaCheckpoint_SnapshotFromCheckpoint(t *testing.T) {
	t.Parallel()
	tableDir, manifest := loadFixture(t, "delta-rs-checkpoint")

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()

	ctx := context.Background()
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err, "the metaData action only exists in the checkpoint")
	assert.Equal(t, manifest.TableName, table.Name)
	require.NotNil(t, table.ReadSchema)
	assert.Len(t, table.ReadSchema.Fields, 3)
	require.Len(t, table.PartitioningFields, 1)
	assert.Equal(t, "region", table.PartitioningFields[0].SourceField.Name)

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, manifest.LatestCommitID, snapshot.SourceIdentifier)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	wantPaths := make(map[string]int64, len(manifest.DataFiles))
	var wantRows int64
	for _, f := range manifest.DataFiles {
		wantPaths[f.Path] = f.RecordCount
		wantRows += f.RecordCount
	}
	var gotRows int64
	for _, df := range snapshot.DataFiles {
		rel := filepath.Base(filepath.Dir(df.PhysicalPath)) + "/" + filepath.Base(df.PhysicalPath)
		want, ok := wantPaths[rel]
		require.True(t, ok, "unexpected data file %s", rel)
		assert.Equal(t, want, df.RecordCount, "record count for %s", rel)
		gotRows += df.RecordCount
	}
	assert.Equal(t, wantRows, gotRows)
}

// TestDeltaCheckpoint_IncrementalSafety asserts the source refuses to claim incremental continuity
// across the cleaned-up history: an instant older than the earliest surviving JSON commit must
// force a snapshot fallback, never a fabricated partial backlog.
func TestDeltaCheckpoint_IncrementalSafety(t *testing.T) {
	t.Parallel()
	tableDir, _ := loadFixture(t, "delta-rs-checkpoint")

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()

	ctx := context.Background()
	safe, err := source.IsIncrementalSyncSafeFrom(ctx, 0)
	require.NoError(t, err)
	assert.False(t, safe, "history before the checkpoint is gone; instant 0 cannot be served incrementally")

	// The changes visible after the latest commit's instant are exactly none.
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	changes, err := source.GetChangesSince(ctx, table.LatestCommitTime)
	require.NoError(t, err)
	assert.Empty(t, changes.TableChanges)
}

// TestDeltaCheckpoint_TruncatedLogWithoutCheckpointFails asserts the failure mode stays loud: a log
// whose head is missing and that has no checkpoint must error, not silently produce the tail's
// partial state.
func TestDeltaCheckpoint_TruncatedLogWithoutCheckpointFails(t *testing.T) {
	t.Parallel()
	tableDir, _ := loadFixture(t, "delta-rs-checkpoint")

	logDir := filepath.Join(tableDir, "_delta_log")
	require.NoError(t, os.Remove(filepath.Join(logDir, "_last_checkpoint")))
	checkpoints, err := filepath.Glob(filepath.Join(logDir, "*.checkpoint.parquet"))
	require.NoError(t, err)
	for _, f := range checkpoints {
		require.NoError(t, os.Remove(f))
	}

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()

	_, err = source.GetCurrentSnapshot(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")
}

// TestDeltaCheckpoint_ConvertsToIceberg runs the fixture through a full conversion, proving the
// checkpoint-sourced state feeds the pipeline end to end.
func TestDeltaCheckpoint_ConvertsToIceberg(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tableDir, manifest := loadFixture(t, "delta-rs-checkpoint")
	storage := io.NewLocalStorage()

	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode,
		results[model.TableFormatIceberg].Error)

	source, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)
	assertFileListMatchesManifest(t, model.TableFormatIceberg, tableDir, manifest, snapshot.DataFiles)
}
