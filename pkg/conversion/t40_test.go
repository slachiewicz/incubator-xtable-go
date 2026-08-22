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

// Tests for T40 (docs/improvement-plan.md): the controller must surface a fallback from
// incremental to full snapshot sync in the SyncResult it returns, rather than silently taking the
// full-sync path the way it did before.

package conversion_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// latestIcebergMetadataPath returns the path of the newest metadata.json file under basePath.
func latestIcebergMetadataPath(t *testing.T, ctx context.Context, storage io.Storage, basePath string) string {
	t.Helper()

	entries, err := storage.List(ctx, io.JoinPath(basePath, "metadata"))
	require.NoError(t, err)

	best := -1
	var bestPath string
	for _, e := range entries {
		v, ok := iceberg.MetadataFileVersion(filepath.Base(e.Path))
		if !ok {
			continue
		}
		if v > best {
			best = v
			bestPath = e.Path
		}
	}
	require.GreaterOrEqual(t, best, 0, "no metadata file found under %s", basePath)
	return bestPath
}

// expireOldestIcebergSnapshots drops every snapshot from the table's retained list up to (but not
// including) keepFromID, simulating an expireSnapshots() run against the source directly — the
// controller and its target are never involved, so this reaches only the source's own history.
func expireOldestIcebergSnapshots(t *testing.T, ctx context.Context, storage io.Storage, basePath string, keepFromID int64) {
	t.Helper()

	path := latestIcebergMetadataPath(t, ctx, storage, basePath)
	data, err := storage.Read(ctx, path)
	require.NoError(t, err)
	var meta iceberg.TableMetadata
	require.NoError(t, json.Unmarshal(data, &meta))

	var kept []*iceberg.TableSnapshot
	keeping := false
	for _, snap := range meta.Snapshots {
		if snap.SnapshotID == keepFromID {
			keeping = true
		}
		if keeping {
			kept = append(kept, snap)
		}
	}
	require.NotEmpty(t, kept, "keepFromID %d is not a snapshot this table has", keepFromID)
	meta.Snapshots = kept

	out, err := json.Marshal(&meta)
	require.NoError(t, err)
	require.NoError(t, storage.Write(ctx, path, out))
}

// buildIcebergSourceCommit writes one Iceberg snapshot directly (bypassing the controller), so the
// test can build up source history the controller never wrote and later expire part of it.
func buildIcebergSourceCommit(t *testing.T, ctx context.Context, storage io.Storage, basePath, tableName string, files []*model.DataFile) *model.Table {
	t.Helper()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	table := &model.Table{
		Name:             tableName,
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       model.NewRecordSchema(tableName, []*model.Field{idField}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{Table: table, DataFiles: files}))
	return table
}

// TestController_IncrementalSyncFallsBackWhenSourceHistoryIsExpired is T40's controller-facing
// acceptance criterion: an incremental sync that cannot safely resume because the source has
// expired the snapshot it would resume from must still complete as a full snapshot sync, and must
// say so in the SyncResult rather than reporting a plain, indistinguishable SUCCESS the way it did
// before (`if err == nil && isSafe` swallowed both an error and an unsafe verdict alike).
func TestController_IncrementalSyncFallsBackWhenSourceHistoryIsExpired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t40_fallback"

	fileA := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "a.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 100,
		RecordCount:   1,
	}
	fileB := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "b.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 200,
		RecordCount:   2,
	}
	fileC := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "c.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 300,
		RecordCount:   3,
	}

	buildIcebergSourceCommit(t, ctx, storage, basePath, "events", []*model.DataFile{fileA})
	time.Sleep(2 * time.Millisecond)
	buildIcebergSourceCommit(t, ctx, storage, basePath, "events", []*model.DataFile{fileA, fileB})

	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: basePath,
		TableName:     "events",
		SyncMode:      spi.SyncModeIncremental,
	}
	controller := conversion.NewController(storage)

	// First sync: the Delta target has no prior metadata, so this is an ordinary full snapshot
	// sync and must not report a fallback — there was no incremental attempt to fall back from.
	first, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	firstResult := first[model.TableFormatDelta]
	require.Equal(t, spi.SyncStatusSuccess, firstResult.StatusCode, firstResult.Error)
	assert.False(t, firstResult.FellBackToFullSync, "an ordinary first sync is not a fallback")
	assert.Empty(t, firstResult.FallbackReason)

	// New data arrives at the source after the sync the target's LastInstantSynced now reflects.
	time.Sleep(2 * time.Millisecond)
	table := buildIcebergSourceCommit(t, ctx, storage, basePath, "events", []*model.DataFile{fileA, fileB, fileC})

	// Simulate the source expiring every snapshot up to and including the one the target last
	// synced from, keeping only the newest. IsIncrementalSyncSafeFrom must now see that the oldest
	// retained snapshot postdates the target's resume point.
	source := iceberg.NewSource(storage, basePath)
	current, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	currentID, err := strconv.ParseInt(current.SourceIdentifier, 10, 64)
	require.NoError(t, err)
	expireOldestIcebergSnapshots(t, ctx, storage, basePath, currentID)

	require.Equal(t, model.TableFormatIceberg, table.TableFormat)

	second, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	secondResult := second[model.TableFormatDelta]
	require.Equal(t, spi.SyncStatusSuccess, secondResult.StatusCode, secondResult.Error)
	assert.True(t, secondResult.FellBackToFullSync, "history no longer covers the resume point; the controller must fall back")
	assert.NotEmpty(t, secondResult.FallbackReason)
	assert.False(t, secondResult.NoOp, "the fallback still does real work: a full snapshot sync")
}
