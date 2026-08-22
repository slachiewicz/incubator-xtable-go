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

// Tests for T40 (docs/improvement-plan.md): the Iceberg source's incremental sync walks snapshot
// history instead of collapsing it into one change, reports a real removal, and its safety check
// tests snapshot retention rather than metadata-file retention.
//
// commitThreeSnapshots below writes each snapshot with iceberg.Target.CommitSnapshot, which always
// replaces the live file set outright with whatever DataFiles it is given (that is what "commit a
// full snapshot" means for this target) rather than layering an add onto the previous commit. That
// makes it a convenient, honest way to script a table that adds a file, adds another, and then
// drops the first — real removal included — without needing pyiceberg for a test that has nothing
// to do with cross-engine compatibility.
package iceberg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// commitThreeSnapshots writes three Iceberg snapshots into storage: the first adds file A, the
// second adds file B alongside A, and the third drops A while keeping B. That is a pure add, a
// pure add, and a pure remove, in that order, which is enough to exercise every branch of the
// snapshot-history walk without needing an overwrite.
//
// A short sleep separates each commit because Target derives both the snapshot id and its
// timestamp from time.Now(); two commits inside the same millisecond would otherwise collide and
// break the parent chain the test depends on.
func commitThreeSnapshots(t *testing.T, ctx context.Context, storage io.Storage, basePath string) *model.Table {
	t.Helper()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	table := &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       model.NewRecordSchema("events", []*model.Field{idField}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

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

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileA}, SourceIdentifier: "s1",
	}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileA, fileB}, SourceIdentifier: "s2",
	}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileB}, SourceIdentifier: "s3",
	}))

	return table
}

// latestMetadataPath returns the path of the newest metadata.json file under basePath.
func latestMetadataPath(t *testing.T, ctx context.Context, storage io.Storage, basePath string) string {
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

func readTableMetadata(t *testing.T, ctx context.Context, storage io.Storage, path string) *iceberg.TableMetadata {
	t.Helper()

	data, err := storage.Read(ctx, path)
	require.NoError(t, err)
	var meta iceberg.TableMetadata
	require.NoError(t, json.Unmarshal(data, &meta))
	return &meta
}

func writeTableMetadata(t *testing.T, ctx context.Context, storage io.Storage, path string, meta *iceberg.TableMetadata) {
	t.Helper()

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, storage.Write(ctx, path, data))
}

// TestIceberg_GetChangesSinceWalksEverySnapshot is T40's core assertion on a source polytable's own
// target wrote: GetChangesSince must report one TableChange per snapshot, and a snapshot that drops
// a file must report a real removal, not silence.
func TestIceberg_GetChangesSinceWalksEverySnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/incremental_walk"
	commitThreeSnapshots(t, ctx, storage, basePath)

	source := iceberg.NewSource(storage, basePath)
	changes, err := source.GetChangesSince(ctx, 0)
	require.NoError(t, err)
	require.Len(t, changes.TableChanges, 3, "one TableChange per snapshot, not one change for the whole table")

	first, second, third := changes.TableChanges[0], changes.TableChanges[1], changes.TableChanges[2]

	assert.Empty(t, first.FilesDiff.FilesRemoved)
	require.Len(t, first.FilesDiff.FilesAdded, 1)
	assert.Contains(t, first.FilesDiff.FilesAdded[0].PhysicalPath, "a.parquet")

	assert.Empty(t, second.FilesDiff.FilesRemoved)
	require.Len(t, second.FilesDiff.FilesAdded, 1)
	assert.Contains(t, second.FilesDiff.FilesAdded[0].PhysicalPath, "b.parquet")

	require.Len(t, third.FilesDiff.FilesRemoved, 1, "the third snapshot must report the dropped file as a removal")
	assert.Contains(t, third.FilesDiff.FilesRemoved[0].PhysicalPath, "a.parquet")
	assert.Empty(t, third.FilesDiff.FilesAdded)

	assert.Less(t, first.CommitTime, second.CommitTime, "CommitTime must strictly increase across the backlog")
	assert.Less(t, second.CommitTime, third.CommitTime)

	// Resuming from the second snapshot's instant must see only the third.
	resumed, err := source.GetChangesSince(ctx, second.CommitTime)
	require.NoError(t, err)
	require.Len(t, resumed.TableChanges, 1)
	assert.Equal(t, third.SourceIdentifier, resumed.TableChanges[0].SourceIdentifier)

	// GetTableChangeForCommit must honor its argument: querying the middle snapshot directly must
	// reproduce the same add it reported inside the backlog above, not the current (third)
	// snapshot's diff against nothing this fixed.
	single, err := source.GetTableChangeForCommit(ctx, second.SourceIdentifier)
	require.NoError(t, err)
	assert.Empty(t, single.FilesDiff.FilesRemoved)
	require.Len(t, single.FilesDiff.FilesAdded, 1)
	assert.Contains(t, single.FilesDiff.FilesAdded[0].PhysicalPath, "b.parquet")
}

// TestIceberg_GetChangesSinceErrorsOnExpiredParent covers the decision T40 asks for explicitly: a
// history walk that meets a parent snapshot expired out of table metadata before it has walked
// back far enough to cover fromInstant must fail loudly, not silently return a partial backlog.
func TestIceberg_GetChangesSinceErrorsOnExpiredParent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/incremental_expired_parent"
	commitThreeSnapshots(t, ctx, storage, basePath)

	// Simulate expireSnapshots() dropping the oldest snapshot from the retained list while leaving
	// the snapshots that survive it untouched — which is what Iceberg's own expiry does: it prunes
	// Snapshots, not the parent-snapshot-id pointers of what remains.
	path := latestMetadataPath(t, ctx, storage, basePath)
	meta := readTableMetadata(t, ctx, storage, path)
	require.Len(t, meta.Snapshots, 3)
	expiredID := meta.Snapshots[0].SnapshotID
	meta.Snapshots = meta.Snapshots[1:]
	writeTableMetadata(t, ctx, storage, path, meta)

	source := iceberg.NewSource(storage, basePath)

	_, err := source.GetChangesSince(ctx, 0)
	require.Error(t, err, "a gap in retained history must not be silently reported as a partial backlog")
	assert.Contains(t, err.Error(), fmt.Sprint(expiredID))

	// The surviving middle snapshot's own parent is the expired one, so looking it up directly
	// must fail the same way rather than silently treating it as a root snapshot with nothing
	// before it.
	middleID := meta.Snapshots[0].SnapshotID
	_, err = source.GetTableChangeForCommit(ctx, strconv.FormatInt(middleID, 10))
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprint(expiredID))

	// But resuming from at-or-after the oldest retained snapshot's own commit needs nothing before
	// it, so the same gap must not be an error there.
	changes, err := source.GetChangesSince(ctx, meta.Snapshots[0].TimestampMs)
	require.NoError(t, err)
	require.Len(t, changes.TableChanges, 1)
}

// TestIceberg_IsIncrementalSyncSafeFromTestsSnapshotRetention is T40's fix for the previous check,
// which compared the oldest surviving metadata *file*'s LastUpdatedMs — metadata-file retention,
// not snapshot retention, and a table can expire snapshots while never touching its metadata
// files. Expiring the resume point's snapshot out of the current metadata file's Snapshots array
// must flip the answer from safe to unsafe.
func TestIceberg_IsIncrementalSyncSafeFromTestsSnapshotRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/incremental_safety"
	commitThreeSnapshots(t, ctx, storage, basePath)

	path := latestMetadataPath(t, ctx, storage, basePath)
	meta := readTableMetadata(t, ctx, storage, path)
	require.Len(t, meta.Snapshots, 3)
	firstTimestamp := meta.Snapshots[0].TimestampMs

	source := iceberg.NewSource(storage, basePath)

	safe, err := source.IsIncrementalSyncSafeFrom(ctx, firstTimestamp)
	require.NoError(t, err)
	assert.True(t, safe, "the oldest snapshot is still retained, so resuming from its own instant must be safe")

	meta.Snapshots = meta.Snapshots[1:]
	writeTableMetadata(t, ctx, storage, path, meta)

	unsafe, err := source.IsIncrementalSyncSafeFrom(ctx, firstTimestamp)
	require.NoError(t, err)
	assert.False(t, unsafe, "the resume point's snapshot has been expired out of table metadata")
}
