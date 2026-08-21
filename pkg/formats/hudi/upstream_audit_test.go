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

package hudi_test

import (
	"context"
	"encoding/json"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/io"
)

// timelineInstant is one hand-written entry in a Hudi timeline. Fixtures are written straight into
// .hoodie rather than through the Hudi target because the target only ever emits plain commits; a
// replacecommit carrying partitionToReplaceFileIds has no other way into a test.
type timelineInstant struct {
	instant string
	action  string
	meta    hudi.HoodieCommitMetadata
}

func writeHudiTimeline(t *testing.T, storage io.Storage, basePath string, instants []timelineInstant) {
	t.Helper()
	ctx := context.Background()

	props := []byte("hoodie.table.name=audit_table\nhoodie.table.type=COPY_ON_WRITE\nhoodie.table.partition.fields=level\n")
	require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, ".hoodie", "hoodie.properties"), props))

	for _, in := range instants {
		body, err := json.Marshal(in.meta)
		require.NoError(t, err)
		name := in.instant + "." + in.action
		require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, ".hoodie", name), body))
	}
}

// statPath is the base-file name Hudi gives a write: <fileId>_<writeToken>_<instantTime>.parquet.
func statPath(fileGroup, partition string) string {
	return path.Join(partition, fileGroup+"_0_20260101000000000.parquet")
}

func writeStat(fileGroup, partition string, records int64) hudi.HoodieWriteStat {
	return hudi.HoodieWriteStat{
		FileID:          fileGroup,
		Path:            statPath(fileGroup, partition),
		NumWrites:       records,
		FileSizeInBytes: records * 10,
	}
}

// TestUpstream816_ReplaceCommitFileGroups ports upstream #816 ("Fix batch INSERT_OVERWRITE
// replacecommits dropping adds in HudiDataFileExtractor", Java 634bcb6,
// ITHudiConversionSource#testMultipleInsertOverwriteOnSamePartitions).
//
// Java built its file-system view over the whole timeline and then subtracted every file group
// replaced *before or on* the instant being read, so with two successive INSERT_OVERWRITEs the
// first replacecommit reported zero adds: its own new groups had already been replaced by the
// second one.
//
// Go's snapshot walk cannot lose adds that way — it applies instants in order and never looks
// ahead. The audit found the mirror-image defect instead: HoodieCommitMetadata had no
// partitionToReplaceFileIds field at all, so superseded groups were never dropped and an
// INSERT_OVERWRITE left both generations of the data in the snapshot. Both halves are asserted
// here, because a test that only checked the adds would have blessed that.
func TestUpstream816_ReplaceCommitFileGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		instants []timelineInstant
		// wantFiles maps the table-relative path of each file the snapshot must serve to its
		// record count. Anything else in the snapshot is a superseded file that survived.
		wantFiles map[string]int64
	}{
		{
			name: "successive insert overwrites on one partition",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType:         "INSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg1", "level=INFO", 50)}},
				}},
				{instant: "20260101000100000", action: "replacecommit", meta: hudi.HoodieCommitMetadata{
					OperationType:             "INSERT_OVERWRITE",
					PartitionToWriteStats:     map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg2", "level=INFO", 30)}},
					PartitionToReplaceFileIds: map[string][]string{"level=INFO": {"fg1"}},
				}},
				{instant: "20260101000200000", action: "replacecommit", meta: hudi.HoodieCommitMetadata{
					OperationType:             "INSERT_OVERWRITE",
					PartitionToWriteStats:     map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg3", "level=INFO", 20)}},
					PartitionToReplaceFileIds: map[string][]string{"level=INFO": {"fg2"}},
				}},
			},
			// The adds of the last replacecommit survive; both replaced generations are gone.
			wantFiles: map[string]int64{statPath("fg3", "level=INFO"): 20},
		},
		{
			name: "one replacecommit overwriting several partitions",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType: "INSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{
						"level=INFO":  {writeStat("info1", "level=INFO", 50)},
						"level=WARN":  {writeStat("warn1", "level=WARN", 40)},
						"level=ERROR": {writeStat("error1", "level=ERROR", 10)},
					},
				}},
				{instant: "20260101000100000", action: "replacecommit", meta: hudi.HoodieCommitMetadata{
					OperationType: "INSERT_OVERWRITE",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{
						"level=INFO": {writeStat("info2", "level=INFO", 30)},
						"level=WARN": {writeStat("warn2", "level=WARN", 25)},
					},
					PartitionToReplaceFileIds: map[string][]string{
						"level=INFO": {"info1"},
						"level=WARN": {"warn1"},
					},
				}},
			},
			// Every partition the batch touched keeps its new group; the untouched one is intact.
			wantFiles: map[string]int64{
				statPath("info2", "level=INFO"):   30,
				statPath("warn2", "level=WARN"):   25,
				statPath("error1", "level=ERROR"): 10,
			},
		},
		{
			name: "a replacecommit naming a group in another partition leaves it alone",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType: "INSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{
						"level=INFO": {writeStat("shared", "level=INFO", 50)},
						"level=WARN": {writeStat("shared", "level=WARN", 40)},
					},
				}},
				{instant: "20260101000100000", action: "replacecommit", meta: hudi.HoodieCommitMetadata{
					OperationType:             "INSERT_OVERWRITE",
					PartitionToWriteStats:     map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fresh", "level=INFO", 5)}},
					PartitionToReplaceFileIds: map[string][]string{"level=INFO": {"shared"}},
				}},
			},
			wantFiles: map[string]int64{
				statPath("fresh", "level=INFO"):  5,
				statPath("shared", "level=WARN"): 40,
			},
		},
		{
			name: "delete partition replaces without writing",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType: "INSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{
						"level=INFO":  {writeStat("info1", "level=INFO", 50)},
						"level=DEBUG": {writeStat("debug1", "level=DEBUG", 15)},
					},
				}},
				{instant: "20260101000100000", action: "replacecommit", meta: hudi.HoodieCommitMetadata{
					OperationType:             "DELETE_PARTITION",
					PartitionToWriteStats:     map[string][]hudi.HoodieWriteStat{},
					PartitionToReplaceFileIds: map[string][]string{"level=DEBUG": {"debug1"}},
				}},
			},
			wantFiles: map[string]int64{statPath("info1", "level=INFO"): 50},
		},
		{
			name: "a plain commit never drops anything",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType:         "INSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg1", "level=INFO", 50)}},
				}},
				{instant: "20260101000100000", action: "commit", meta: hudi.HoodieCommitMetadata{
					OperationType:         "UPSERT",
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg2", "level=INFO", 30)}},
				}},
			},
			wantFiles: map[string]int64{
				statPath("fg1", "level=INFO"): 50,
				statPath("fg2", "level=INFO"): 30,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/upstream816"
			writeHudiTimeline(t, storage, basePath, tt.instants)

			snapshot, err := hudi.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
			require.NoError(t, err)

			// Compared as a set: the snapshot is assembled from a map, so its order carries no
			// meaning.
			got := make(map[string]int64, len(snapshot.DataFiles))
			for _, df := range snapshot.DataFiles {
				got[df.PhysicalPath] = df.RecordCount
			}
			want := make(map[string]int64, len(tt.wantFiles))
			for relPath, records := range tt.wantFiles {
				want[io.JoinPath(basePath, relPath)] = records
			}
			assert.Equal(t, want, got)
		})
	}
}

// TestUpstream732_UnusableRetentionMetadataFallsBack ports the intent of upstream #732 ("Handle
// empty EarliestCommitToRetain", Java 2ae9642, TestHudiConversionSource).
//
// Java's incremental-safety check parsed earliestCommitToRetain out of the newest clean instant;
// when a clean wrote that field empty the parse threw, and the fix made the check err on the side
// of caution — trigger a full snapshot sync if any clean happened after the last synced instant.
//
// Go reads no clean metadata whatsoever: IsIncrementalSyncSafeFrom compares the first instant still
// in the active timeline against the requested one, so the empty-field branch cannot arise (the
// missing clean and archival handling is recorded as a parity gap under T25). What is portable is
// the invariant the Java fix protects: retention metadata that cannot be read must resolve to
// "unsafe", never to "safe by default".
func TestUpstream732_UnusableRetentionMetadataFallsBack(t *testing.T) {
	t.Parallel()

	const requested = int64(1800000000000) // well after every fixture instant

	tests := []struct {
		name     string
		instants []timelineInstant
		wantSafe bool
	}{
		{
			name: "readable timeline older than the requested instant",
			instants: []timelineInstant{
				{instant: "20260101000000000", action: "commit", meta: hudi.HoodieCommitMetadata{
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg1", "level=INFO", 5)}},
				}},
			},
			wantSafe: true,
		},
		{
			name: "earliest instant is unparseable",
			instants: []timelineInstant{
				{instant: "not-a-timestamp00", action: "commit", meta: hudi.HoodieCommitMetadata{
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg1", "level=INFO", 5)}},
				}},
			},
			wantSafe: false,
		},
		{
			name: "earliest instant is truncated below the instant format",
			instants: []timelineInstant{
				{instant: "2026", action: "commit", meta: hudi.HoodieCommitMetadata{
					PartitionToWriteStats: map[string][]hudi.HoodieWriteStat{"level=INFO": {writeStat("fg1", "level=INFO", 5)}},
				}},
			},
			wantSafe: false,
		},
		{
			name:     "no completed instants at all",
			instants: nil,
			wantSafe: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/upstream732"
			writeHudiTimeline(t, storage, basePath, tt.instants)

			safe, _ := hudi.NewSource(storage, basePath).IsIncrementalSyncSafeFrom(ctx, requested)
			assert.Equal(t, tt.wantSafe, safe)
		})
	}
}
