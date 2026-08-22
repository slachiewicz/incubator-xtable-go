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

// Tests for T65 (docs/improvement-plan.md): the Iceberg source must refuse a table whose
// format-version is above what this reader implements, or missing/non-positive, rather than
// silently reading it as version 2 — which is what happened before this change, because nothing
// in the source consulted TableMetadata.FormatVersion at all.
package iceberg_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// commitOneSnapshot writes a single Iceberg snapshot into storage using the real target, giving
// each test in this file a table whose metadata can then be corrupted to a chosen format-version.
func commitOneSnapshot(t *testing.T, ctx context.Context, storage io.Storage, basePath string) *model.Table {
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

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileA}, SourceIdentifier: "s1",
	}))

	return table
}

// TestIceberg_FormatVersionGatesEveryReadPath is T65's core assertion: a table whose
// format-version exceeds what this reader implements, or is missing/non-positive, must be refused
// at every read entry point the Iceberg source exposes — GetCurrentTable, GetTable,
// GetCurrentSnapshot, GetTableChangeForCommit, GetChangesSince and IsIncrementalSyncSafeFrom — not
// just one of them. Versions 1 and 2 must keep reading exactly as before.
func TestIceberg_FormatVersionGatesEveryReadPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		formatVersion int
		wantErr       bool
	}{
		{name: "version 1 reads as today", formatVersion: 1, wantErr: false},
		{name: "version 2 reads as today", formatVersion: 2, wantErr: false},
		{name: "version 3 is refused", formatVersion: 3, wantErr: true},
		{name: "version 0 (absent) is refused", formatVersion: 0, wantErr: true},
		{name: "negative version is refused", formatVersion: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/format_version_" + strings.ReplaceAll(tt.name, " ", "_")
			commitOneSnapshot(t, ctx, storage, basePath)

			path := latestMetadataPath(t, ctx, storage, basePath)
			meta := readTableMetadata(t, ctx, storage, path)
			require.NotNil(t, meta.CurrentSnapshotID, "test fixture must have committed a snapshot")
			snapshotID := *meta.CurrentSnapshotID

			meta.FormatVersion = tt.formatVersion
			writeTableMetadata(t, ctx, storage, path, meta)

			source := iceberg.NewSource(storage, basePath)

			checkErr := func(t *testing.T, err error) {
				t.Helper()
				if tt.wantErr {
					require.Error(t, err)
					assert.Contains(t, err.Error(), fmt.Sprint(tt.formatVersion),
						"error must name the format-version found")
					assert.Contains(t, err.Error(), path,
						"error must name the metadata file")
				} else {
					require.NoError(t, err)
				}
			}

			t.Run("GetCurrentTable", func(t *testing.T) {
				t.Parallel()
				_, err := source.GetCurrentTable(ctx)
				checkErr(t, err)
			})

			t.Run("GetTable", func(t *testing.T) {
				t.Parallel()
				_, err := source.GetTable(ctx, "1")
				checkErr(t, err)
			})

			t.Run("GetCurrentSnapshot", func(t *testing.T) {
				t.Parallel()
				_, err := source.GetCurrentSnapshot(ctx)
				checkErr(t, err)
			})

			t.Run("GetTableChangeForCommit", func(t *testing.T) {
				t.Parallel()
				_, err := source.GetTableChangeForCommit(ctx, strconv.FormatInt(snapshotID, 10))
				checkErr(t, err)
			})

			t.Run("GetChangesSince", func(t *testing.T) {
				t.Parallel()
				_, err := source.GetChangesSince(ctx, 0)
				checkErr(t, err)
			})

			t.Run("IsIncrementalSyncSafeFrom", func(t *testing.T) {
				t.Parallel()
				_, err := source.IsIncrementalSyncSafeFrom(ctx, 0)
				checkErr(t, err)
			})
		})
	}
}

// TestIceberg_PyIcebergFixtureStillReads guards against a false positive: the format-version gate
// must not regress test/testdata/fixtures/pyiceberg, a real pyiceberg-written v2 table that is not
// under polytable's control. This reads the fixture's metadata.json in place, which is enough to
// exercise the version gate readMetadata now applies; the fixture's manifest paths need the
// placeholder rewriting test/foreign_fixtures_test.go's loadFixture does before manifests and
// data files resolve, and TestForeignFixtures_ReadIcebergSnapshot /
// TestForeignFixtures_ConvertIceberg already cover that end to end.
func TestIceberg_PyIcebergFixtureStillReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage, err := io.NewStorageForPath(ctx, "../../../test/testdata/fixtures/pyiceberg/events")
	require.NoError(t, err)

	source := iceberg.NewSource(storage, "../../../test/testdata/fixtures/pyiceberg/events")
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err, "a real pyiceberg v2 fixture must still be readable after the format-version gate")
	assert.NotNil(t, table)
}
