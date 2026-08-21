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

package parquet_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pqformat "github.com/slachiewicz/polytable/pkg/formats/parquet"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// TestUpstream806_SnapshotAcrossSeveralWrites ports upstream #806 ("Parquet Source: snapshot sync
// fails on multiple commits with partitions", Java f991e31, ITParquetConversionSource).
//
// The Java change is test-only: it merged the partitioned and non-partitioned cases into one
// parameterized test that writes a second wave of data and re-syncs, and pointed the target tables
// at getBasePath() rather than getDataPath(). So the deliverable here is the test — a Go source
// that only ever saw one wave of files would have gone unnoticed the same way.
//
// A Parquet "commit" is just more files appearing under the base path: the source has no log and
// re-crawls on every call, so the second wave is a second snapshot over the union.
func TestUpstream806_SnapshotAcrossSeveralWrites(t *testing.T) {
	t.Parallel()

	type wave struct {
		// files maps a table-relative path to the records written there.
		files map[string][]TestRecord
	}

	tests := []struct {
		name           string
		waves          []wave
		wantPartitions []string
		wantRecords    int64
		wantFiles      int
	}{
		{
			name: "unpartitioned, two waves",
			waves: []wave{
				{files: map[string][]TestRecord{
					"part-0.parquet": {{ID: 1, Name: "Alice", Score: 30.1}, {ID: 2, Name: "Bob", Score: 24.6}},
				}},
				{files: map[string][]TestRecord{
					"part-1.parquet": {{ID: 10, Name: "BobAppended", Score: 20.6}},
				}},
			},
			wantPartitions: nil,
			wantRecords:    3,
			wantFiles:      2,
		},
		{
			name: "partitioned, second wave lands in an existing partition",
			waves: []wave{
				{files: map[string][]TestRecord{
					"year=2026/month=01/part-0.parquet": {{ID: 1, Name: "Alice", Score: 30.1}},
					"year=2026/month=02/part-0.parquet": {{ID: 2, Name: "Bob", Score: 24.6}},
				}},
				{files: map[string][]TestRecord{
					"year=2026/month=02/part-1.parquet": {{ID: 10, Name: "BobAppended", Score: 20.6}},
				}},
			},
			wantPartitions: []string{"year", "month"},
			wantRecords:    3,
			wantFiles:      3,
		},
		{
			name: "partitioned, second wave opens a new partition",
			waves: []wave{
				{files: map[string][]TestRecord{
					"year=2026/month=01/part-0.parquet": {{ID: 1, Name: "Alice", Score: 30.1}},
				}},
				{files: map[string][]TestRecord{
					"year=2027/month=03/part-0.parquet": {{ID: 3, Name: "Charlie", Score: 35.2}},
				}},
				{files: map[string][]TestRecord{
					"year=2027/month=03/part-1.parquet": {{ID: 4, Name: "David", Score: 29.5}},
					"year=2027/month=04/part-0.parquet": {{ID: 5, Name: "Eve", Score: 22.2}},
				}},
			},
			wantPartitions: []string{"year", "month"},
			wantRecords:    4,
			wantFiles:      4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/upstream806"
			source := pqformat.NewSource(storage, basePath)

			var (
				written     []string
				lastRecords int64
			)
			for i, w := range tt.waves {
				for relPath, records := range w.files {
					full := io.JoinPath(basePath, relPath)
					require.NoError(t, storage.Write(ctx, full, createParquetBytes(t, records)))
					written = append(written, full)
				}

				// Every wave is synced, not just the last: #806's failure only appeared on the
				// second one.
				snapshot, err := source.GetCurrentSnapshot(ctx)
				require.NoError(t, err, "snapshot after wave %d", i+1)

				gotPaths := make([]string, 0, len(snapshot.DataFiles))
				lastRecords = 0
				for _, df := range snapshot.DataFiles {
					gotPaths = append(gotPaths, df.PhysicalPath)
					lastRecords += df.RecordCount
					// Every file carries a value for every discovered partition column, whichever
					// wave wrote it.
					assert.Len(t, df.PartitionValues, len(tt.wantPartitions))
				}
				assert.ElementsMatch(t, written, gotPaths, "wave %d", i+1)
			}

			snapshot, err := source.GetCurrentSnapshot(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRecords, lastRecords)
			assert.Len(t, snapshot.DataFiles, tt.wantFiles)

			var gotPartitions []string
			for _, pf := range snapshot.Table.PartitioningFields {
				gotPartitions = append(gotPartitions, pf.SourceField.Name)
			}
			// Order is the directory nesting, outermost first, and no longer a map's iteration
			// order.
			assert.Equal(t, tt.wantPartitions, gotPartitions)

			// The three columns the data files carry, plus one synthesized per partition column:
			// year and month live in the directory names only, and T33 puts them in the schema the
			// table is partitioned by.
			require.NotNil(t, snapshot.Table.ReadSchema)
			assert.Len(t, snapshot.Table.ReadSchema.Fields, 3+len(tt.wantPartitions))
			for _, column := range tt.wantPartitions {
				field := snapshot.Table.ReadSchema.FieldByPath(column)
				require.NotNil(t, field, "the partition column %s is missing from the schema", column)
				// year=2026 and month=01 are integers, whatever the directory spelling.
				assert.Equal(t, model.TypeLong, field.Schema.DataType, "type of %s", column)
			}
			assert.Equal(t, model.TableFormatParquet, snapshot.Table.TableFormat)
		})
	}
}
