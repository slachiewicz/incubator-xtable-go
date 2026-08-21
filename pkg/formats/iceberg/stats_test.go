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

package iceberg_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestIceberg_BoundEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  *model.Schema
		value   any
		pos     iceberg.BoundPosition
		wantOK  bool
		want    any
		checkFn func(t *testing.T, decoded any)
	}{
		{
			name:   "int",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  int32(42),
			want:   int32(42),
			wantOK: true,
		},
		{
			// A bound that has round-tripped through Delta's stats JSON arrives as float64.
			name:   "int from json float",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  float64(42),
			want:   int32(42),
			wantOK: true,
		},
		{
			name:   "int out of range",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  int64(math.MaxInt32) + 1,
			wantOK: false,
		},
		{
			name:   "long",
			schema: model.NewPrimitiveSchema(model.TypeLong, true),
			value:  int64(-1 << 40),
			want:   int64(-1 << 40),
			wantOK: true,
		},
		{
			name:   "string",
			schema: model.NewPrimitiveSchema(model.TypeString, true),
			value:  "hello",
			want:   "hello",
			wantOK: true,
		},
		{
			name:   "boolean",
			schema: model.NewPrimitiveSchema(model.TypeBoolean, true),
			value:  true,
			want:   true,
			wantOK: true,
		},
		{
			name:   "double",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  1.5,
			want:   1.5,
			wantOK: true,
		},
		{
			name:   "float",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(2.5),
			want:   float32(2.5),
			wantOK: true,
		},
		{
			name:   "date",
			schema: model.NewPrimitiveSchema(model.TypeDate, true),
			value:  int32(19000),
			want:   int32(19000),
			wantOK: true,
		},
		{
			name:   "timestamp",
			schema: model.NewPrimitiveSchema(model.TypeTimestamp, true),
			value:  int64(1690000000000000),
			want:   int64(1690000000000000),
			wantOK: true,
		},
		{
			name:   "uuid",
			schema: model.NewPrimitiveSchema(model.TypeUUID, true),
			value:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			want:   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			wantOK: true,
		},
		{
			name:   "bytes",
			schema: model.NewPrimitiveSchema(model.TypeBytes, true),
			value:  []byte{0x01, 0xff},
			want:   []byte{0x01, 0xff},
			wantOK: true,
		},
		{
			// Iceberg forbids a NaN bound: it would prune nothing and cannot be JSON-encoded.
			name:   "double NaN",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.NaN(),
			wantOK: false,
		},
		{
			name:   "float NaN",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(math.NaN()),
			wantOK: false,
		},
		{
			name:   "double negative zero stays negative as a lower bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.Copysign(0, -1),
			pos:    iceberg.LowerBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.True(t, math.Signbit(f), "lower bound must keep the negative zero")
			},
		},
		{
			name:   "double positive zero widens to negative as a lower bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  0.0,
			pos:    iceberg.LowerBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.True(t, math.Signbit(f), "lower bound must widen 0.0 to -0.0")
			},
		},
		{
			name:   "double negative zero widens to positive as an upper bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.Copysign(0, -1),
			pos:    iceberg.UpperBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.False(t, math.Signbit(f), "upper bound must widen -0.0 to 0.0")
			},
		},
		{
			name:   "float negative zero widens to positive as an upper bound",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(math.Copysign(0, -1)),
			pos:    iceberg.UpperBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float32)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.False(t, math.Signbit(float64(f)))
			},
		},
		{
			name:   "decimal is not serialized",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  1.25,
			wantOK: false,
		},
		{
			name:   "nil value",
			schema: model.NewPrimitiveSchema(model.TypeLong, true),
			value:  nil,
			wantOK: false,
		},
		{
			name:   "nil schema",
			schema: nil,
			value:  int64(1),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, ok := iceberg.EncodeBound(tt.schema, tt.value, tt.pos)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				assert.Empty(t, encoded)
				return
			}

			decoded, ok := iceberg.DecodeBound(tt.schema, encoded)
			require.True(t, ok)
			if tt.checkFn != nil {
				tt.checkFn(t, decoded)
				return
			}
			assert.Equal(t, tt.want, decoded)
		})
	}
}

func TestIceberg_DecodeBoundRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  *model.Schema
		encoded string
	}{
		{name: "empty", schema: model.NewPrimitiveSchema(model.TypeLong, true), encoded: ""},
		{name: "not base64", schema: model.NewPrimitiveSchema(model.TypeLong, true), encoded: "!!!"},
		{name: "wrong length for long", schema: model.NewPrimitiveSchema(model.TypeLong, true), encoded: "AQI="},
		{name: "wrong length for uuid", schema: model.NewPrimitiveSchema(model.TypeUUID, true), encoded: "AQI="},
		// Eight bytes of a quiet NaN, little-endian: what a writer that ignores the Iceberg rule
		// would emit. Decoding must refuse it rather than let a NaN into the model.
		{name: "nan payload for double", schema: model.NewPrimitiveSchema(model.TypeDouble, true), encoded: "AAAAAAAA+H8="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, ok := iceberg.DecodeBound(tt.schema, tt.encoded)
			assert.False(t, ok)
			assert.Nil(t, decoded)
		})
	}
}

func TestIceberg_ColumnStatsSurviveACommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_stats"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	scoreField := &model.Field{Name: "score", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	schema := model.NewRecordSchema("events", []*model.Field{idField, nameField, scoreField}, false)

	table := &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 2048,
		RecordCount:   10,
		ColumnStats: []*model.ColumnStat{
			{Field: idField, Range: model.NewRange(int64(1), int64(10)), TotalValues: 10},
			{Field: nameField, Range: model.NewRange("alice", "zoe"), NumNulls: 2, TotalValues: 10},
			{Field: scoreField, Range: model.NewRange(-1.5, 99.25), NumNulls: 1, NumNaNs: 3, TotalValues: 10},
		},
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "snap-1",
	}))

	source := iceberg.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)

	byName := statsByFieldName(t, snapshot.DataFiles[0].ColumnStats)
	require.Len(t, byName, 3)

	require.NotNil(t, byName["id"].Range)
	assert.Equal(t, int64(1), byName["id"].Range.MinValue)
	assert.Equal(t, int64(10), byName["id"].Range.MaxValue)
	assert.Equal(t, int64(10), byName["id"].TotalValues)

	require.NotNil(t, byName["name"].Range)
	assert.Equal(t, "alice", byName["name"].Range.MinValue)
	assert.Equal(t, "zoe", byName["name"].Range.MaxValue)
	assert.Equal(t, int64(2), byName["name"].NumNulls)

	require.NotNil(t, byName["score"].Range)
	assert.InDelta(t, -1.5, byName["score"].Range.MinValue, 0)
	assert.InDelta(t, 99.25, byName["score"].Range.MaxValue, 0)
	assert.Equal(t, int64(1), byName["score"].NumNulls)
	assert.Equal(t, int64(3), byName["score"].NumNaNs)
}

// TestIceberg_ColumnStatsKeyedByFieldID pins the rename behaviour: the manifest records bounds
// against the Iceberg field ID, so reading a snapshot whose column has since been renamed still
// attaches the bounds — under the new name.
func TestIceberg_ColumnStatsKeyedByFieldID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_rename"
	amountSchema := model.NewPrimitiveSchema(model.TypeLong, true)

	lower, ok := iceberg.EncodeBound(amountSchema, int64(5), iceberg.LowerBound)
	require.True(t, ok)
	upper, ok := iceberg.EncodeBound(amountSchema, int64(500), iceberg.UpperBound)
	require.True(t, ok)

	manifestPath := io.JoinPath(basePath, "metadata", "abc-m0.json")
	manifestListPath := io.JoinPath(basePath, "metadata", "snap-1.json")

	writeJSON(t, storage, manifestPath, []iceberg.ManifestEntry{{
		Status:     1,
		SnapshotID: 1,
		DataFile: &iceberg.ManifestDataFile{
			FilePath:        io.JoinPath(basePath, "data", "part-0.parquet"),
			FileFormat:      string(model.FileFormatParquet),
			RecordCount:     7,
			FileSizeInBytes: 512,
			ValueCounts:     map[int]int64{1: 7},
			NullValueCounts: map[int]int64{1: 1},
			LowerBounds:     map[int]string{1: lower},
			UpperBounds:     map[int]string{1: upper},
		},
	}})
	writeJSON(t, storage, manifestListPath, []iceberg.ManifestListEntry{{
		ManifestPath:    manifestPath,
		ManifestLength:  1,
		AddedSnapshotID: 1,
		AddedFilesCount: 1,
	}})

	snapshotID := int64(1)
	// The column was called "amount" when the manifest was written; the current schema calls the
	// same field ID "amount_eur".
	writeJSON(t, storage, io.JoinPath(basePath, "metadata", "v1.metadata.json"), &iceberg.TableMetadata{
		FormatVersion:     2,
		TableUUID:         "11111111-1111-1111-1111-111111111111",
		Location:          basePath,
		LastUpdatedMs:     time.Now().UnixMilli(),
		LastColumnID:      1,
		CurrentSchemaID:   0,
		Schemas:           []*iceberg.TableSchema{{Type: "struct", SchemaID: 0, Fields: []*iceberg.NestedField{{ID: 1, Name: "amount_eur", Type: "long"}}}},
		CurrentSnapshotID: &snapshotID,
		Snapshots: []*iceberg.TableSnapshot{{
			SnapshotID:   snapshotID,
			ManifestList: manifestListPath,
		}},
	})

	source := iceberg.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)
	require.Len(t, snapshot.DataFiles[0].ColumnStats, 1)

	stat := snapshot.DataFiles[0].ColumnStats[0]
	assert.Equal(t, "amount_eur", stat.Field.Name)
	require.NotNil(t, stat.Range)
	assert.Equal(t, int64(5), stat.Range.MinValue)
	assert.Equal(t, int64(500), stat.Range.MaxValue)
	assert.Equal(t, int64(1), stat.NumNulls)
	assert.Equal(t, int64(7), stat.TotalValues)
}

func statsByFieldName(t *testing.T, stats []*model.ColumnStat) map[string]*model.ColumnStat {
	t.Helper()
	byName := make(map[string]*model.ColumnStat, len(stats))
	for _, cs := range stats {
		require.NotNil(t, cs.Field)
		byName[cs.Field.Name] = cs
	}
	return byName
}

func writeJSON(t *testing.T, storage io.Storage, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, storage.Write(context.Background(), path, data))
}
