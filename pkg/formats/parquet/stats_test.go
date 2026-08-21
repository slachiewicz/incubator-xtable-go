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
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pqformat "github.com/slachiewicz/xtable-go/pkg/formats/parquet"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
)

// StatsRecord carries an optional column so that the null counts in the footer are non-zero.
type StatsRecord struct {
	ID    int64    `parquet:"id"`
	Name  *string  `parquet:"name,optional"`
	Score float64  `parquet:"score"`
	Ratio *float32 `parquet:"ratio,optional"`
	Ok    bool     `parquet:"ok"`
}

func strPtr(s string) *string { return &s }

func f32Ptr(f float32) *float32 { return &f }

func openStatsFile(t *testing.T, records []StatsRecord, options ...parquet.WriterOption) *parquet.File {
	t.Helper()

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[StatsRecord](&buf, options...)
	_, err := writer.Write(records)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	raw := buf.Bytes()
	file, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err)
	return file
}

func TestParquet_ColumnStatsFromFooter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []StatsRecord
		options []parquet.WriterOption
		check   func(t *testing.T, byName map[string]*model.ColumnStat)
	}{
		{
			name: "bounds and null counts for a single row group",
			records: []StatsRecord{
				{ID: 3, Name: strPtr("carol"), Score: 12.5, Ratio: f32Ptr(0.25), Ok: true},
				{ID: 1, Name: nil, Score: -4.5, Ratio: nil, Ok: false},
				{ID: 7, Name: strPtr("alice"), Score: 99.0, Ratio: f32Ptr(0.75), Ok: true},
			},
			check: func(t *testing.T, byName map[string]*model.ColumnStat) {
				require.Len(t, byName, 5)

				require.NotNil(t, byName["id"].Range)
				assert.Equal(t, int64(1), byName["id"].Range.MinValue)
				assert.Equal(t, int64(7), byName["id"].Range.MaxValue)
				assert.Equal(t, int64(3), byName["id"].TotalValues)
				assert.Equal(t, int64(0), byName["id"].NumNulls)

				require.NotNil(t, byName["name"].Range)
				assert.Equal(t, "alice", byName["name"].Range.MinValue)
				assert.Equal(t, "carol", byName["name"].Range.MaxValue)
				assert.Equal(t, int64(1), byName["name"].NumNulls)

				require.NotNil(t, byName["score"].Range)
				assert.InDelta(t, -4.5, byName["score"].Range.MinValue, 0)
				assert.InDelta(t, 99.0, byName["score"].Range.MaxValue, 0)

				require.NotNil(t, byName["ratio"].Range)
				assert.Equal(t, float32(0.25), byName["ratio"].Range.MinValue)
				assert.Equal(t, float32(0.75), byName["ratio"].Range.MaxValue)
				assert.Equal(t, int64(1), byName["ratio"].NumNulls)

				require.NotNil(t, byName["ok"].Range)
				assert.Equal(t, false, byName["ok"].Range.MinValue)
				assert.Equal(t, true, byName["ok"].Range.MaxValue)
			},
		},
		{
			name: "bounds are merged across row groups",
			records: []StatsRecord{
				{ID: 50, Score: 5},
				{ID: 10, Score: 1},
				{ID: 90, Score: 9},
				{ID: 30, Score: 3},
			},
			options: []parquet.WriterOption{parquet.MaxRowsPerRowGroup(1)},
			check: func(t *testing.T, byName map[string]*model.ColumnStat) {
				require.NotNil(t, byName["id"].Range)
				assert.Equal(t, int64(10), byName["id"].Range.MinValue)
				assert.Equal(t, int64(90), byName["id"].Range.MaxValue)
				assert.Equal(t, int64(4), byName["id"].TotalValues)
			},
		},
		{
			// A NaN bound prunes nothing and would fail every JSON encoder downstream, so the
			// float column loses its range while the rest of the file keeps its statistics.
			name: "NaN drops only the affected range",
			records: []StatsRecord{
				{ID: 1, Score: math.NaN()},
				{ID: 2, Score: math.NaN()},
			},
			check: func(t *testing.T, byName map[string]*model.ColumnStat) {
				assert.Nil(t, byName["score"].Range)
				assert.Equal(t, int64(2), byName["score"].TotalValues)

				require.NotNil(t, byName["id"].Range)
				assert.Equal(t, int64(1), byName["id"].Range.MinValue)
				assert.Equal(t, int64(2), byName["id"].Range.MaxValue)
			},
		},
		{
			// -0.0 and 0.0 are equal under IEEE 754, so whichever the footer reports is a correct
			// bound; the value must survive as a zero either way.
			name: "negative zero is a usable bound",
			records: []StatsRecord{
				{ID: 1, Score: math.Copysign(0, -1)},
				{ID: 2, Score: 0},
			},
			check: func(t *testing.T, byName map[string]*model.ColumnStat) {
				require.NotNil(t, byName["score"].Range)
				minValue, ok := byName["score"].Range.MinValue.(float64)
				require.True(t, ok)
				assert.Zero(t, minValue)
				maxValue, ok := byName["score"].Range.MaxValue.(float64)
				require.True(t, ok)
				assert.Zero(t, maxValue)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := openStatsFile(t, tt.records, tt.options...)
			schema := pqformat.ParquetSchemaToModel(file.Schema())
			require.NotNil(t, schema)

			stats := pqformat.ColumnStatsFromFooter(file, schema)
			byName := make(map[string]*model.ColumnStat, len(stats))
			for _, cs := range stats {
				require.NotNil(t, cs.Field)
				byName[cs.Field.Name] = cs
			}
			tt.check(t, byName)
		})
	}
}

func TestParquet_ColumnStatsFromFooterNilInputs(t *testing.T) {
	t.Parallel()

	file := openStatsFile(t, []StatsRecord{{ID: 1}})
	assert.Nil(t, pqformat.ColumnStatsFromFooter(nil, pqformat.ParquetSchemaToModel(file.Schema())))
	assert.Nil(t, pqformat.ColumnStatsFromFooter(file, nil))
}

// TestParquet_SourceCarriesColumnStats checks that the crawler attaches the footer statistics to
// the data files it discovers, which is what every target reads them from.
func TestParquet_SourceCarriesColumnStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/parquet_stats"

	file := openStatsFile(t, []StatsRecord{
		{ID: 4, Name: strPtr("dana"), Score: 1.5},
		{ID: 9, Name: nil, Score: 8.5},
	})
	raw := make([]byte, file.Size())
	_, err := file.ReadAt(raw, 0)
	require.NoError(t, err)
	require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, "part-0.parquet"), raw))

	snapshot, err := pqformat.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)

	byName := make(map[string]*model.ColumnStat)
	for _, cs := range snapshot.DataFiles[0].ColumnStats {
		byName[cs.Field.Name] = cs
	}

	require.NotNil(t, byName["id"])
	require.NotNil(t, byName["id"].Range)
	assert.Equal(t, int64(4), byName["id"].Range.MinValue)
	assert.Equal(t, int64(9), byName["id"].Range.MaxValue)
	assert.Equal(t, int64(1), byName["name"].NumNulls)
}
