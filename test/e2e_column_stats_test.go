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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/formats/iceberg"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// ScoredRecord is written to Parquet by the stats tests below; Bonus is optional so that the
// footer records a non-zero null count.
type ScoredRecord struct {
	ID    int64    `parquet:"id"`
	Team  string   `parquet:"team"`
	Score float64  `parquet:"score"`
	Bonus *float64 `parquet:"bonus,optional"`
}

// readDeltaAddStats parses the first Delta commit file and returns the parsed stats of every add
// action in it, together with the raw stats strings so a caller can tell "absent" from "empty".
func readDeltaAddStats(t *testing.T, storage io.Storage, basePath string) ([]delta.StatsJSON, []string) {
	t.Helper()

	commitPath := io.JoinPath(basePath, "_delta_log", fmt.Sprintf("%020d.json", 0))
	data, err := storage.Read(context.Background(), commitPath)
	require.NoError(t, err)

	var parsed []delta.StatsJSON
	var raw []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var action delta.SingleAction
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &action))
		if action.Add == nil {
			continue
		}
		raw = append(raw, action.Add.Stats)
		var stats delta.StatsJSON
		require.NoError(t, json.Unmarshal([]byte(action.Add.Stats), &stats))
		parsed = append(parsed, stats)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, parsed, "commit carried no add actions")
	return parsed, raw
}

func writeIcebergTableWithStats(t *testing.T, storage io.Storage, basePath string) *model.Table {
	t.Helper()
	ctx := context.Background()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	teamField := &model.Field{Name: "team", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	scoreField := &model.Field{Name: "score", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	schema := model.NewRecordSchema("scores", []*model.Field{idField, teamField, scoreField}, false)

	table := &model.Table{
		Name:             "scores",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 4096,
		RecordCount:   25,
		ColumnStats: []*model.ColumnStat{
			{Field: idField, Range: model.NewRange(int64(100), int64(900)), TotalValues: 25},
			{Field: teamField, Range: model.NewRange("blue", "red"), NumNulls: 4, TotalValues: 25},
			{Field: scoreField, Range: model.NewRange(-12.5, 87.25), NumNulls: 1, TotalValues: 25},
		},
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "snap-1",
	}))
	require.NoError(t, target.Close())
	return table
}

// TestE2E_IcebergToDeltaCarriesColumnStats is the acceptance check for the Iceberg source: an
// Iceberg table whose manifest holds bounds must produce Delta add actions whose stats carry
// minValues, maxValues and nullCount.
func TestE2E_IcebergToDeltaCarriesColumnStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_to_delta_stats"
	writeIcebergTableWithStats(t, storage, basePath)

	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: basePath,
		TableName:     "scores",
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)

	stats, _ := readDeltaAddStats(t, storage, basePath)
	require.Len(t, stats, 1)

	assert.Equal(t, int64(25), stats[0].NumRecords)
	assert.InDelta(t, 100, stats[0].MinValues["id"], 0)
	assert.InDelta(t, 900, stats[0].MaxValues["id"], 0)
	assert.Equal(t, "blue", stats[0].MinValues["team"])
	assert.Equal(t, "red", stats[0].MaxValues["team"])
	assert.InDelta(t, -12.5, stats[0].MinValues["score"], 0)
	assert.InDelta(t, 87.25, stats[0].MaxValues["score"], 0)
	assert.Equal(t, int64(4), stats[0].NullCount["team"])
	assert.Equal(t, int64(1), stats[0].NullCount["score"])
}

// TestE2E_ParquetToDeltaCarriesColumnStats is the acceptance check for the Parquet source. The
// NaN subtest is the regression guard for the silent failure mode: encoding/json refuses NaN, and
// the Delta target discards a marshal error, so one NaN bound used to empty a file's whole stats
// object rather than just its own range.
func TestE2E_ParquetToDeltaCarriesColumnStats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []ScoredRecord
		check   func(t *testing.T, stats delta.StatsJSON, rawStats string)
	}{
		{
			name: "finite bounds",
			records: []ScoredRecord{
				{ID: 5, Team: "blue", Score: 3.5, Bonus: nil},
				{ID: 2, Team: "red", Score: -1.25, Bonus: func() *float64 { v := 7.0; return &v }()},
			},
			check: func(t *testing.T, stats delta.StatsJSON, _ string) {
				assert.Equal(t, int64(2), stats.NumRecords)
				assert.InDelta(t, 2, stats.MinValues["id"], 0)
				assert.InDelta(t, 5, stats.MaxValues["id"], 0)
				assert.Equal(t, "blue", stats.MinValues["team"])
				assert.Equal(t, "red", stats.MaxValues["team"])
				assert.InDelta(t, -1.25, stats.MinValues["score"], 0)
				assert.InDelta(t, 3.5, stats.MaxValues["score"], 0)
				assert.Equal(t, int64(1), stats.NullCount["bonus"])
			},
		},
		{
			name: "NaN keeps the rest of the stats",
			records: []ScoredRecord{
				{ID: 11, Team: "blue", Score: math.NaN()},
				{ID: 12, Team: "red", Score: math.NaN()},
			},
			check: func(t *testing.T, stats delta.StatsJSON, rawStats string) {
				require.NotEmpty(t, rawStats, "a NaN bound must not empty the stats object")
				assert.NotContains(t, rawStats, "NaN")
				assert.Equal(t, int64(2), stats.NumRecords)
				assert.InDelta(t, 11, stats.MinValues["id"], 0)
				assert.InDelta(t, 12, stats.MaxValues["id"], 0)
				assert.NotContains(t, stats.MinValues, "score")
				assert.NotContains(t, stats.MaxValues, "score")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/parquet_to_delta_stats/" + tt.name

			var buf bytes.Buffer
			writer := parquet.NewGenericWriter[ScoredRecord](&buf)
			_, err := writer.Write(tt.records)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, "part-0.parquet"), buf.Bytes()))

			results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
				SourceFormat:  model.TableFormatParquet,
				TargetFormats: []model.TableFormat{model.TableFormatDelta},
				TableBasePath: basePath,
				TableName:     "scores",
				SyncMode:      spi.SyncModeFull,
			})
			require.NoError(t, err)
			require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)

			stats, raw := readDeltaAddStats(t, storage, basePath)
			require.Len(t, stats, 1)
			tt.check(t, stats[0], raw[0])
		})
	}
}

// TestE2E_IcebergDeltaIcebergPreservesBounds walks a table out to Delta and back, then reads the
// bounds off the second Iceberg snapshot. Values are held under 2^53 because the Delta leg carries
// them through JSON numbers.
func TestE2E_IcebergDeltaIcebergPreservesBounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_round_trip"
	writeIcebergTableWithStats(t, storage, basePath)

	controller := conversion.NewController(storage)
	results, err := controller.Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: basePath,
		TableName:     "scores",
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)

	results, err = controller.Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "scores",
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)

	// Without this the test would pass on the metadata the first leg wrote, never reading the
	// round trip at all.
	roundTripped, err := storage.Exists(ctx, io.JoinPath(basePath, "metadata", "v2.metadata.json"))
	require.NoError(t, err)
	require.True(t, roundTripped, "the second sync wrote no new Iceberg metadata version")

	snapshot, err := iceberg.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)

	byName := make(map[string]*model.ColumnStat)
	for _, cs := range snapshot.DataFiles[0].ColumnStats {
		require.NotNil(t, cs.Field)
		byName[cs.Field.Name] = cs
	}
	require.Len(t, byName, 3)

	require.NotNil(t, byName["id"].Range)
	assert.Equal(t, int64(100), byName["id"].Range.MinValue)
	assert.Equal(t, int64(900), byName["id"].Range.MaxValue)

	require.NotNil(t, byName["team"].Range)
	assert.Equal(t, "blue", byName["team"].Range.MinValue)
	assert.Equal(t, "red", byName["team"].Range.MaxValue)
	assert.Equal(t, int64(4), byName["team"].NumNulls)

	require.NotNil(t, byName["score"].Range)
	assert.InDelta(t, -12.5, byName["score"].Range.MinValue, 0)
	assert.InDelta(t, 87.25, byName["score"].Range.MaxValue, 0)
	assert.Equal(t, int64(1), byName["score"].NumNulls)
}
