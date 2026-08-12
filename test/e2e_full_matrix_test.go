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
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	"github.com/apache/incubator-xtable-go/pkg/formats/delta"
	"github.com/apache/incubator-xtable-go/pkg/formats/hudi"
	"github.com/apache/incubator-xtable-go/pkg/formats/iceberg"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

type CustomerRecord struct {
	ID       int64   `parquet:"id"`
	Name     string  `parquet:"name"`
	Country  string  `parquet:"country"`
	Active   bool    `parquet:"active"`
	Balance  float64 `parquet:"balance"`
	JoinedAt int64   `parquet:"joined_at,timestamp(millisecond)"`
}

func writeSampleParquetFile(t *testing.T, filePath string, records []CustomerRecord) {
	t.Helper()
	err := os.MkdirAll(filepath.Dir(filePath), 0755)
	require.NoError(t, err)

	f, err := os.Create(filePath)
	require.NoError(t, err)
	defer f.Close()

	w := parquet.NewGenericWriter[CustomerRecord](f)
	_, err = w.Write(records)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
}

func TestE2E_FullOmniDirectionalMatrix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := t.TempDir()
	tableDir := filepath.Join(tmpDir, "customers_lakehouse")
	storage := io.NewLocalStorage()

	// 1. Generate partitioned physical Parquet files (Hive style partitioning: country=US/, country=DE/)
	usRecords := []CustomerRecord{
		{ID: 101, Name: "Alice Smith", Country: "US", Active: true, Balance: 1500.50, JoinedAt: time.Now().UnixMilli()},
		{ID: 102, Name: "Bob Jones", Country: "US", Active: false, Balance: 350.00, JoinedAt: time.Now().UnixMilli()},
	}
	deRecords := []CustomerRecord{
		{ID: 201, Name: "Clara Weber", Country: "DE", Active: true, Balance: 8420.75, JoinedAt: time.Now().UnixMilli()},
	}

	writeSampleParquetFile(t, filepath.Join(tableDir, "country=US", "part-0000.parquet"), usRecords)
	writeSampleParquetFile(t, filepath.Join(tableDir, "country=DE", "part-0001.parquet"), deRecords)

	controller := conversion.NewController(storage)

	// STEP 1: PARQUET -> [DELTA, ICEBERG, HUDI]
	t.Run("ParquetToAllFormats", func(t *testing.T) {
		cfg := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatParquet,
			TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg, model.TableFormatHudi},
			TableName:     "customers",
			TableBasePath: tableDir,
			SyncMode:      spi.SyncModeFull,
		}

		results, err := controller.Sync(ctx, cfg)
		require.NoError(t, err)
		require.Len(t, results, 3)

		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatHudi].StatusCode)

		// Verify Delta log exists
		exists, err := storage.Exists(ctx, filepath.Join(tableDir, "_delta_log", "00000000000000000000.json"))
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify Iceberg metadata exists
		exists, err = storage.Exists(ctx, filepath.Join(tableDir, "metadata", "version-hint.text"))
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify Hudi properties exists
		exists, err = storage.Exists(ctx, filepath.Join(tableDir, ".hoodie", "hoodie.properties"))
		require.NoError(t, err)
		assert.True(t, exists)
	})

	// STEP 2: DELTA -> ICEBERG & HUDI (incremental verification)
	t.Run("DeltaToIcebergAndHudi_Incremental", func(t *testing.T) {
		// Append new record in country=US/
		newRecords := []CustomerRecord{
			{ID: 103, Name: "David Miller", Country: "US", Active: true, Balance: 999.00, JoinedAt: time.Now().UnixMilli()},
		}
		writeSampleParquetFile(t, filepath.Join(tableDir, "country=US", "part-0002.parquet"), newRecords)

		// Commit new file into Delta log
		deltaSource := delta.NewSource(storage, tableDir)
		deltaSnap, err := deltaSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)

		partField := &model.PartitionField{
			SourceField:   &model.Field{Name: "country", Schema: model.NewPrimitiveSchema(model.TypeString, false)},
			TransformType: model.PartitionTransformValue,
		}

		newDF := &model.DataFile{
			PhysicalPath:  filepath.Join(tableDir, "country=US", "part-0002.parquet"),
			FileFormat:    model.FileFormatParquet,
			FileSizeBytes: 1024,
			RecordCount:   1,
			PartitionValues: []*model.PartitionValue{
				{PartitionField: partField, Range: model.NewScalarRange("country=US")},
			},
			LastModified: time.Now().UnixMilli(),
		}
		deltaSnap.DataFiles = append(deltaSnap.DataFiles, newDF)
		deltaSnap.Table.LatestCommitTime = time.Now().UnixMilli()

		deltaTarget := delta.NewTarget(storage)
		err = deltaTarget.Init(ctx, deltaSnap.Table)
		require.NoError(t, err)
		err = deltaTarget.CommitSnapshot(ctx, deltaSnap)
		require.NoError(t, err)

		// Sync Delta -> [ICEBERG, HUDI]
		cfg := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
			TableName:     "customers",
			TableBasePath: tableDir,
			SyncMode:      spi.SyncModeFull,
		}

		results, err := controller.Sync(ctx, cfg)
		require.NoError(t, err)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatHudi].StatusCode)

		// Read back via Iceberg Source
		icebergSource := iceberg.NewSource(storage, tableDir)
		icebergSnap, err := icebergSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		assert.Len(t, icebergSnap.DataFiles, 3)

		// Read back via Hudi Source
		hudiSource := hudi.NewSource(storage, tableDir)
		hudiSnap, err := hudiSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		assert.Len(t, hudiSnap.DataFiles, 3)
	})

	// STEP 3: HUDI -> ICEBERG & DELTA
	t.Run("HudiToDeltaAndIceberg", func(t *testing.T) {
		cfg := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatHudi,
			TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg},
			TableName:     "customers",
			TableBasePath: tableDir,
			SyncMode:      spi.SyncModeFull,
		}

		results, err := controller.Sync(ctx, cfg)
		require.NoError(t, err)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
	})
}
