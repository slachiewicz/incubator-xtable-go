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

// Paimon round trip, both directions against Delta.
//
// It lives beside the full matrix rather than inside it because Paimon joined the matrix late
// (T32): until then the target wrote a layout its own source could not read, and no suite in this
// repository crossed the two.
package test_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/formats/paimon"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// fileRecordCounts maps each data file's path, relative to the table directory, to its row count.
func fileRecordCounts(tableDir string, files []*model.DataFile) map[string]int64 {
	counts := make(map[string]int64, len(files))
	for _, file := range files {
		path := strings.TrimPrefix(file.PhysicalPath, tableDir)
		counts[strings.TrimPrefix(path, string(filepath.Separator))] = file.RecordCount
	}
	return counts
}

// schemaFieldTypes maps each top-level field name to its canonical type.
func schemaFieldTypes(schema *model.Schema) map[string]model.Type {
	types := make(map[string]model.Type, len(schema.Fields))
	for _, field := range schema.Fields {
		types[field.Name] = field.Schema.DataType
	}
	return types
}

// TestE2E_PaimonDeltaRoundTrip converts a Hive-partitioned table Delta → Paimon and back, checking
// after each leg that the target's own source recovers the schema, the file list and the row
// counts.
func TestE2E_PaimonDeltaRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir := filepath.Join(t.TempDir(), "customers_lakehouse")
	storage := io.NewLocalStorage()
	controller := conversion.NewController(storage)

	joined := time.Now().UnixMilli()
	writeSampleParquetFile(t, filepath.Join(tableDir, "country=US", "part-0000.parquet"), []CustomerRecord{
		{ID: 101, Name: "Alice Smith", Country: "US", Active: true, Balance: 1500.50, JoinedAt: joined},
		{ID: 102, Name: "Bob Jones", Country: "US", Active: false, Balance: 350.00, JoinedAt: joined},
	})
	writeSampleParquetFile(t, filepath.Join(tableDir, "country=DE", "part-0001.parquet"), []CustomerRecord{
		{ID: 201, Name: "Clara Weber", Country: "DE", Active: true, Balance: 8420.75, JoinedAt: joined},
	})

	// Seed the Delta table the round trip starts from.
	results, err := controller.Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatParquet,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableName:     "customers",
		TableBasePath: tableDir,
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)

	deltaSource := delta.NewSource(storage, tableDir)
	t.Cleanup(func() { _ = deltaSource.Close() })
	deltaSnap, err := deltaSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, deltaSnap.DataFiles, 2)

	expectedFiles := fileRecordCounts(tableDir, deltaSnap.DataFiles)
	expectedTypes := schemaFieldTypes(deltaSnap.Table.ReadSchema)

	var paimonSnap *model.Snapshot

	t.Run("DeltaToPaimon", func(t *testing.T) {
		results, err := controller.Sync(ctx, &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatPaimon},
			TableName:     "customers",
			TableBasePath: tableDir,
			SyncMode:      spi.SyncModeFull,
		})
		require.NoError(t, err)
		require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatPaimon].StatusCode,
			results[model.TableFormatPaimon].Error)

		// The layout is the one a Paimon reader expects, not a polytable invention.
		for _, expected := range []string{"schema/schema-0", "snapshot/snapshot-1", "snapshot/LATEST"} {
			exists, err := storage.Exists(ctx, filepath.Join(tableDir, filepath.FromSlash(expected)))
			require.NoError(t, err)
			assert.True(t, exists, "the Paimon target did not write %s", expected)
		}

		paimonSource := paimon.NewSource(storage, tableDir)
		t.Cleanup(func() { _ = paimonSource.Close() })

		paimonSnap, err = paimonSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)

		assert.Equal(t, expectedFiles, fileRecordCounts(tableDir, paimonSnap.DataFiles))
		assert.Equal(t, expectedTypes, schemaFieldTypes(paimonSnap.Table.ReadSchema))
		require.Len(t, paimonSnap.Table.PartitioningFields, 1)
		assert.Equal(t, "country", paimonSnap.Table.PartitioningFields[0].SourceField.Name)
	})

	t.Run("PaimonToDelta", func(t *testing.T) {
		require.NotNil(t, paimonSnap, "the Delta → Paimon leg did not produce a snapshot")

		results, err := controller.Sync(ctx, &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatPaimon,
			TargetFormats: []model.TableFormat{model.TableFormatDelta},
			TableName:     "customers",
			TableBasePath: tableDir,
			SyncMode:      spi.SyncModeFull,
		})
		require.NoError(t, err)
		require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode,
			results[model.TableFormatDelta].Error)

		roundTripped, err := delta.NewSource(storage, tableDir).GetCurrentSnapshot(ctx)
		require.NoError(t, err)

		assert.Equal(t, expectedFiles, fileRecordCounts(tableDir, roundTripped.DataFiles))
		assert.Equal(t, expectedTypes, schemaFieldTypes(roundTripped.Table.ReadSchema))
	})
}
