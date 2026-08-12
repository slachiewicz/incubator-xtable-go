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
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	pqformat "github.com/apache/incubator-xtable-go/pkg/formats/parquet"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

type TestRecord struct {
	ID    int64   `parquet:"id"`
	Name  string  `parquet:"name"`
	Score float64 `parquet:"score"`
}

func createParquetBytes(t *testing.T, records []TestRecord) []byte {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[TestRecord](&buf)
	_, err := writer.Write(records)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
	return buf.Bytes()
}

func TestParquet_SourceSnapshotDiscovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/raw_parquet_table"

	recordsUS := []TestRecord{
		{ID: 1, Name: "Alice", Score: 95.5},
		{ID: 2, Name: "Bob", Score: 88.0},
	}
	recordsFR := []TestRecord{
		{ID: 3, Name: "Claire", Score: 92.0},
	}

	usBytes := createParquetBytes(t, recordsUS)
	frBytes := createParquetBytes(t, recordsFR)

	fileUS := io.JoinPath(basePath, "country=US", "part-0.parquet")
	fileFR := io.JoinPath(basePath, "country=FR", "part-1.parquet")

	err := memStorage.Write(ctx, fileUS, usBytes)
	require.NoError(t, err)
	err = memStorage.Write(ctx, fileFR, frBytes)
	require.NoError(t, err)

	source := pqformat.NewSource(memStorage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	// Check table metadata
	assert.Equal(t, model.TableFormatParquet, snapshot.Table.TableFormat)
	require.Len(t, snapshot.Table.ReadSchema.Fields, 3) // id, name, score

	// Check partition fields
	require.Len(t, snapshot.Table.PartitioningFields, 1)
	assert.Equal(t, "country", snapshot.Table.PartitioningFields[0].SourceField.Name)

	// Check data files
	require.Len(t, snapshot.DataFiles, 2)
	var totalRecords int64
	for _, df := range snapshot.DataFiles {
		totalRecords += df.RecordCount
		require.Len(t, df.PartitionValues, 1)
		assert.Equal(t, "country", df.PartitionValues[0].PartitionField.SourceField.Name)
	}
	assert.Equal(t, int64(3), totalRecords)
}

func TestParquet_SyncToDeltaAndIceberg(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/raw_data"

	records := []TestRecord{
		{ID: 101, Name: "DeltaUser", Score: 99.9},
		{ID: 102, Name: "IcebergUser", Score: 100.0},
	}
	fileBytes := createParquetBytes(t, records)
	filePath := io.JoinPath(basePath, "dept=eng", "data.parquet")
	err := memStorage.Write(ctx, filePath, fileBytes)
	require.NoError(t, err)

	controller := conversion.NewController(memStorage)
	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatParquet,
		TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "employee_scores",
		SyncMode:      spi.SyncModeFull,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
	assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
}
