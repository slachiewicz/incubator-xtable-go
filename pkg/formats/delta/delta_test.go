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

package delta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestDelta_SchemaRoundTrip(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	priceField := &model.Field{Name: "price", Schema: model.NewDecimalSchema(10, 2, true)}
	createdField := &model.Field{Name: "created_at", Schema: model.NewPrimitiveSchema(model.TypeTimestamp, false)}

	origSchema := model.NewRecordSchema("item", []*model.Field{idField, nameField, priceField, createdField}, false)

	jsonStr, err := delta.SchemaToDeltaJSON(origSchema)
	require.NoError(t, err)
	assert.Contains(t, jsonStr, `"type":"integer"`)
	assert.Contains(t, jsonStr, `"type":"string"`)
	assert.Contains(t, jsonStr, `"type":"decimal(10,2)"`)
	assert.Contains(t, jsonStr, `"type":"timestamp"`)

	parsedSchema, err := delta.DeltaJSONToSchema(jsonStr)
	require.NoError(t, err)
	require.Len(t, parsedSchema.Fields, 4)

	assert.Equal(t, "id", parsedSchema.Fields[0].Name)
	assert.Equal(t, model.TypeInt, parsedSchema.Fields[0].Schema.DataType)
	assert.Equal(t, "price", parsedSchema.Fields[2].Name)
	assert.Equal(t, model.TypeDecimal, parsedSchema.Fields[2].Schema.DataType)
}

func TestDelta_SnapshotCommitAndRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/delta_table"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	cityField := &model.Field{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("people", []*model.Field{idField, cityField}, false)

	partField := &model.PartitionField{
		SourceField:   cityField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "people",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/delta_table/city=NYC/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   50,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("NYC")},
		},
		ColumnStats: []*model.ColumnStat{
			{Field: idField, Range: model.NewRange(1, 50), NumNulls: 0},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1},
		SourceIdentifier: "snap-1",
	}

	// 1. Commit snapshot using Target
	target := delta.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Read snapshot using Source
	source := delta.NewSource(memStorage, basePath)
	currentTable, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, "people", currentTable.Name)
	assert.Equal(t, model.TableFormatDelta, currentTable.TableFormat)
	require.Len(t, currentTable.PartitioningFields, 1)
	assert.Equal(t, "city", currentTable.PartitioningFields[0].SourceField.Name)

	readSnapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnapshot.DataFiles, 1)

	readDF := readSnapshot.DataFiles[0]
	assert.Equal(t, int64(50), readDF.RecordCount)
	assert.Equal(t, int64(1024), readDF.FileSizeBytes)
	require.Len(t, readDF.PartitionValues, 1)
	assert.Equal(t, "NYC", readDF.PartitionValues[0].Range.MinValue)

	// 3. Verify TableSyncMetadata
	meta, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, table.LatestCommitTime, meta.LastInstantSynced)
}

func TestDelta_DeletionVectors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/delta_dv_table"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("dv_test", []*model.Field{idField}, false)

	table := &model.Table{
		Name:             "dv_test",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dv := &model.DeletionVector{
		StoragePath: "ab89-deletion-vector.bin",
		Offset:      4,
		SizeInBytes: 32,
		Cardinality: 5,
	}

	dataFile := &model.DataFile{
		PhysicalPath:   "mem://lake/delta_dv_table/data.parquet",
		FileFormat:     model.FileFormatParquet,
		FileSizeBytes:  2048,
		RecordCount:    100,
		LastModified:   time.Now().UnixMilli(),
		DeletionVector: dv,
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	target := delta.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	source := delta.NewSource(memStorage, basePath)
	readSnap, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnap.DataFiles, 1)

	readDF := readSnap.DataFiles[0]
	require.NotNil(t, readDF.DeletionVector)
	assert.Equal(t, "ab89-deletion-vector.bin", readDF.DeletionVector.StoragePath)
	assert.Equal(t, int64(4), readDF.DeletionVector.Offset)
	assert.Equal(t, int64(32), readDF.DeletionVector.SizeInBytes)
	assert.Equal(t, int64(5), readDF.DeletionVector.Cardinality)
}
