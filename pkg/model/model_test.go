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

package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

func TestType_Properties(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		dataType    model.Type
		isNonScalar bool
	}{
		{name: "primitive string", dataType: model.TypeString, isNonScalar: false},
		{name: "primitive int", dataType: model.TypeInt, isNonScalar: false},
		{name: "primitive timestamp", dataType: model.TypeTimestamp, isNonScalar: false},
		{name: "complex record", dataType: model.TypeRecord, isNonScalar: true},
		{name: "complex list", dataType: model.TypeList, isNonScalar: true},
		{name: "complex map", dataType: model.TypeMap, isNonScalar: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.isNonScalar, tc.dataType.IsNonScalar())
		})
	}
}

func TestSchema_AllFieldsAndPathLookup(t *testing.T) {
	t.Parallel()

	// Build a nested schema:
	// id: int
	// name: string
	// location: struct { city: string, zip: string, coordinates: struct { lat: double, lon: double } }
	fieldID := 1
	idField := &model.Field{
		Name:    "id",
		FieldID: &fieldID,
		Schema:  model.NewPrimitiveSchema(model.TypeInt, false),
	}

	fieldID = 2
	nameField := &model.Field{
		Name:    "name",
		FieldID: &fieldID,
		Schema:  model.NewPrimitiveSchema(model.TypeString, true),
	}

	coordLat := &model.Field{
		Name:       "lat",
		ParentPath: "location.coordinates",
		Schema:     model.NewPrimitiveSchema(model.TypeDouble, false),
	}
	coordLon := &model.Field{
		Name:       "lon",
		ParentPath: "location.coordinates",
		Schema:     model.NewPrimitiveSchema(model.TypeDouble, false),
	}
	coordSchema := model.NewRecordSchema("coordinates", []*model.Field{coordLat, coordLon}, false)

	locCity := &model.Field{
		Name:       "city",
		ParentPath: "location",
		Schema:     model.NewPrimitiveSchema(model.TypeString, false),
	}
	locCoords := &model.Field{
		Name:       "coordinates",
		ParentPath: "location",
		Schema:     coordSchema,
	}
	locSchema := model.NewRecordSchema("location", []*model.Field{locCity, locCoords}, true)

	locField := &model.Field{
		Name:   "location",
		Schema: locSchema,
	}

	rootSchema := model.NewRecordSchema("root", []*model.Field{idField, nameField, locField}, false)

	// Verify AllFields traversal
	allFields := rootSchema.AllFields()
	require.Len(t, allFields, 7) // id, name, location, city, coordinates, lat, lon

	// Verify path lookup
	foundLat := rootSchema.FieldByPath("location.coordinates.lat")
	require.NotNil(t, foundLat)
	assert.Equal(t, "lat", foundLat.Name)
	assert.Equal(t, "location.coordinates.lat", foundLat.Path())

	foundCity := rootSchema.FieldByPath("location.city")
	require.NotNil(t, foundCity)
	assert.Equal(t, "city", foundCity.Name)

	assert.Nil(t, rootSchema.FieldByPath("location.unknown"))
	assert.Nil(t, rootSchema.FieldByPath("nonexistent"))
}

func TestDiffFiles(t *testing.T) {
	t.Parallel()

	fileA := &model.DataFile{PhysicalPath: "s3://bucket/table/part-0.parquet", FileSizeBytes: 1024, RecordCount: 100}
	fileB := &model.DataFile{PhysicalPath: "s3://bucket/table/part-1.parquet", FileSizeBytes: 2048, RecordCount: 200}
	fileC := &model.DataFile{PhysicalPath: "s3://bucket/table/part-2.parquet", FileSizeBytes: 4096, RecordCount: 400}

	oldFiles := []*model.DataFile{fileA, fileB}
	newFiles := []*model.DataFile{fileB, fileC}

	diff := model.DiffFiles(oldFiles, newFiles)
	require.True(t, diff.HasChanges())

	require.Len(t, diff.FilesAdded, 1)
	assert.Equal(t, "s3://bucket/table/part-2.parquet", diff.FilesAdded[0].PhysicalPath)

	require.Len(t, diff.FilesRemoved, 1)
	assert.Equal(t, "s3://bucket/table/part-0.parquet", diff.FilesRemoved[0].PhysicalPath)
}

func TestTable_GetDataPathAndPartitioning(t *testing.T) {
	t.Parallel()

	tableUnpartitioned := &model.Table{
		Name:        "unpartitioned_tbl",
		TableFormat: model.TableFormatDelta,
		BasePath:    "s3://bucket/unpartitioned",
	}
	assert.Equal(t, "s3://bucket/unpartitioned", tableUnpartitioned.GetDataPath())
	assert.False(t, tableUnpartitioned.IsPartitioned())

	partField := &model.PartitionField{
		SourceField:   &model.Field{Name: "dt"},
		TransformType: model.PartitionTransformDay,
		Format:        "yyyy-MM-dd",
	}
	tablePartitioned := &model.Table{
		Name:               "partitioned_tbl",
		TableFormat:        model.TableFormatIceberg,
		BasePath:           "s3://bucket/partitioned",
		DataPath:           "s3://bucket/partitioned/data",
		PartitioningFields: []*model.PartitionField{partField},
	}
	assert.Equal(t, "s3://bucket/partitioned/data", tablePartitioned.GetDataPath())
	assert.True(t, tablePartitioned.IsPartitioned())
}
