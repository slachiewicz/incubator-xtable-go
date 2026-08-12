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

func TestParseTableFormatIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  model.TableFormat
	}{
		{name: "upper", input: "DELTA", want: model.TableFormatDelta},
		{name: "lower", input: "delta", want: model.TableFormatDelta},
		{name: "mixed", input: "Delta", want: model.TableFormatDelta},
		{name: "mixed iceberg", input: "IceBerg", want: model.TableFormatIceberg},
		{name: "surrounding whitespace", input: "  hudi  ", want: model.TableFormatHudi},
		{name: "mixed paimon", input: "Paimon", want: model.TableFormatPaimon},
		{name: "mixed parquet", input: "Parquet", want: model.TableFormatParquet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := model.ParseTableFormat(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTableFormatRejectsUnknownWithGuidance(t *testing.T) {
	t.Parallel()

	_, err := model.ParseTableFormat("orc")
	require.Error(t, err)
	// The message must name the accepted values; "unknown table format: orc" alone left the user
	// guessing at the spelling.
	assert.Contains(t, err.Error(), "DELTA")
	assert.Contains(t, err.Error(), "PARQUET")
}

func TestDiffFilesDetectsRewrittenFiles(t *testing.T) {
	t.Parallel()

	file := func(path string, size, records int64) *model.DataFile {
		return &model.DataFile{PhysicalPath: path, FileSizeBytes: size, RecordCount: records}
	}

	tests := []struct {
		name        string
		oldFiles    []*model.DataFile
		newFiles    []*model.DataFile
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:      "identical file is unchanged",
			oldFiles:  []*model.DataFile{file("a.parquet", 100, 10)},
			newFiles:  []*model.DataFile{file("a.parquet", 100, 10)},
			wantAdded: nil, wantRemoved: nil,
		},
		{
			// Previously reported as unchanged: the diff keyed on path alone.
			name:        "same path with a different size is a rewrite",
			oldFiles:    []*model.DataFile{file("a.parquet", 100, 10)},
			newFiles:    []*model.DataFile{file("a.parquet", 250, 10)},
			wantAdded:   []string{"a.parquet"},
			wantRemoved: []string{"a.parquet"},
		},
		{
			name:        "same path with a different record count is a rewrite",
			oldFiles:    []*model.DataFile{file("a.parquet", 100, 10)},
			newFiles:    []*model.DataFile{file("a.parquet", 100, 99)},
			wantAdded:   []string{"a.parquet"},
			wantRemoved: []string{"a.parquet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			diff := model.DiffFiles(tt.oldFiles, tt.newFiles)

			added := make([]string, 0, len(diff.FilesAdded))
			for _, f := range diff.FilesAdded {
				added = append(added, f.PhysicalPath)
			}
			removed := make([]string, 0, len(diff.FilesRemoved))
			for _, f := range diff.FilesRemoved {
				removed = append(removed, f.PhysicalPath)
			}

			assert.Equal(t, tt.wantAdded, nilIfEmpty(added))
			assert.Equal(t, tt.wantRemoved, nilIfEmpty(removed))
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestDiffFilesIsDeterministic(t *testing.T) {
	t.Parallel()

	mk := func(paths ...string) []*model.DataFile {
		out := make([]*model.DataFile, 0, len(paths))
		for _, p := range paths {
			out = append(out, &model.DataFile{PhysicalPath: p, FileSizeBytes: 1, RecordCount: 1})
		}
		return out
	}
	oldFiles := mk("c.parquet", "a.parquet", "b.parquet")
	newFiles := mk("e.parquet", "d.parquet", "a.parquet")

	first := model.DiffFiles(oldFiles, newFiles)
	for range 20 {
		got := model.DiffFiles(oldFiles, newFiles)
		assert.Equal(t, first.FilesAdded, got.FilesAdded, "FilesAdded ordering must not depend on map iteration")
		assert.Equal(t, first.FilesRemoved, got.FilesRemoved, "FilesRemoved ordering must not depend on map iteration")
	}
	// Sorted, so callers can assert on it.
	assert.Equal(t, "d.parquet", first.FilesAdded[0].PhysicalPath)
	assert.Equal(t, "b.parquet", first.FilesRemoved[0].PhysicalPath)
}

func TestFieldByPathPrefersExactCase(t *testing.T) {
	t.Parallel()

	lower := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	upper := &model.Field{Name: "Name", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	// "Name" is declared second on purpose: a fold-only match would return "name" for both lookups.
	schema := model.NewRecordSchema("rec", []*model.Field{lower, upper}, false)

	assert.Same(t, lower, schema.FieldByPath("name"), "exact match must win")
	assert.Same(t, upper, schema.FieldByPath("Name"), "exact match must win regardless of declaration order")

	// The case-insensitive fallback survives, since format metadata does not always agree on case.
	only := model.NewRecordSchema("rec", []*model.Field{lower}, false)
	assert.Same(t, lower, only.FieldByPath("NAME"), "fallback must still resolve when no exact match exists")
	assert.Nil(t, only.FieldByPath("missing"))
}
