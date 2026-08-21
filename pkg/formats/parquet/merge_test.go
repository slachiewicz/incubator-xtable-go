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

	pqformat "github.com/slachiewicz/polytable/pkg/formats/parquet"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// The three generations of one evolving table: the second adds a column, the third gives that
// column a different physical type.
type (
	evoV1 struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	evoV2 struct {
		ID    int64   `parquet:"id"`
		Name  string  `parquet:"name"`
		Score float64 `parquet:"score"`
	}
	evoConflict struct {
		ID    int64  `parquet:"id"`
		Name  string `parquet:"name"`
		Score string `parquet:"score"`
	}
)

// parquetRows writes one file holding rows of T and returns its bytes.
func parquetRows[T any](t *testing.T, rows []T) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buf)
	_, err := writer.Write(rows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

// sourceFile is one file of a fixture directory. The slice order is the write order, so the last
// entry is the newest file whatever it is called.
type sourceFile struct {
	path string
	data func(t *testing.T) []byte
}

func v1File(t *testing.T) []byte {
	t.Helper()
	return parquetRows(t, []evoV1{{ID: 1, Name: "alice"}})
}

func v2File(t *testing.T) []byte {
	t.Helper()
	return parquetRows(t, []evoV2{{ID: 2, Name: "bob", Score: 4.5}})
}

func conflictFile(t *testing.T) []byte {
	t.Helper()
	return parquetRows(t, []evoConflict{{ID: 3, Name: "carol", Score: "high"}})
}

// wantField is one expected column of the merged schema.
type wantField struct {
	name     string
	dataType model.Type
	nullable bool
}

// writeSourceFiles lays the files down under basePath in slice order, so that the modification
// times follow the generations.
func writeSourceFiles(t *testing.T, storage *io.MemoryStorage, basePath string, files []sourceFile) {
	t.Helper()

	ctx := context.Background()
	for _, f := range files {
		require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, f.path), f.data(t)))
	}
}

// TestParquet_SourceMergesFooterSchemas pins T33: the read schema of an unmanaged Parquet directory
// is the merge of every file's footer, not the footer of whichever file the listing returned first.
// A column only some files carry is nullable in the merge, and two files disagreeing on a column's
// type is an error rather than a silent pick.
func TestParquet_SourceMergesFooterSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		files   []sourceFile
		want    []wantField
		wantErr []string
	}{
		{
			name: "the newest generation adds a column",
			files: []sourceFile{
				{path: "part-a.parquet", data: v1File},
				{path: "part-b.parquet", data: v2File},
			},
			want: []wantField{
				{name: "id", dataType: model.TypeLong},
				{name: "name", dataType: model.TypeString},
				{name: "score", dataType: model.TypeDouble, nullable: true},
			},
		},
		{
			// Same two generations, names swapped, so the file that sorts first is now the newer
			// one. The merged schema may not move with it.
			name: "renaming the files does not change the merge",
			files: []sourceFile{
				{path: "part-z.parquet", data: v1File},
				{path: "part-a.parquet", data: v2File},
			},
			want: []wantField{
				{name: "id", dataType: model.TypeLong},
				{name: "name", dataType: model.TypeString},
				{name: "score", dataType: model.TypeDouble, nullable: true},
			},
		},
		{
			// The newest generation dropped a column: the merge is a superset, so the column
			// survives and becomes nullable.
			name: "a column the newest generation dropped survives as nullable",
			files: []sourceFile{
				{path: "part-a.parquet", data: v2File},
				{path: "part-b.parquet", data: v1File},
			},
			want: []wantField{
				{name: "id", dataType: model.TypeLong},
				{name: "name", dataType: model.TypeString},
				{name: "score", dataType: model.TypeDouble, nullable: true},
			},
		},
		{
			name: "one generation only",
			files: []sourceFile{
				{path: "part-a.parquet", data: v1File},
				{path: "part-b.parquet", data: v1File},
			},
			want: []wantField{
				{name: "id", dataType: model.TypeLong},
				{name: "name", dataType: model.TypeString},
			},
		},
		{
			name: "a type conflict names the column, both types and both files",
			files: []sourceFile{
				{path: "part-a.parquet", data: v2File},
				{path: "part-b.parquet", data: conflictFile},
			},
			wantErr: []string{"score", "DOUBLE", "STRING", "part-a.parquet", "part-b.parquet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/merge/" + tt.name
			writeSourceFiles(t, storage, basePath, tt.files)

			snapshot, err := pqformat.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				for _, fragment := range tt.wantErr {
					assert.ErrorContains(t, err, fragment)
				}
				return
			}
			require.NoError(t, err)

			schema := snapshot.Table.ReadSchema
			require.NotNil(t, schema)
			require.Len(t, schema.Fields, len(tt.want))
			for i, expected := range tt.want {
				field := schema.Fields[i]
				assert.Equal(t, expected.name, field.Name)
				require.NotNil(t, field.Schema)
				assert.Equal(t, expected.dataType, field.Schema.DataType, "type of %s", expected.name)
				assert.Equal(t, expected.nullable, field.Schema.IsNullable, "nullability of %s", expected.name)
			}
		})
	}
}

// TestParquet_SourceSynthesizesPartitionColumn pins the second half of T33: a Hive partition column
// lives in the directory name only, so the source has to put it in the schema itself, typed from
// the values it observed. A column the data files already carry wins over the synthesized one.
func TestParquet_SourceSynthesizesPartitionColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// dirs are the partition directories, one file of TestRecord data in each.
		dirs []string
		// want lists the columns the schema must hold after the three physical ones.
		want []wantField
		// wantPartitions is the partition spec, in directory order.
		wantPartitions []string
		wantFieldCount int
		wantErr        []string
	}{
		{
			name:           "non-numeric values are strings",
			dirs:           []string{"region=east", "region=west"},
			want:           []wantField{{name: "region", dataType: model.TypeString, nullable: true}},
			wantPartitions: []string{"region"},
			wantFieldCount: 4,
		},
		{
			name:           "integral values are longs",
			dirs:           []string{"year=2026", "year=2027"},
			want:           []wantField{{name: "year", dataType: model.TypeLong, nullable: true}},
			wantPartitions: []string{"year"},
			wantFieldCount: 4,
		},
		{
			name:           "one fractional value makes the column a double",
			dirs:           []string{"ratio=2", "ratio=1.5"},
			want:           []wantField{{name: "ratio", dataType: model.TypeDouble, nullable: true}},
			wantPartitions: []string{"ratio"},
			wantFieldCount: 4,
		},
		{
			name:           "ISO dates are dates",
			dirs:           []string{"day=2026-01-31", "day=2026-02-01"},
			want:           []wantField{{name: "day", dataType: model.TypeDate, nullable: true}},
			wantPartitions: []string{"day"},
			wantFieldCount: 4,
		},
		{
			name:           "a value that does not fit falls back to string",
			dirs:           []string{"year=2026", "year=unknown"},
			want:           []wantField{{name: "year", dataType: model.TypeString, nullable: true}},
			wantPartitions: []string{"year"},
			wantFieldCount: 4,
		},
		{
			name:           "the Hive null marker is ambiguous, so the column is a string",
			dirs:           []string{"year=2026", "year=__HIVE_DEFAULT_PARTITION__"},
			want:           []wantField{{name: "year", dataType: model.TypeString, nullable: true}},
			wantPartitions: []string{"year"},
			wantFieldCount: 4,
		},
		{
			name: "nested partitions keep the directory order",
			dirs: []string{"year=2026/month=01", "year=2026/month=02"},
			want: []wantField{
				{name: "year", dataType: model.TypeLong, nullable: true},
				{name: "month", dataType: model.TypeLong, nullable: true},
			},
			wantPartitions: []string{"year", "month"},
			wantFieldCount: 5,
		},
		{
			// The data files carry a "name" column already, so nothing is synthesized and the
			// partition field points at the physical column.
			name:           "a physical column of the same name wins",
			dirs:           []string{"name=alice", "name=bob"},
			want:           []wantField{{name: "name", dataType: model.TypeString}},
			wantPartitions: []string{"name"},
			wantFieldCount: 3,
		},
		{
			// "id" is a physical LONG column; a directory value that is not a number would make the
			// table describe itself wrongly, so it is an error.
			name:    "directory values must fit the physical column they collide with",
			dirs:    []string{"id=abc"},
			wantErr: []string{"id", "abc", "LONG"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/partition/" + tt.name
			records := []TestRecord{{ID: 1, Name: "alice", Score: 1.5}}
			for _, dir := range tt.dirs {
				path := io.JoinPath(basePath, dir, "part-0.parquet")
				require.NoError(t, storage.Write(ctx, path, createParquetBytes(t, records)))
			}

			snapshot, err := pqformat.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				for _, fragment := range tt.wantErr {
					assert.ErrorContains(t, err, fragment)
				}
				return
			}
			require.NoError(t, err)

			schema := snapshot.Table.ReadSchema
			require.NotNil(t, schema)
			require.Len(t, schema.Fields, tt.wantFieldCount)

			for _, expected := range tt.want {
				field := schema.FieldByPath(expected.name)
				require.NotNil(t, field, "the partition column %s is missing from the schema", expected.name)
				assert.Equal(t, expected.dataType, field.Schema.DataType, "type of %s", expected.name)
				assert.Equal(t, expected.nullable, field.Schema.IsNullable, "nullability of %s", expected.name)
			}

			gotPartitions := make([]string, 0, len(snapshot.Table.PartitioningFields))
			for _, pf := range snapshot.Table.PartitioningFields {
				gotPartitions = append(gotPartitions, pf.SourceField.Name)
				// The partition field and the schema must describe one column, not two.
				assert.Same(t, schema.FieldByPath(pf.SourceField.Name), pf.SourceField,
					"the partition field %s is not the schema's field", pf.SourceField.Name)
			}
			assert.Equal(t, tt.wantPartitions, gotPartitions)

			for _, df := range snapshot.DataFiles {
				require.Len(t, df.PartitionValues, len(tt.wantPartitions))
			}
		})
	}
}

// TestParquet_StatsFollowEachFilesOwnColumns checks that the merged schema does not make the
// per-file statistics lie: a column a file does not carry contributes no statistics for that file,
// rather than a zero-valued entry, and the statistics that are reported point at the merged
// schema's fields.
func TestParquet_StatsFollowEachFilesOwnColumns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/heterogeneous_stats"
	writeSourceFiles(t, storage, basePath, []sourceFile{
		{path: "part-old.parquet", data: v1File},
		{path: "part-new.parquet", data: v2File},
	})

	snapshot, err := pqformat.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 2)

	schema := snapshot.Table.ReadSchema
	byFile := make(map[string]map[string]*model.ColumnStat, len(snapshot.DataFiles))
	for _, df := range snapshot.DataFiles {
		stats := make(map[string]*model.ColumnStat, len(df.ColumnStats))
		for _, cs := range df.ColumnStats {
			require.NotNil(t, cs.Field)
			assert.Same(t, schema.FieldByPath(cs.Field.Name), cs.Field,
				"the statistics of %s point at a field outside the read schema", cs.Field.Name)
			stats[cs.Field.Name] = cs
		}
		byFile[df.PhysicalPath] = stats
	}

	older := byFile[io.JoinPath(basePath, "part-old.parquet")]
	require.NotNil(t, older)
	assert.NotContains(t, older, "score", "the older file has no score column, so it has no score statistics")
	require.Contains(t, older, "id")

	newer := byFile[io.JoinPath(basePath, "part-new.parquet")]
	require.NotNil(t, newer)
	require.Contains(t, newer, "score")
	require.NotNil(t, newer["score"].Range)
	assert.InDelta(t, 4.5, newer["score"].Range.MinValue, 0)
}
