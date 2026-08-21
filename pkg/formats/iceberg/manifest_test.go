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

// Tests on the bytes a commit leaves behind, rather than on what polytable's own reader makes of
// them. A manifest that both sides of this port agree on can still be one no Iceberg engine can
// open — that is how the JSON manifests survived until DuckDB looked at them — so these assertions
// go through a general Avro decoder and the raw metadata JSON.
package iceberg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// commitPartitionedTable writes one snapshot of a table partitioned by a string column and returns
// the storage it was written into together with its base path.
func commitPartitionedTable(t *testing.T, basePath string) (io.Storage, *model.Table) {
	t.Helper()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	partField := &model.PartitionField{SourceField: regionField, TransformType: model.PartitionTransformValue}

	table := &model.Table{
		Name:               "orders",
		TableFormat:        model.TableFormatIceberg,
		ReadSchema:         model.NewRecordSchema("orders", []*model.Field{idField, regionField}, false),
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table,
		DataFiles: []*model.DataFile{{
			PhysicalPath:  io.JoinPath(basePath, "data", "region=eu", "part-0.parquet"),
			FileFormat:    model.FileFormatParquet,
			FileSizeBytes: 4096,
			RecordCount:   100,
			PartitionValues: []*model.PartitionValue{
				{PartitionField: partField, Range: model.NewScalarRange("eu")},
			},
			ColumnStats: []*model.ColumnStat{
				{Field: idField, Range: model.NewRange(int64(1), int64(100)), TotalValues: 100},
			},
		}},
		SourceIdentifier: "snap-1",
	}))
	return storage, table
}

// metadataFiles maps the base name of every file in a table's metadata directory to its contents.
func metadataFiles(t *testing.T, storage io.Storage, basePath string) map[string][]byte {
	t.Helper()

	entries, err := storage.List(context.Background(), io.JoinPath(basePath, "metadata"))
	require.NoError(t, err)

	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		data, err := storage.Read(context.Background(), entry.Path)
		require.NoError(t, err)
		files[filepath.Base(entry.Path)] = data
	}
	return files
}

// avroHeaderAndRecords decodes an Avro container file with a general decoder, returning its
// key/value metadata and its records.
func avroHeaderAndRecords(t *testing.T, data []byte) (map[string][]byte, []map[string]any) {
	t.Helper()

	require.Equal(t, []byte("Obj\x01"), data[:4], "not an avro object container file")
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)

	var records []map[string]any
	for dec.HasNext() {
		record := make(map[string]any)
		require.NoError(t, dec.Decode(&record))
		records = append(records, record)
	}
	require.NoError(t, dec.Error())
	return dec.Metadata(), records
}

// TestIceberg_CommitWritesAvroManifests is the regression test for T31: the metadata directory
// holds exactly one JSON file per commit — the table metadata — and everything else is Avro.
func TestIceberg_CommitWritesAvroManifests(t *testing.T) {
	t.Parallel()

	basePath := "mem://lake/iceberg_avro"
	storage, _ := commitPartitionedTable(t, basePath)

	var manifests, lists, jsonFiles int
	for name, data := range metadataFiles(t, storage, basePath) {
		switch {
		case strings.HasSuffix(name, ".avro") && strings.HasPrefix(name, "snap-"):
			lists++
			assert.Regexp(t, `^snap-\d+-0-[0-9a-f-]{36}\.avro$`, name)
			assert.Equal(t, []byte("Obj\x01"), data[:4])
		case strings.HasSuffix(name, ".avro"):
			manifests++
			assert.Regexp(t, `^[0-9a-f-]{36}-m0\.avro$`, name)
			assert.Equal(t, []byte("Obj\x01"), data[:4])
		case strings.HasSuffix(name, ".metadata.json"):
			jsonFiles++
		}
	}
	assert.Equal(t, 1, manifests)
	assert.Equal(t, 1, lists)
	assert.Equal(t, 1, jsonFiles)
}

// TestIceberg_ManifestCarriesFieldIDsAndHeader pins the two things an engine needs from a manifest
// that polytable's own reader never looks at: the `field-id` properties of the Avro schema, which
// are how a reader resolves columns, and the key/value header the specification requires.
func TestIceberg_ManifestCarriesFieldIDsAndHeader(t *testing.T) {
	t.Parallel()

	basePath := "mem://lake/iceberg_manifest_header"
	storage, _ := commitPartitionedTable(t, basePath)

	var manifest []byte
	for name, data := range metadataFiles(t, storage, basePath) {
		if strings.HasSuffix(name, "-m0.avro") {
			manifest = data
		}
	}
	require.NotNil(t, manifest, "the commit wrote no manifest")

	meta, records := avroHeaderAndRecords(t, manifest)

	// The header. A reader takes the schema and the partition spec from here, not from the table
	// metadata, because a manifest outlives the spec it was written under.
	assert.Equal(t, []byte("2"), meta["format-version"])
	assert.Equal(t, []byte("data"), meta["content"])
	assert.Equal(t, []byte("0"), meta["partition-spec-id"])
	assert.Contains(t, string(meta["schema"]), `"name":"region"`)
	assert.Contains(t, string(meta["partition-spec"]), `"transform":"identity"`)

	// The field ids, which a schema that has been through a parser and back can silently lose.
	schema := string(meta["avro.schema"])
	for _, want := range []int{
		0,    // status
		2,    // data_file
		100,  // file_path
		102,  // partition
		125,  // lower_bounds
		1000, // the partition field itself
	} {
		assert.Regexp(t, fmt.Sprintf(`"field-id":\s*%d\b`, want), schema, "the manifest schema lost a field id")
	}

	require.Len(t, records, 1)
	dataFile, ok := records[0]["data_file"].(map[string]any)
	require.True(t, ok)
	// PARQUET, not the canonical APACHE_PARQUET: DuckDB rejects anything but the three spellings
	// the specification admits.
	assert.Equal(t, "PARQUET", dataFile["file_format"])
	assert.Equal(t, map[string]any{"region": "eu"}, dataFile["partition"])
	assert.Equal(t, int64(100), dataFile["record_count"])
}

// TestIceberg_MetadataCarriesNameMapping guards what made every column of a polytable-written table
// read as null in DuckDB. The data files polytable describes were written by something else and
// carry no field ids, so the fallback name mapping is the only thing that binds a Parquet column to
// a schema field. It has to be asserted on the raw JSON: a round trip through polytable's own
// source passes either way, because the reader never consults the mapping.
func TestIceberg_MetadataCarriesNameMapping(t *testing.T) {
	t.Parallel()

	basePath := "mem://lake/iceberg_name_mapping"
	storage, _ := commitPartitionedTable(t, basePath)

	raw, err := storage.Read(context.Background(), io.JoinPath(basePath, "metadata", "v1.metadata.json"))
	require.NoError(t, err)

	var metadata struct {
		Properties map[string]string `json:"properties"`
		Schemas    []struct {
			Fields []struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	require.NoError(t, json.Unmarshal(raw, &metadata))

	encoded, ok := metadata.Properties[iceberg.NameMappingProperty]
	require.True(t, ok, "the table property %s is missing", iceberg.NameMappingProperty)

	var mapping []struct {
		FieldID int      `json:"field-id"`
		Names   []string `json:"names"`
	}
	require.NoError(t, json.Unmarshal([]byte(encoded), &mapping))

	require.Len(t, metadata.Schemas, 1)
	require.Len(t, mapping, len(metadata.Schemas[0].Fields))
	for i, field := range metadata.Schemas[0].Fields {
		assert.Equal(t, field.ID, mapping[i].FieldID)
		assert.Equal(t, []string{field.Name}, mapping[i].Names)
	}
}

// TestIceberg_UnsupportedPartitionsFailLoudly covers the two shapes this port cannot put into a
// manifest. Writing them anyway would produce a partition tuple an engine reads as a value the
// table does not hold, which prunes away live rows — a failed sync is the better outcome.
func TestIceberg_UnsupportedPartitionsFailLoudly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		partition *model.PartitionField
		value     any
		wantErr   string
	}{
		{
			name: "non-identity transform",
			partition: &model.PartitionField{
				SourceField:   &model.Field{Name: "day", Schema: model.NewPrimitiveSchema(model.TypeString, true)},
				TransformType: model.PartitionTransformDay,
			},
			value:   "2026-08-21",
			wantErr: "only the identity transform is implemented",
		},
		{
			name: "value that does not fit the column type",
			partition: &model.PartitionField{
				SourceField:   &model.Field{Name: "shard", Schema: model.NewPrimitiveSchema(model.TypeInt, true)},
				TransformType: model.PartitionTransformValue,
			},
			value:   "not-a-number",
			wantErr: "is not a valid int",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/iceberg_bad_partition"
			table := &model.Table{
				Name:               "events",
				TableFormat:        model.TableFormatIceberg,
				ReadSchema:         model.NewRecordSchema("events", []*model.Field{tt.partition.SourceField}, false),
				BasePath:           basePath,
				PartitioningFields: []*model.PartitionField{tt.partition},
				LatestCommitTime:   time.Now().UnixMilli(),
			}

			target := iceberg.NewTarget(storage)
			require.NoError(t, target.Init(ctx, table))
			err := target.CommitSnapshot(ctx, &model.Snapshot{
				Table: table,
				DataFiles: []*model.DataFile{{
					PhysicalPath: io.JoinPath(basePath, "data", "part-0.parquet"),
					FileFormat:   model.FileFormatParquet,
					RecordCount:  1,
					PartitionValues: []*model.PartitionValue{
						{PartitionField: tt.partition, Range: model.NewScalarRange(tt.value)},
					},
				}},
				SourceIdentifier: "snap-1",
			})
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
