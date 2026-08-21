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

// Reader tests against tables written by other engines.
//
// Every other suite in this repository reads metadata polytable itself wrote, so a reader that
// agrees with polytable's writer passes even where both disagree with the format. The fixtures under
// testdata/fixtures come from delta-rs and pyiceberg, neither of which has ever seen this code, and
// `test/fixtures/generate.py` writes the manifest.json each assertion below is checked against —
// regenerating a fixture regenerates its expectations, so nothing here is a literal copied from a
// past run.
//
// Spark- and Hudi-written fixtures are deliberately absent: both need a JVM, which is T30's problem.
package test_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

const fixtureRoot = "testdata/fixtures"

// fixtureField is one column as the writer reported it.
type fixtureField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	FieldID  *int   `json:"field_id,omitempty"`
}

// fixtureDataFile is one data file as the writer reported it, its path relative to the table dir.
type fixtureDataFile struct {
	Path            string            `json:"path"`
	RecordCount     int64             `json:"record_count"`
	SizeBytes       int64             `json:"size_bytes"`
	PartitionValues map[string]string `json:"partition_values"`
}

// fixtureBounds is a writer-reported min/max pair. Both are read as float64 because that is what a
// Delta stats string and an Iceberg bound both decode to once they have been through JSON.
type fixtureBounds struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// fixtureManifest is the record generate.py leaves beside each fixture.
type fixtureManifest struct {
	Format           string                   `json:"format"`
	TableName        string                   `json:"table_name"`
	TableDir         string                   `json:"table_dir"`
	CommitCount      int                      `json:"commit_count"`
	SnapshotCount    int                      `json:"snapshot_count"`
	TotalRows        int64                    `json:"total_rows"`
	DataFileCount    int                      `json:"data_file_count"`
	Schema           []fixtureField           `json:"schema"`
	PartitionColumns []string                 `json:"partition_columns"`
	PartitionValues  []string                 `json:"partition_values"`
	ColumnBounds     map[string]fixtureBounds `json:"column_bounds"`
	DataFiles        []fixtureDataFile        `json:"data_files"`
	PathPlaceholder  string                   `json:"path_placeholder"`
	SchemaEvolution  struct {
		AddedColumn   string `json:"added_column"`
		AddedAtCommit string `json:"added_at_commit"`
	} `json:"schema_evolution"`
	Writer struct {
		Library string `json:"library"`
		Version string `json:"version"`
	} `json:"writer"`
}

// loadFixture copies a fixture's table directory into a temporary directory and returns the copy's
// path together with the manifest. The copy is not a convenience: a conversion writes its target
// metadata into the table's base path, and testdata must not be dirtied by running the tests.
//
// Iceberg records absolute locations inside its metadata, so generate.py replaced the
// generation-time warehouse path with a placeholder; substituting the temporary directory here is
// what makes the fixture relocatable.
func loadFixture(t *testing.T, name string) (string, *fixtureManifest) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(fixtureRoot, name, "manifest.json"))
	require.NoError(t, err)

	var manifest fixtureManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotEmpty(t, manifest.TableDir, "manifest is missing table_dir")

	source := filepath.Join(fixtureRoot, name, manifest.TableDir)
	dest := filepath.Join(t.TempDir(), manifest.TableDir)
	require.NoError(t, os.CopyFS(dest, os.DirFS(source)))

	if manifest.PathPlaceholder != "" {
		rewriteMetadataPaths(t, dest, manifest.PathPlaceholder, "file://"+dest)
	}
	return dest, &manifest
}

func rewriteMetadataPaths(t *testing.T, dir, from, to string) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "metadata", "*.metadata.json"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "fixture declares a path placeholder but has no JSON metadata")

	for _, match := range matches {
		data, err := os.ReadFile(match)
		require.NoError(t, err)
		rewritten := []byte(strings.ReplaceAll(string(data), from, to))
		//nolint:gosec // G703: the path is a glob of this test's own temporary directory
		require.NoError(t, os.WriteFile(match, rewritten, 0o600))
	}
}

// numericValue coerces a bound to float64 whatever concrete numeric type the reader produced.
func numericValue(t *testing.T, value any) float64 {
	t.Helper()

	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	case int:
		return float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		require.NoError(t, err)
		return parsed
	default:
		t.Fatalf("bound %v has non-numeric type %T", value, value)
		return 0
	}
}

// assertSchemaMatchesManifest checks the read schema field for field, in order.
func assertSchemaMatchesManifest(t *testing.T, manifest *fixtureManifest, schema *model.Schema) {
	t.Helper()

	require.NotNil(t, schema)
	require.Len(t, schema.Fields, len(manifest.Schema))
	for i, expected := range manifest.Schema {
		field := schema.Fields[i]
		assert.Equal(t, expected.Name, field.Name)
		require.NotNil(t, field.Schema, "field %s carries no schema", expected.Name)
		assert.Equal(t, model.Type(expected.Type), field.Schema.DataType, "field %s type", expected.Name)
		assert.Equal(t, expected.Nullable, field.Schema.IsNullable, "field %s nullability", expected.Name)
		if expected.FieldID != nil {
			require.NotNil(t, field.FieldID, "field %s lost its field ID", expected.Name)
			assert.Equal(t, *expected.FieldID, *field.FieldID, "field %s ID", expected.Name)
		}
	}

	// The column the writer added mid-history has to be present, or the reader stopped at the
	// first schema it saw instead of following the evolution.
	if added := manifest.SchemaEvolution.AddedColumn; added != "" {
		assert.NotNil(t, schema.FieldByPath(added), "the mid-history column %q is missing", added)
	}
}

// relativeFilePaths maps each data file's path relative to the table directory to its record count.
func relativeFilePaths(t *testing.T, tableDir string, files []*model.DataFile) map[string]int64 {
	t.Helper()

	byPath := make(map[string]int64, len(files))
	for _, file := range files {
		path := strings.TrimPrefix(file.PhysicalPath, tableDir)
		path = strings.TrimPrefix(path, "/")
		require.NotEmpty(t, path, "data file path %q is not under %q", file.PhysicalPath, tableDir)
		byPath[path] = file.RecordCount
	}
	return byPath
}

// TestForeignFixtures_ReadTable is the part both fixtures pass: the table descriptor and schema,
// including the mid-history column addition and the partition spec.
func TestForeignFixtures_ReadTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fixture string
		format  model.TableFormat
	}{
		{name: "delta-rs", fixture: "delta-rs", format: model.TableFormatDelta},
		{name: "pyiceberg", fixture: "pyiceberg", format: model.TableFormatIceberg},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			tableDir, manifest := loadFixture(t, tt.fixture)
			require.Equal(t, string(tt.format), manifest.Format)

			source, err := formats.NewSource(tt.format, io.NewLocalStorage(), tableDir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })

			table, err := source.GetCurrentTable(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.format, table.TableFormat)
			assert.Positive(t, table.LatestCommitTime, "no commit instant was derived")

			assertSchemaMatchesManifest(t, manifest, table.ReadSchema)

			require.Len(t, table.PartitioningFields, len(manifest.PartitionColumns))
			for i, column := range manifest.PartitionColumns {
				partition := table.PartitioningFields[i]
				require.NotNil(t, partition.SourceField)
				assert.Equal(t, column, partition.SourceField.Name)
				assert.Equal(t, model.PartitionTransformValue, partition.TransformType)
			}
		})
	}
}

// TestForeignFixtures_ReadDeltaSnapshot checks the delta-rs file list, row counts, partition values
// and the statistics delta-rs recorded, all against the manifest rather than against literals.
func TestForeignFixtures_ReadDeltaSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadFixture(t, "delta-rs")

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	byPath := relativeFilePaths(t, tableDir, snapshot.DataFiles)
	var total int64
	for _, expected := range manifest.DataFiles {
		actual, ok := byPath[expected.Path]
		require.True(t, ok, "the reader did not report %s", expected.Path)
		assert.Equal(t, expected.RecordCount, actual, "row count of %s", expected.Path)
		total += actual
	}
	assert.Equal(t, manifest.TotalRows, total)

	// Partition values come from the add action's partitionValues map, not from the directory name.
	partitionValues := make(map[string]int)
	for _, file := range snapshot.DataFiles {
		require.Len(t, file.PartitionValues, len(manifest.PartitionColumns))
		for _, value := range file.PartitionValues {
			require.NotNil(t, value.Range)
			partitionValues[fmt.Sprint(value.Range.MinValue)]++
		}
		assert.Positive(t, file.FileSizeBytes, "%s has no size", file.PhysicalPath)
	}
	for _, expected := range manifest.PartitionValues {
		assert.Contains(t, partitionValues, expected)
	}

	// Fold the per-file bounds back into table-wide ones; delta-rs recorded the same fold.
	bounds := make(map[string]fixtureBounds)
	for _, file := range snapshot.DataFiles {
		for _, stat := range file.ColumnStats {
			if stat.Range == nil || stat.Range.MinValue == nil {
				continue
			}
			name := stat.Field.Name
			low, high := numericValue(t, stat.Range.MinValue), numericValue(t, stat.Range.MaxValue)
			if current, seen := bounds[name]; seen {
				low = min(low, current.Min)
				high = max(high, current.Max)
			}
			bounds[name] = fixtureBounds{Min: low, Max: high}
		}
	}
	for name, expected := range manifest.ColumnBounds {
		actual, ok := bounds[name]
		require.True(t, ok, "no statistics were read for column %s", name)
		assert.InDelta(t, expected.Min, actual.Min, 1e-9, "minimum of %s", name)
		assert.InDelta(t, expected.Max, actual.Max, 1e-9, "maximum of %s", name)
	}
}

// TestForeignFixtures_ReadDeltaHistory walks the delta-rs log as an incremental backlog: every
// commit the writer made has to come back, and the schema reported for each one has to be the schema
// as of that commit, not the latest. The fixture adds its column in the last commit, so a reader
// that reconstructs history from the newest metaData action alone fails here.
func TestForeignFixtures_ReadDeltaHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadFixture(t, "delta-rs")

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	changes, err := source.GetChangesSince(ctx, 0)
	require.NoError(t, err)
	require.Len(t, changes.TableChanges, manifest.CommitCount)

	added := manifest.SchemaEvolution.AddedColumn
	require.NotEmpty(t, added)

	var total int64
	for _, change := range changes.TableChanges {
		for _, file := range change.FilesDiff.FilesAdded {
			total += file.RecordCount
		}
		assert.Empty(t, change.FilesDiff.FilesRemoved, "commit %s removed a file", change.SourceIdentifier)

		field := change.TableAsOfChange.ReadSchema.FieldByPath(added)
		if change.SourceIdentifier == manifest.SchemaEvolution.AddedAtCommit {
			assert.NotNil(t, field, "commit %s added %s but does not report it", change.SourceIdentifier, added)
			continue
		}
		assert.Nil(t, field, "commit %s predates %s but already reports it", change.SourceIdentifier, added)
	}
	assert.Equal(t, manifest.TotalRows, total, "the commits do not add up to the table")
}

// TestForeignFixtures_ReadIcebergSnapshotIsUnsupported pins the gap T28 found: polytable's Iceberg
// source parses manifest lists and manifests as JSON, which only its own target writes. Every real
// writer emits the Avro the spec mandates, so the file list of a foreign Iceberg table cannot be
// read at all.
//
// The assertion distinguishes the parse failure from a missing file on purpose: a NotFound here
// would mean the fixture's path rewrite is broken rather than the reader.
func TestForeignFixtures_ReadIcebergSnapshotIsUnsupported(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, _ := loadFixture(t, "pyiceberg")

	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	_, err = source.GetCurrentSnapshot(ctx)
	require.Error(t, err, "reading Avro manifests now works; drop this pin and assert the file list")
	assert.NotErrorIs(t, err, io.ErrNotFound, "the manifest list was not found, so the path rewrite is wrong")
	assert.ErrorContains(t, err, "avro manifest lists are not supported yet")
}

// convertExpectation records how far a target can be verified today. Everything the conversion
// really does — file list, row counts — is asserted for every target; the two fields mark the two
// places where T28 found the round trip stops short, so that a fix turns a pin into a failure.
type convertExpectation struct {
	// readBackError is the error the target's own source returns instead of a snapshot.
	readBackError string
	// schemaCheck asserts the schema the target's source recovered.
	schemaCheck func(t *testing.T, manifest *fixtureManifest, schema *model.Schema)
}

// TestForeignFixtures_ConvertDelta syncs the delta-rs table into every other supported target and
// reads the result back through that format's own source, checking the file list and row count
// survived the translation.
func TestForeignFixtures_ConvertDelta(t *testing.T) {
	t.Parallel()

	expectations := map[model.TableFormat]convertExpectation{
		model.TableFormatIceberg: {schemaCheck: assertSchemaMatchesManifest},
		model.TableFormatHudi:    {schemaCheck: assertSchemaMatchesManifest},
		// The Parquet source rebuilds the schema from a data file footer, which is missing the
		// Hive partition column entirely.
		model.TableFormatParquet: {schemaCheck: assertParquetSchemaGaps},
		// T32 aligned the Paimon target with the schema/ + snapshot/ layout its source reads, so a
		// Paimon table polytable wrote is now read back like any other target.
		model.TableFormatPaimon: {schemaCheck: assertSchemaMatchesManifest},
	}

	for _, target := range formats.SupportedTargets() {
		if target == model.TableFormatDelta {
			continue
		}
		expected, known := expectations[target]
		require.True(t, known, "target %s is new; decide what a converted fixture must prove", target)

		t.Run(strings.ToLower(string(target)), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			tableDir, manifest := loadFixture(t, "delta-rs")
			storage := io.NewLocalStorage()

			results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
				SourceFormat:  model.TableFormatDelta,
				TargetFormats: []model.TableFormat{target},
				TableBasePath: tableDir,
				TableName:     manifest.TableName,
				SyncMode:      spi.SyncModeFull,
			})
			require.NoError(t, err)
			require.Equal(t, spi.SyncStatusSuccess, results[target].StatusCode, results[target].Error)

			source, err := formats.NewSource(target, storage, tableDir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })

			snapshot, err := source.GetCurrentSnapshot(ctx)
			if expected.readBackError != "" {
				require.Error(t, err, "%s can be read back now; drop the pin and assert the file list", target)
				assert.ErrorContains(t, err, expected.readBackError)
				return
			}
			require.NoError(t, err)
			require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

			byPath := relativeFilePaths(t, tableDir, snapshot.DataFiles)
			var total int64
			for _, file := range manifest.DataFiles {
				actual, ok := byPath[file.Path]
				require.True(t, ok, "%s dropped %s", target, file.Path)
				assert.Equal(t, file.RecordCount, actual, "row count of %s", file.Path)
				total += actual
			}
			assert.Equal(t, manifest.TotalRows, total)

			require.NotNil(t, expected.schemaCheck)
			expected.schemaCheck(t, manifest, snapshot.Table.ReadSchema)
		})
	}
}

// assertParquetSchemaGaps pins what the raw-Parquet source recovers from a Hive-partitioned,
// schema-evolved table. Two things it does not, both found by this fixture and recorded under T28:
//
//   - The partition column lives in the directory name, not in the data files, so it is absent from
//     the schema even though the source does report it as a partitioning field. The table it hands
//     back is partitioned by a column its own schema does not have.
//   - The schema comes from the footer of a single data file, so a column added mid-history is
//     present or absent depending on which file sorts first. Nothing is asserted about the evolved
//     column here for exactly that reason — a regenerated fixture mints new file names.
func assertParquetSchemaGaps(t *testing.T, manifest *fixtureManifest, schema *model.Schema) {
	t.Helper()

	require.NotNil(t, schema)
	partitionColumns := make(map[string]bool, len(manifest.PartitionColumns))
	for _, column := range manifest.PartitionColumns {
		partitionColumns[column] = true
	}

	for _, field := range manifest.Schema {
		switch {
		case partitionColumns[field.Name]:
			assert.Nil(t, schema.FieldByPath(field.Name),
				"the partition column %s is now recovered; assert it instead of pinning its absence",
				field.Name)
		case field.Name == manifest.SchemaEvolution.AddedColumn:
			continue
		default:
			assert.NotNil(t, schema.FieldByPath(field.Name), "column %s was dropped", field.Name)
		}
	}
}

// TestForeignFixtures_ConvertIcebergIsUnsupported records the consequence of the Avro gap for the
// conversion path: a real Iceberg table cannot be a conversion source at all, because the controller
// needs the snapshot's file list.
func TestForeignFixtures_ConvertIcebergIsUnsupported(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadFixture(t, "pyiceberg")

	results, err := conversion.NewController(io.NewLocalStorage()).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
	})
	if err == nil {
		require.NotEqual(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode,
			"converting a pyiceberg table now works; drop this pin and assert the target's file list")
		assert.Contains(t, results[model.TableFormatDelta].Error, "avro manifest lists are not supported yet")
		return
	}
	assert.ErrorContains(t, err, "avro manifest lists are not supported yet")
}
