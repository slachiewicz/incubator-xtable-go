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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
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
	ManifestEncoding  string                   `json:"manifest_encoding"`
	CurrentSnapshotID string                   `json:"current_snapshot_id"`
	Format            string                   `json:"format"`
	TableName         string                   `json:"table_name"`
	TableDir          string                   `json:"table_dir"`
	CommitCount       int                      `json:"commit_count"`
	SnapshotCount     int                      `json:"snapshot_count"`
	TotalRows         int64                    `json:"total_rows"`
	DataFileCount     int                      `json:"data_file_count"`
	Schema            []fixtureField           `json:"schema"`
	PartitionColumns  []string                 `json:"partition_columns"`
	PartitionValues   []string                 `json:"partition_values"`
	ColumnBounds      map[string]fixtureBounds `json:"column_bounds"`
	DataFiles         []fixtureDataFile        `json:"data_files"`
	PathPlaceholder   string                   `json:"path_placeholder"`
	SchemaEvolution   struct {
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
	if manifest.ManifestEncoding == "avro" {
		relocateAvroManifests(t, dest)
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

// manifestPathPattern matches an Avro manifest or manifest list inside a table's metadata
// directory, capturing the table location the writer recorded.
var manifestPathPattern = regexp.MustCompile(`^(.*)/metadata/[^/]+\.avro$`)

// relocateAvroManifests rewrites the table location recorded inside a fixture's Avro manifests to
// the directory the fixture was copied into.
//
// The placeholder substitution generate.py applies reaches the JSON metadata only: an Avro object
// container file is compressed, so no textual replacement can touch the manifest paths and data
// file paths inside it, and those are absolute paths from the machine that generated the fixture.
// Decoding the records, replacing the location and encoding them again under the file's own schema
// leaves the shape pyiceberg wrote intact — the schema, the field ids and the metadata all come
// straight back out of the file — while making the fixture readable from wherever it now lives.
func relocateAvroManifests(t *testing.T, dir string) {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(dir, "metadata", "*.avro"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "the fixture declares avro manifests but has none")

	from := recordedTableLocation(t, files)
	to := "file://" + dir

	// Manifests are rewritten first so that the manifest lists can record the length each one ends
	// up with: re-encoding does not preserve the byte count, and manifest_length is part of what a
	// reader plans splits with.
	lengths := make(map[string]int64, len(files))
	for _, file := range files {
		if isManifestList(file) {
			continue
		}
		lengths[fileURI(file)] = rewriteAvroRecords(t, file, from, to, nil)
	}
	for _, file := range files {
		if !isManifestList(file) {
			continue
		}
		rewriteAvroRecords(t, file, from, to, lengths)
	}
}

// isManifestList reports whether a metadata file is a manifest list rather than a manifest.
func isManifestList(path string) bool {
	return strings.HasPrefix(filepath.Base(path), "snap-")
}

// fileURI is the location a relocated manifest will be referred to by.
func fileURI(path string) string {
	return "file://" + path
}

// recordedTableLocation recovers the table location the fixture's writer recorded, by reading a
// manifest path back out of one of the manifest lists.
func recordedTableLocation(t *testing.T, files []string) string {
	t.Helper()

	for _, file := range files {
		if !isManifestList(file) {
			continue
		}
		for _, record := range decodeAvroRecords(t, file) {
			path, ok := record["manifest_path"].(string)
			if !ok {
				continue
			}
			if match := manifestPathPattern.FindStringSubmatch(path); match != nil {
				return match[1]
			}
		}
	}
	t.Fatal("no manifest list in the fixture records a manifest path")
	return ""
}

// decodeAvroRecords reads every record of an Avro container file as a map.
func decodeAvroRecords(t *testing.T, path string) []map[string]any {
	t.Helper()

	raw, err := os.ReadFile(path) //nolint:gosec // the path is a glob of this test's own temporary directory
	require.NoError(t, err)
	dec, err := ocf.NewDecoder(bytes.NewReader(raw))
	require.NoError(t, err)

	var records []map[string]any
	for dec.HasNext() {
		record := make(map[string]any)
		require.NoError(t, dec.Decode(&record))
		records = append(records, record)
	}
	require.NoError(t, dec.Error())
	return records
}

// rewriteAvroRecords replaces a path prefix everywhere it appears in an Avro container file and,
// for a manifest list, updates each entry's manifest_length. It returns the new file size.
func rewriteAvroRecords(t *testing.T, path, from, to string, lengths map[string]int64) int64 {
	t.Helper()

	raw, err := os.ReadFile(path) //nolint:gosec // the path is a glob of this test's own temporary directory
	require.NoError(t, err)
	dec, err := ocf.NewDecoder(bytes.NewReader(raw))
	require.NoError(t, err)

	meta := make(map[string][]byte)
	for key, value := range dec.Metadata() {
		// The encoder owns these two keys; handing them back would collide with the schema and
		// codec it writes itself.
		if key == "avro.schema" || key == "avro.codec" {
			continue
		}
		meta[key] = value
	}
	schema := dec.Schema()
	schemaJSON := dec.Metadata()["avro.schema"]

	var records []map[string]any
	for dec.HasNext() {
		record := make(map[string]any)
		require.NoError(t, dec.Decode(&record))
		rewritten, ok := replacePaths(record, from, to).(map[string]any)
		require.True(t, ok)
		if lengths != nil {
			manifestPath, ok := rewritten["manifest_path"].(string)
			require.True(t, ok, "a manifest list entry carries no manifest path")
			size, known := lengths[manifestPath]
			require.True(t, known, "the manifest list points at %s, which the fixture does not hold", manifestPath)
			rewritten["manifest_length"] = size
		}
		records = append(records, rewritten)
	}
	require.NoError(t, dec.Error())

	var buf bytes.Buffer
	enc, err := ocf.NewEncoderWithSchema(schema, &buf,
		ocf.WithCodec(ocf.Deflate),
		ocf.WithMetadata(meta),
		ocf.WithSchemaMarshaler(func(avro.Schema) ([]byte, error) { return schemaJSON, nil }),
	)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, enc.Encode(record))
	}
	require.NoError(t, enc.Close())

	//nolint:gosec // the path is a glob of this test's own temporary directory
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	return int64(buf.Len())
}

// replacePaths walks a decoded Avro value, replacing a prefix in every string it holds.
func replacePaths(value any, from, to string) any {
	switch typed := value.(type) {
	case string:
		return strings.Replace(typed, from, to, 1)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nested := range typed {
			out[key] = replacePaths(nested, from, to)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, nested := range typed {
			out = append(out, replacePaths(nested, from, to))
		}
		return out
	default:
		return value
	}
}

// numericBound coerces a bound to float64 whatever concrete numeric type the reader produced,
// reporting false for a bound that is not a number at all.
func numericBound(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
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
		// Iceberg records a location with its scheme; Delta and Hudi do not.
		path := strings.TrimPrefix(file.PhysicalPath, "file://")
		path = strings.TrimPrefix(path, tableDir)
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
	bounds := foldColumnBounds(t, snapshot.DataFiles)
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

// TestForeignFixtures_ReadIcebergSnapshot is what T31 unblocked: the file list of a table written
// by pyiceberg, read out of the Avro manifest list and Avro manifests the specification mandates.
// Until then this test asserted the failure instead.
func TestForeignFixtures_ReadIcebergSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadFixture(t, "pyiceberg")

	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, manifest.CurrentSnapshotID, snapshot.SourceIdentifier)
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

	// The partition tuple lives in the manifest, not in the directory name, and it is typed after
	// the partition spec rather than being a string by construction.
	partitionValues := make(map[string]int)
	for _, file := range snapshot.DataFiles {
		require.Len(t, file.PartitionValues, len(manifest.PartitionColumns))
		for _, value := range file.PartitionValues {
			require.NotNil(t, value.Range)
			partitionValues[fmt.Sprint(value.Range.MinValue)]++
		}
		assert.Positive(t, file.FileSizeBytes, "%s has no size", file.PhysicalPath)
		assert.NotEmpty(t, file.ColumnStats, "%s carries no statistics", file.PhysicalPath)
	}
	for _, expected := range manifest.PartitionValues {
		assert.Contains(t, partitionValues, expected)
	}

	// pyiceberg records bounds against the field id, and the id survives a rename. Folding the
	// per-file bounds back into table-wide ones has to reproduce the writer's own numbers.
	bounds := foldColumnBounds(t, snapshot.DataFiles)
	for name, expected := range manifest.ColumnBounds {
		actual, ok := bounds[name]
		require.True(t, ok, "no statistics were read for column %s", name)
		assert.InDelta(t, expected.Min, actual.Min, 1e-9, "minimum of %s", name)
		assert.InDelta(t, expected.Max, actual.Max, 1e-9, "maximum of %s", name)
	}
}

// foldColumnBounds reduces the per-file bounds of a snapshot to table-wide ones.
func foldColumnBounds(t *testing.T, files []*model.DataFile) map[string]fixtureBounds {
	t.Helper()

	bounds := make(map[string]fixtureBounds)
	for _, file := range files {
		for _, stat := range file.ColumnStats {
			if stat.Range == nil || stat.Range.MinValue == nil {
				continue
			}
			// A string column carries bounds too, and only the numeric ones are comparable with
			// what the writer reported.
			low, lowOK := numericBound(stat.Range.MinValue)
			high, highOK := numericBound(stat.Range.MaxValue)
			if !lowOK || !highOK {
				continue
			}
			name := stat.Field.Name
			if current, seen := bounds[name]; seen {
				low = min(low, current.Min)
				high = max(high, current.Max)
			}
			bounds[name] = fixtureBounds{Min: low, Max: high}
		}
	}
	return bounds
}

// convertExpectation records how far a target can be verified today. Everything the conversion
// really does — file list, row counts — is asserted for every target; the two fields mark the two
// places where T28 found the round trip stops short, so that a fix turns a pin into a failure.
type convertExpectation struct {
	// readBackError is the error the target's own source returns instead of a snapshot.
	readBackError string
	// schemaCheck asserts the schema the target's source recovered.
	schemaCheck func(t *testing.T, manifest *fixtureManifest, schema *model.Schema)
	// pathsDoubled marks a target that reports each data file under the table's base path twice.
	// See F4.
	pathsDoubled bool
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

			assertFileListMatchesManifest(t, target, tableDir, manifest, snapshot.DataFiles, expected)

			require.NotNil(t, expected.schemaCheck)
			expected.schemaCheck(t, manifest, snapshot.Table.ReadSchema)
		})
	}
}

// assertFileListMatchesManifest checks that a converted table still lists every data file the
// writer of the fixture reported, with the row count it reported.
func assertFileListMatchesManifest(
	t *testing.T,
	target model.TableFormat,
	tableDir string,
	manifest *fixtureManifest,
	files []*model.DataFile,
	expected convertExpectation,
) {
	t.Helper()

	require.Len(t, files, manifest.DataFileCount)
	byPath := relativeFilePaths(t, tableDir, files)

	var total int64
	for _, file := range manifest.DataFiles {
		key := file.Path
		if expected.pathsDoubled {
			key = "file:" + tableDir + "/" + file.Path
		}
		actual, ok := byPath[key]
		require.True(t, ok, "%s dropped %s", target, file.Path)
		assert.Equal(t, file.RecordCount, actual, "row count of %s", file.Path)
		total += actual
	}
	assert.Equal(t, manifest.TotalRows, total)
}

// assertSchemaWithoutFieldIDs is assertSchemaMatchesManifest for a target that identifies columns
// by name. Delta and Hudi have no field ids, so an Iceberg source's ids cannot survive into them,
// and asserting on them would pin a property the format does not have.
func assertSchemaWithoutFieldIDs(t *testing.T, manifest *fixtureManifest, schema *model.Schema) {
	t.Helper()

	byName := *manifest
	byName.Schema = make([]fixtureField, len(manifest.Schema))
	for i, field := range manifest.Schema {
		field.FieldID = nil
		byName.Schema[i] = field
	}
	assertSchemaMatchesManifest(t, &byName, schema)
}

// assertParquetSchemaFromFooter pins what the raw-Parquet source recovers from a table whose data
// files do carry the partition column: every column except the one added mid-history, which is
// present or absent depending on which file's footer the source happened to read first. That half
// of F3 is tracked as T33.
func assertParquetSchemaFromFooter(t *testing.T, manifest *fixtureManifest, schema *model.Schema) {
	t.Helper()

	require.NotNil(t, schema)
	for _, field := range manifest.Schema {
		if field.Name == manifest.SchemaEvolution.AddedColumn {
			continue
		}
		assert.NotNil(t, schema.FieldByPath(field.Name), "column %s was dropped", field.Name)
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

// TestForeignFixtures_ConvertIceberg is the other half of what T31 unblocked: a table written by
// pyiceberg is a conversion source like any other, and its file list survives into every target.
func TestForeignFixtures_ConvertIceberg(t *testing.T) {
	t.Parallel()

	expectations := map[model.TableFormat]convertExpectation{
		model.TableFormatDelta: {schemaCheck: assertSchemaWithoutFieldIDs},
		// F4: the Hudi target trims the base path off a data file path by string prefix, and the
		// Iceberg source reports the location with the scheme the manifest recorded, so the prefix
		// never matches and the Hudi source joins the whole absolute path onto the base path again.
		model.TableFormatHudi: {schemaCheck: assertSchemaWithoutFieldIDs, pathsDoubled: true},
		// F3, tracked as T33: the raw-Parquet source rebuilds the schema from one data file footer,
		// so a column added mid-history is present or absent depending on which file sorts first.
		model.TableFormatParquet: {schemaCheck: assertParquetSchemaFromFooter},
		// F4, tracked as T35: like Hudi, the Paimon target trims the base path by string prefix,
		// so the Iceberg source's scheme-qualified locations survive whole and read back doubled.
		model.TableFormatPaimon: {schemaCheck: assertSchemaMatchesManifest, pathsDoubled: true},
	}

	for _, target := range formats.SupportedTargets() {
		if target == model.TableFormatIceberg {
			continue
		}
		expected, known := expectations[target]
		require.True(t, known, "target %s is new; decide what a converted fixture must prove", target)

		t.Run(strings.ToLower(string(target)), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			tableDir, manifest := loadFixture(t, "pyiceberg")
			storage := io.NewLocalStorage()

			results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
				SourceFormat:  model.TableFormatIceberg,
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
			assertFileListMatchesManifest(t, target, tableDir, manifest, snapshot.DataFiles, expected)

			require.NotNil(t, expected.schemaCheck)
			expected.schemaCheck(t, manifest, snapshot.Table.ReadSchema)
		})
	}
}
