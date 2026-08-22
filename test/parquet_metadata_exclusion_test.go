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

// T45: the Parquet source must not admit another format's metadata as a data file.
//
// The directory these tests point the Parquet source at is built the way a real polytable-synced
// directory is built: a genuine foreign-writer fixture (delta-rs-checkpoint, which already carries
// a real "_delta_log/…checkpoint.parquet" — the exact upstream #813/#814 shape) run through
// conversion.Controller.Sync into every other target format, so "_delta_log", "metadata",
// ".hoodie", "schema"/"snapshot"/"manifest" and "_polytable_metadata" all exist for the reason a
// production polytable dataset has them, not because the test asserted they should be there.
package test_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/formats/parquet"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// TestParquetSource_ExcludesForeignMetadata is T45's acceptance test: a Parquet source pointed at
// a directory that also holds every other format's metadata reports only the real data files.
func TestParquetSource_ExcludesForeignMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadFixture(t, "delta-rs-checkpoint")
	storage := io.NewLocalStorage()

	// Sync the real Delta fixture into every other target format, all sharing tableDir — exactly
	// how a polytable dataset ends up with every format's metadata side by side.
	targets := []model.TableFormat{
		model.TableFormatIceberg,
		model.TableFormatHudi,
		model.TableFormatPaimon,
		model.TableFormatParquet,
	}
	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: targets,
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	for _, target := range targets {
		require.Equal(t, spi.SyncStatusSuccess, results[target].StatusCode, "%s: %v", target, results[target].Error)
	}

	// Confirm the metadata directories this test relies on actually exist — a passing test that
	// silently exercised none of them would prove nothing.
	for _, dir := range []string{"_delta_log", "metadata", ".hoodie", "schema", "snapshot", "manifest", "_polytable_metadata"} {
		exists, err := storage.Exists(ctx, filepath.Join(tableDir, dir))
		require.NoError(t, err)
		require.True(t, exists, "expected the sync to have created %s", dir)
	}
	// _delta_log must still carry the fixture's genuine checkpoint: it is the file the pre-T45
	// code admitted as data.
	checkpoints, err := filepath.Glob(filepath.Join(tableDir, "_delta_log", "*.checkpoint.parquet"))
	require.NoError(t, err)
	require.NotEmpty(t, checkpoints, "the delta-rs-checkpoint fixture should still have its checkpoint")

	// Hadoop's "_temporary" staging directory and "_SUCCESS" marker are not produced by any writer
	// in this repository, so — unlike every directory above — there is no sync that builds them.
	// They are added by hand for that reason alone, with a stray ".parquet" inside "_temporary" to
	// prove the exclusion is the directory, not the file's own name.
	require.NoError(t, os.MkdirAll(filepath.Join(tableDir, "_temporary", "0", "task_0"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(tableDir, "_temporary", "0", "task_0", "part-00000.parquet"), []byte("not a real parquet file"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(tableDir, "_SUCCESS"), nil, 0o600))

	source, err := formats.NewSource(model.TableFormatParquet, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)

	// The real data files, and only the real data files: no checkpoint, no Iceberg manifest, no
	// Paimon schema/snapshot/manifest entry, no Parquet target manifest, no Hadoop staging file.
	assertFileListMatchesManifest(t, model.TableFormatParquet, tableDir, manifest, snapshot.DataFiles)

	// The genuine Hive partition survives being crawled alongside all of that: "region" is still
	// recovered as a partitioning field, not swallowed by the metadata exclusion.
	require.NotNil(t, snapshot.Table)
	require.Len(t, snapshot.Table.PartitioningFields, 1)
	assert.Equal(t, "region", snapshot.Table.PartitioningFields[0].SourceField.Name)
}

// TestParquetSource_MetadataExclusionIsComponentWise asserts the exclusion is checked against
// every path component, not the file's own base name or a whole-path suffix: a data-looking file
// nested inside a metadata directory must still be excluded.
func TestParquetSource_MetadataExclusionIsComponentWise(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir := t.TempDir()
	storage := io.NewLocalStorage()

	// A real data file at the root.
	writeSampleParquetFile(t, filepath.Join(tableDir, "part-00000.parquet"), []CustomerRecord{
		{ID: 1, Name: "Real Data", Country: "US", Active: true, Balance: 1.0},
	})

	// A file whose own base name is unremarkable, nested under a metadata directory that is
	// itself nested under a directory that looks like ordinary data. The old suffix-only,
	// base-name-only filter would have admitted this.
	writeSampleParquetFile(t, filepath.Join(tableDir, "data", "_delta_log", "x.parquet"), []CustomerRecord{
		{ID: 2, Name: "Should Be Excluded", Country: "US", Active: true, Balance: 2.0},
	})

	source := parquet.NewSource(storage, tableDir)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)
	assert.Equal(t, filepath.Join(tableDir, "part-00000.parquet"), snapshot.DataFiles[0].PhysicalPath)
}

// TestParquetSource_RealHivePartitionSurvives asserts a genuinely named Hive partition directory
// — one that is not any format's metadata — is still crawled as data.
func TestParquetSource_RealHivePartitionSurvives(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir := t.TempDir()
	storage := io.NewLocalStorage()

	writeSampleParquetFile(t, filepath.Join(tableDir, "region=east", "part-00000.parquet"), []CustomerRecord{
		{ID: 1, Name: "East", Country: "US", Active: true, Balance: 1.0},
	})
	writeSampleParquetFile(t, filepath.Join(tableDir, "region=west", "part-00000.parquet"), []CustomerRecord{
		{ID: 2, Name: "West", Country: "US", Active: true, Balance: 2.0},
	})

	source := parquet.NewSource(storage, tableDir)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 2)
	require.Len(t, snapshot.Table.PartitioningFields, 1)
	assert.Equal(t, "region", snapshot.Table.PartitioningFields[0].SourceField.Name)
}
