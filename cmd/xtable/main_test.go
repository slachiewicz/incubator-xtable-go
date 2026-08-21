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

// Tests for T22 (docs/improvement-plan.md): the JSON output shape, --mode/--dry-run/--timeout
// wiring, and the CLI-level dry-run/NO_OP acceptance criteria.
//
// This is package main rather than an external <pkg>_test package: main packages cannot be
// imported, so the black-box convention CLAUDE.md otherwise requires is not available here. See
// docs/improvement-plan.md T22's Outcome note.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	xtio "github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

func TestParseOutputFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantOut bool
	}{
		{name: "empty defaults to text", input: "", wantOut: false},
		{name: "text lowercase", input: "text", wantOut: false},
		{name: "text uppercase", input: "TEXT", wantOut: false},
		{name: "json", input: "json", wantOut: true},
		{name: "json uppercase", input: "JSON", wantOut: true},
		{name: "padded", input: "  json  ", wantOut: true},
		{name: "invalid", input: "yaml", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseOutputFormat(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOut, got)
		})
	}
}

func TestParseSyncModeFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    spi.SyncMode
		wantErr bool
	}{
		{name: "empty means no override", input: "", want: ""},
		{name: "full lowercase", input: "full", want: spi.SyncModeFull},
		{name: "full uppercase", input: "FULL", want: spi.SyncModeFull},
		{name: "incremental", input: "incremental", want: spi.SyncModeIncremental},
		{name: "invalid", input: "partial", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSyncModeFlag(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWithDatasetTimeout(t *testing.T) {
	t.Parallel()

	t.Run("zero timeout does not add a deadline", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := withDatasetTimeout(context.Background(), 0)
		defer cancel()

		_, ok := ctx.Deadline()
		assert.False(t, ok, "a zero timeout must not wrap the context in a deadline")
	})

	t.Run("positive timeout adds a deadline", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := withDatasetTimeout(context.Background(), time.Minute)
		defer cancel()

		_, ok := ctx.Deadline()
		assert.True(t, ok, "a positive timeout must add a context deadline")
	})
}

func TestBuildTableSyncOutput_SortsTargetsDeterministically(t *testing.T) {
	t.Parallel()

	ds := &conversion.DatasetConfig{TableName: "orders", SourceFormat: model.TableFormatDelta}
	results := map[model.TableFormat]*spi.SyncResult{
		model.TableFormatParquet: spi.NewSuccessSyncResult(model.TableFormatParquet, 300, time.Millisecond),
		model.TableFormatIceberg: spi.NewSuccessSyncResult(model.TableFormatIceberg, 100, time.Millisecond),
		model.TableFormatHudi:    spi.NewErrorSyncResult(model.TableFormatHudi, assert.AnError, time.Millisecond),
	}

	// Run several times: map iteration order is randomized per run, so this is the regression test
	// for "sorted by format name" rather than "happened to come out sorted once".
	for i := 0; i < 5; i++ {
		out := buildTableSyncOutput(ds, results, nil)
		require.Len(t, out.Targets, 3)
		assert.Equal(t, []string{"HUDI", "ICEBERG", "PARQUET"}, []string{
			out.Targets[0].TargetFormat, out.Targets[1].TargetFormat, out.Targets[2].TargetFormat,
		})
	}
}

func TestBuildTableSyncOutput_DatasetLevelError(t *testing.T) {
	t.Parallel()

	ds := &conversion.DatasetConfig{TableName: "orders", SourceFormat: model.TableFormatDelta}
	out := buildTableSyncOutput(ds, nil, assert.AnError)

	assert.Equal(t, assert.AnError.Error(), out.Error)
	assert.Empty(t, out.Targets)
	assert.True(t, out.hasFailure())
}

func TestSyncOutput_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := fixedSyncOutput()

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var roundTripped SyncOutput
	require.NoError(t, json.Unmarshal(data, &roundTripped))

	assert.Equal(t, original, roundTripped)
}

func TestSyncOutput_GoldenFile(t *testing.T) {
	t.Parallel()

	data, err := json.MarshalIndent(fixedSyncOutput(), "", "  ")
	require.NoError(t, err)
	assertGolden(t, filepath.Join("testdata", "sync_output.golden.json"), append(data, '\n'))
}

func TestInspectOutput_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	original := fixedInspectOutput()

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var roundTripped InspectOutput
	require.NoError(t, json.Unmarshal(data, &roundTripped))

	assert.Equal(t, original, roundTripped)
}

func TestInspectOutput_GoldenFile(t *testing.T) {
	t.Parallel()

	data, err := json.MarshalIndent(fixedInspectOutput(), "", "  ")
	require.NoError(t, err)
	assertGolden(t, filepath.Join("testdata", "inspect_output.golden.json"), append(data, '\n'))
}

// fixedSyncOutput is a deterministic SyncOutput fixture, independent of wall-clock time, used by
// both the round-trip and golden-file tests.
func fixedSyncOutput() SyncOutput {
	started := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	return SyncOutput{
		StartedAt: started,
		Duration:  1500 * time.Millisecond,
		DryRun:    true,
		HasErrors: true,
		Tables: []TableSyncOutput{
			{
				TableName:    "orders",
				SourceFormat: "DELTA",
				Targets: []TargetSyncOutput{
					{TargetFormat: "ICEBERG", Verdict: "SUCCESS", LastInstantSynced: 1755777600000, Duration: 200 * time.Millisecond},
					{TargetFormat: "PARQUET", Verdict: "NO_OP", LastInstantSynced: 1755777600000, Duration: 50 * time.Millisecond},
				},
			},
			{
				TableName:    "shipments",
				SourceFormat: "ICEBERG",
				Error:        "failed to initialize storage for s3://bad-bucket: access denied",
			},
			{
				TableName:    "customers",
				SourceFormat: "HUDI",
				Targets: []TargetSyncOutput{
					{TargetFormat: "DELTA", Verdict: "FAILED", Duration: 10 * time.Millisecond, Error: "failed to extract current snapshot: commit log unreadable"},
				},
			},
		},
	}
}

// fixedInspectOutput is a deterministic InspectOutput fixture used by the round-trip and
// golden-file tests.
func fixedInspectOutput() InspectOutput {
	return InspectOutput{
		TableName:              "orders",
		Format:                 "DELTA",
		BasePath:               "s3://lake/orders",
		LatestCommitTimeMillis: 1755777600000,
		ActiveDataFiles:        3,
		Fields: []InspectField{
			{Name: "id", DataType: "int", Nullable: false},
			{Name: "country", DataType: "string", Nullable: true},
		},
		PartitionFields: []InspectPartitionField{
			{Name: "country", Transform: "VALUE"},
		},
	}
}

// assertGolden compares got against the contents of path, byte for byte. Set UPDATE_GOLDEN=1 to
// (re)write the golden file from got.
func assertGolden(t *testing.T, path string, got []byte) {
	t.Helper()

	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, got, 0o600))
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s: run with UPDATE_GOLDEN=1 to create it", path)
	assert.Equal(t, string(want), string(got))
}

// buildLocalDeltaSource writes a one-commit Delta table straight to the local filesystem, standing
// in for a real dataset without needing S3 or the in-memory storage (which xtio.NewStorageForPathWithOptions
// constructs fresh per call, so it cannot be pre-seeded across two calls the way the CLI's own
// storage construction works).
func buildLocalDeltaSource(t *testing.T, ctx context.Context, basePath, tableName string) {
	t.Helper()

	storage := xtio.NewLocalStorage()
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema(tableName, []*model.Field{idField}, false)
	table := &model.Table{
		Name:             tableName,
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}
	dataFile := &model.DataFile{
		PhysicalPath:  filepath.Join(basePath, "part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 128,
		RecordCount:   2,
		LastModified:  time.Now().UnixMilli(),
	}

	target := delta.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "delta-v0",
	}))
}

// hashLocalTree hashes every file under dir, keyed by relative path, for byte-identical comparison.
func hashLocalTree(t *testing.T, dir string) map[string][32]byte {
	t.Helper()

	out := make(map[string][32]byte)
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// p is walked from dir, which this test wrote itself via t.TempDir(); nothing else can
		// race a symlink into it, so the TOCTOU gosec is a false positive here.
		data, rErr := os.ReadFile(p) //nolint:gosec // G122: test-local path under t.TempDir()
		if rErr != nil {
			return rErr
		}
		rel, rErr := filepath.Rel(dir, p)
		if rErr != nil {
			return rErr
		}
		out[rel] = sha256.Sum256(data)
		return nil
	})
	require.NoError(t, err)
	return out
}

func TestSyncOneDataset_DryRunLeavesTargetByteIdentical(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	basePath := t.TempDir()
	buildLocalDeltaSource(t, ctx, basePath, "events")

	ds := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "events",
		SyncMode:      spi.SyncModeFull,
	}

	var progress bytes.Buffer
	real := syncOneDataset(ctx, ds, false, 0, &progress)
	require.False(t, real.hasFailure(), "real sync must succeed: %+v", real)

	metadataDir := filepath.Join(basePath, "metadata")
	before := hashLocalTree(t, metadataDir)
	require.NotEmpty(t, before, "the real sync must have written Iceberg metadata files")

	dryRun := syncOneDataset(ctx, ds, true, 0, &progress)
	require.False(t, dryRun.hasFailure(), "dry run must still report success: %+v", dryRun)
	require.Len(t, dryRun.Targets, 1)
	assert.Equal(t, "SUCCESS", dryRun.Targets[0].Verdict)

	after := hashLocalTree(t, metadataDir)
	assert.Equal(t, before, after, "--dry-run must leave the target's metadata directory byte-identical")
}

func TestSyncOneDataset_NoNewCommitsReportsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	basePath := t.TempDir()
	buildLocalDeltaSource(t, ctx, basePath, "orders")

	ds := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "orders",
		SyncMode:      spi.SyncModeIncremental,
	}

	var progress bytes.Buffer
	first := syncOneDataset(ctx, ds, false, 0, &progress)
	require.False(t, first.hasFailure())
	require.Len(t, first.Targets, 1)
	assert.Equal(t, "SUCCESS", first.Targets[0].Verdict)

	second := syncOneDataset(ctx, ds, false, 0, &progress)
	require.False(t, second.hasFailure(), "a NO_OP sync is not a failure")
	require.Len(t, second.Targets, 1)
	assert.Equal(t, "NO_OP", second.Targets[0].Verdict, "a table with no new commits since the last sync must report NO_OP")
}

func TestSyncOneDataset_ModeFullForcesSnapshotSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	basePath := t.TempDir()
	buildLocalDeltaSource(t, ctx, basePath, "shipments")

	incremental := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "shipments",
		SyncMode:      spi.SyncModeIncremental,
	}

	var progress bytes.Buffer
	_ = syncOneDataset(ctx, incremental, false, 0, &progress)
	baseline := syncOneDataset(ctx, incremental, false, 0, &progress)
	require.Len(t, baseline.Targets, 1)
	require.Equal(t, "NO_OP", baseline.Targets[0].Verdict, "precondition: incremental mode must be a no-op here")

	full := *incremental
	full.SyncMode = spi.SyncModeFull
	forced := syncOneDataset(ctx, &full, false, 0, &progress)
	require.Len(t, forced.Targets, 1)
	assert.Equal(t, "SUCCESS", forced.Targets[0].Verdict, "--mode full must force a snapshot sync, never NO_OP")
}
