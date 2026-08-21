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

// Tests for T22 (docs/improvement-plan.md): dry run, --mode override semantics, and the NO_OP
// verdict for an incremental sync that finds no new commits.

package conversion_test

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/catalog"
	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// snapshotFiles reads every object under prefix and hashes its bytes, so two calls can be compared
// for byte-for-byte equality without depending on modification times.
func snapshotFiles(t *testing.T, ctx context.Context, storage io.Storage, prefix string) map[string][32]byte {
	t.Helper()

	infos, err := storage.List(ctx, prefix)
	require.NoError(t, err)

	out := make(map[string][32]byte, len(infos))
	for _, info := range infos {
		data, err := storage.Read(ctx, info.Path)
		require.NoError(t, err)
		out[info.Path] = sha256.Sum256(data)
	}
	return out
}

// buildSingleCommitDeltaSource writes a one-commit Delta table (via delta.Target, which is also how
// the delta.Source it stands in for reads its commit log) so IsIncrementalSyncSafeFrom and
// GetChangesSince have real commit metadata to reason about instead of a hand-built fixture.
func buildSingleCommitDeltaSource(t *testing.T, ctx context.Context, storage io.Storage, basePath, tableName string) {
	t.Helper()

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
		PhysicalPath:  basePath + "/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 256,
		RecordCount:   3,
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

func TestController_IncrementalSyncWithNoNewCommitsReportsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t22_noop"
	buildSingleCommitDeltaSource(t, ctx, storage, basePath, "orders")

	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "orders",
		SyncMode:      spi.SyncModeIncremental,
	}
	controller := conversion.NewController(storage)

	// First sync: the target has no prior metadata, so this is a full snapshot sync regardless of
	// SyncMode, and must succeed as ordinary work (not NO_OP).
	first, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, first[model.TableFormatIceberg].StatusCode)
	assert.False(t, first[model.TableFormatIceberg].NoOp, "the first sync writes real data and must not be NO_OP")
	assert.Equal(t, spi.SyncVerdictSuccess, first[model.TableFormatIceberg].Verdict())

	// Second sync: same single Delta commit, no new commits since the first sync. The incremental
	// path must report NO_OP rather than an indistinguishable SUCCESS.
	second, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	secondResult := second[model.TableFormatIceberg]
	require.Equal(t, spi.SyncStatusSuccess, secondResult.StatusCode, "a no-op sync is still a successful sync")
	assert.True(t, secondResult.NoOp, "no new commits since the last synced instant must be reported as NO_OP")
	assert.Equal(t, spi.SyncVerdictNoOp, secondResult.Verdict())
}

func TestController_ModeFullForcesSnapshotSyncEvenWithNoNewCommits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t22_mode_full"
	buildSingleCommitDeltaSource(t, ctx, storage, basePath, "shipments")

	incrementalCfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "shipments",
		SyncMode:      spi.SyncModeIncremental,
	}
	controller := conversion.NewController(storage)

	// Seed the target with a first sync, then confirm the table would otherwise go NO_OP on
	// incremental mode — this is the baseline "otherwise incremental" the acceptance criterion
	// requires --mode full to override.
	_, err := controller.Sync(ctx, incrementalCfg)
	require.NoError(t, err)
	baseline, err := controller.Sync(ctx, incrementalCfg)
	require.NoError(t, err)
	require.Equal(t, spi.SyncVerdictNoOp, baseline[model.TableFormatIceberg].Verdict(),
		"precondition: incremental mode must be a no-op here for the forced-full comparison to mean anything")

	// The CLI's --mode full sets DatasetConfig.SyncMode = spi.SyncModeFull before calling Sync;
	// reproduce that here directly against the controller.
	fullCfg := *incrementalCfg
	fullCfg.SyncMode = spi.SyncModeFull

	forced, err := controller.Sync(ctx, &fullCfg)
	require.NoError(t, err)
	forcedResult := forced[model.TableFormatIceberg]
	require.Equal(t, spi.SyncStatusSuccess, forcedResult.StatusCode)
	assert.False(t, forcedResult.NoOp, "--mode full must force a snapshot sync, never a NO_OP incremental result")
	assert.Equal(t, spi.SyncVerdictSuccess, forcedResult.Verdict())
}

func TestController_DryRunLeavesTargetByteIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t22_dry_run"
	buildSingleCommitDeltaSource(t, ctx, storage, basePath, "events")

	// SyncMode full on every call, so both the real and the dry-run sync take the snapshot path
	// (which always writes new manifest/version files if committed) rather than risking the
	// incremental path masking the write with its own NO_OP.
	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "events",
		SyncMode:      spi.SyncModeFull,
	}
	controller := conversion.NewController(storage)

	// Real sync: writes the target's Iceberg metadata.
	real, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, real[model.TableFormatIceberg].StatusCode)

	metadataPrefix := basePath + "/metadata"
	before := snapshotFiles(t, ctx, storage, metadataPrefix)
	require.NotEmpty(t, before, "the real sync must have written Iceberg metadata files")

	// Dry run: SyncMode full would otherwise write a brand-new manifest, manifest list, and
	// version-hint file (new snapshot ID, new UUID) on every call. WithDryRun must skip every one
	// of those writes.
	dryRun, err := controller.Sync(ctx, cfg, conversion.WithDryRun())
	require.NoError(t, err)
	dryRunResult := dryRun[model.TableFormatIceberg]
	require.Equal(t, spi.SyncStatusSuccess, dryRunResult.StatusCode, "a dry run still reports what it would have done")
	assert.Equal(t, spi.SyncVerdictSuccess, dryRunResult.Verdict())

	after := snapshotFiles(t, ctx, storage, metadataPrefix)
	assert.Equal(t, before, after, "a dry run must leave the target's metadata directory byte-identical")
}

func TestController_DryRunDoesNotRegisterWithCatalogs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t22_dry_run_catalog"
	buildSingleCommitDeltaSource(t, ctx, storage, basePath, "customers")

	called := false
	controller := conversion.NewController(storage,
		conversion.WithCatalogClientFactory(func(_ context.Context, _ *catalog.Config) (catalog.SyncClient, error) {
			called = true
			return &noopSyncClient{}, nil
		}))

	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "customers",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      []catalog.Config{{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"}},
	}

	results, err := controller.Sync(ctx, cfg, conversion.WithDryRun())
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
	assert.False(t, called, "a dry run must not register the (unwritten) target with any catalog")
}

// noopSyncClient is a minimal catalog.SyncClient used only to prove it is never invoked.
type noopSyncClient struct{}

func (n *noopSyncClient) CreateOrUpdateTable(context.Context, *model.Table, *model.Snapshot) error {
	return nil
}
func (n *noopSyncClient) DropTable(context.Context, string, string) error { return nil }
func (n *noopSyncClient) Close() error                                    { return nil }
func (n *noopSyncClient) CatalogType() catalog.CatalogType                { return catalog.CatalogTypeGlue }
