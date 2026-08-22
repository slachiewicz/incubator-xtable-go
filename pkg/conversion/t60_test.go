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

// Test for T60 (docs/improvement-plan.md): a table carrying only Java XTable's XTABLE_METADATA
// property -- none of polytable's own flat keys -- must be recognized as already synced.

package conversion_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// rewriteIcebergPropertiesToJavaOnlyShape drops polytable's own flat sync-metadata keys from the
// latest Iceberg metadata.json, leaving only XTABLE_METADATA -- the shape a real Java XTable sync
// leaves on disk (confirmed in T60 against a live Azure table). This simulates a Java-synced table
// without depending on the live fixture.
func rewriteIcebergPropertiesToJavaOnlyShape(t *testing.T, ctx context.Context, storage io.Storage, basePath string) {
	t.Helper()

	metadataDir := io.JoinPath(basePath, "metadata")
	infos, err := storage.List(ctx, metadataDir)
	require.NoError(t, err)

	latestVersion, latestPath := -1, ""
	for _, info := range infos {
		if v, ok := iceberg.MetadataFileVersion(filepath.Base(info.Path)); ok && v > latestVersion {
			latestVersion, latestPath = v, info.Path
		}
	}
	require.NotEqual(t, -1, latestVersion, "no Iceberg metadata.json found under %s", metadataDir)

	data, err := storage.Read(ctx, latestPath)
	require.NoError(t, err)

	var meta iceberg.TableMetadata
	require.NoError(t, json.Unmarshal(data, &meta))
	require.NotEmpty(t, meta.Properties[model.KeyXTableMetadata], "CommitSnapshot must have written XTABLE_METADATA")
	meta.Properties = map[string]string{model.KeyXTableMetadata: meta.Properties[model.KeyXTableMetadata]}

	rewritten, err := json.MarshalIndent(&meta, "", "  ")
	require.NoError(t, err)
	require.NoError(t, storage.Write(ctx, latestPath, rewritten))
}

// TestController_RecognizesJavaOnlyShapedSyncState is T60's acceptance criterion: polytable syncing
// a Java-shaped table reports NO_OP rather than a full snapshot. Before this change, GetTableMetadata
// only understood polytable's own flat keys, so a table carrying only XTABLE_METADATA looked
// unsynced and every sync repeated the full snapshot with a fresh instant.
func TestController_RecognizesJavaOnlyShapedSyncState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t60_java_only"
	buildSingleCommitDeltaSource(t, ctx, storage, basePath, "orders")

	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "orders",
		SyncMode:      spi.SyncModeIncremental,
	}
	controller := conversion.NewController(storage)

	first, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, first[model.TableFormatIceberg].StatusCode)
	require.False(t, first[model.TableFormatIceberg].NoOp)

	// Reduce the target's properties to exactly what a Java XTable sync would have left, dropping
	// polytable's own flat keys entirely.
	rewriteIcebergPropertiesToJavaOnlyShape(t, ctx, storage, basePath)

	// Same single Delta commit, no new commits since the (Java-shaped) last sync: this must be
	// recognized and reported as NO_OP, not repeated as a fresh full sync.
	second, err := controller.Sync(ctx, cfg)
	require.NoError(t, err)
	secondResult := second[model.TableFormatIceberg]
	require.Equal(t, spi.SyncStatusSuccess, secondResult.StatusCode)
	assert.True(t, secondResult.NoOp, "a table carrying only XTABLE_METADATA must be recognized as already synced")
	assert.Equal(t, spi.SyncVerdictNoOp, secondResult.Verdict())
}
