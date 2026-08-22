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

package test_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// azureLiveContainer is the container the Entra app registration backing this test is scoped to.
// It already exists on the account; this test never creates or removes it, only the blobs it
// writes under a run-unique prefix inside it.
const azureLiveContainer = "lake"

// TestAzureLive_ADLSGen2Account exercises polytable against a real Azure Data Lake Storage Gen2
// account instead of Azurite (test/dockertest_azurite_test.go) or MinIO. It is gated on
// POLYTABLE_AZURE_ACCOUNT so that make check, a plain go test ./..., and the container-based
// dockertest lane are completely unaffected: nobody without that variable set ever runs this
// test. This is a different axis from testing.Short() and deliberately does not consult it.
//
// Credentials: nothing is configured here. Storage construction below passes no Azure options, so
// pkg/io/azure.go's NewAzureStorage falls through to azidentity.NewDefaultAzureCredential, which
// in CI resolves through the Azure CLI session azure/login@v2 established and locally through
// whatever the developer already has (Azure CLI, VS Code, environment credential, ...).
func TestAzureLive_ADLSGen2Account(t *testing.T) {
	account := os.Getenv("POLYTABLE_AZURE_ACCOUNT")
	if account == "" {
		t.Skip("POLYTABLE_AZURE_ACCOUNT is not set; skipping the real-Azure live test")
	}

	// A developer's ambient AZURE_STORAGE_KEY or AZURE_STORAGE_SAS_TOKEN would let
	// pkg/io/azure.go authenticate with a shared key or SAS instead of DefaultAzureCredential,
	// which would mask a broken Entra/OIDC path behind a credential this test is not meant to
	// exercise. Blank both so only the default credential chain can possibly succeed.
	t.Setenv("AZURE_STORAGE_KEY", "")
	t.Setenv("AZURE_STORAGE_SAS_TOKEN", "")

	ctx := context.Background()
	host := account + ".dfs.core.windows.net"

	// time.Now().UnixNano() plus a uuid is nondeterministic, which is fine in a test (unlike in
	// the workflow scripts elsewhere in this repo): it only has to be unique per run, not
	// reproducible, so that two nightly runs -- or a nightly run and a manual workflow_dispatch
	// -- never collide under the same prefix.
	runID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), uuid.NewString())
	runRootPath := fmt.Sprintf("abfss://%s@%s/polytable-azure-live/%s", azureLiveContainer, host, runID)
	tableBasePath := runRootPath + "/orders"

	storage, err := io.NewStorageForPath(ctx, runRootPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = storage.Close() })

	// Delete everything this run wrote, regardless of how the test ends. A cleanup failure is
	// reported, not failed on: masking a real assertion failure behind a cleanup error would be
	// worse than leaving a stray blob prefix behind for manual removal.
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		infos, listErr := storage.List(cleanupCtx, runRootPath)
		if listErr != nil {
			t.Logf("cleanup: failed to list %s: %v", runRootPath, listErr)
			return
		}
		// Longest path first, so a deeper blob (or a data file inside a directory) is removed
		// before the ADLS Gen2 directory object that contains it.
		sort.Slice(infos, func(i, j int) bool { return len(infos[i].Path) > len(infos[j].Path) })
		for _, info := range infos {
			if delErr := storage.Delete(cleanupCtx, info.Path); delErr != nil {
				t.Logf("cleanup: failed to delete %s: %v", info.Path, delErr)
			}
		}
	})

	// Seed the delta-rs-checkpoint fixture into the run's prefix by writing each file through
	// io.Storage, mirroring how test/dockertest_azurite_test.go builds its Azure-backed tables --
	// except here every file comes from a real fixture on disk rather than a single hand-built
	// mock Parquet payload.
	fixtureDir := filepath.Join(fixtureRoot, "delta-rs-checkpoint", "orders")
	walkErr := filepath.WalkDir(fixtureDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // G122: path comes from walking this repo's own checked-in fixture directory
		if readErr != nil {
			return readErr
		}
		rel, relErr := filepath.Rel(fixtureDir, path)
		if relErr != nil {
			return relErr
		}
		target := tableBasePath + "/" + filepath.ToSlash(rel)
		return storage.Write(ctx, target, data)
	})
	require.NoError(t, walkErr, "seeding the delta-rs-checkpoint fixture into Azure failed")

	controller := conversion.NewController(storage)
	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
		TableBasePath: tableBasePath,
		TableName:     "orders",
		// Incremental (rather than Full) so the second, identical Sync in the ReSyncIsNoOp
		// subtest below has a NO_OP verdict to report. A dataset with no target metadata yet
		// still gets a full snapshot sync on the first call regardless of SyncMode, matching
		// pkg/conversion/t22_test.go.
		SyncMode: spi.SyncModeIncremental,
	}

	// 1. Sync over abfss://: convert the seeded Delta table to both Iceberg and Hudi against the
	// real account.
	t.Run("SyncOverAbfss", func(t *testing.T) {
		results, syncErr := controller.Sync(ctx, cfg)
		require.NoError(t, syncErr)
		require.Len(t, results, 2)

		icebergResult := results[model.TableFormatIceberg]
		require.NotNil(t, icebergResult)
		assert.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode, icebergResult.Error)
		assert.False(t, icebergResult.NoOp, "the first sync writes real data and must not be NO_OP")

		hudiResult := results[model.TableFormatHudi]
		require.NotNil(t, hudiResult)
		assert.Equal(t, spi.SyncStatusSuccess, hudiResult.StatusCode, hudiResult.Error)
		assert.False(t, hudiResult.NoOp, "the first sync writes real data and must not be NO_OP")

		icebergSource, srcErr := formats.NewSource(model.TableFormatIceberg, storage, tableBasePath)
		require.NoError(t, srcErr)
		defer func() { _ = icebergSource.Close() }()
		icebergSnapshot, snapErr := icebergSource.GetCurrentSnapshot(ctx)
		require.NoError(t, snapErr)
		assert.Len(t, icebergSnapshot.DataFiles, 6, "delta-rs-checkpoint/orders carries 6 data files")

		hudiSource, srcErr := formats.NewSource(model.TableFormatHudi, storage, tableBasePath)
		require.NoError(t, srcErr)
		defer func() { _ = hudiSource.Close() }()
		hudiSnapshot, snapErr := hudiSource.GetCurrentSnapshot(ctx)
		require.NoError(t, snapErr)
		assert.Len(t, hudiSnapshot.DataFiles, 6, "delta-rs-checkpoint/orders carries 6 data files")
	})

	// 2. Re-sync is a no-op: running the identical conversion again, with no new Delta commits,
	// must report NO_OP for both targets, proving TableSyncMetadata round-tripped through Azure
	// rather than being silently dropped or misread.
	t.Run("ReSyncIsNoOp", func(t *testing.T) {
		results, syncErr := controller.Sync(ctx, cfg)
		require.NoError(t, syncErr)
		require.Len(t, results, 2)

		icebergResult := results[model.TableFormatIceberg]
		require.NotNil(t, icebergResult)
		require.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode, "a no-op sync is still a successful sync")
		assert.True(t, icebergResult.NoOp, "no new commits since the last synced instant must be NO_OP")
		assert.Equal(t, spi.SyncVerdictNoOp, icebergResult.Verdict())

		hudiResult := results[model.TableFormatHudi]
		require.NotNil(t, hudiResult)
		require.Equal(t, spi.SyncStatusSuccess, hudiResult.StatusCode, "a no-op sync is still a successful sync")
		assert.True(t, hudiResult.NoOp, "no new commits since the last synced instant must be NO_OP")
		assert.Equal(t, spi.SyncVerdictNoOp, hudiResult.Verdict())
	})

	// 3. Hierarchical-namespace directories: no emulator (including Azurite, which is what
	// test/dockertest_azurite_test.go is limited to) reproduces this. On a real ADLS Gen2
	// account, writing a blob under a path also materializes real directory objects for every
	// path segment above it, distinguishable by IsDir plus a zero size -- unlike a flat Blob
	// Storage account, where isDirectory (pkg/io/azure.go) can only infer a trailing-slash
	// convention that a plain upload never produces.
	t.Run("HierarchicalNamespaceDirectories", func(t *testing.T) {
		infos, listErr := storage.List(ctx, tableBasePath)
		require.NoError(t, listErr)
		require.NotEmpty(t, infos)

		var sawDir bool
		for _, info := range infos {
			if !info.IsDir {
				continue
			}
			sawDir = true
			assert.Zero(t, info.Size, "directory object %s must report zero size, got %d", info.Path, info.Size)
		}
		assert.True(t, sawDir, "expected at least one real ADLS Gen2 directory object under %s", tableBasePath)
	})

	// 4. Exists semantics: a blob this test actually wrote must report (true, nil), and a blob
	// that was never written must report (false, nil) with no error -- pinning that a genuinely
	// missing object stays distinguishable from a request that failed outright.
	t.Run("ExistsSemantics", func(t *testing.T) {
		writtenPath := tableBasePath + "/_delta_log/_last_checkpoint"
		exists, existsErr := storage.Exists(ctx, writtenPath)
		require.NoError(t, existsErr)
		assert.True(t, exists, "expected %s, seeded from the fixture, to exist", writtenPath)

		missingPath := tableBasePath + "/_delta_log/this-blob-was-never-written-" + uuid.NewString() + ".json"
		exists, existsErr = storage.Exists(ctx, missingPath)
		require.NoError(t, existsErr, "a genuinely missing blob must not surface as an error")
		assert.False(t, exists, "expected %s to be reported missing", missingPath)
	})
}
