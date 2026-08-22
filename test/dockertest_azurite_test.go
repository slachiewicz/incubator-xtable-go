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
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Azurite's well-known development storage account. These are fixed, publicly documented test
// credentials shipped with the emulator, not secrets.
const (
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteContainer   = "lakehouse-e2e"
	azuriteHost        = "devstoreaccount1.dfs.core.windows.net"
)

func TestDockertest_Azurite_FullLakehouseMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "failed to connect to Docker daemon")

	err = pool.Client.Ping()
	require.NoError(t, err, "failed to ping Docker daemon")

	// 1. Run Azurite container with 120s auto-expiry to prevent orphan containers
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "latest",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"10000/tcp": {{HostIP: "127.0.0.1", HostPort: ""}},
		},
	})
	require.NoError(t, err, "failed to start Azurite container")
	_ = resource.Expire(120)
	defer func() {
		_ = pool.Purge(resource)
	}()

	azuritePort := resource.GetPort("10000/tcp")
	blobServiceURL := fmt.Sprintf("http://127.0.0.1:%s/%s", azuritePort, azuriteAccountName)

	// 2. Wait for Azurite readiness. Azurite has no health endpoint, so any HTTP response —
	// including the 400 Azurite returns for an unauthenticated GET on the service root — proves
	// the listener is up.
	err = pool.Retry(func() error {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, blobServiceURL, nil)
		if reqErr != nil {
			return reqErr
		}
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return getErr
		}
		defer func() { _ = resp.Body.Close() }()
		return nil
	})
	require.NoError(t, err, "azurite failed to become ready in time")

	ctx := context.Background()

	// 3. Configure the Azure-backed storage through conversion.StorageConfig, the same path the
	// CLI, daemon and REST server use. AccountKey has no StorageConfig field — credentials are
	// deliberately excluded from that type — so it is appended as an extra option func alongside
	// the ones ToOptionFuncs produces, rather than reimplementing the closures it already builds.
	storageConfig := conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			Endpoint:    blobServiceURL,
			AccountName: azuriteAccountName,
		},
	}
	optFns := storageConfig.ToOptionFuncs()
	optFns = append(optFns, func(opts *io.Options) { opts.Azure.AccountKey = azuriteAccountKey })

	tableBasePath := fmt.Sprintf("abfss://%s@%s/tables/financial_events", azuriteContainer, azuriteHost)

	testStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFns...)
	require.NoError(t, err)

	// 4. Create the Azure test container using a raw azblob client, mirroring how the MinIO suite
	// makes its bucket with a raw s3.Client.
	cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
	require.NoError(t, err)
	azClient, err := azblob.NewClientWithSharedKeyCredential(blobServiceURL, cred, nil)
	require.NoError(t, err)

	_, err = azClient.CreateContainer(ctx, azuriteContainer, nil)
	require.NoError(t, err, "failed to create test container in Azurite")

	// Write mock physical Parquet data file into Azurite
	mockParquetBytes := []byte("PAR1-MOCK-PARQUET-BINARY-PAYLOAD-FOR-TEST-ROW-COUNT-500")
	parquetFilePath := fmt.Sprintf("%s/region=EU/data-001.parquet", tableBasePath)
	err = testStorage.Write(ctx, parquetFilePath, mockParquetBytes)
	require.NoError(t, err)

	// 5. Build initial Delta Table Seed on Azurite
	idField := &model.Field{Name: "transaction_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	userField := &model.Field{Name: "user_uuid", Schema: model.NewPrimitiveSchema(model.TypeUUID, false)}
	amountField := &model.Field{Name: "amount", Schema: model.NewDecimalSchema(18, 2, false)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("financial_events", []*model.Field{idField, userField, amountField, regionField}, false)

	partField := &model.PartitionField{
		SourceField:   regionField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "financial_events",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           tableBasePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  parquetFilePath,
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: int64(len(mockParquetBytes)),
		RecordCount:   500,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("region=EU")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	// Commit initial Delta snapshot on Azurite
	deltaTarget := delta.NewTarget(testStorage)
	err = deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 6. RUN FULL MATRIX CONVERSIONS ON AZURITE
	controller := conversion.NewController(testStorage)

	t.Run("DeltaToIcebergAndHudi_OnAzurite", func(t *testing.T) {
		datasetConfig := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
			TableName:     "financial_events",
			TableBasePath: tableBasePath,
			SyncMode:      spi.SyncModeFull,
			Storage:       &storageConfig,
		}

		results, syncErr := controller.Sync(ctx, datasetConfig)
		require.NoError(t, syncErr)
		require.Len(t, results, 2)

		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatHudi].StatusCode)

		// 7. Verify Iceberg Metadata on Azurite
		icebergSource := iceberg.NewSource(testStorage, tableBasePath)
		icebergTable, err := icebergSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatIceberg, icebergTable.TableFormat)
		assert.Len(t, icebergTable.ReadSchema.Fields, 4)

		icebergSnap, err := icebergSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, icebergSnap.DataFiles, 1)
		assert.Equal(t, int64(500), icebergSnap.DataFiles[0].RecordCount)

		// 8. Verify Hudi Metadata on Azurite
		hudiSource := hudi.NewSource(testStorage, tableBasePath)
		hudiTable, err := hudiSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatHudi, hudiTable.TableFormat)

		hudiSnap, err := hudiSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, hudiSnap.DataFiles, 1)
		assert.Equal(t, int64(500), hudiSnap.DataFiles[0].RecordCount)
	})

	t.Run("HudiToDeltaAndIceberg_OnAzurite", func(t *testing.T) {
		datasetConfig := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatHudi,
			TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg},
			TableName:     "financial_events",
			TableBasePath: tableBasePath,
			SyncMode:      spi.SyncModeFull,
			Storage:       &storageConfig,
		}

		results, syncErr := controller.Sync(ctx, datasetConfig)
		require.NoError(t, syncErr)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
	})

	t.Run("RoundTripsAbfssPaths", func(t *testing.T) {
		roundTripPath := fmt.Sprintf("%s/roundtrip/probe.txt", tableBasePath)
		err := testStorage.Write(ctx, roundTripPath, []byte("abfss round trip probe"))
		require.NoError(t, err)

		listPrefix := fmt.Sprintf("%s/roundtrip", tableBasePath)
		infos, err := testStorage.List(ctx, listPrefix)
		require.NoError(t, err)
		require.NotEmpty(t, infos)

		for _, info := range infos {
			assert.True(t, len(info.Path) > len("abfss://") && info.Path[:len("abfss://")] == "abfss://",
				"expected FileInfo.Path %q to start with abfss://", info.Path)

			container, blobPath, host, scheme, parseErr := io.ParseAzureURI(info.Path)
			require.NoError(t, parseErr, "expected %q to parse back through ParseAzureURI", info.Path)
			assert.Equal(t, "abfss", scheme)
			assert.Equal(t, azuriteContainer, container)
			assert.Equal(t, azuriteHost, host)
			assert.Equal(t, "tables/financial_events/roundtrip/probe.txt", blobPath)
		}
	})
}
