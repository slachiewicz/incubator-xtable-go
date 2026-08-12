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

package conversion_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/slachiewicz/xtable-go/pkg/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/formats/iceberg"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

func TestController_DeltaToIcebergE2E(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/sync_delta_to_iceberg"

	// 1. Create a Delta source table with 2 data files
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	countryField := &model.Field{Name: "country", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("users", []*model.Field{idField, countryField}, false)

	partField := &model.PartitionField{
		SourceField:   countryField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "users",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/sync_delta_to_iceberg/country=US/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 2048,
		RecordCount:   100,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("US")},
		},
		LastModified: time.Now().UnixMilli(),
	}
	dataFile2 := &model.DataFile{
		PhysicalPath:  "mem://lake/sync_delta_to_iceberg/country=DE/part-1.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 4096,
		RecordCount:   200,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("DE")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1, dataFile2},
		SourceIdentifier: "delta-v0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Run Conversion Controller to sync Delta -> Iceberg
	controller := conversion.NewController(memStorage)
	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "users",
		SyncMode:      spi.SyncModeFull,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 1)

	icebergResult := results[model.TableFormatIceberg]
	require.NotNil(t, icebergResult)
	assert.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode)
	assert.InDelta(t, table.LatestCommitTime, icebergResult.LastInstantSynced, 5000)

	// 3. Verify target Iceberg table can be read by Iceberg Source
	icebergSource := iceberg.NewSource(memStorage, basePath)
	icebergTable, err := icebergSource.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, model.TableFormatIceberg, icebergTable.TableFormat)
	require.Len(t, icebergTable.ReadSchema.Fields, 2)
	assert.Equal(t, "id", icebergTable.ReadSchema.Fields[0].Name)
	assert.Equal(t, "country", icebergTable.ReadSchema.Fields[1].Name)

	icebergSnapshot, err := icebergSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, icebergSnapshot.DataFiles, 2)

	// Total record count across files
	var totalRecords int64
	for _, df := range icebergSnapshot.DataFiles {
		totalRecords += df.RecordCount
	}
	assert.Equal(t, int64(300), totalRecords)
}

func TestController_IcebergToDeltaE2E(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/sync_iceberg_to_delta"

	// 1. Create an Iceberg source table
	idField := &model.Field{Name: "sensor_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	tempField := &model.Field{Name: "temp", Schema: model.NewPrimitiveSchema(model.TypeDouble, false)}
	schema := model.NewRecordSchema("telemetry", []*model.Field{idField, tempField}, false)

	table := &model.Table{
		Name:             "telemetry",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/sync_iceberg_to_delta/data/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 8192,
		RecordCount:   500,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1},
		SourceIdentifier: "iceberg-snap-1",
	}

	icebergTarget := iceberg.NewTarget(memStorage)
	err := icebergTarget.Init(ctx, table)
	require.NoError(t, err)
	err = icebergTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Run Conversion Controller to sync Iceberg -> Delta
	controller := conversion.NewController(memStorage)
	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: basePath,
		TableName:     "telemetry",
		SyncMode:      spi.SyncModeFull,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 1)

	deltaResult := results[model.TableFormatDelta]
	require.NotNil(t, deltaResult)
	assert.Equal(t, spi.SyncStatusSuccess, deltaResult.StatusCode)

	// 3. Verify target Delta table can be read by Delta Source
	deltaSource := delta.NewSource(memStorage, basePath)
	deltaTable, err := deltaSource.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, model.TableFormatDelta, deltaTable.TableFormat)
	require.Len(t, deltaTable.ReadSchema.Fields, 2)
	assert.Equal(t, "sensor_id", deltaTable.ReadSchema.Fields[0].Name)
	assert.Equal(t, model.TypeLong, deltaTable.ReadSchema.Fields[0].Schema.DataType)

	deltaSnapshot, err := deltaSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, deltaSnapshot.DataFiles, 1)
	assert.Equal(t, int64(500), deltaSnapshot.DataFiles[0].RecordCount)
}

func TestController_CatalogSyncRegistersTargetTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/catalog_target_test"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("products", []*model.Field{idField}, false)
	table := &model.Table{
		Name:             "products",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/catalog_target_test/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   10,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "delta-v0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	fakeClient := &fakeSyncClient{registeredTables: make(chan *model.Table, 10)}
	controller := conversion.NewController(memStorage,
		conversion.WithCatalogClientFactory(func(_ context.Context, _ *catalog.Config) (catalog.SyncClient, error) {
			return fakeClient, nil
		}))

	fakeCatalogCfg := catalog.Config{
		Type:         catalog.CatalogTypeGlue,
		DatabaseName: "test_db",
		CatalogID:    "fake-catalog-1",
	}

	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "products",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      []catalog.Config{fakeCatalogCfg},
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 1)

	icebergResult := results[model.TableFormatIceberg]
	require.NotNil(t, icebergResult)
	assert.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode)
	t.Logf("Iceberg result: Status=%v, Error=%v", icebergResult.StatusCode, icebergResult.Error)

	select {
	case registeredTable := <-fakeClient.registeredTables:
		assert.Equal(t, model.TableFormatIceberg, registeredTable.TableFormat, "target table format should be ICEBERG, not DELTA source")
		assert.Equal(t, "catalog_target_test", registeredTable.Name)
	case <-time.After(2 * time.Second):
		t.Fatal("expected RegisterCall within timeout")
	}
}

func TestController_CatalogSyncMultipleCatalogsPartialFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/catalog_fail_test"

	idField := &model.Field{Name: "sku", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("inventory", []*model.Field{idField}, false)
	table := &model.Table{
		Name:             "inventory",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/catalog_fail_test/data.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   5,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "delta-v0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	catalogCallCount := 0
	setFakeCatalogFunc := func(_ context.Context, cfg *catalog.Config) (catalog.SyncClient, error) {
		catalogCallCount++
		if cfg.CatalogID == "failing-catalog" {
			return &failingSyncClient{}, nil
		}
		return &fakeSyncClient{registeredTables: make(chan *model.Table, 10)}, nil
	}

	controller := conversion.NewController(memStorage, conversion.WithCatalogClientFactory(setFakeCatalogFunc))

	catalogs := []catalog.Config{
		{
			Type:         catalog.CatalogTypeGlue,
			DatabaseName: "test_db",
			CatalogID:    "failing-catalog",
		},
		{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: "test_db",
			CatalogID:    "success-catalog",
		},
	}

	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "inventory",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      catalogs,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)

	icebergResult := results[model.TableFormatIceberg]
	require.NotNil(t, icebergResult)
	assert.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode, "conversion success should not be changed by catalog failure")
	assert.Contains(t, icebergResult.Error, "catalog sync error", "catalog error should be reported separately")
	assert.Equal(t, 2, catalogCallCount, "both catalogs should be attempted even if first fails")
}

func TestController_CatalogFailureDoesNotOverwriteSuccessStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/catalog_status_test"

	idField := &model.Field{Name: "metric", Schema: model.NewPrimitiveSchema(model.TypeDouble, false)}
	schema := model.NewRecordSchema("metrics", []*model.Field{idField}, false)
	table := &model.Table{
		Name:             "metrics",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/catalog_status_test/part.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 512,
		RecordCount:   20,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "delta-v0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	controller := conversion.NewController(memStorage,
		conversion.WithCatalogClientFactory(func(_ context.Context, _ *catalog.Config) (catalog.SyncClient, error) {
			return &failingSyncClient{}, nil
		}))

	catalogs := []catalog.Config{
		{
			Type:         catalog.CatalogTypeGlue,
			DatabaseName: "test_db",
			CatalogID:    "failing-catalog",
		},
	}

	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
		TableBasePath: basePath,
		TableName:     "metrics",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      catalogs,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 2)

	for _, result := range results {
		if result.StatusCode == spi.SyncStatusSuccess {
			assert.Contains(t, result.Error, "catalog sync error", "catalog error should be appended to success results")
			assert.Equal(t, spi.SyncStatusSuccess, result.StatusCode, "success status should be preserved, not overwritten with error")
		}
	}
}

type fakeSyncClient struct {
	registeredTables chan *model.Table
}

func (f *fakeSyncClient) CreateOrUpdateTable(ctx context.Context, table *model.Table, snapshot *model.Snapshot) error {
	f.registeredTables <- table
	return nil
}

func (f *fakeSyncClient) DropTable(ctx context.Context, databaseName, tableName string) error {
	return nil
}

func (f *fakeSyncClient) Close() error {
	return nil
}

func (f *fakeSyncClient) CatalogType() catalog.CatalogType {
	return catalog.CatalogTypeGlue
}

type failingSyncClient struct{}

func (f *failingSyncClient) CreateOrUpdateTable(ctx context.Context, table *model.Table, snapshot *model.Snapshot) error {
	return fmt.Errorf("mock catalog failure")
}

func (f *failingSyncClient) DropTable(ctx context.Context, databaseName, tableName string) error {
	return nil
}

func (f *failingSyncClient) Close() error {
	return nil
}

func (f *failingSyncClient) CatalogType() catalog.CatalogType {
	return catalog.CatalogTypeGlue
}

// partitionAwareSyncClient is a fake catalog client that also tracks partitions, standing in for a
// Hive-style catalog such as Glue.
type partitionAwareSyncClient struct {
	mu         sync.Mutex
	existing   []catalog.Partition
	added      []catalog.Partition
	dropped    []catalog.Partition
	listCalled bool
}

func (p *partitionAwareSyncClient) CatalogType() catalog.CatalogType                { return catalog.CatalogTypeGlue }
func (p *partitionAwareSyncClient) DropTable(context.Context, string, string) error { return nil }
func (p *partitionAwareSyncClient) Close() error                                    { return nil }
func (p *partitionAwareSyncClient) CreateOrUpdateTable(context.Context, *model.Table, *model.Snapshot) error {
	return nil
}

func (p *partitionAwareSyncClient) GetAllPartitions(context.Context, catalog.TableIdentifier) ([]catalog.Partition, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listCalled = true
	return p.existing, nil
}

func (p *partitionAwareSyncClient) AddPartitions(_ context.Context, _ catalog.TableIdentifier, parts []catalog.Partition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.added = append(p.added, parts...)
	return nil
}

func (p *partitionAwareSyncClient) UpdatePartitions(context.Context, catalog.TableIdentifier, []catalog.Partition) error {
	return nil
}

func (p *partitionAwareSyncClient) DropPartitions(_ context.Context, _ catalog.TableIdentifier, parts []catalog.Partition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped = append(p.dropped, parts...)
	return nil
}

// buildPartitionedDeltaTable writes a Delta table partitioned by country with two partitions.
func buildPartitionedDeltaTable(t *testing.T, ctx context.Context, storage io.Storage, basePath string) {
	t.Helper()

	countryField := &model.Field{Name: "country", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	partField := &model.PartitionField{SourceField: countryField, TransformType: model.PartitionTransformValue}
	schema := model.NewRecordSchema("sales", []*model.Field{countryField}, false)

	table := &model.Table{
		Name:               "sales",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	file := func(country string) *model.DataFile {
		return &model.DataFile{
			PhysicalPath:    basePath + "/country=" + country + "/part-0.parquet",
			FileFormat:      model.FileFormatParquet,
			FileSizeBytes:   512,
			RecordCount:     5,
			PartitionValues: []*model.PartitionValue{{PartitionField: partField, Range: model.NewScalarRange(country)}},
			LastModified:    time.Now().UnixMilli(),
		}
	}

	target := delta.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{file("US"), file("DE")},
		SourceIdentifier: "delta-v0",
	}))
}

func TestController_SyncsPartitionsToPartitionAwareCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage := io.NewMemoryStorage()
	basePath := "mem://lake/partition_sync_test"
	buildPartitionedDeltaTable(t, ctx, storage, basePath)

	fake := &partitionAwareSyncClient{
		// The catalog already holds a partition the table no longer has.
		existing: []catalog.Partition{{Values: []string{"FR"}, StorageLocation: basePath + "/country=FR"}},
	}
	controller := conversion.NewController(storage,
		conversion.WithCatalogClientFactory(func(_ context.Context, _ *catalog.Config) (catalog.SyncClient, error) {
			return fake, nil
		}))

	results, err := controller.Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "sales",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      []catalog.Config{{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"}},
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.True(t, fake.listCalled, "partition sync must read the catalog's current partitions")

	added := make([]string, 0, len(fake.added))
	for _, p := range fake.added {
		added = append(added, p.Values[0])
	}
	sort.Strings(added)
	assert.Equal(t, []string{"DE", "US"}, added, "both table partitions must be registered")

	require.Len(t, fake.dropped, 1)
	assert.Equal(t, []string{"FR"}, fake.dropped[0].Values, "a partition no longer in the table must be dropped")
}

func TestController_UnpartitionedTableDoesNotTouchCatalogPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage := io.NewMemoryStorage()
	basePath := "mem://lake/unpartitioned_sync_test"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	table := &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       model.NewRecordSchema("events", []*model.Field{idField}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}
	target := delta.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:     table,
		DataFiles: []*model.DataFile{{PhysicalPath: basePath + "/part-0.parquet", FileFormat: model.FileFormatParquet, RecordCount: 1}},
	}))

	// The catalog holds partitions; an unpartitioned table must not be read as "drop them all".
	fake := &partitionAwareSyncClient{
		existing: []catalog.Partition{{Values: []string{"US"}, StorageLocation: basePath + "/country=US"}},
	}
	controller := conversion.NewController(storage,
		conversion.WithCatalogClientFactory(func(_ context.Context, _ *catalog.Config) (catalog.SyncClient, error) {
			return fake, nil
		}))

	_, err := controller.Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "events",
		SyncMode:      spi.SyncModeFull,
		Catalogs:      []catalog.Config{{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"}},
	})
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.False(t, fake.listCalled, "an unpartitioned table must not reconcile partitions at all")
	assert.Empty(t, fake.dropped, "the catalog's existing partitions must survive")
}
