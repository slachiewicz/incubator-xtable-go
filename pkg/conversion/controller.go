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

package conversion

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/incubator-xtable-go/pkg/catalog"
	"github.com/apache/incubator-xtable-go/pkg/formats"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

// Controller orchestrates the end-to-end table format metadata synchronization process.
type Controller struct {
	storage io.Storage
}

// NewController creates a new ConversionController instance.
func NewController(storage io.Storage) *Controller {
	return &Controller{
		storage: storage,
	}
}

// Sync synchronizes a source table to all configured target formats.
func (c *Controller) Sync(ctx context.Context, cfg *DatasetConfig) (map[model.TableFormat]*spi.SyncResult, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid dataset configuration: %w", err)
	}

	source, err := c.createSource(cfg.SourceFormat, cfg.TableBasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create source converter: %w", err)
	}
	defer func() { _ = source.Close() }()

	results := make(map[model.TableFormat]*spi.SyncResult)

	for _, targetFormat := range cfg.TargetFormats {
		if targetFormat == cfg.SourceFormat {
			continue // Skip syncing to same format
		}

		startTime := time.Now()
		target, err := c.createTarget(ctx, targetFormat, cfg.TableBasePath, cfg.TableName)
		if err != nil {
			results[targetFormat] = spi.NewErrorSyncResult(targetFormat, err, time.Since(startTime))
			continue
		}

		syncResult := c.syncToTarget(ctx, cfg, source, target, targetFormat, startTime)
		_ = target.Close()
		results[targetFormat] = syncResult
	}

	if c.hasSuccessfulSyncs(results) && len(cfg.Catalogs) > 0 {
		catalogErr := c.syncToCatalogs(ctx, cfg, source)
		for format, result := range results {
			if result.StatusCode == spi.SyncStatusSuccess && catalogErr != nil {
				results[format] = &spi.SyncResult{
					TableFormat:       format,
					StatusCode:        spi.SyncStatusError,
					Error:             fmt.Sprintf("conversion succeeded but catalog sync failed: %v", catalogErr),
					LastInstantSynced: result.LastInstantSynced,
					Duration:          result.Duration,
				}
			}
		}
	}

	return results, nil
}

func (c *Controller) hasSuccessfulSyncs(results map[model.TableFormat]*spi.SyncResult) bool {
	for _, result := range results {
		if result.StatusCode == spi.SyncStatusSuccess {
			return true
		}
	}
	return false
}

func (c *Controller) syncToCatalogs(ctx context.Context, cfg *DatasetConfig, source spi.ConversionSource) error {
	snapshot, err := source.GetCurrentSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to get snapshot for catalog sync: %w", err)
	}

	for i := range cfg.Catalogs {
		client, err := catalog.NewSyncClient(ctx, &cfg.Catalogs[i])
		if err != nil {
			return fmt.Errorf("failed to create catalog sync client for %s: %w", cfg.Catalogs[i].Type, err)
		}

		if err := client.CreateOrUpdateTable(ctx, snapshot.Table, snapshot); err != nil {
			_ = client.Close()
			return fmt.Errorf("failed to sync to catalog %s: %w", cfg.Catalogs[i].Type, err)
		}
		_ = client.Close()
	}

	return nil
}

func (c *Controller) syncToTarget(
	ctx context.Context,
	cfg *DatasetConfig,
	source spi.ConversionSource,
	target spi.ConversionTarget,
	targetFormat model.TableFormat,
	startTime time.Time,
) *spi.SyncResult {
	// Check existing target sync metadata
	meta, _ := target.GetTableMetadata(ctx)

	canSyncIncrementally := false
	var lastSyncedInstant int64
	if meta != nil && meta.LastInstantSynced > 0 && cfg.SyncMode != spi.SyncModeFull {
		lastSyncedInstant = meta.LastInstantSynced
		isSafe, err := source.IsIncrementalSyncSafeFrom(ctx, lastSyncedInstant)
		if err == nil && isSafe {
			canSyncIncrementally = true
		}
	}

	if canSyncIncrementally {
		// Incremental sync
		changes, err := source.GetChangesSince(ctx, lastSyncedInstant)
		if err != nil {
			return spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to extract changes: %w", err), time.Since(startTime))
		}
		if len(changes.TableChanges) > 0 {
			if err := target.CommitChanges(ctx, changes); err != nil {
				return spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to commit changes: %w", err), time.Since(startTime))
			}
			lastInstant := changes.TableChanges[len(changes.TableChanges)-1].CommitTime
			return spi.NewSuccessSyncResult(targetFormat, lastInstant, time.Since(startTime))
		}
		return spi.NewSuccessSyncResult(targetFormat, lastSyncedInstant, time.Since(startTime))
	}

	// Full snapshot sync
	snapshot, err := source.GetCurrentSnapshot(ctx)
	if err != nil {
		return spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to extract current snapshot: %w", err), time.Since(startTime))
	}

	if err := target.CommitSnapshot(ctx, snapshot); err != nil {
		return spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to commit snapshot: %w", err), time.Since(startTime))
	}

	return spi.NewSuccessSyncResult(targetFormat, snapshot.Table.LatestCommitTime, time.Since(startTime))
}

func (c *Controller) createSource(format model.TableFormat, basePath string) (spi.ConversionSource, error) {
	return formats.NewSource(format, c.storage, basePath)
}

func (c *Controller) createTarget(ctx context.Context, format model.TableFormat, basePath, tableName string) (spi.ConversionTarget, error) {
	return formats.NewTarget(ctx, format, c.storage, basePath, tableName)
}
