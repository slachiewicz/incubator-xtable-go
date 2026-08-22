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
	"errors"
	"fmt"
	"time"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// CatalogClientFactory constructs a catalog sync client from its configuration.
type CatalogClientFactory func(ctx context.Context, cfg *catalog.Config) (catalog.SyncClient, error)

// Controller orchestrates the end-to-end table format metadata synchronization process.
type Controller struct {
	storage io.Storage
	// newCatalogClient is injected so tests can substitute a fake without global state.
	newCatalogClient CatalogClientFactory
}

// Option customizes a Controller at construction time.
type Option func(*Controller)

// WithCatalogClientFactory overrides how catalog sync clients are constructed. Intended for tests;
// production callers should rely on the default, which is catalog.NewSyncClient.
func WithCatalogClientFactory(factory CatalogClientFactory) Option {
	return func(c *Controller) {
		c.newCatalogClient = factory
	}
}

// syncOptions carries the per-call settings for Sync, as opposed to Option's per-Controller ones.
type syncOptions struct {
	dryRun bool
}

// SyncOption customizes a single Sync call.
type SyncOption func(*syncOptions)

// WithDryRun makes Sync resolve the source, compute the changes it would write to each target, and
// report the result without committing anything — no CommitSnapshot/CommitChanges call on any
// target, and no catalog registration. Init and GetTableMetadata still run against the target,
// since every target adapter's implementation is in-memory only and reading the target's existing
// sync metadata is what decides full vs. incremental in the first place.
func WithDryRun() SyncOption {
	return func(o *syncOptions) { o.dryRun = true }
}

// NewController creates a new ConversionController instance.
func NewController(storage io.Storage, opts ...Option) *Controller {
	c := &Controller{
		storage:          storage,
		newCatalogClient: catalog.NewSyncClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Sync synchronizes a source table to all configured target formats. opts is currently only
// WithDryRun; pass none for the normal, committing behavior.
func (c *Controller) Sync(ctx context.Context, cfg *DatasetConfig, opts ...SyncOption) (map[model.TableFormat]*spi.SyncResult, error) {
	var so syncOptions
	for _, opt := range opts {
		opt(&so)
	}

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

		syncResult := c.syncToTarget(ctx, cfg, source, target, targetFormat, startTime, so.dryRun)
		_ = target.Close()
		results[targetFormat] = syncResult

		// A dry run never registers with a catalog: the metadata it would register was never
		// written to the target, so a catalog client reading it back would see stale or absent
		// state.
		if !so.dryRun && syncResult.StatusCode == spi.SyncStatusSuccess && len(cfg.Catalogs) > 0 {
			if catalogErr := c.syncTargetToCatalogs(ctx, cfg, targetFormat); catalogErr != nil {
				results[targetFormat].Error = appendCatalogError(results[targetFormat].Error, catalogErr.Error())
			}
		}
	}

	return results, nil
}

func (c *Controller) syncTargetToCatalogs(ctx context.Context, cfg *DatasetConfig, targetFormat model.TableFormat) error {
	targetSource, err := c.createSource(targetFormat, cfg.TableBasePath)
	if err != nil {
		return fmt.Errorf("failed to create target source for catalog sync: %w", err)
	}
	defer func() { _ = targetSource.Close() }()

	snapshot, err := targetSource.GetCurrentSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to get target snapshot for catalog sync: %w", err)
	}

	var catalogErrors []error
	for i := range cfg.Catalogs {
		client, err := c.newCatalogClient(ctx, &cfg.Catalogs[i])
		if err != nil {
			catalogErrors = append(catalogErrors, fmt.Errorf("failed to create catalog sync client for %s: %w", cfg.Catalogs[i].Type, err))
			continue
		}

		if err := client.CreateOrUpdateTable(ctx, snapshot.Table, snapshot); err != nil {
			catalogErrors = append(catalogErrors, fmt.Errorf("failed to sync to catalog %s: %w", cfg.Catalogs[i].Type, err))
			_ = client.Close()
			continue
		}

		if err := syncPartitions(ctx, client, &cfg.Catalogs[i], snapshot); err != nil {
			catalogErrors = append(catalogErrors, fmt.Errorf("failed to sync partitions to catalog %s: %w", cfg.Catalogs[i].Type, err))
		}
		_ = client.Close()
	}

	if len(catalogErrors) > 0 {
		return fmt.Errorf("catalog sync errors: %v", errors.Join(catalogErrors...))
	}
	return nil
}

// syncPartitions reconciles the table's partitions when the catalog tracks them separately. Iceberg
// REST does not — partition data lives in the Iceberg metadata — so its client does not implement
// PartitionSyncOperations and this is a no-op for it.
func syncPartitions(ctx context.Context, client catalog.SyncClient, cfg *catalog.Config, snapshot *model.Snapshot) error {
	ops, ok := client.(catalog.PartitionSyncOperations)
	if !ok {
		return nil
	}

	// Guard: an unpartitioned table yields no partitions, and reconciling that against a catalog
	// would drop every partition it holds. Absence of partitioning fields means "not our business",
	// not "delete everything".
	if snapshot == nil || snapshot.Table == nil || len(snapshot.Table.PartitioningFields) == 0 {
		return nil
	}

	id := catalog.TableIdentifier{Database: cfg.DatabaseName, Table: snapshot.Table.Name}
	return catalog.SyncPartitions(ctx, ops, id, catalog.PartitionsFromSnapshot(snapshot), cfg.MaxPartitionsPerRequest)
}

func appendCatalogError(baseErr, catalogErr string) string {
	if baseErr == "" {
		return fmt.Sprintf("catalog sync error: %s", catalogErr)
	}
	return fmt.Sprintf("%s; catalog sync error: %s", baseErr, catalogErr)
}

func (c *Controller) syncToTarget(
	ctx context.Context,
	cfg *DatasetConfig,
	source spi.ConversionSource,
	target spi.ConversionTarget,
	targetFormat model.TableFormat,
	startTime time.Time,
	dryRun bool,
) *spi.SyncResult {
	// Check existing target sync metadata. GetTableMetadata is read-only on every adapter, so this
	// is safe to call under a dry run.
	meta, _ := target.GetTableMetadata(ctx)

	canSyncIncrementally := false
	var lastSyncedInstant int64
	// fallbackReason is set only when an incremental sync was actually on the table (SyncModeFull
	// was not requested and a prior sync exists) and the safety check then said no: either it
	// reported the resume point unsafe, or it could not be evaluated at all. Recorded here and
	// attached to whichever result the full-sync path below returns, so "fell back" and "errored"
	// stop being indistinguishable from a SUCCESS with no history — the defect this fixes.
	var fallbackReason string
	if meta != nil && meta.LastInstantSynced > 0 && cfg.SyncMode != spi.SyncModeFull {
		lastSyncedInstant = meta.LastInstantSynced
		isSafe, err := source.IsIncrementalSyncSafeFrom(ctx, lastSyncedInstant)
		switch {
		case err != nil:
			fallbackReason = fmt.Sprintf("could not verify incremental sync safety from instant %d: %v", lastSyncedInstant, err)
		case !isSafe:
			fallbackReason = fmt.Sprintf("incremental sync from instant %d is not safe: the source no longer retains history back to that point", lastSyncedInstant)
		default:
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
			if !dryRun {
				if err := target.CommitChanges(ctx, changes); err != nil {
					return spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to commit changes: %w", err), time.Since(startTime))
				}
			}
			lastInstant := changes.TableChanges[len(changes.TableChanges)-1].CommitTime
			return spi.NewSuccessSyncResult(targetFormat, lastInstant, time.Since(startTime))
		}
		// No new commits since the last synced instant: nothing was (or would be) written.
		// Report NO_OP rather than a SUCCESS indistinguishable from real work.
		result := spi.NewSuccessSyncResult(targetFormat, lastSyncedInstant, time.Since(startTime))
		result.NoOp = true
		return result
	}

	// Full snapshot sync
	snapshot, err := source.GetCurrentSnapshot(ctx)
	if err != nil {
		result := spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to extract current snapshot: %w", err), time.Since(startTime))
		applyFallbackReason(result, fallbackReason)
		return result
	}

	if !dryRun {
		if err := target.CommitSnapshot(ctx, snapshot); err != nil {
			result := spi.NewErrorSyncResult(targetFormat, fmt.Errorf("failed to commit snapshot: %w", err), time.Since(startTime))
			applyFallbackReason(result, fallbackReason)
			return result
		}
	}

	result := spi.NewSuccessSyncResult(targetFormat, snapshot.Table.LatestCommitTime, time.Since(startTime))
	applyFallbackReason(result, fallbackReason)
	return result
}

// applyFallbackReason records on result that the controller ran a full snapshot sync in place of
// the incremental sync it was actually asked for. A no-op when reason is empty: an ordinary first
// sync (no prior target metadata) or an explicit SyncModeFull request never sets one, and neither
// is a fallback.
func applyFallbackReason(result *spi.SyncResult, reason string) {
	if reason == "" {
		return
	}
	result.FellBackToFullSync = true
	result.FallbackReason = reason
}

func (c *Controller) createSource(format model.TableFormat, basePath string) (spi.ConversionSource, error) {
	return formats.NewSource(format, c.storage, basePath)
}

func (c *Controller) createTarget(ctx context.Context, format model.TableFormat, basePath, tableName string) (spi.ConversionTarget, error) {
	return formats.NewTarget(ctx, format, c.storage, basePath, tableName)
}
