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
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package parquet

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

const (
	parquetMetadataDir      = "_polytable_metadata"
	parquetManifestFile     = "manifest.json"
	parquetSyncMetadataFile = "sync_metadata.json"
)

// Target implements spi.ConversionTarget for Parquet directory datasets.
// It writes metadata files alongside Parquet data files without touching the data files themselves.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	metadataDir string
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Parquet ConversionTarget instance.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatParquet
}

// Init initializes the target with target table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.metadataDir = filepath.Join(targetTable.BasePath, parquetMetadataDir)
	return nil
}

// GetTableMetadata retrieves synchronization metadata previously stored in Parquet metadata directory.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	metadataPath := filepath.Join(t.metadataDir, parquetSyncMetadataFile)
	data, err := t.storage.Read(ctx, metadataPath)
	if err != nil {
		return nil, nil
	}

	var metadata model.TableSyncMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse sync metadata: %w", err)
	}

	return &metadata, nil
}

// CommitSnapshot synchronizes a full Snapshot into the Parquet format metadata.
// It writes a manifest file containing schema and data file information without touching data files.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	if snapshot == nil || snapshot.Table == nil || snapshot.Table.ReadSchema == nil {
		return fmt.Errorf("invalid snapshot: missing table or schema")
	}

	manifest := struct {
		Version         string                  `json:"version"`
		ManifestID      string                  `json:"manifest_id"`
		Timestamp       time.Time               `json:"timestamp"`
		Schema          *model.Schema           `json:"schema"`
		DataFiles       []*model.DataFile       `json:"data_files"`
		PartitionFields []*model.PartitionField `json:"partition_fields,omitempty"`
	}{
		Version:         "1.0",
		ManifestID:      uuid.New().String(),
		Timestamp:       time.Now(),
		Schema:          snapshot.Table.ReadSchema,
		DataFiles:       snapshot.DataFiles,
		PartitionFields: snapshot.Table.PartitioningFields,
	}

	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestPath := filepath.Join(t.metadataDir, parquetManifestFile)
	if err := t.storage.Write(ctx, manifestPath, manifestData); err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}

	syncMetadata := model.TableSyncMetadata{
		LastInstantSynced: snapshot.Table.LatestCommitTime,
		SourceFormat:      snapshot.Table.TableFormat,
		TargetFormat:      model.TableFormatParquet,
	}

	if err := t.writeSyncMetadata(ctx, &syncMetadata); err != nil {
		return fmt.Errorf("failed to write sync metadata: %w", err)
	}

	return nil
}

// CommitChanges synchronizes an incremental sequence of TableChange commits to Parquet format.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	if changes == nil || changes.CurrentTable == nil {
		return fmt.Errorf("invalid incremental changes")
	}

	for _, change := range changes.TableChanges {
		manifest := struct {
			Version         string                  `json:"version"`
			ChangeID        string                  `json:"change_id"`
			SnapshotID      string                  `json:"snapshot_id"`
			Timestamp       time.Time               `json:"timestamp"`
			Schema          *model.Schema           `json:"schema,omitempty"`
			AddedFiles      []*model.DataFile       `json:"added_files"`
			RemovedFiles    []*model.DataFile       `json:"removed_files"`
			PartitionFields []*model.PartitionField `json:"partition_fields,omitempty"`
		}{
			Version:    "1.0",
			ChangeID:   uuid.New().String(),
			SnapshotID: change.SourceIdentifier,
			Timestamp:  time.Now(),
		}

		if change.FilesDiff != nil {
			manifest.AddedFiles = change.FilesDiff.FilesAdded
			manifest.RemovedFiles = change.FilesDiff.FilesRemoved
		}

		if change.TableAsOfChange != nil {
			manifest.Schema = change.TableAsOfChange.ReadSchema
			if change.TableAsOfChange.PartitioningFields != nil {
				manifest.PartitionFields = change.TableAsOfChange.PartitioningFields
			}
		}

		changeData, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal change manifest: %w", err)
		}

		changeFileName := fmt.Sprintf("change_%s.json", change.SourceIdentifier)
		changePath := filepath.Join(t.metadataDir, changeFileName)
		if err := t.storage.Write(ctx, changePath, changeData); err != nil {
			return fmt.Errorf("failed to write change manifest: %w", err)
		}
	}

	var latestSyncInstant int64

	for _, change := range changes.TableChanges {
		if change.CommitTime > latestSyncInstant {
			latestSyncInstant = change.CommitTime
		}
	}

	sourceFormat := model.TableFormat("")
	if changes.CurrentTable != nil {
		sourceFormat = changes.CurrentTable.TableFormat
	}

	syncMetadata := model.TableSyncMetadata{
		LastInstantSynced: latestSyncInstant,
		SourceFormat:      sourceFormat,
		TargetFormat:      model.TableFormatParquet,
	}

	if err := t.writeSyncMetadata(ctx, &syncMetadata); err != nil {
		return fmt.Errorf("failed to write sync metadata: %w", err)
	}

	return nil
}

// Close releases any resources.
func (t *Target) Close() error {
	return nil
}

// writeSyncMetadata writes synchronization metadata to Parquet metadata directory.
func (t *Target) writeSyncMetadata(ctx context.Context, metadata *model.TableSyncMetadata) error {
	metadataData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal sync metadata: %w", err)
	}

	metadataPath := filepath.Join(t.metadataDir, parquetSyncMetadataFile)
	if err := t.storage.Write(ctx, metadataPath, metadataData); err != nil {
		return fmt.Errorf("failed to write sync metadata file: %w", err)
	}

	return nil
}
