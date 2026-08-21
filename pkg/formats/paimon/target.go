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

package paimon

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
	paimonMetadataDir      = "metadata"
	paimonSchemaFile       = "schema.json"
	paimonManifestFile     = "manifest.json"
	paimonSyncMetadataFile = "sync_metadata.json"
)

// Target implements spi.ConversionTarget for Apache Paimon tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	metadataDir string
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Paimon ConversionTarget instance.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatPaimon
}

// Init initializes the target with target table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.metadataDir = filepath.Join(targetTable.BasePath, paimonMetadataDir)
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves synchronization metadata from the Paimon metadata directory.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	if t.source == nil {
		return nil, nil
	}

	syncMetadataPath := filepath.Join(t.metadataDir, paimonSyncMetadataFile)
	data, err := t.storage.Read(ctx, syncMetadataPath)
	if err != nil {
		return nil, nil
	}

	var syncMetadata model.TableSyncMetadata
	if err := json.Unmarshal(data, &syncMetadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync metadata: %w", err)
	}

	return &syncMetadata, nil
}

// CommitSnapshot synchronizes a full Snapshot into the Paimon format without rewriting data files.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	if snapshot == nil || snapshot.Table == nil || snapshot.Table.ReadSchema == nil {
		return fmt.Errorf("invalid snapshot: missing table or schema")
	}

	schemaFileName := fmt.Sprintf("schema-%s.json", epochString())
	if err := t.writeSchemaFile(ctx, schemaFileName, snapshot.Table.ReadSchema); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	manifest := struct {
		Version         string                  `json:"version"`
		ManifestID      string                  `json:"manifest_id"`
		SchemaFile      string                  `json:"schema_file"`
		Timestamp       time.Time               `json:"timestamp"`
		SnapshotID      string                  `json:"snapshot_id"`
		DataFiles       []*model.DataFile       `json:"data_files"`
		PartitionFields []*model.PartitionField `json:"partition_fields,omitempty"`
	}{
		Version:         "1.0",
		ManifestID:      uuid.New().String(),
		SchemaFile:      schemaFileName,
		Timestamp:       time.Now(),
		SnapshotID:      snapshot.SourceIdentifier,
		DataFiles:       snapshot.DataFiles,
		PartitionFields: snapshot.Table.PartitioningFields,
	}

	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	manifestFileName := fmt.Sprintf("manifest-%s.json", epochString())
	manifestPath := filepath.Join(t.metadataDir, manifestFileName)
	if err := t.storage.Write(ctx, manifestPath, manifestData); err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}

	return nil
}

// CommitChanges synchronizes an incremental sequence of TableChange commits to Paimon format.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	if changes == nil || changes.CurrentTable == nil {
		return fmt.Errorf("invalid incremental changes")
	}

	for _, change := range changes.TableChanges {
		changeManifest := struct {
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
			changeManifest.AddedFiles = change.FilesDiff.FilesAdded
			changeManifest.RemovedFiles = change.FilesDiff.FilesRemoved
		}

		if change.TableAsOfChange != nil {
			changeManifest.Schema = change.TableAsOfChange.ReadSchema
			if change.TableAsOfChange.PartitioningFields != nil {
				changeManifest.PartitionFields = change.TableAsOfChange.PartitioningFields
			}
		}

		changeData, err := json.MarshalIndent(changeManifest, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal change manifest: %w", err)
		}

		changeFileName := fmt.Sprintf("change-%s.json", epochString())
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
		TargetFormat:      model.TableFormatPaimon,
	}

	syncMetadataData, err := json.MarshalIndent(syncMetadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal sync metadata: %w", err)
	}

	syncMetadataPath := filepath.Join(t.metadataDir, paimonSyncMetadataFile)
	if err := t.storage.Write(ctx, syncMetadataPath, syncMetadataData); err != nil {
		return fmt.Errorf("failed to write sync metadata: %w", err)
	}

	return nil
}

// Close releases any resources.
func (t *Target) Close() error {
	return nil
}

func (t *Target) writeSchemaFile(ctx context.Context, fileName string, schema *model.Schema) error {
	schemaData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	schemaPath := filepath.Join(t.metadataDir, fileName)
	if err := t.storage.Write(ctx, schemaPath, schemaData); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	return nil
}

func epochString() string {
	return fmt.Sprintf("%d", time.Now().UnixMilli())
}
