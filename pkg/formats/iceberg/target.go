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

package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Target implements spi.ConversionTarget for Apache Iceberg tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Iceberg ConversionTarget instance.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatIceberg
}

// Init initializes the target with table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves previously recorded TableSyncMetadata from Iceberg table properties.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	if t.source == nil {
		return nil, nil
	}
	versions, paths, err := t.source.listMetadataFiles(ctx)
	if err != nil || len(versions) == 0 {
		return nil, nil
	}

	latestVer := versions[len(versions)-1]
	meta, err := t.source.readMetadata(ctx, paths[latestVer])
	if err != nil {
		return nil, err
	}

	if meta.Properties == nil {
		return nil, nil
	}

	syncMeta := &model.TableSyncMetadata{
		TargetFormat:     model.TableFormatIceberg,
		CustomProperties: meta.Properties,
	}
	if lastInstantStr, ok := meta.Properties[model.KeyLastInstantSynced]; ok {
		if lastInstant, err := strconv.ParseInt(lastInstantStr, 10, 64); err == nil {
			syncMeta.LastInstantSynced = lastInstant
		}
	}
	if srcFormatStr, ok := meta.Properties[model.KeySourceFormat]; ok {
		syncMeta.SourceFormat = model.TableFormat(srcFormatStr)
	}

	return syncMeta, nil
}

// CommitSnapshot writes a full snapshot into Apache Iceberg metadata and manifests.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	versions, paths, _ := t.source.listMetadataFiles(ctx)
	nextVersion := 1
	var prevMeta *TableMetadata
	if len(versions) > 0 {
		latestVer := versions[len(versions)-1]
		nextVersion = latestVer + 1
		prevMeta, _ = t.source.readMetadata(ctx, paths[latestVer])
	}

	now := time.Now().UnixMilli()
	snapshotID := now
	schemaID := 0
	if prevMeta != nil {
		schemaID = prevMeta.CurrentSchemaID + 1
	}

	// 1. Convert Schema
	tableSchema, lastColID, err := SchemaToIceberg(snapshot.Table.ReadSchema, schemaID)
	if err != nil {
		return fmt.Errorf("failed to convert schema to iceberg: %w", err)
	}

	// 2. Convert Partition Spec
	var partitionFieldDefs []*PartitionFieldDef
	for idx, pf := range snapshot.Table.PartitioningFields {
		sourceID := idx + 1
		for _, f := range tableSchema.Fields {
			if f.Name == pf.SourceField.Name {
				sourceID = f.ID
				break
			}
		}
		partitionFieldDefs = append(partitionFieldDefs, &PartitionFieldDef{
			SourceID:  sourceID,
			FieldID:   1000 + idx,
			Name:      pf.SourceField.Name,
			Transform: "identity",
		})
	}
	partitionSpec := &PartitionSpec{
		SpecID: 0,
		Fields: partitionFieldDefs,
	}

	// 3. Write Manifest File
	var manifestEntries []ManifestEntry
	for _, df := range snapshot.AllDataFiles() {
		partitionVals := make(map[string]any)
		for _, pv := range df.PartitionValues {
			if pv.PartitionField != nil && pv.PartitionField.SourceField != nil && pv.Range != nil {
				partitionVals[pv.PartitionField.SourceField.Name] = pv.Range.MinValue
			}
		}
		manifestDataFile := &ManifestDataFile{
			FilePath:        df.PhysicalPath,
			FileFormat:      string(df.FileFormat),
			Partition:       partitionVals,
			RecordCount:     df.RecordCount,
			FileSizeInBytes: df.FileSizeBytes,
		}
		columnStatsToManifest(manifestDataFile, df.ColumnStats, tableSchema)

		manifestEntries = append(manifestEntries, ManifestEntry{
			Status:     1, // ADDED
			SnapshotID: snapshotID,
			DataFile:   manifestDataFile,
		})
	}

	manifestUUID := uuid.New().String()
	manifestFileName := fmt.Sprintf("%s-m0.json", manifestUUID)
	manifestPath := io.JoinPath(t.targetTable.BasePath, "metadata", manifestFileName)
	manifestBytes, err := json.Marshal(manifestEntries)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest entries: %w", err)
	}
	if err := t.storage.Write(ctx, manifestPath, manifestBytes); err != nil {
		return fmt.Errorf("failed to write manifest file %s: %w", manifestPath, err)
	}

	// 4. Write Manifest List File
	manifestListEntry := ManifestListEntry{
		ManifestPath:       manifestPath,
		ManifestLength:     int64(len(manifestBytes)),
		PartitionSpecID:    0,
		AddedSnapshotID:    snapshotID,
		AddedFilesCount:    len(manifestEntries),
		ExistingFilesCount: 0,
		DeletedFilesCount:  0,
	}
	manifestList := []ManifestListEntry{manifestListEntry}
	manifestListFileName := fmt.Sprintf("snap-%d-%s.json", snapshotID, uuid.New().String())
	manifestListPath := io.JoinPath(t.targetTable.BasePath, "metadata", manifestListFileName)
	manifestListBytes, err := json.Marshal(manifestList)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest list: %w", err)
	}
	if err := t.storage.Write(ctx, manifestListPath, manifestListBytes); err != nil {
		return fmt.Errorf("failed to write manifest list %s: %w", manifestListPath, err)
	}

	// 5. Build Properties
	props := make(map[string]string)
	if prevMeta != nil && prevMeta.Properties != nil {
		for k, v := range prevMeta.Properties {
			props[k] = v
		}
	}
	props[model.KeyLastInstantSynced] = strconv.FormatInt(snapshot.Table.LatestCommitTime, 10)
	props[model.KeySourceFormat] = string(snapshot.Table.TableFormat)

	// 6. Build Snapshot
	var parentSnapID *int64
	if prevMeta != nil && prevMeta.CurrentSnapshotID != nil {
		parentSnapID = prevMeta.CurrentSnapshotID
	}
	seqNumber := int64(1)
	if prevMeta != nil {
		seqNumber = prevMeta.LastSequenceNumber + 1
	}

	tableSnapshot := &TableSnapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapID,
		SequenceNumber:   seqNumber,
		TimestampMs:      now,
		ManifestList:     manifestListPath,
		Summary: map[string]string{
			"operation":        "replace",
			"added-data-files": strconv.Itoa(len(manifestEntries)),
			"total-data-files": strconv.Itoa(len(manifestEntries)),
		},
		SchemaID: &schemaID,
	}

	var snapshots []*TableSnapshot
	if prevMeta != nil {
		snapshots = append(snapshots, prevMeta.Snapshots...)
	}
	snapshots = append(snapshots, tableSnapshot)

	tableUUID := uuid.New().String()
	if prevMeta != nil && prevMeta.TableUUID != "" {
		tableUUID = prevMeta.TableUUID
	}

	metadata := &TableMetadata{
		FormatVersion:      2,
		TableUUID:          tableUUID,
		Location:           t.targetTable.BasePath,
		LastSequenceNumber: seqNumber,
		LastUpdatedMs:      now,
		LastColumnID:       lastColID,
		CurrentSchemaID:    schemaID,
		Schemas:            []*TableSchema{tableSchema},
		DefaultSpecID:      0,
		PartitionSpecs:     []*PartitionSpec{partitionSpec},
		LastPartitionID:    1000 + len(partitionFieldDefs),
		Properties:         props,
		CurrentSnapshotID:  &snapshotID,
		Snapshots:          snapshots,
	}

	// 7. Write v{N}.metadata.json
	metaFileName := fmt.Sprintf("v%d.metadata.json", nextVersion)
	metaFilePath := io.JoinPath(t.targetTable.BasePath, "metadata", metaFileName)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata JSON: %w", err)
	}
	if err := t.storage.Write(ctx, metaFilePath, metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata file %s: %w", metaFilePath, err)
	}

	// 8. Update version-hint.text
	hintPath := io.JoinPath(t.targetTable.BasePath, "metadata", "version-hint.text")
	return t.storage.Write(ctx, hintPath, []byte(strconv.Itoa(nextVersion)))
}

// CommitChanges writes incremental changes.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	for _, change := range changes.TableChanges {
		snap := &model.Snapshot{
			Table:            change.TableAsOfChange,
			DataFiles:        change.FilesDiff.FilesAdded,
			SourceIdentifier: change.SourceIdentifier,
		}
		if err := t.CommitSnapshot(ctx, snap); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for Iceberg target.
func (t *Target) Close() error {
	return nil
}
