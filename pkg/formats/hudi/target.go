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

package hudi

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// Target implements spi.ConversionTarget for Apache Hudi tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Hudi ConversionTarget.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatHudi
}

// Init initializes the target with table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves previously saved TableSyncMetadata from hoodie.properties.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	props, err := t.source.ReadProperties(ctx)
	if err != nil {
		return nil, nil
	}

	syncMeta := &model.TableSyncMetadata{
		TargetFormat:     model.TableFormatHudi,
		CustomProperties: props.Properties,
	}
	if lastInstantStr := props.Get(model.KeyLastInstantSynced); lastInstantStr != "" {
		if lastInstant, err := strconv.ParseInt(lastInstantStr, 10, 64); err == nil {
			syncMeta.LastInstantSynced = lastInstant
		}
	}
	if srcFormatStr := props.Get(model.KeySourceFormat); srcFormatStr != "" {
		syncMeta.SourceFormat = model.TableFormat(srcFormatStr)
	}

	return syncMeta, nil
}

// CommitSnapshot writes a full table snapshot into Hudi table format.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	now := time.Now()
	instant := InstantFromTime(now)

	// 1. Convert Schema to Avro JSON
	avroJSON, err := SchemaToAvroJSON(snapshot.Table.ReadSchema, snapshot.Table.Name, "hoodie."+snapshot.Table.Name)
	if err != nil {
		return fmt.Errorf("failed to convert schema to avro JSON: %w", err)
	}

	// 2. Prepare partition field names
	var partFieldNames []string
	for _, pf := range snapshot.Table.PartitioningFields {
		partFieldNames = append(partFieldNames, pf.SourceField.Name)
	}

	// 3. Build Write Stats
	partitionStats := make(map[string][]HoodieWriteStat)
	for _, df := range snapshot.AllDataFiles() {
		partPath := ""
		if len(df.PartitionValues) > 0 && df.PartitionValues[0].Range != nil {
			partPath = fmt.Sprintf("%v", df.PartitionValues[0].Range.MinValue)
		}

		relPath := strings.TrimPrefix(df.PhysicalPath, t.targetTable.BasePath)
		relPath = strings.TrimPrefix(relPath, "/")

		ws := HoodieWriteStat{
			FileID:          uuid.New().String(),
			Path:            relPath,
			NumWrites:       df.RecordCount,
			TotalWriteBytes: df.FileSizeBytes,
			FileSizeInBytes: df.FileSizeBytes,
		}
		partitionStats[partPath] = append(partitionStats[partPath], ws)
	}

	// 4. Build and write Commit Metadata
	extraMeta := make(map[string]string)
	extraMeta[model.KeyLastInstantSynced] = strconv.FormatInt(snapshot.Table.LatestCommitTime, 10)
	extraMeta[model.KeySourceFormat] = string(snapshot.Table.TableFormat)

	commitMeta := HoodieCommitMetadata{
		PartitionToWriteStats: partitionStats,
		ExtraMetadata:         extraMeta,
		OperationType:         "XTABLE_SYNC",
	}

	commitBytes, err := json.Marshal(commitMeta)
	if err != nil {
		return fmt.Errorf("failed to serialize commit metadata: %w", err)
	}

	commitFilePath := io.JoinPath(t.targetTable.BasePath, ".hoodie", fmt.Sprintf("%s.commit", instant))
	if err := t.storage.Write(ctx, commitFilePath, commitBytes); err != nil {
		return fmt.Errorf("failed to write commit file %s: %w", commitFilePath, err)
	}

	// 5. Update hoodie.properties
	props := NewTableProperties()
	if existingProps, err := t.source.ReadProperties(ctx); err == nil {
		props = existingProps
	}

	props.Set(PropTableName, snapshot.Table.Name)
	props.Set(PropTableType, "COPY_ON_WRITE")
	props.Set(PropTableVersion, "6")
	props.Set(PropBaseFileFormat, "PARQUET")
	if len(partFieldNames) > 0 {
		props.Set(PropPartitionFields, strings.Join(partFieldNames, ","))
	}
	props.Set(PropTableSchema, avroJSON)
	props.Set(model.KeyLastInstantSynced, strconv.FormatInt(snapshot.Table.LatestCommitTime, 10))
	props.Set(model.KeySourceFormat, string(snapshot.Table.TableFormat))

	propsFilePath := io.JoinPath(t.targetTable.BasePath, ".hoodie", "hoodie.properties")
	return t.storage.Write(ctx, propsFilePath, props.Serialize())
}

// CommitChanges commits incremental changes.
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

// Close is a no-op.
func (t *Target) Close() error {
	return nil
}
