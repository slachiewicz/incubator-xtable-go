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
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Source implements spi.ConversionSource for Apache Iceberg tables.
type Source struct {
	storage  io.Storage
	basePath string
}

var _ spi.ConversionSource = (*Source)(nil)

// NewSource creates a new Iceberg ConversionSource instance.
func NewSource(storage io.Storage, basePath string) *Source {
	return &Source{
		storage:  storage,
		basePath: basePath,
	}
}

// Format returns the format identifier.
func (s *Source) Format() model.TableFormat {
	return model.TableFormatIceberg
}

// listMetadataFiles finds all metadata.json files and extracts their version numbers.
func (s *Source) listMetadataFiles(ctx context.Context) ([]int, error) {
	metaDir := io.JoinPath(s.basePath, "metadata")
	files, err := s.storage.List(ctx, metaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list iceberg metadata directory %s: %w", metaDir, err)
	}

	var versions []int
	for _, f := range files {
		base := filepath.Base(f.Path)
		if strings.HasPrefix(base, "v") && strings.HasSuffix(base, ".metadata.json") {
			verStr := strings.TrimPrefix(base, "v")
			verStr = strings.TrimSuffix(verStr, ".metadata.json")
			if v, err := strconv.Atoi(verStr); err == nil {
				versions = append(versions, v)
			}
		}
	}
	sort.Ints(versions)
	return versions, nil
}

// readMetadata reads and parses a specific version of Iceberg TableMetadata.
func (s *Source) readMetadata(ctx context.Context, version int) (*TableMetadata, error) {
	fileName := fmt.Sprintf("v%d.metadata.json", version)
	filePath := io.JoinPath(s.basePath, "metadata", fileName)

	data, err := s.storage.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read iceberg metadata %s: %w", filePath, err)
	}

	var meta TableMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse iceberg metadata JSON: %w", err)
	}
	return &meta, nil
}

// GetCurrentTable returns the Table descriptor at the latest Iceberg metadata version.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	versions, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no iceberg metadata files found in %s", s.basePath)
	}
	latestVer := versions[len(versions)-1]
	return s.GetTable(ctx, strconv.Itoa(latestVer))
}

// GetTable returns the Table descriptor at a specific metadata version.
func (s *Source) GetTable(ctx context.Context, commitID string) (*model.Table, error) {
	ver, err := strconv.Atoi(commitID)
	if err != nil {
		return nil, fmt.Errorf("invalid iceberg metadata version %s: %w", commitID, err)
	}

	meta, err := s.readMetadata(ctx, ver)
	if err != nil {
		return nil, err
	}

	// Find current schema
	var activeSchema *TableSchema
	for _, sc := range meta.Schemas {
		if sc.SchemaID == meta.CurrentSchemaID {
			activeSchema = sc
			break
		}
	}
	if activeSchema == nil && len(meta.Schemas) > 0 {
		activeSchema = meta.Schemas[0]
	}
	if activeSchema == nil {
		return nil, fmt.Errorf("no schema found in iceberg metadata v%d", ver)
	}

	readSchema, err := IcebergToSchema(activeSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert iceberg schema: %w", err)
	}

	// Find partition fields
	var partitionFields []*model.PartitionField
	var activeSpec *PartitionSpec
	for _, ps := range meta.PartitionSpecs {
		if ps.SpecID == meta.DefaultSpecID {
			activeSpec = ps
			break
		}
	}
	if activeSpec != nil {
		for _, pf := range activeSpec.Fields {
			field := readSchema.FieldByPath(pf.Name)
			if field == nil {
				field = &model.Field{Name: pf.Name, Schema: model.NewPrimitiveSchema(model.TypeString, true)}
			}
			partitionFields = append(partitionFields, &model.PartitionField{
				SourceField:   field,
				TransformType: model.PartitionTransformValue,
			})
		}
	}

	return &model.Table{
		Name:               filepath.Base(s.basePath),
		TableFormat:        model.TableFormatIceberg,
		ReadSchema:         readSchema,
		BasePath:           s.basePath,
		PartitioningFields: partitionFields,
		LatestCommitTime:   meta.LastUpdatedMs,
	}, nil
}

// GetCurrentSnapshot constructs the complete Snapshot from Iceberg manifests.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	versions, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no iceberg metadata files found in %s", s.basePath)
	}
	latestVer := versions[len(versions)-1]
	meta, err := s.readMetadata(ctx, latestVer)
	if err != nil {
		return nil, err
	}

	table, err := s.GetTable(ctx, strconv.Itoa(latestVer))
	if err != nil {
		return nil, err
	}

	if meta.CurrentSnapshotID == nil || len(meta.Snapshots) == 0 {
		return &model.Snapshot{
			Table:            table,
			SourceIdentifier: strconv.Itoa(latestVer),
		}, nil
	}

	var currSnapshot *TableSnapshot
	for _, snap := range meta.Snapshots {
		if snap.SnapshotID == *meta.CurrentSnapshotID {
			currSnapshot = snap
			break
		}
	}
	if currSnapshot == nil {
		currSnapshot = meta.Snapshots[len(meta.Snapshots)-1]
	}

	// Read manifest list
	manifestListData, err := s.storage.Read(ctx, currSnapshot.ManifestList)
	if err != nil {
		return nil, fmt.Errorf("failed to read iceberg manifest list %s: %w", currSnapshot.ManifestList, err)
	}

	var manifestList []ManifestListEntry
	if err := json.Unmarshal(manifestListData, &manifestList); err != nil {
		return nil, fmt.Errorf("failed to parse manifest list JSON: %w", err)
	}

	var dataFiles []*model.DataFile
	for _, mle := range manifestList {
		manifestData, err := s.storage.Read(ctx, mle.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest %s: %w", mle.ManifestPath, err)
		}
		var entries []ManifestEntry
		if err := json.Unmarshal(manifestData, &entries); err != nil {
			return nil, fmt.Errorf("failed to parse manifest entries: %w", err)
		}
		for _, e := range entries {
			if e.Status != 2 && e.DataFile != nil { // Not deleted
				dataFiles = append(dataFiles, s.convertManifestDataFile(e.DataFile, table))
			}
		}
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        dataFiles,
		SourceIdentifier: strconv.FormatInt(currSnapshot.SnapshotID, 10),
	}, nil
}

// GetTableChangeForCommit returns the diff of added and removed files in a snapshot.
func (s *Source) GetTableChangeForCommit(ctx context.Context, commitID string) (*model.TableChange, error) {
	snap, err := s.GetCurrentSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &model.TableChange{
		FilesDiff:        model.NewFilesDiff(snap.DataFiles, nil),
		TableAsOfChange:  snap.Table,
		SourceIdentifier: commitID,
		CommitTime:       snap.Table.LatestCommitTime,
	}, nil
}

// GetChangesSince returns incremental changes since a timestamp.
func (s *Source) GetChangesSince(ctx context.Context, fromInstant int64) (*model.IncrementalTableChanges, error) {
	currentTable, err := s.GetCurrentTable(ctx)
	if err != nil {
		return nil, err
	}
	snap, err := s.GetCurrentSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	var changes []*model.TableChange
	if snap.Table.LatestCommitTime > fromInstant {
		change, err := s.GetTableChangeForCommit(ctx, snap.SourceIdentifier)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}

	return &model.IncrementalTableChanges{
		TableChanges: changes,
		CurrentTable: currentTable,
	}, nil
}

// IsIncrementalSyncSafeFrom checks if snapshot history is available.
func (s *Source) IsIncrementalSyncSafeFrom(ctx context.Context, earliestInstant int64) (bool, error) {
	versions, err := s.listMetadataFiles(ctx)
	if err != nil || len(versions) == 0 {
		return false, err
	}
	firstMeta, err := s.readMetadata(ctx, versions[0])
	if err != nil {
		return false, err
	}
	return firstMeta.LastUpdatedMs <= earliestInstant, nil
}

// Close is a no-op for Iceberg source.
func (s *Source) Close() error {
	return nil
}

func (s *Source) convertManifestDataFile(mdf *ManifestDataFile, table *model.Table) *model.DataFile {
	dataFile := &model.DataFile{
		PhysicalPath:  mdf.FilePath,
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: mdf.FileSizeInBytes,
		RecordCount:   mdf.RecordCount,
	}

	if table != nil {
		dataFile.ColumnStats = columnStatsFromManifest(mdf, table.ReadSchema)
	}

	if table != nil && len(mdf.Partition) > 0 {
		for _, pf := range table.PartitioningFields {
			if val, ok := mdf.Partition[pf.SourceField.Name]; ok {
				dataFile.PartitionValues = append(dataFile.PartitionValues, &model.PartitionValue{
					PartitionField: pf,
					Range:          model.NewScalarRange(val),
				})
			}
		}
	}
	return dataFile
}
