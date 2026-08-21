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
	"path/filepath"
	"sort"
	"strings"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Source implements spi.ConversionSource for Apache Hudi tables.
type Source struct {
	storage  io.Storage
	basePath string
}

var _ spi.ConversionSource = (*Source)(nil)

// NewSource creates a new Hudi ConversionSource.
func NewSource(storage io.Storage, basePath string) *Source {
	return &Source{
		storage:  storage,
		basePath: basePath,
	}
}

// Format returns the format identifier.
func (s *Source) Format() model.TableFormat {
	return model.TableFormatHudi
}

// ReadProperties loads .hoodie/hoodie.properties.
func (s *Source) ReadProperties(ctx context.Context) (*TableProperties, error) {
	propsPath := io.JoinPath(s.basePath, ".hoodie", "hoodie.properties")
	data, err := s.storage.Read(ctx, propsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read hoodie.properties at %s: %w", propsPath, err)
	}
	return ParseProperties(data)
}

// ListCompletedCommits finds all completed commit/deltacommit instants in the timeline.
func (s *Source) ListCompletedCommits(ctx context.Context) ([]InstantAction, error) {
	hoodieDir := io.JoinPath(s.basePath, ".hoodie")
	files, err := s.storage.List(ctx, hoodieDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list .hoodie directory %s: %w", hoodieDir, err)
	}

	var commits []InstantAction
	for _, f := range files {
		base := filepath.Base(f.Path)
		if strings.HasPrefix(base, ".") {
			continue
		}
		if strings.HasSuffix(base, ".commit") || strings.HasSuffix(base, ".deltacommit") || strings.HasSuffix(base, ".replacecommit") {
			parts := strings.Split(base, ".")
			if len(parts) >= 2 {
				commits = append(commits, InstantAction{
					InstantTime: parts[0],
					Action:      parts[1],
					State:       "completed",
					FileName:    base,
				})
			}
		}
	}

	sort.Slice(commits, func(i, j int) bool {
		return commits[i].InstantTime < commits[j].InstantTime
	})
	return commits, nil
}

// GetCurrentTable returns the Table descriptor at the latest commit.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	commits, err := s.ListCompletedCommits(ctx)
	if err != nil {
		return nil, err
	}
	if len(commits) == 0 {
		return nil, fmt.Errorf("no completed commits found in timeline for %s", s.basePath)
	}

	latestCommit := commits[len(commits)-1]
	return s.GetTable(ctx, latestCommit.InstantTime)
}

// GetTable returns the Table descriptor at a specific commit instant.
func (s *Source) GetTable(ctx context.Context, commitID string) (*model.Table, error) {
	props, err := s.ReadProperties(ctx)
	if err != nil {
		return nil, err
	}

	tableName := props.Get(PropTableName)
	if tableName == "" {
		tableName = filepath.Base(s.basePath)
	}

	// Resolve Schema
	schemaJSON := props.Get(PropTableSchema)
	var readSchema *model.Schema
	if schemaJSON != "" {
		readSchema, _ = AvroJSONToSchema(schemaJSON)
	}

	if readSchema == nil {
		// Try reading .hoodie/.schema/<instant>.avsc
		schemaPath := io.JoinPath(s.basePath, ".hoodie", ".schema", fmt.Sprintf("%s.avsc", commitID))
		if data, err := s.storage.Read(ctx, schemaPath); err == nil {
			readSchema, _ = AvroJSONToSchema(string(data))
		}
	}

	if readSchema == nil {
		readSchema = model.NewRecordSchema(tableName, nil, false)
	}

	// Resolve Partition Fields
	var partitionFields []*model.PartitionField
	if partStr := props.Get(PropPartitionFields); partStr != "" {
		for _, pfName := range strings.Split(partStr, ",") {
			pfName = strings.TrimSpace(pfName)
			if pfName == "" {
				continue
			}
			f := readSchema.FieldByPath(pfName)
			if f == nil {
				f = &model.Field{Name: pfName, Schema: model.NewPrimitiveSchema(model.TypeString, true)}
			}
			partitionFields = append(partitionFields, &model.PartitionField{
				SourceField:   f,
				TransformType: model.PartitionTransformValue,
			})
		}
	}

	t, err := TimeFromInstant(commitID)
	commitTimeMs := int64(0)
	if err == nil {
		commitTimeMs = t.UnixMilli()
	}

	return &model.Table{
		Name:               tableName,
		TableFormat:        model.TableFormatHudi,
		ReadSchema:         readSchema,
		BasePath:           s.basePath,
		PartitioningFields: partitionFields,
		LatestCommitTime:   commitTimeMs,
	}, nil
}

// activeFile pairs a data file with the partition and file group it was written into. The snapshot
// walk is keyed by path, but a replacecommit names the groups it supersedes by file group ID, so
// the group has to be carried alongside to resolve one against the other.
type activeFile struct {
	file      *model.DataFile
	partition string
	fileGroup string
}

// applyReplacedFileGroups drops every active file belonging to a superseded file group. Without it
// an INSERT_OVERWRITE leaves the overwritten files in the snapshot next to their replacements, so
// the table reads back with both generations of every row.
func applyReplacedFileGroups(activeFiles map[string]*activeFile, partitionToReplaceFileIDs map[string][]string) {
	for partitionPath, fileIDs := range partitionToReplaceFileIDs {
		if len(fileIDs) == 0 {
			continue
		}
		replaced := make(map[string]struct{}, len(fileIDs))
		for _, id := range fileIDs {
			replaced[id] = struct{}{}
		}
		for key, af := range activeFiles {
			if af.partition != partitionPath {
				continue
			}
			if _, ok := replaced[af.fileGroup]; ok {
				delete(activeFiles, key)
			}
		}
	}
}

// GetCurrentSnapshot builds the Snapshot of active data files by reading Hudi commit metadata.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	commits, err := s.ListCompletedCommits(ctx)
	if err != nil {
		return nil, err
	}
	if len(commits) == 0 {
		return nil, fmt.Errorf("no completed commits in %s", s.basePath)
	}

	latestCommit := commits[len(commits)-1]
	table, err := s.GetTable(ctx, latestCommit.InstantTime)
	if err != nil {
		return nil, err
	}

	// Traverse commit timeline and build active file map
	activeFiles := make(map[string]*activeFile)

	for _, c := range commits {
		commitFilePath := io.JoinPath(s.basePath, ".hoodie", c.FileName)
		data, err := s.storage.Read(ctx, commitFilePath)
		if err != nil {
			continue
		}

		var meta HoodieCommitMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}

		commitTime, _ := TimeFromInstant(c.InstantTime)
		commitTimeMs := commitTime.UnixMilli()

		// A replacecommit supersedes whole file groups written by earlier instants. Apply the
		// replacements before this instant's own writes: the two sets are disjoint in Hudi (a
		// replacement always lands in a new file group), and dropping first keeps a group that an
		// instant both replaces and rewrites from being removed again.
		applyReplacedFileGroups(activeFiles, meta.PartitionToReplaceFileIds)

		for partitionPath, writeStats := range meta.PartitionToWriteStats {
			for _, ws := range writeStats {
				fullDataPath := io.JoinPath(s.basePath, ws.Path)
				var partValues []*model.PartitionValue
				for _, pf := range table.PartitioningFields {
					partValues = append(partValues, &model.PartitionValue{
						PartitionField: pf,
						Range:          model.NewScalarRange(partitionPath),
					})
				}

				activeFiles[ws.Path] = &activeFile{
					partition: partitionPath,
					fileGroup: ws.FileGroupID(),
					file: &model.DataFile{
						PhysicalPath:    fullDataPath,
						FileFormat:      model.FileFormatParquet,
						FileSizeBytes:   ws.FileSizeInBytes,
						RecordCount:     ws.NumWrites,
						PartitionValues: partValues,
						LastModified:    commitTimeMs,
					},
				}
			}
		}
	}

	dataFiles := make([]*model.DataFile, 0, len(activeFiles))
	for _, af := range activeFiles {
		dataFiles = append(dataFiles, af.file)
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        dataFiles,
		SourceIdentifier: latestCommit.InstantTime,
	}, nil
}

// GetTableChangeForCommit returns the diff of files in a commit.
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
		CurrentTable: snap.Table,
	}, nil
}

// IsIncrementalSyncSafeFrom checks if history is intact.
func (s *Source) IsIncrementalSyncSafeFrom(ctx context.Context, earliestInstant int64) (bool, error) {
	commits, err := s.ListCompletedCommits(ctx)
	if err != nil || len(commits) == 0 {
		return false, err
	}
	t, err := TimeFromInstant(commits[0].InstantTime)
	if err != nil {
		return false, err
	}
	return t.UnixMilli() <= earliestInstant, nil
}

// Close is a no-op.
func (s *Source) Close() error {
	return nil
}
