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

package paimon

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Source implements spi.ConversionSource for Apache Paimon tables.
type Source struct {
	storage  io.Storage
	basePath string
}

var _ spi.ConversionSource = (*Source)(nil)

// NewSource creates a new Apache Paimon ConversionSource.
func NewSource(storage io.Storage, basePath string) *Source {
	return &Source{
		storage:  storage,
		basePath: basePath,
	}
}

// Format returns TableFormatPaimon.
func (s *Source) Format() model.TableFormat {
	return model.TableFormatPaimon
}

// GetCurrentTable returns the Table descriptor of the Paimon table.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	schemaDir := io.JoinPath(s.basePath, "schema")
	files, err := s.storage.List(ctx, schemaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list paimon schema directory: %w", err)
	}

	var schemaFiles []string
	for _, f := range files {
		base := path.Base(f.Path)
		if strings.HasPrefix(base, "schema-") {
			schemaFiles = append(schemaFiles, f.Path)
		}
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no paimon schema files found in %s", schemaDir)
	}

	sort.Strings(schemaFiles)
	latestSchemaPath := schemaFiles[len(schemaFiles)-1]

	data, err := s.storage.Read(ctx, latestSchemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file %s: %w", latestSchemaPath, err)
	}

	ts, err := ParseTableSchemaJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema %s: %w", latestSchemaPath, err)
	}

	schema, err := PaimonToSchema(ts)
	if err != nil {
		return nil, err
	}

	var partFields []*model.PartitionField
	for _, pk := range ts.PartitionKeys {
		field := schema.FieldByPath(pk)
		if field != nil {
			partFields = append(partFields, &model.PartitionField{
				SourceField:   field,
				TransformType: model.PartitionTransformValue,
			})
		}
	}

	return &model.Table{
		Name:               path.Base(s.basePath),
		TableFormat:        model.TableFormatPaimon,
		ReadSchema:         schema,
		BasePath:           s.basePath,
		PartitioningFields: partFields,
	}, nil
}

// GetTable returns the Table descriptor at a specific commit instant / snapshot.
func (s *Source) GetTable(ctx context.Context, _ string) (*model.Table, error) {
	return s.GetCurrentTable(ctx)
}

// GetCurrentSnapshot returns the active Snapshot state.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	table, err := s.GetCurrentTable(ctx)
	if err != nil {
		return nil, err
	}

	snapDir := io.JoinPath(s.basePath, "snapshot")
	files, err := s.storage.List(ctx, snapDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list paimon snapshot directory: %w", err)
	}

	var snapshotFiles []string
	for _, f := range files {
		base := path.Base(f.Path)
		if strings.HasPrefix(base, "snapshot-") {
			snapshotFiles = append(snapshotFiles, f.Path)
		}
	}

	if len(snapshotFiles) == 0 {
		return &model.Snapshot{
			Table:            table,
			DataFiles:        nil,
			SourceIdentifier: "0",
		}, nil
	}

	sort.Strings(snapshotFiles)
	latestSnapPath := snapshotFiles[len(snapshotFiles)-1]

	data, err := s.storage.Read(ctx, latestSnapPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot %s: %w", latestSnapPath, err)
	}

	pSnap, err := ParseSnapshotJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot %s: %w", latestSnapPath, err)
	}

	table.LatestCommitTime = pSnap.TimeMillis

	return &model.Snapshot{
		Table:            table,
		DataFiles:        nil,
		SourceIdentifier: strconv.FormatInt(pSnap.ID, 10),
	}, nil
}

// GetSnapshot returns the Snapshot at a specific snapshot ID.
func (s *Source) GetSnapshot(ctx context.Context, _ string) (*model.Snapshot, error) {
	return s.GetCurrentSnapshot(ctx)
}

// GetTableChangeForCommit returns the diff and schema changes for a specific commit version.
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

// GetChangesSince returns table changes since a given timestamp.
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

// IsIncrementalSyncSafeFrom returns false for full sync default.
func (s *Source) IsIncrementalSyncSafeFrom(_ context.Context, _ int64) (bool, error) {
	return false, nil
}

// Close releases any allocated resources.
func (s *Source) Close() error {
	return nil
}
