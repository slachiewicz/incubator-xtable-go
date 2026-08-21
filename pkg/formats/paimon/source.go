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

// versionedFile is a metadata file whose name ends in a numeric version, as Paimon's
// SnapshotManager and SchemaManager both name their files.
type versionedFile struct {
	id   int64
	path string
}

// listVersioned returns the files under dir whose base name starts with prefix, ordered by the
// numeric suffix. Ordering numerically rather than lexicographically matters from ten files on:
// "snapshot-10" sorts before "snapshot-9" as a string.
func (s *Source) listVersioned(ctx context.Context, dir, prefix string) ([]versionedFile, error) {
	files, err := s.storage.List(ctx, dir)
	if err != nil {
		return nil, err
	}

	var versioned []versionedFile
	for _, f := range files {
		if f.IsDir {
			continue
		}
		base := path.Base(f.Path)
		if !strings.HasPrefix(base, prefix) {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(base, prefix), 10, 64)
		if err != nil {
			continue
		}
		versioned = append(versioned, versionedFile{id: id, path: f.Path})
	}

	sort.Slice(versioned, func(i, j int) bool { return versioned[i].id < versioned[j].id })
	return versioned, nil
}

// readHint reads a Paimon hint file (snapshot/LATEST, snapshot/EARLIEST), which holds a bare
// snapshot id. A missing or unreadable hint is not an error: it is an optimization Paimon can
// always rebuild by listing the directory.
func (s *Source) readHint(ctx context.Context, dir, name string) (int64, bool) {
	data, err := s.storage.Read(ctx, io.JoinPath(dir, name))
	if err != nil {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// readSchema reads schema/schema-<id>, or the highest-numbered schema when id is negative.
func (s *Source) readSchema(ctx context.Context, id int64) (*TableSchema, error) {
	dir := io.JoinPath(s.basePath, schemaDir)

	schemaPath := io.JoinPath(dir, fmt.Sprintf("%s%d", schemaPrefix, id))
	if id < 0 {
		schemas, err := s.listVersioned(ctx, dir, schemaPrefix)
		if err != nil {
			return nil, fmt.Errorf("failed to list paimon schema directory: %w", err)
		}
		if len(schemas) == 0 {
			return nil, fmt.Errorf("no paimon schema files found in %s", dir)
		}
		schemaPath = schemas[len(schemas)-1].path
	}

	data, err := s.storage.Read(ctx, schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file %s: %w", schemaPath, err)
	}

	ts, err := ParseTableSchemaJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema %s: %w", schemaPath, err)
	}
	return ts, nil
}

// tableForSchema builds the canonical table descriptor from a Paimon schema.
func (s *Source) tableForSchema(ts *TableSchema) (*model.Table, error) {
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

// GetCurrentTable returns the Table descriptor of the Paimon table.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	ts, err := s.readSchema(ctx, -1)
	if err != nil {
		return nil, err
	}
	return s.tableForSchema(ts)
}

// GetTable returns the Table descriptor at a specific commit instant / snapshot.
func (s *Source) GetTable(ctx context.Context, _ string) (*model.Table, error) {
	return s.GetCurrentTable(ctx)
}

// latestSnapshot reads the newest snapshot file, preferring the snapshot/LATEST hint and falling
// back to the highest-numbered snapshot file. It returns nil when the table has no snapshot yet.
func (s *Source) latestSnapshot(ctx context.Context) (*Snapshot, error) {
	dir := io.JoinPath(s.basePath, snapshotDir)

	if id, ok := s.readHint(ctx, dir, latestHintFile); ok {
		if data, err := s.storage.Read(ctx, io.JoinPath(dir, fmt.Sprintf("%s%d", snapshotPrefix, id))); err == nil {
			return ParseSnapshotJSON(data)
		}
	}

	snapshots, err := s.listVersioned(ctx, dir, snapshotPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list paimon snapshot directory: %w", err)
	}
	if len(snapshots) == 0 {
		return nil, nil
	}

	latest := snapshots[len(snapshots)-1].path
	data, err := s.storage.Read(ctx, latest)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot %s: %w", latest, err)
	}
	snap, err := ParseSnapshotJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot %s: %w", latest, err)
	}
	return snap, nil
}

// readManifestList resolves a manifest list by name and returns every entry its manifests hold, in
// manifest order.
func (s *Source) readManifestList(ctx context.Context, name string) ([]ManifestEntry, error) {
	if name == "" {
		return nil, nil
	}

	dir := io.JoinPath(s.basePath, manifestDir)
	data, err := s.storage.Read(ctx, io.JoinPath(dir, name))
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list %s: %w", name, err)
	}

	list, err := ParseManifestListJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest list %s: %w", name, err)
	}

	var entries []ManifestEntry
	for _, meta := range list.Manifests {
		manifestData, err := s.storage.Read(ctx, io.JoinPath(dir, meta.FileName))
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest %s: %w", meta.FileName, err)
		}
		manifest, err := ParseManifestFileJSON(manifestData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse manifest %s: %w", meta.FileName, err)
		}
		entries = append(entries, manifest.Entries...)
	}
	return entries, nil
}

// liveFiles replays the base and delta manifests of a snapshot into the set of files it holds,
// the way Paimon's FileStoreScan does: the base is the previous state and the delta adds to and
// deletes from it.
func (s *Source) liveFiles(ctx context.Context, snap *Snapshot, table *model.Table) ([]*model.DataFile, error) {
	base, err := s.readManifestList(ctx, snap.BaseManifestList)
	if err != nil {
		return nil, err
	}
	delta, err := s.readManifestList(ctx, snap.DeltaManifestList)
	if err != nil {
		return nil, err
	}

	partitionFields := make(map[string]*model.PartitionField, len(table.PartitioningFields))
	for _, pf := range table.PartitioningFields {
		if pf != nil && pf.SourceField != nil {
			partitionFields[pf.SourceField.Name] = pf
		}
	}

	order := make([]string, 0, len(base)+len(delta))
	live := make(map[string]*model.DataFile, len(base)+len(delta))
	for _, entry := range append(base, delta...) {
		name := entry.File.FileName
		switch entry.Kind {
		case manifestEntryKindDelete:
			delete(live, name)
		default:
			if _, seen := live[name]; !seen {
				order = append(order, name)
			}
			live[name] = dataFileForEntry(s.basePath, entry, partitionFields)
		}
	}

	files := make([]*model.DataFile, 0, len(live))
	for _, name := range order {
		if file, ok := live[name]; ok {
			files = append(files, file)
		}
	}
	return files, nil
}

// GetCurrentSnapshot returns the active Snapshot state.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	snap, err := s.latestSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	if snap == nil {
		table, err := s.GetCurrentTable(ctx)
		if err != nil {
			return nil, err
		}
		return &model.Snapshot{
			Table:            table,
			DataFiles:        nil,
			SourceIdentifier: "0",
		}, nil
	}

	ts, err := s.readSchema(ctx, snap.SchemaID)
	if err != nil {
		return nil, err
	}
	table, err := s.tableForSchema(ts)
	if err != nil {
		return nil, err
	}
	table.LatestCommitTime = snap.TimeMillis

	files, err := s.liveFiles(ctx, snap, table)
	if err != nil {
		return nil, err
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        files,
		SourceIdentifier: strconv.FormatInt(snap.ID, 10),
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
