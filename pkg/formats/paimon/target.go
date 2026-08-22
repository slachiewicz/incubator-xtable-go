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
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Target implements spi.ConversionTarget for Apache Paimon tables.
//
// The layout it writes is the one Paimon itself reads: schema/schema-<id>, snapshot/snapshot-<id>
// with the LATEST and EARLIEST hints, and manifest/manifest-list-<uuid>-<n> pointing at
// manifest/manifest-<uuid>-<n>. The manifests are JSON rather than the Avro real Paimon writes, so
// a Paimon engine cannot open them yet; everything else is at the paths and under the names
// paimon-bundle 1.3.1 uses.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	basePath    string
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
	t.basePath = targetTable.BasePath
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves synchronization metadata from the latest snapshot's properties, which
// is where CommitSnapshot and CommitChanges embed it.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	if t.source == nil {
		return nil, nil
	}

	snap, err := t.source.latestSnapshot(ctx)
	if err != nil || snap == nil || len(snap.Properties) == 0 {
		return nil, nil
	}

	syncMeta := model.ReadSyncMetadataFromProperties(snap.Properties)
	if syncMeta == nil {
		return nil, nil
	}
	syncMeta.TargetFormat = model.TableFormatPaimon
	syncMeta.CustomProperties = snap.Properties
	return syncMeta, nil
}

// CommitSnapshot synchronizes a full Snapshot into the Paimon format without rewriting data files.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	if snapshot == nil || snapshot.Table == nil || snapshot.Table.ReadSchema == nil {
		return fmt.Errorf("invalid snapshot: missing table or schema")
	}

	schemaID, err := t.ensureSchema(ctx, snapshot.Table)
	if err != nil {
		return err
	}

	// A full snapshot replaces the table state, so the commit carries no base: the delta alone is
	// the whole file list.
	return t.commit(ctx, commitParams{
		schemaID:   schemaID,
		added:      snapshot.AllDataFiles(),
		commitKind: commitKindOverwrite,
		properties: syncProperties(snapshot.Table.LatestCommitTime, snapshot.Table.TableFormat, snapshot.SourceIdentifier),
	})
}

// CommitChanges synchronizes an incremental sequence of TableChange commits to Paimon format. Each
// change becomes one snapshot whose base is the previous state, so reading the newest snapshot back
// always yields the full file list.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	if changes == nil || changes.CurrentTable == nil {
		return fmt.Errorf("invalid incremental changes")
	}

	base, err := t.currentFiles(ctx)
	if err != nil {
		return err
	}

	var latestSyncInstant int64
	for _, change := range changes.TableChanges {
		table := change.TableAsOfChange
		if table == nil {
			table = changes.CurrentTable
		}

		schemaID, err := t.ensureSchema(ctx, table)
		if err != nil {
			return err
		}

		var added, removed []*model.DataFile
		if change.FilesDiff != nil {
			added = change.FilesDiff.FilesAdded
			removed = change.FilesDiff.FilesRemoved
		}

		if change.CommitTime > latestSyncInstant {
			latestSyncInstant = change.CommitTime
		}

		if err := t.commit(ctx, commitParams{
			schemaID:   schemaID,
			base:       base,
			added:      added,
			removed:    removed,
			commitKind: commitKindAppend,
			properties: syncProperties(latestSyncInstant, changes.CurrentTable.TableFormat, change.SourceIdentifier),
		}); err != nil {
			return err
		}

		base = applyDiff(t.basePath, base, added, removed)
	}

	return nil
}

// Close releases any resources.
func (t *Target) Close() error {
	return nil
}

// entriesFor converts a file list into manifest entries of one kind.
func (t *Target) entriesFor(files []*model.DataFile, kind int, schemaID int64) ([]ManifestEntry, error) {
	entries := make([]ManifestEntry, 0, len(files))
	for _, file := range files {
		entry, err := entryForDataFile(t.basePath, file, kind, schemaID)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// commitParams describes one Paimon commit.
type commitParams struct {
	schemaID   int64
	base       []*model.DataFile
	added      []*model.DataFile
	removed    []*model.DataFile
	commitKind string
	properties map[string]string
}

// commit writes the manifests, the manifest lists, the snapshot and the snapshot hints of a single
// commit.
func (t *Target) commit(ctx context.Context, params commitParams) error {
	baseEntries, err := t.entriesFor(params.base, manifestEntryKindAdd, params.schemaID)
	if err != nil {
		return err
	}

	deltaEntries, err := t.entriesFor(params.removed, manifestEntryKindDelete, params.schemaID)
	if err != nil {
		return err
	}
	addedEntries, err := t.entriesFor(params.added, manifestEntryKindAdd, params.schemaID)
	if err != nil {
		return err
	}
	deltaEntries = append(deltaEntries, addedEntries...)

	baseList, err := t.writeManifestList(ctx, baseEntries, params.schemaID)
	if err != nil {
		return err
	}
	deltaList, err := t.writeManifestList(ctx, deltaEntries, params.schemaID)
	if err != nil {
		return err
	}

	snapshotID, err := t.nextSnapshotID(ctx)
	if err != nil {
		return err
	}

	var totalRecords, deltaRecords int64
	for _, file := range params.base {
		totalRecords += file.RecordCount
	}
	for _, file := range params.added {
		totalRecords += file.RecordCount
		deltaRecords += file.RecordCount
	}
	for _, file := range params.removed {
		totalRecords -= file.RecordCount
		deltaRecords -= file.RecordCount
	}

	snap := Snapshot{
		Version:           snapshotFormatVersion,
		ID:                snapshotID,
		SchemaID:          params.schemaID,
		BaseManifestList:  baseList,
		DeltaManifestList: deltaList,
		CommitUser:        polytableCommitUser,
		CommitIdentifier:  snapshotID,
		CommitKind:        params.commitKind,
		TimeMillis:        time.Now().UnixMilli(),
		TotalRecordCount:  &totalRecords,
		DeltaRecordCount:  &deltaRecords,
		Properties:        params.properties,
	}

	snapData, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal paimon snapshot: %w", err)
	}

	dir := io.JoinPath(t.basePath, snapshotDir)
	snapPath := io.JoinPath(dir, fmt.Sprintf("%s%d", snapshotPrefix, snapshotID))
	if err := t.storage.Write(ctx, snapPath, snapData); err != nil {
		return fmt.Errorf("failed to write snapshot %s: %w", snapPath, err)
	}

	// The hints are what Paimon reads before listing the directory; EARLIEST is only written when
	// this is the first snapshot.
	if snapshotID == 1 {
		if err := t.writeHint(ctx, dir, earliestHintFile, snapshotID); err != nil {
			return err
		}
	}
	return t.writeHint(ctx, dir, latestHintFile, snapshotID)
}

func (t *Target) writeHint(ctx context.Context, dir, name string, id int64) error {
	hintPath := io.JoinPath(dir, name)
	if err := t.storage.Write(ctx, hintPath, []byte(strconv.FormatInt(id, 10))); err != nil {
		return fmt.Errorf("failed to write snapshot hint %s: %w", hintPath, err)
	}
	return nil
}

// writeManifestList writes the entries into one manifest and returns the name of the manifest list
// referencing it. An empty entry list still produces a list file, so that a snapshot always
// resolves.
func (t *Target) writeManifestList(ctx context.Context, entries []ManifestEntry, schemaID int64) (string, error) {
	dir := io.JoinPath(t.basePath, manifestDir)
	batch := uuid.New().String()

	var metas []ManifestFileMeta
	if len(entries) > 0 {
		data, err := json.MarshalIndent(ManifestFile{Entries: entries}, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal paimon manifest: %w", err)
		}

		name := fmt.Sprintf("%s%s-0", manifestPrefix, batch)
		if err := t.storage.Write(ctx, io.JoinPath(dir, name), data); err != nil {
			return "", fmt.Errorf("failed to write manifest %s: %w", name, err)
		}

		meta := ManifestFileMeta{
			FileName: name,
			FileSize: int64(len(data)),
			SchemaID: schemaID,
		}
		for _, entry := range entries {
			if entry.Kind == manifestEntryKindDelete {
				meta.NumDeletedFiles++
				continue
			}
			meta.NumAddedFiles++
		}
		metas = append(metas, meta)
	}

	listData, err := json.MarshalIndent(ManifestList{Manifests: metas}, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal paimon manifest list: %w", err)
	}

	listName := fmt.Sprintf("%s%s-0", manifestListPrefix, batch)
	if err := t.storage.Write(ctx, io.JoinPath(dir, listName), listData); err != nil {
		return "", fmt.Errorf("failed to write manifest list %s: %w", listName, err)
	}
	return listName, nil
}

// ensureSchema writes schema/schema-<id> when the table's schema is not the one already recorded,
// and returns the id the commit should reference. Paimon schema files are immutable, so an
// unchanged schema reuses its id instead of being rewritten.
func (t *Target) ensureSchema(ctx context.Context, table *model.Table) (int64, error) {
	if table == nil || table.ReadSchema == nil {
		return 0, fmt.Errorf("invalid table: missing schema")
	}

	var partitionKeys []string
	for _, pf := range table.PartitioningFields {
		if pf != nil && pf.SourceField != nil {
			partitionKeys = append(partitionKeys, pf.SourceField.Name)
		}
	}

	ts, err := SchemaToPaimon(table.ReadSchema, partitionKeys)
	if err != nil {
		return 0, fmt.Errorf("failed to convert schema to paimon: %w", err)
	}

	dir := io.JoinPath(t.basePath, schemaDir)
	existing, err := t.source.listVersioned(ctx, dir, schemaPrefix)
	if err != nil {
		return 0, fmt.Errorf("failed to list paimon schema directory: %w", err)
	}

	nextID := int64(0)
	if len(existing) > 0 {
		latest := existing[len(existing)-1]
		nextID = latest.id + 1

		previous, err := t.source.readSchema(ctx, latest.id)
		if err != nil {
			return 0, err
		}
		candidate := *ts
		candidate.ID = previous.ID
		if reflect.DeepEqual(&candidate, previous) {
			return previous.ID, nil
		}
	}

	ts.ID = nextID
	data, err := json.MarshalIndent(ts, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("failed to marshal paimon schema: %w", err)
	}

	schemaPath := io.JoinPath(dir, fmt.Sprintf("%s%d", schemaPrefix, nextID))
	if err := t.storage.Write(ctx, schemaPath, data); err != nil {
		return 0, fmt.Errorf("failed to write schema file %s: %w", schemaPath, err)
	}
	return nextID, nil
}

// nextSnapshotID returns the id of the snapshot about to be written; Paimon numbers them from 1.
func (t *Target) nextSnapshotID(ctx context.Context) (int64, error) {
	snapshots, err := t.source.listVersioned(ctx, io.JoinPath(t.basePath, snapshotDir), snapshotPrefix)
	if err != nil {
		return 0, fmt.Errorf("failed to list paimon snapshot directory: %w", err)
	}
	if len(snapshots) == 0 {
		return 1, nil
	}
	return snapshots[len(snapshots)-1].id + 1, nil
}

// currentFiles returns the file list of the latest snapshot, or nil when the table is empty.
func (t *Target) currentFiles(ctx context.Context) ([]*model.DataFile, error) {
	snap, err := t.source.latestSnapshot(ctx)
	if err != nil || snap == nil {
		return nil, err
	}

	ts, err := t.source.readSchema(ctx, snap.SchemaID)
	if err != nil {
		return nil, err
	}
	table, err := t.source.tableForSchema(ts)
	if err != nil {
		return nil, err
	}
	return t.source.liveFiles(ctx, snap, table)
}

// applyDiff folds one commit's additions and removals into a file list, keyed the way the manifest
// keys entries: by the path relative to the table base.
func applyDiff(basePath string, base, added, removed []*model.DataFile) []*model.DataFile {
	dropped := make(map[string]struct{}, len(removed))
	for _, file := range removed {
		dropped[fileKey(basePath, file)] = struct{}{}
	}

	result := make([]*model.DataFile, 0, len(base)+len(added))
	seen := make(map[string]struct{}, len(base)+len(added))
	for _, file := range base {
		name := fileKey(basePath, file)
		if _, gone := dropped[name]; gone {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, file)
	}

	// A file rewritten in place is reported as both removed and added, so the additions are folded
	// in after the removals rather than filtered by them.
	for _, file := range added {
		name := fileKey(basePath, file)
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, file)
	}
	return result
}

// fileKey identifies a data file the way a manifest entry does, by its path relative to the table.
// A file outside the table has no such path; its physical path is the key instead, which keeps this
// comparison consistent without deciding anything — writing that file still fails in
// entryForDataFile, so the fallback cannot hide a broken manifest.
func fileKey(basePath string, file *model.DataFile) string {
	if name, err := io.RelativizePath(file.PhysicalPath, basePath); err == nil {
		return name
	}
	return file.PhysicalPath
}

// syncProperties builds the sync metadata a commit embeds in its snapshot properties.
func syncProperties(lastInstant int64, sourceFormat model.TableFormat, sourceIdentifier string) map[string]string {
	props := make(map[string]string)
	model.WriteSyncMetadataProperties(props, &model.TableSyncMetadata{
		LastInstantSynced: lastInstant,
		SourceFormat:      sourceFormat,
		SourceIdentifier:  sourceIdentifier,
	})
	return props
}
