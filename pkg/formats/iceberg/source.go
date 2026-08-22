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

// MetadataFileVersion extracts the version number from an Iceberg metadata file name, reporting
// false for a name that is not one.
//
// Two conventions are in use and both must be read. Polytable's own target writes `v<N>.metadata.json`,
// the form the Hadoop table layout uses. Every catalog-backed writer — the Java library, pyiceberg,
// Spark — writes `<%05d version>-<uuid>.metadata.json`, keeping the version zero-padded and the UUID
// to make the name unique without a catalog round trip. Matching only the first form made every table
// written by a real engine look like it had no metadata at all.
func MetadataFileVersion(fileName string) (int, bool) {
	stem, ok := strings.CutSuffix(fileName, ".metadata.json")
	if !ok || stem == "" {
		return 0, false
	}

	if digits, ok := strings.CutPrefix(stem, "v"); ok {
		if v, err := strconv.Atoi(digits); err == nil && v >= 0 {
			return v, true
		}
		return 0, false
	}

	digits, _, _ := strings.Cut(stem, "-")
	if v, err := strconv.Atoi(digits); err == nil && v >= 0 {
		return v, true
	}
	return 0, false
}

// listMetadataFiles finds all metadata.json files and maps their version numbers to their paths.
// The path is carried rather than rebuilt from the version because the catalog-backed naming
// convention embeds a UUID that cannot be reconstructed.
func (s *Source) listMetadataFiles(ctx context.Context) ([]int, map[int]string, error) {
	metaDir := io.JoinPath(s.basePath, "metadata")
	files, err := s.storage.List(ctx, metaDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list iceberg metadata directory %s: %w", metaDir, err)
	}

	var versions []int
	paths := make(map[int]string)
	for _, f := range files {
		v, ok := MetadataFileVersion(filepath.Base(f.Path))
		if !ok {
			continue
		}
		// A rewritten metadata file keeps its version and gets a new UUID, so the same version can
		// appear twice; the lexically last name is the newer one.
		if existing, seen := paths[v]; seen {
			if f.Path <= existing {
				continue
			}
		} else {
			versions = append(versions, v)
		}
		paths[v] = f.Path
	}
	sort.Ints(versions)
	return versions, paths, nil
}

// readMetadata reads and parses the Iceberg TableMetadata at the given file path.
func (s *Source) readMetadata(ctx context.Context, filePath string) (*TableMetadata, error) {
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
	versions, _, err := s.listMetadataFiles(ctx)
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

	_, paths, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	path, ok := paths[ver]
	if !ok {
		return nil, fmt.Errorf("no iceberg metadata file for version %d in %s", ver, s.basePath)
	}

	meta, err := s.readMetadata(ctx, path)
	if err != nil {
		return nil, err
	}

	readSchema, partitionFields, err := resolveSchemaAndPartitions(meta, meta.CurrentSchemaID)
	if err != nil {
		return nil, fmt.Errorf("iceberg metadata v%d: %w", ver, err)
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

// resolveSchemaAndPartitions reads the schema identified by schemaID out of meta (falling back to
// the first schema present if the id is not found, which is the same tolerance GetTable has always
// had for a metadata file whose current-schema-id does not match any entry) and resolves the
// table's default partition spec against it.
//
// This is shared between GetTable, which resolves against the metadata file's own
// current-schema-id, and tableAsOfSnapshot, which resolves against the schema-id a specific
// snapshot recorded — the same schema can be active across many snapshots, and a snapshot taken
// before any schema evolution has no schema-id of its own to fall back on.
func resolveSchemaAndPartitions(meta *TableMetadata, schemaID int) (*model.Schema, []*model.PartitionField, error) {
	var activeSchema *TableSchema
	for _, sc := range meta.Schemas {
		if sc.SchemaID == schemaID {
			activeSchema = sc
			break
		}
	}
	if activeSchema == nil && len(meta.Schemas) > 0 {
		activeSchema = meta.Schemas[0]
	}
	if activeSchema == nil {
		return nil, nil, fmt.Errorf("no schema found")
	}

	readSchema, err := IcebergToSchema(activeSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert iceberg schema: %w", err)
	}

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
	return readSchema, partitionFields, nil
}

// tableAsOfSnapshot builds the Table descriptor as it read at the time snap committed: the schema
// is the one snap.SchemaID names, not the metadata file's current one, so a change reported for an
// old snapshot carries the schema that was active when it was made rather than the latest.
func (s *Source) tableAsOfSnapshot(meta *TableMetadata, snap *TableSnapshot) (*model.Table, error) {
	schemaID := meta.CurrentSchemaID
	if snap.SchemaID != nil {
		schemaID = *snap.SchemaID
	}
	readSchema, partitionFields, err := resolveSchemaAndPartitions(meta, schemaID)
	if err != nil {
		return nil, fmt.Errorf("iceberg snapshot %d: %w", snap.SnapshotID, err)
	}
	return &model.Table{
		Name:               filepath.Base(s.basePath),
		TableFormat:        model.TableFormatIceberg,
		ReadSchema:         readSchema,
		BasePath:           s.basePath,
		PartitioningFields: partitionFields,
		LatestCommitTime:   snap.TimestampMs,
	}, nil
}

// snapshotsByID indexes a metadata file's Snapshots by id, for parent-link lookups during a history
// walk. Snapshots, not the snapshot-log, is what the walk resolves against: see
// IsIncrementalSyncSafeFrom for why the two are not interchangeable.
func snapshotsByID(meta *TableMetadata) map[int64]*TableSnapshot {
	byID := make(map[int64]*TableSnapshot, len(meta.Snapshots))
	for _, snap := range meta.Snapshots {
		byID[snap.SnapshotID] = snap
	}
	return byID
}

// GetCurrentSnapshot constructs the complete Snapshot from Iceberg manifests.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	versions, paths, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no iceberg metadata files found in %s", s.basePath)
	}
	latestVer := versions[len(versions)-1]
	meta, err := s.readMetadata(ctx, paths[latestVer])
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

	dataFiles, err := s.dataFilesForSnapshot(ctx, currSnapshot, table)
	if err != nil {
		return nil, err
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        dataFiles,
		SourceIdentifier: strconv.FormatInt(currSnapshot.SnapshotID, 10),
	}, nil
}

// dataFilesForSnapshot reads the complete, live data file set as of snap: every manifest its
// manifest list carries, with deleted and existing-but-superseded entries filtered by each
// manifest's own per-entry Status. This is the full file set at that point in the table's history,
// not merely what snap's own commit changed — which is exactly what a caller diffing two snapshots
// against each other (changeForSnapshot) needs from each side of the comparison.
func (s *Source) dataFilesForSnapshot(ctx context.Context, snap *TableSnapshot, table *model.Table) ([]*model.DataFile, error) {
	manifestListData, err := s.storage.Read(ctx, snap.ManifestList)
	if err != nil {
		return nil, fmt.Errorf("failed to read iceberg manifest list %s: %w", snap.ManifestList, err)
	}

	manifestList, err := readManifestList(manifestListData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse iceberg manifest list %s: %w", snap.ManifestList, err)
	}

	var dataFiles []*model.DataFile
	for _, mle := range manifestList {
		// A manifest of delete files describes rows removed from data files, not files of its own.
		// Reading its entries as data files would invent files that do not exist.
		if mle.Content != contentData {
			continue
		}
		manifestData, err := s.storage.Read(ctx, mle.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest %s: %w", mle.ManifestPath, err)
		}
		entries, err := readManifest(manifestData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse manifest %s: %w", mle.ManifestPath, err)
		}
		for _, e := range entries {
			if e.Status == manifestStatusDeleted || e.DataFile == nil {
				continue
			}
			if e.DataFile.Content != contentData {
				continue
			}
			dataFiles = append(dataFiles, s.convertManifestDataFile(e.DataFile, table))
		}
	}
	return dataFiles, nil
}

// changeForSnapshot builds the TableChange for snap by diffing its full live file set (see
// dataFilesForSnapshot) against its parent's, via model.DiffFiles. byID is the current metadata
// file's Snapshots indexed by id; callers that already have it (the history walk in
// GetChangesSince) do not pay to rebuild it once per snapshot.
//
// Cost note: this reads snap's own manifest list and manifests, and — unless snap has no parent —
// the parent's manifest list and manifests too, even for manifests both snapshots share unchanged.
// Reading only the manifests snap's own commit newly wrote (identified by
// ManifestListEntry.AddedSnapshotID) would usually be cheaper, but model.DiffFiles is the
// differ this was asked to use, and it operates on two complete file sets rather than a
// pre-computed delta. Walking N pending snapshots this way costs O(N x manifests-per-snapshot)
// reads rather than the one-time cost GetCurrentSnapshot pays for a full sync; see the T40 report
// for the concrete tradeoff.
func (s *Source) changeForSnapshot(
	ctx context.Context,
	meta *TableMetadata,
	byID map[int64]*TableSnapshot,
	snap *TableSnapshot,
) (*model.TableChange, error) {
	table, err := s.tableAsOfSnapshot(meta, snap)
	if err != nil {
		return nil, err
	}
	newFiles, err := s.dataFilesForSnapshot(ctx, snap, table)
	if err != nil {
		return nil, err
	}

	var oldFiles []*model.DataFile
	if snap.ParentSnapshotID != nil {
		parent, ok := byID[*snap.ParentSnapshotID]
		if !ok {
			return nil, fmt.Errorf(
				"iceberg snapshot %d's parent %d has been expired out of table metadata; "+
					"its diff cannot be computed and a full snapshot sync is required to recover",
				snap.SnapshotID, *snap.ParentSnapshotID)
		}
		parentTable, err := s.tableAsOfSnapshot(meta, parent)
		if err != nil {
			return nil, err
		}
		oldFiles, err = s.dataFilesForSnapshot(ctx, parent, parentTable)
		if err != nil {
			return nil, err
		}
	}

	return &model.TableChange{
		FilesDiff:        model.DiffFiles(oldFiles, newFiles),
		TableAsOfChange:  table,
		SourceIdentifier: strconv.FormatInt(snap.SnapshotID, 10),
		CommitTime:       snap.TimestampMs,
	}, nil
}

// GetTableChangeForCommit returns the diff of added and removed files for the snapshot commitID
// names, computed against its parent snapshot — not, as before, always the current snapshot
// diffed against nothing.
func (s *Source) GetTableChangeForCommit(ctx context.Context, commitID string) (*model.TableChange, error) {
	snapID, err := strconv.ParseInt(commitID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid iceberg snapshot id %s: %w", commitID, err)
	}

	versions, paths, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no iceberg metadata files found in %s", s.basePath)
	}
	meta, err := s.readMetadata(ctx, paths[versions[len(versions)-1]])
	if err != nil {
		return nil, err
	}

	byID := snapshotsByID(meta)
	snap, ok := byID[snapID]
	if !ok {
		return nil, fmt.Errorf("iceberg snapshot %d not found in table metadata of %s", snapID, s.basePath)
	}

	return s.changeForSnapshot(ctx, meta, byID, snap)
}

// advanceCommitTime folds a snapshot's raw timestamp-ms into a strictly increasing instant, given
// the instant already derived for the snapshot immediately before it in commit order.
//
// Two Iceberg snapshots can carry the same timestamp-ms — nothing in the specification rules out a
// writer committing twice inside one millisecond — and GetChangesSince persists the last emitted
// change's CommitTime as the next sync's fromInstant. Comparing raw timestamps against that
// persisted value would then read a tied pair as one commit and silently drop the second on the
// following incremental sync. Deriving the instant from commit order instead keeps it a strictly
// increasing, injective proxy for position in the chain, which is monotonic by construction. This
// mirrors pkg/formats/delta/source.go's advanceCommitTime, which fixes the identical hazard on
// Delta's commitInfo timestamps.
func advanceCommitTime(previous, raw int64) int64 {
	if raw > previous {
		return raw
	}
	return previous + 1
}

// GetChangesSince returns one TableChange per snapshot committed after fromInstant, walking parent
// links from the table's current snapshot back through its retained history and replaying the
// result oldest-first.
//
// The walk always starts at the current snapshot and always runs to the oldest snapshot the
// metadata file still retains, even when that reaches back further than fromInstant: computing
// every snapshot's derived instant in a fixed, deterministic order — the same order every call
// makes, regardless of fromInstant — is what lets advanceCommitTime's timestamp-tie handling agree
// with itself release over release. Only afterward is the result filtered down to the changes
// whose derived instant is actually newer than fromInstant. This is more work than filtering first
// and walking only the tail, but it is the same tradeoff pkg/formats/delta/source.go's
// GetChangesSince already makes for the identical reason.
//
// If the walk reaches a snapshot whose parent has been expired out of the metadata's Snapshots
// list — the parent id is set but absent from it — before it has walked back as far as fromInstant,
// the requested range is not fully covered by retained history and this returns an error rather
// than the partial backlog it could otherwise silently build from what remains. A caller is
// expected to have checked IsIncrementalSyncSafeFrom first, which reports exactly this condition
// unsafe; reaching it here means that check was skipped or the table changed between the two calls.
func (s *Source) GetChangesSince(ctx context.Context, fromInstant int64) (*model.IncrementalTableChanges, error) {
	versions, paths, err := s.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("no iceberg metadata files found in %s", s.basePath)
	}
	latestVer := versions[len(versions)-1]
	meta, err := s.readMetadata(ctx, paths[latestVer])
	if err != nil {
		return nil, err
	}

	currentTable, err := s.GetTable(ctx, strconv.Itoa(latestVer))
	if err != nil {
		return nil, err
	}

	if meta.CurrentSnapshotID == nil {
		// A table that has never had a commit has no history to walk.
		return &model.IncrementalTableChanges{CurrentTable: currentTable}, nil
	}

	byID := snapshotsByID(meta)
	current, ok := byID[*meta.CurrentSnapshotID]
	if !ok {
		return nil, fmt.Errorf("iceberg current snapshot %d is not present in table metadata of %s", *meta.CurrentSnapshotID, s.basePath)
	}

	// Walk backward from current, newest first, stopping at the table's genesis (no parent) or at
	// the oldest ancestor the metadata still retains (a non-nil parent id absent from byID).
	newestFirst := []*TableSnapshot{current}
	cursor := current
	for cursor.ParentSnapshotID != nil {
		parent, ok := byID[*cursor.ParentSnapshotID]
		if !ok {
			break
		}
		newestFirst = append(newestFirst, parent)
		cursor = parent
	}
	oldest := newestFirst[len(newestFirst)-1]

	if oldest.ParentSnapshotID != nil {
		if _, retained := byID[*oldest.ParentSnapshotID]; !retained && oldest.TimestampMs > fromInstant {
			return nil, fmt.Errorf(
				"iceberg snapshot %d's parent %d has been expired out of table metadata of %s, and "+
					"%d (the oldest snapshot still retained) was committed after fromInstant=%d: "+
					"the requested incremental range is not fully covered by retained history; "+
					"a full snapshot sync is required to recover",
				oldest.SnapshotID, *oldest.ParentSnapshotID, s.basePath, oldest.SnapshotID, fromInstant)
		}
	}

	var changes []*model.TableChange
	var lastInstant int64
	for i := len(newestFirst) - 1; i >= 0; i-- {
		snap := newestFirst[i]
		lastInstant = advanceCommitTime(lastInstant, snap.TimestampMs)
		if lastInstant <= fromInstant {
			continue
		}
		change, err := s.changeForSnapshot(ctx, meta, byID, snap)
		if err != nil {
			return nil, err
		}
		change.CommitTime = lastInstant
		changes = append(changes, change)
	}

	return &model.IncrementalTableChanges{
		TableChanges: changes,
		CurrentTable: currentTable,
	}, nil
}

// IsIncrementalSyncSafeFrom reports whether the table's retained snapshot history reaches back to
// earliestInstant, so that GetChangesSince can walk parent links all the way there without meeting
// an expired snapshot.
//
// This deliberately reads the latest metadata file's Snapshots array, not the oldest surviving
// metadata file's LastUpdatedMs (the previous check) and not snapshot-log (see SnapshotLogEntry).
// Metadata-file retention and snapshot expiration are independent cleanup policies in Iceberg: a
// catalog can keep every old metadata.json indefinitely while a background expireSnapshots run
// prunes Snapshots on the very next commit, and the previous check reported that state safe.
// snapshot-log is not a substitute either: the specification does not require an implementation to
// drop a snapshot-log entry when the snapshot it names expires, so its oldest entry can still name
// a snapshot Snapshots no longer carries.
func (s *Source) IsIncrementalSyncSafeFrom(ctx context.Context, earliestInstant int64) (bool, error) {
	versions, paths, err := s.listMetadataFiles(ctx)
	if err != nil || len(versions) == 0 {
		return false, err
	}
	meta, err := s.readMetadata(ctx, paths[versions[len(versions)-1]])
	if err != nil {
		return false, err
	}
	if len(meta.Snapshots) == 0 {
		return false, nil
	}
	oldest := meta.Snapshots[0].TimestampMs
	for _, snap := range meta.Snapshots[1:] {
		if snap.TimestampMs < oldest {
			oldest = snap.TimestampMs
		}
	}
	return oldest <= earliestInstant, nil
}

// Close is a no-op for Iceberg source.
func (s *Source) Close() error {
	return nil
}

func (s *Source) convertManifestDataFile(mdf *ManifestDataFile, table *model.Table) *model.DataFile {
	dataFile := &model.DataFile{
		PhysicalPath:  mdf.FilePath,
		FileFormat:    modelFileFormat(mdf.FileFormat),
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
