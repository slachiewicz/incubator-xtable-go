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

package parquet

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Source implements spi.ConversionSource for crawling unmanaged Parquet directory datasets.
type Source struct {
	storage  io.Storage
	basePath string
}

var _ spi.ConversionSource = (*Source)(nil)

// NewSource creates a new Parquet ConversionSource.
func NewSource(storage io.Storage, basePath string) *Source {
	return &Source{
		storage:  storage,
		basePath: basePath,
	}
}

// Format returns the table format.
func (s *Source) Format() model.TableFormat {
	return model.TableFormatParquet
}

// GetCurrentTable returns table metadata discovered from Parquet files.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	snap, err := s.GetCurrentSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.Table, nil
}

// GetTable returns table metadata.
func (s *Source) GetTable(ctx context.Context, _ string) (*model.Table, error) {
	return s.GetCurrentTable(ctx)
}

// crawledFile is everything one pass over a data file recovered from it. The file's bytes are not
// kept: the directory can be large, and nothing below needs them a second time.
type crawledFile struct {
	info       io.FileInfo
	numRows    int64
	aggregates []*columnAggregate
	partitions []HivePartition
}

// GetCurrentSnapshot crawls the directory tree, inspects Parquet footers, and builds a complete Snapshot.
//
// Every footer is read exactly once and the schema is the merge of all of them, so the columns the
// table reports do not depend on which file the listing returned first. The Hive partition columns
// are then added to that schema: they live in directory names, and a table partitioned by a column
// its own schema does not define is one no engine can read back.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	parquetFiles, err := s.listDataFiles(ctx)
	if err != nil {
		return nil, err
	}

	crawled := make([]*crawledFile, 0, len(parquetFiles))
	footers := make([]FooterSchema, 0, len(parquetFiles))
	var latestModTime int64

	for _, pf := range parquetFiles {
		fileData, err := s.storage.Read(ctx, pf.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", pf.Path, err)
		}

		pfObj, err := parquet.OpenFile(bytes.NewReader(fileData), int64(len(fileData)))
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", pf.Path, err)
		}

		if modTimeMs := pf.ModTime.UnixMilli(); modTimeMs > latestModTime {
			latestModTime = modTimeMs
		}

		crawled = append(crawled, &crawledFile{
			info:       pf,
			numRows:    pfObj.NumRows(),
			aggregates: footerAggregates(pfObj),
			partitions: HivePartitionsForFile(pf.Path, s.basePath),
		})
		footers = append(footers, FooterSchema{
			Path:    pf.Path,
			ModTime: pf.ModTime,
			Schema:  ParquetSchemaToModel(pfObj.Schema()),
		})
	}

	tableSchema, err := MergeFooterSchemas(footers)
	if err != nil {
		return nil, fmt.Errorf("failed to merge the parquet schemas under %s: %w", s.basePath, err)
	}

	observed := observedPartitions(crawled)
	tableSchema, partitioningFields, err := partitionSpec(tableSchema, observed)
	if err != nil {
		return nil, fmt.Errorf("failed to read the partition layout of %s: %w", s.basePath, err)
	}

	// Keyed by the directory name rather than by the column's, which a case-insensitive match may
	// have spelled differently.
	fieldsByKey := make(map[string]*model.PartitionField, len(partitioningFields))
	for i, field := range partitioningFields {
		fieldsByKey[observed[i].key] = field
	}

	dataFiles := make([]*model.DataFile, 0, len(crawled))
	for _, file := range crawled {
		dataFiles = append(dataFiles, &model.DataFile{
			PhysicalPath:    file.info.Path,
			FileFormat:      model.FileFormatParquet,
			FileSizeBytes:   file.info.Size,
			RecordCount:     file.numRows,
			PartitionValues: partitionValues(file, fieldsByKey),
			ColumnStats:     columnStatsForSchema(file.aggregates, tableSchema),
			LastModified:    file.info.ModTime.UnixMilli(),
		})
	}

	if latestModTime == 0 {
		latestModTime = time.Now().UnixMilli()
	}

	table := &model.Table{
		Name:               filepath.Base(s.basePath),
		TableFormat:        model.TableFormatParquet,
		ReadSchema:         tableSchema,
		BasePath:           s.basePath,
		PartitioningFields: partitioningFields,
		LatestCommitTime:   latestModTime,
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        dataFiles,
		SourceIdentifier: "0",
	}, nil
}

// listDataFiles returns the dataset's data files, ordered by path so that everything derived from
// the listing is derived in the same order on every call.
func (s *Source) listDataFiles(ctx context.Context) ([]io.FileInfo, error) {
	allFiles, err := s.storage.List(ctx, s.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %w", s.basePath, err)
	}

	var parquetFiles []io.FileInfo
	for _, f := range allFiles {
		if !f.IsDir && strings.HasSuffix(f.Path, ".parquet") && !strings.HasPrefix(filepath.Base(f.Path), ".") {
			parquetFiles = append(parquetFiles, f)
		}
	}
	if len(parquetFiles) == 0 {
		return nil, fmt.Errorf("no parquet files found under %s", s.basePath)
	}

	slices.SortFunc(parquetFiles, func(a, b io.FileInfo) int {
		return strings.Compare(a.Path, b.Path)
	})
	return parquetFiles, nil
}

// observedPartitions collects the partition columns of the whole directory, in the order the
// directory nesting introduces them, with the values seen for each.
func observedPartitions(files []*crawledFile) []observedPartition {
	var observed []observedPartition
	index := make(map[string]int)
	seen := make(map[string]map[string]bool)

	for _, file := range files {
		for _, partition := range file.partitions {
			at, known := index[partition.Key]
			if !known {
				at = len(observed)
				index[partition.Key] = at
				seen[partition.Key] = make(map[string]bool)
				observed = append(observed, observedPartition{key: partition.Key})
			}
			if seen[partition.Key][partition.Value] {
				continue
			}
			seen[partition.Key][partition.Value] = true
			observed[at].samples = append(observed[at].samples, partitionSample{
				value: partition.Value,
				file:  file.info.Path,
			})
		}
	}
	return observed
}

// partitionValues pairs the partition values in one file's path with the table's partition fields.
func partitionValues(file *crawledFile, fieldsByKey map[string]*model.PartitionField) []*model.PartitionValue {
	values := make([]*model.PartitionValue, 0, len(file.partitions))
	for _, partition := range file.partitions {
		field, ok := fieldsByKey[partition.Key]
		if !ok {
			continue
		}
		values = append(values, &model.PartitionValue{
			PartitionField: field,
			// The raw directory value: it is what the path says, and every target formats it
			// itself.
			Range: model.NewScalarRange(partition.Value),
		})
	}
	return values
}

// GetTableChangeForCommit returns the diff of files.
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

// GetChangesSince returns changes since fromInstant.
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

// IsIncrementalSyncSafeFrom returns false for unmanaged directories (falling back to snapshot sync).
func (s *Source) IsIncrementalSyncSafeFrom(_ context.Context, _ int64) (bool, error) {
	return false, nil
}

// Close is a no-op.
func (s *Source) Close() error {
	return nil
}
