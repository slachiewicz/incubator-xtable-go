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
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
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

// GetCurrentSnapshot crawls the directory tree, inspects Parquet footers, and builds a complete Snapshot.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
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

	// 1. Read first file to extract Schema
	firstFileData, err := s.storage.Read(ctx, parquetFiles[0].Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet file %s: %w", parquetFiles[0].Path, err)
	}

	reader := bytes.NewReader(firstFileData)
	pqFile, err := parquet.OpenFile(reader, int64(len(firstFileData)))
	if err != nil {
		return nil, fmt.Errorf("failed to parse parquet file %s: %w", parquetFiles[0].Path, err)
	}

	tableSchema := ParquetSchemaToModel(pqFile.Schema())

	// 2. Discover all data files and partition keys
	var dataFiles []*model.DataFile
	partFieldsMap := make(map[string]*model.PartitionField)
	var latestModTime int64

	for _, pf := range parquetFiles {
		fileData, err := s.storage.Read(ctx, pf.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", pf.Path, err)
		}

		fReader := bytes.NewReader(fileData)
		pfObj, err := parquet.OpenFile(fReader, int64(len(fileData)))
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", pf.Path, err)
		}

		numRows := pfObj.NumRows()
		modTimeMs := pf.ModTime.UnixMilli()
		if modTimeMs > latestModTime {
			latestModTime = modTimeMs
		}

		partFields, partValues := ExtractHivePartitions(pf.Path, s.basePath, tableSchema)
		for _, field := range partFields {
			partFieldsMap[field.SourceField.Name] = field
		}

		dataFiles = append(dataFiles, &model.DataFile{
			PhysicalPath:    pf.Path,
			FileFormat:      model.FileFormatParquet,
			FileSizeBytes:   pf.Size,
			RecordCount:     numRows,
			PartitionValues: partValues,
			LastModified:    modTimeMs,
		})
	}

	if latestModTime == 0 {
		latestModTime = time.Now().UnixMilli()
	}

	var partitioningFields []*model.PartitionField
	for _, pf := range partFieldsMap {
		partitioningFields = append(partitioningFields, pf)
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
