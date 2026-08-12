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
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package paimon_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/formats/paimon"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
)

func TestPaimon_TargetCommitSnapshot(t *testing.T) {
	t.Parallel()

	storage := io.NewMemoryStorage()
	ctx := context.Background()

	target := paimon.NewTarget(storage)
	err := target.Init(ctx, &model.Table{
		Name:        "test_table",
		TableFormat: model.TableFormatPaimon,
		BasePath:    "mem://test",
	})
	require.NoError(t, err)

	snapshot := &model.Snapshot{
		SourceIdentifier: "snap1",
		DataFiles:        []*model.DataFile{{PhysicalPath: "data1.parquet"}},
		Table: &model.Table{
			Name:        "test_table",
			TableFormat: model.TableFormatPaimon,
			BasePath:    "mem://test",
			ReadSchema:  &model.Schema{DataType: model.TypeString},
		},
	}

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	syncMetadata, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.Nil(t, syncMetadata)

	err = target.Close()
	require.NoError(t, err)
}

func TestPaimon_TargetCommitChanges(t *testing.T) {
	t.Parallel()

	storage := io.NewMemoryStorage()
	ctx := context.Background()

	target := paimon.NewTarget(storage)
	err := target.Init(ctx, &model.Table{
		Name:        "test_table",
		TableFormat: model.TableFormatPaimon,
		BasePath:    "mem://test",
	})
	require.NoError(t, err)

	changes := &model.IncrementalTableChanges{
		TableChanges: []*model.TableChange{
			{
				SourceIdentifier: "snap2",
				CommitTime:       123456789,
				FilesDiff: &model.FilesDiff{
					FilesAdded:   []*model.DataFile{{PhysicalPath: "data2.parquet"}},
					FilesRemoved: []*model.DataFile{},
				},
				TableAsOfChange: &model.Table{
					Name:        "test_table",
					TableFormat: model.TableFormatPaimon,
					BasePath:    "mem://test",
				},
			},
		},
		CurrentTable: &model.Table{
			Name:        "test_table",
			TableFormat: model.TableFormatDelta,
			BasePath:    "mem://test",
		},
	}

	err = target.CommitChanges(ctx, changes)
	require.NoError(t, err)

	syncMetadata, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, syncMetadata)
	assert.Equal(t, int64(123456789), syncMetadata.LastInstantSynced)
	assert.Equal(t, model.TableFormatDelta, syncMetadata.SourceFormat)
	assert.Equal(t, model.TableFormatPaimon, syncMetadata.TargetFormat)

	err = target.Close()
	require.NoError(t, err)
}

func TestPaimon_TargetGetTableMetadataEmpty(t *testing.T) {
	t.Parallel()

	storage := io.NewMemoryStorage()
	ctx := context.Background()

	target := paimon.NewTarget(storage)
	err := target.Init(ctx, &model.Table{
		Name:        "test_table",
		TableFormat: model.TableFormatPaimon,
		BasePath:    "mem://test",
	})
	require.NoError(t, err)

	syncMetadata, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.Nil(t, syncMetadata)

	err = target.Close()
	require.NoError(t, err)
}
