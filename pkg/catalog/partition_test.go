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

package catalog_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/catalog"
	"github.com/apache/incubator-xtable-go/pkg/model"
)

func TestDiffPartitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing []catalog.Partition
		desired  []catalog.Partition
		want     []catalog.PartitionEvent
	}{
		{
			name:     "new partition is added",
			existing: nil,
			desired:  []catalog.Partition{{Values: []string{"US"}, StorageLocation: "s3://b/country=US"}},
			want: []catalog.PartitionEvent{
				{Type: catalog.PartitionEventAdd, Partition: catalog.Partition{Values: []string{"US"}, StorageLocation: "s3://b/country=US"}},
			},
		},
		{
			name:     "unchanged partition yields no event",
			existing: []catalog.Partition{{Values: []string{"US"}, StorageLocation: "s3://b/country=US"}},
			desired:  []catalog.Partition{{Values: []string{"US"}, StorageLocation: "s3://b/country=US"}},
			want:     nil,
		},
		{
			name:     "relocated partition is updated, not re-added",
			existing: []catalog.Partition{{Values: []string{"US"}, StorageLocation: "s3://old/country=US"}},
			desired:  []catalog.Partition{{Values: []string{"US"}, StorageLocation: "s3://new/country=US"}},
			want: []catalog.PartitionEvent{
				{Type: catalog.PartitionEventUpdate, Partition: catalog.Partition{Values: []string{"US"}, StorageLocation: "s3://new/country=US"}},
			},
		},
		{
			name:     "vanished partition is dropped",
			existing: []catalog.Partition{{Values: []string{"DE"}, StorageLocation: "s3://b/country=DE"}},
			desired:  nil,
			want: []catalog.PartitionEvent{
				{Type: catalog.PartitionEventDrop, Partition: catalog.Partition{Values: []string{"DE"}, StorageLocation: "s3://b/country=DE"}},
			},
		},
		{
			name: "multi-key partitions compare on the full tuple",
			existing: []catalog.Partition{
				{Values: []string{"2024", "01"}, StorageLocation: "s3://b/y=2024/m=01"},
			},
			desired: []catalog.Partition{
				{Values: []string{"2024", "01"}, StorageLocation: "s3://b/y=2024/m=01"},
				{Values: []string{"2024", "02"}, StorageLocation: "s3://b/y=2024/m=02"},
			},
			want: []catalog.PartitionEvent{
				{Type: catalog.PartitionEventAdd, Partition: catalog.Partition{Values: []string{"2024", "02"}, StorageLocation: "s3://b/y=2024/m=02"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, catalog.DiffPartitions(tt.existing, tt.desired))
		})
	}
}

func TestDiffPartitionsIsDeterministic(t *testing.T) {
	t.Parallel()

	existing := []catalog.Partition{
		{Values: []string{"a"}, StorageLocation: "s3://b/a"},
		{Values: []string{"b"}, StorageLocation: "s3://b/b"},
		{Values: []string{"c"}, StorageLocation: "s3://b/c"},
	}
	desired := []catalog.Partition{
		{Values: []string{"c"}, StorageLocation: "s3://b/c"},
		{Values: []string{"d"}, StorageLocation: "s3://b/d"},
		{Values: []string{"e"}, StorageLocation: "s3://b/e"},
	}

	// Map iteration order varies per run, so repeat: the output must not.
	first := catalog.DiffPartitions(existing, desired)
	for range 20 {
		assert.Equal(t, first, catalog.DiffPartitions(existing, desired),
			"DiffPartitions must be deterministic despite ranging over maps")
	}
}

func TestPartitionsFromSnapshot(t *testing.T) {
	t.Parallel()

	countryField := &model.Field{Name: "country", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	partField := &model.PartitionField{SourceField: countryField, TransformType: model.PartitionTransformValue}

	partitioned := func(country, path string) *model.DataFile {
		return &model.DataFile{
			PhysicalPath: path,
			PartitionValues: []*model.PartitionValue{
				{PartitionField: partField, Range: model.NewScalarRange(country)},
			},
		}
	}

	tests := []struct {
		name     string
		snapshot *model.Snapshot
		want     []catalog.Partition
	}{
		{name: "nil snapshot", snapshot: nil, want: nil},
		{
			name:     "unpartitioned files produce no partitions",
			snapshot: &model.Snapshot{DataFiles: []*model.DataFile{{PhysicalPath: "s3://b/part-0.parquet"}}},
			want:     []catalog.Partition{},
		},
		{
			name: "files in the same partition collapse to one entry",
			snapshot: &model.Snapshot{DataFiles: []*model.DataFile{
				partitioned("US", "s3://b/country=US/part-0.parquet"),
				partitioned("US", "s3://b/country=US/part-1.parquet"),
				partitioned("DE", "s3://b/country=DE/part-0.parquet"),
			}},
			want: []catalog.Partition{
				{Values: []string{"DE"}, StorageLocation: "s3://b/country=DE"},
				{Values: []string{"US"}, StorageLocation: "s3://b/country=US"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, catalog.PartitionsFromSnapshot(tt.snapshot))
		})
	}
}

// recordingPartitionOps captures calls so batching and error handling can be asserted.
type recordingPartitionOps struct {
	existing   []catalog.Partition
	addBatches [][]catalog.Partition
	updated    []catalog.Partition
	dropped    []catalog.Partition
	addErr     error
	listErr    error
}

func (r *recordingPartitionOps) GetAllPartitions(context.Context, catalog.TableIdentifier) ([]catalog.Partition, error) {
	return r.existing, r.listErr
}

func (r *recordingPartitionOps) AddPartitions(_ context.Context, _ catalog.TableIdentifier, p []catalog.Partition) error {
	r.addBatches = append(r.addBatches, append([]catalog.Partition(nil), p...))
	return r.addErr
}

func (r *recordingPartitionOps) UpdatePartitions(_ context.Context, _ catalog.TableIdentifier, p []catalog.Partition) error {
	r.updated = append(r.updated, p...)
	return nil
}

func (r *recordingPartitionOps) DropPartitions(_ context.Context, _ catalog.TableIdentifier, p []catalog.Partition) error {
	r.dropped = append(r.dropped, p...)
	return nil
}

func TestSyncPartitions(t *testing.T) {
	t.Parallel()

	id := catalog.TableIdentifier{Database: "db", Table: "events"}

	t.Run("splits adds into batches of the configured size", func(t *testing.T) {
		t.Parallel()

		desired := make([]catalog.Partition, 0, 5)
		for _, v := range []string{"a", "b", "c", "d", "e"} {
			desired = append(desired, catalog.Partition{Values: []string{v}, StorageLocation: "s3://b/" + v})
		}

		ops := &recordingPartitionOps{}
		require.NoError(t, catalog.SyncPartitions(context.Background(), ops, id, desired, 2))

		require.Len(t, ops.addBatches, 3, "5 partitions at batch size 2 should be 3 calls")
		assert.Len(t, ops.addBatches[0], 2)
		assert.Len(t, ops.addBatches[1], 2)
		assert.Len(t, ops.addBatches[2], 1)
	})

	t.Run("applies adds, updates and drops together", func(t *testing.T) {
		t.Parallel()

		ops := &recordingPartitionOps{existing: []catalog.Partition{
			{Values: []string{"keep"}, StorageLocation: "s3://b/keep"},
			{Values: []string{"move"}, StorageLocation: "s3://old/move"},
			{Values: []string{"gone"}, StorageLocation: "s3://b/gone"},
		}}
		desired := []catalog.Partition{
			{Values: []string{"keep"}, StorageLocation: "s3://b/keep"},
			{Values: []string{"move"}, StorageLocation: "s3://new/move"},
			{Values: []string{"new"}, StorageLocation: "s3://b/new"},
		}

		require.NoError(t, catalog.SyncPartitions(context.Background(), ops, id, desired, 0))

		require.Len(t, ops.addBatches, 1)
		assert.Equal(t, []string{"new"}, ops.addBatches[0][0].Values)
		require.Len(t, ops.updated, 1)
		assert.Equal(t, "s3://new/move", ops.updated[0].StorageLocation)
		require.Len(t, ops.dropped, 1)
		assert.Equal(t, []string{"gone"}, ops.dropped[0].Values)
	})

	t.Run("a failing add batch does not skip drops", func(t *testing.T) {
		t.Parallel()

		ops := &recordingPartitionOps{
			existing: []catalog.Partition{{Values: []string{"gone"}, StorageLocation: "s3://b/gone"}},
			addErr:   errors.New("glue rejected the batch"),
		}
		desired := []catalog.Partition{{Values: []string{"new"}, StorageLocation: "s3://b/new"}}

		err := catalog.SyncPartitions(context.Background(), ops, id, desired, 0)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "glue rejected the batch")
		assert.Len(t, ops.dropped, 1, "drops must still be attempted after an add failure")
	})

	t.Run("listing failure aborts before mutating anything", func(t *testing.T) {
		t.Parallel()

		ops := &recordingPartitionOps{listErr: errors.New("access denied")}
		err := catalog.SyncPartitions(context.Background(), ops, id, nil, 0)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
		assert.Empty(t, ops.addBatches)
		assert.Empty(t, ops.dropped)
	})

	t.Run("rejects an incomplete identifier", func(t *testing.T) {
		t.Parallel()

		err := catalog.SyncPartitions(context.Background(), &recordingPartitionOps{},
			catalog.TableIdentifier{Table: "events"}, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database")
	})
}
