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

package delta_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
)

// commitSpec describes one hand-written Delta log commit. The fixtures are written as raw log
// files rather than through the Delta target because the cases under test need timestamps the
// target would never produce: repeated, decreasing, or absent.
type commitSpec struct {
	// timestamp is the commitInfo timestamp; ignored when noCommitInfo is set.
	timestamp int64
	// noCommitInfo omits the commitInfo action entirely, which the Delta protocol allows.
	noCommitInfo bool
	// columns, when non-empty, writes a metaData action carrying a schema of these int columns.
	columns []string
	// addPath, when non-empty, writes an add action for that relative path.
	addPath string
}

func deltaSchemaJSON(t *testing.T, columns []string) string {
	t.Helper()

	fields := make([]*model.Field, 0, len(columns))
	for _, name := range columns {
		fields = append(fields, &model.Field{Name: name, Schema: model.NewPrimitiveSchema(model.TypeInt, true)})
	}
	schemaJSON, err := delta.SchemaToDeltaJSON(model.NewRecordSchema("fixture", fields, false))
	require.NoError(t, err)
	return schemaJSON
}

// writeDeltaLog materializes commits as _delta_log/<version>.json files in storage.
func writeDeltaLog(t *testing.T, storage io.Storage, basePath string, commits []commitSpec) {
	t.Helper()
	ctx := context.Background()

	for version, spec := range commits {
		var actions []delta.SingleAction
		if len(spec.columns) > 0 {
			actions = append(actions, delta.SingleAction{MetaData: &delta.MetadataAction{
				ID:           "fixture",
				Name:         "fixture",
				Format:       delta.FormatProvider{Provider: "parquet"},
				SchemaString: deltaSchemaJSON(t, spec.columns),
			}})
		}
		if spec.addPath != "" {
			actions = append(actions, delta.SingleAction{Add: &delta.AddAction{
				Path:       spec.addPath,
				Size:       128,
				DataChange: true,
			}})
		}
		if !spec.noCommitInfo {
			actions = append(actions, delta.SingleAction{CommitInfo: &delta.CommitInfoAction{
				Timestamp: spec.timestamp,
				Operation: "WRITE",
			}})
		}

		var buf bytes.Buffer
		for _, action := range actions {
			line, err := json.Marshal(action)
			require.NoError(t, err)
			buf.Write(line)
			buf.WriteByte('\n')
		}

		path := io.JoinPath(basePath, "_delta_log", fmt.Sprintf("%020d.json", version))
		require.NoError(t, storage.Write(ctx, path, buf.Bytes()))
	}
}

// countingStorage counts object reads so a test can assert on read amplification rather than on
// wall-clock time.
type countingStorage struct {
	io.Storage
	mu    sync.Mutex
	reads int
}

func (c *countingStorage) Read(ctx context.Context, path string) ([]byte, error) {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	return c.Storage.Read(ctx, path)
}

func (c *countingStorage) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func syncedVersions(changes []*model.TableChange) []string {
	ids := make([]string, 0, len(changes))
	for _, change := range changes {
		ids = append(ids, change.SourceIdentifier)
	}
	return ids
}

// TestSource_IsIncrementalSyncSafeFrom pins the retention comparison, the one place that still
// reads the raw commitInfo timestamp.
func TestSource_IsIncrementalSyncSafeFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		commits         []commitSpec
		earliestInstant int64
		want            bool
	}{
		{
			name: "log retains a commit older than the instant",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
			},
			earliestInstant: 1500,
			want:            true,
		},
		{
			name: "log was truncated past the instant",
			commits: []commitSpec{
				{timestamp: 3000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 4000, addPath: "part-1.parquet"},
			},
			earliestInstant: 1500,
			want:            false,
		},
		{
			name: "earliest retained commit carries no timestamp",
			commits: []commitSpec{
				{noCommitInfo: true, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 4000, addPath: "part-1.parquet"},
			},
			earliestInstant: 1500,
			want:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/t26-retention"
			writeDeltaLog(t, storage, basePath, tt.commits)

			safe, err := delta.NewSource(storage, basePath).IsIncrementalSyncSafeFrom(ctx, tt.earliestInstant)
			require.NoError(t, err)
			assert.Equal(t, tt.want, safe)
		})
	}
}

// TestSource_GetChangesSince_CommitTimeAnomalies covers T26: the incremental selection keys on the
// commit timestamp, so a repeated, decreasing or absent commitInfo timestamp used to drop commits
// silently while the sync still reported success.
func TestSource_GetChangesSince_CommitTimeAnomalies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		commits     []commitSpec
		fromInstant int64
		want        []string
	}{
		{
			name: "strictly increasing timestamps",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
				{timestamp: 3000, addPath: "part-2.parquet"},
			},
			fromInstant: 2000,
			want:        []string{"2"},
		},
		{
			name: "two commits in the same millisecond",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
				{timestamp: 2000, addPath: "part-2.parquet"},
			},
			fromInstant: 2000,
			want:        []string{"2"},
		},
		{
			name: "timestamp goes backwards under clock skew",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
				{timestamp: 1500, addPath: "part-2.parquet"},
			},
			fromInstant: 2000,
			want:        []string{"2"},
		},
		{
			name: "commit without a commitInfo action",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
				{noCommitInfo: true, addPath: "part-2.parquet"},
			},
			fromInstant: 2000,
			want:        []string{"2"},
		},
		{
			name: "whole backlog from an instant before the first commit",
			commits: []commitSpec{
				{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
				{timestamp: 2000, addPath: "part-1.parquet"},
				{timestamp: 2000, addPath: "part-2.parquet"},
			},
			fromInstant: 500,
			want:        []string{"0", "1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/t26"
			writeDeltaLog(t, storage, basePath, tt.commits)

			source := delta.NewSource(storage, basePath)
			changes, err := source.GetChangesSince(ctx, tt.fromInstant)
			require.NoError(t, err)
			assert.Equal(t, tt.want, syncedVersions(changes.TableChanges))

			// The instants reported back to the caller must be strictly increasing: the controller
			// persists the last one and feeds it to the next sync as fromInstant.
			var previous int64
			for _, change := range changes.TableChanges {
				assert.Greater(t, change.CommitTime, previous, "instant for version %s", change.SourceIdentifier)
				previous = change.CommitTime
			}

			if len(changes.TableChanges) == 0 {
				return
			}

			// Resuming from the reported instant must neither replay nor drop anything.
			resumed, err := source.GetChangesSince(ctx, previous)
			require.NoError(t, err)
			assert.Empty(t, syncedVersions(resumed.TableChanges))
		})
	}
}

// TestSource_GetChangesSince_BacklogReadsEachCommitOnce covers T21: walking a backlog used to read
// every commit twice and rebuild the table from the whole log prefix for each one.
func TestSource_GetChangesSince_BacklogReadsEachCommitOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		backlogSize    = 100
		schemaChangeAt = 50
	)

	commits := make([]commitSpec, 0, backlogSize)
	for version := range backlogSize {
		spec := commitSpec{
			timestamp: int64(1000 + version*10),
			addPath:   fmt.Sprintf("part-%d.parquet", version),
		}
		switch version {
		case 0:
			spec.columns = []string{"id"}
		case schemaChangeAt:
			spec.columns = []string{"id", "amount"}
		}
		commits = append(commits, spec)
	}

	storage := &countingStorage{Storage: io.NewMemoryStorage()}
	basePath := "mem://lake/t21"
	writeDeltaLog(t, storage, basePath, commits)
	require.Zero(t, storage.readCount(), "building the fixture must not read")

	changes, err := delta.NewSource(storage, basePath).GetChangesSince(ctx, 0)
	require.NoError(t, err)
	require.Len(t, changes.TableChanges, backlogSize)

	// One read per commit file, and nothing else. Before T21 the same fixture cost 5350 reads:
	// every commit was read twice and the table rebuilt from the whole log prefix for each one.
	assert.Equal(t, backlogSize, storage.readCount(), "object reads for a %d-commit backlog", backlogSize)

	for version, change := range changes.TableChanges {
		assert.Equal(t, strconv.Itoa(version), change.SourceIdentifier)
		assert.Equal(t, int64(1000+version*10), change.CommitTime)

		table := change.TableAsOfChange
		require.NotNil(t, table)
		assert.Equal(t, change.CommitTime, table.LatestCommitTime)

		// The schema of the commit that introduced it must be carried forward across the commits
		// that follow, none of which repeats the metaData action.
		wantColumns := 1
		if version >= schemaChangeAt {
			wantColumns = 2
		}
		require.Len(t, table.ReadSchema.Fields, wantColumns, "schema at version %d", version)

		require.Len(t, change.FilesDiff.FilesAdded, 1)
		assert.Equal(t, io.JoinPath(basePath, fmt.Sprintf("part-%d.parquet", version)), change.FilesDiff.FilesAdded[0].PhysicalPath)
	}

	require.NotNil(t, changes.CurrentTable)
	assert.Len(t, changes.CurrentTable.ReadSchema.Fields, 2)
	assert.Equal(t, int64(1000+(backlogSize-1)*10), changes.CurrentTable.LatestCommitTime)
}

// TestSource_GetChangesSince_SchemaChangedBeforeTheBacklog covers the case the per-commit table
// rebuild used to serve: the metaData action that describes the backlog sits in a commit older than
// fromInstant, so it is walked but never emitted.
func TestSource_GetChangesSince_SchemaChangedBeforeTheBacklog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/t21-schema"
	writeDeltaLog(t, storage, basePath, []commitSpec{
		{timestamp: 1000, columns: []string{"id"}, addPath: "part-0.parquet"},
		{timestamp: 2000, addPath: "part-1.parquet"},
		{timestamp: 3000, columns: []string{"id", "amount"}, addPath: "part-2.parquet"},
		{timestamp: 4000, addPath: "part-3.parquet"},
	})

	changes, err := delta.NewSource(storage, basePath).GetChangesSince(ctx, 3000)
	require.NoError(t, err)
	require.Len(t, changes.TableChanges, 1)
	assert.Equal(t, []string{"3"}, syncedVersions(changes.TableChanges))
	require.Len(t, changes.TableChanges[0].TableAsOfChange.ReadSchema.Fields, 2)
}
