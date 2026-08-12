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

package conversion_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// fileCounts spans the range that distinguishes a toy table from a realistic one. Snapshot sync is
// linear in the number of data files, so the shape of the curve matters more than any single point.
var fileCounts = []int{10, 100, 1_000, 10_000}

// buildDeltaTable materialises a partitioned Delta table with fileCount data files in memory
// storage. Setup is excluded from the timed region by the callers.
func buildDeltaTable(b *testing.B, ctx context.Context, storage io.Storage, basePath string, fileCount int) {
	b.Helper()

	countryField := &model.Field{Name: "country", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	partField := &model.PartitionField{SourceField: countryField, TransformType: model.PartitionTransformValue}

	table := &model.Table{
		Name:               "bench",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         model.NewRecordSchema("bench", []*model.Field{idField, countryField}, false),
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	files := make([]*model.DataFile, 0, fileCount)
	for i := range fileCount {
		country := []string{"US", "DE", "PL", "JP"}[i%4]
		files = append(files, &model.DataFile{
			PhysicalPath:    fmt.Sprintf("%s/country=%s/part-%06d.parquet", basePath, country, i),
			FileFormat:      model.FileFormatParquet,
			FileSizeBytes:   int64(1024 * (i%16 + 1)),
			RecordCount:     int64(100 * (i%32 + 1)),
			PartitionValues: []*model.PartitionValue{{PartitionField: partField, Range: model.NewScalarRange(country)}},
			LastModified:    time.Now().UnixMilli(),
		})
	}

	target := delta.NewTarget(storage)
	if err := target.Init(ctx, table); err != nil {
		b.Fatalf("delta init: %v", err)
	}
	if err := target.CommitSnapshot(ctx, &model.Snapshot{Table: table, DataFiles: files, SourceIdentifier: "bench-v0"}); err != nil {
		b.Fatalf("delta commit: %v", err)
	}
}

// BenchmarkSnapshotSync measures a full Delta to Iceberg snapshot sync, the operation SPEC.md
// section 9 sets a target for. Storage is in-memory so the figure isolates translation cost from
// object-store latency; a network backend adds its own round trips on top.
func BenchmarkSnapshotSync(b *testing.B) {
	for _, fileCount := range fileCounts {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			ctx := context.Background()

			for b.Loop() {
				b.StopTimer()
				// A fresh store per iteration: committing into a table that already has an Iceberg
				// snapshot measures a different operation.
				storage := io.NewMemoryStorage()
				basePath := "mem://bench/snapshot"
				buildDeltaTable(b, ctx, storage, basePath, fileCount)
				controller := conversion.NewController(storage)
				cfg := &conversion.DatasetConfig{
					SourceFormat:  model.TableFormatDelta,
					TargetFormats: []model.TableFormat{model.TableFormatIceberg},
					TableBasePath: basePath,
					TableName:     "bench",
					SyncMode:      spi.SyncModeFull,
				}
				b.StartTimer()

				results, err := controller.Sync(ctx, cfg)
				if err != nil {
					b.Fatalf("sync: %v", err)
				}
				if got := results[model.TableFormatIceberg]; got == nil || got.StatusCode != spi.SyncStatusSuccess {
					b.Fatalf("sync did not succeed: %+v", got)
				}
			}
		})
	}
}

// BenchmarkSnapshotRead measures reading a table's current snapshot, the read half of every sync.
func BenchmarkSnapshotRead(b *testing.B) {
	for _, fileCount := range fileCounts {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			ctx := context.Background()
			storage := io.NewMemoryStorage()
			basePath := "mem://bench/read"
			buildDeltaTable(b, ctx, storage, basePath, fileCount)
			source := delta.NewSource(storage, basePath)

			b.ReportAllocs()
			for b.Loop() {
				snap, err := source.GetCurrentSnapshot(ctx)
				if err != nil {
					b.Fatalf("read: %v", err)
				}
				if len(snap.DataFiles) != fileCount {
					b.Fatalf("expected %d files, got %d", fileCount, len(snap.DataFiles))
				}
			}
		})
	}
}
