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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
)

func main() {
	basePath, _ := filepath.Abs("./demo/sample_delta_table")
	_ = os.RemoveAll(basePath)

	idField := &model.Field{Name: "customer_id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	nameField := &model.Field{Name: "customer_name", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	spendField := &model.Field{Name: "total_spend", Schema: model.NewDecimalSchema(10, 2, true)}

	schema := model.NewRecordSchema("customers", []*model.Field{idField, nameField, regionField, spendField}, false)
	partField := &model.PartitionField{
		SourceField:   regionField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "customers",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  filepath.Join(basePath, "region=us", "part-00000.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1048576,
		RecordCount:   10000,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("us")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	dataFile2 := &model.DataFile{
		PhysicalPath:  filepath.Join(basePath, "region=apac", "part-00001.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 2097152,
		RecordCount:   25000,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("apac")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1, dataFile2},
		SourceIdentifier: "0",
	}

	storage := io.NewLocalStorage()
	target := delta.NewTarget(storage)
	ctx := context.Background()

	if err := target.Init(ctx, table); err != nil {
		panic(err)
	}
	if err := target.CommitSnapshot(ctx, snapshot); err != nil {
		panic(err)
	}

	fmt.Printf("Created sample Delta table at %s with 2 partition files (35,000 records total)\n", basePath)
}
