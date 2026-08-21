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

package spi

import (
	"context"

	"github.com/slachiewicz/polytable/pkg/model"
)

// ConversionTarget defines the interface for writing translated table metadata to a target table format.
type ConversionTarget interface {
	// Format returns the target table format identifier (e.g. DELTA, ICEBERG, HUDI).
	Format() model.TableFormat

	// Init initializes the target converter for a specific target table configuration.
	Init(ctx context.Context, targetTable *model.Table) error

	// GetTableMetadata retrieves previously stored TableSyncMetadata (e.g. last synced instant) from target table.
	GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error)

	// CommitSnapshot synchronizes a full Snapshot into the target format without rewriting data files.
	CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error

	// CommitChanges synchronizes an incremental sequence of TableChange commits to the target format.
	CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error

	// Close releases any resources.
	Close() error
}
