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

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// ConversionSource defines the interface for reading table metadata, snapshots, and commit logs from a source table format.
type ConversionSource interface {
	// Format returns the source table format identifier (e.g. DELTA, ICEBERG, HUDI).
	Format() model.TableFormat

	// GetTable returns the Table descriptor at a specific commit version or timestamp.
	GetTable(ctx context.Context, commitID string) (*model.Table, error)

	// GetCurrentTable returns the Table descriptor at the latest commit.
	GetCurrentTable(ctx context.Context) (*model.Table, error)

	// GetCurrentSnapshot returns the complete Snapshot (active files, schema, stats) at the latest commit.
	GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error)

	// GetTableChangeForCommit returns the diff and schema changes for a specific commit version.
	GetTableChangeForCommit(ctx context.Context, commitID string) (*model.TableChange, error)

	// GetChangesSince returns all incremental table changes between two commit instants.
	GetChangesSince(ctx context.Context, fromInstant int64) (*model.IncrementalTableChanges, error)

	// IsIncrementalSyncSafeFrom checks if incremental history is intact and available since earliestInstant.
	IsIncrementalSyncSafeFrom(ctx context.Context, earliestInstant int64) (bool, error)

	// Close releases any open resources or connections.
	Close() error
}
