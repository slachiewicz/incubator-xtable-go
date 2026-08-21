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
	"sort"
	"time"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// SyncOutput is the machine-readable document `xtable sync --output json` writes to stdout. It is
// the whole reason for this file: an agent driving the CLI parses this rather than screen-scraping
// the human-readable progress text, which goes to stderr instead.
type SyncOutput struct {
	// StartedAt is when the sync run began.
	StartedAt time.Time `json:"startedAt"`
	// Duration is the wall-clock time for the whole run, across every dataset.
	Duration time.Duration `json:"duration"`
	// DryRun reports whether this run resolved and reported changes without committing them.
	DryRun bool `json:"dryRun"`
	// Tables holds one entry per dataset in the config file, in config order.
	Tables []TableSyncOutput `json:"tables"`
	// HasErrors is true if any target of any table reported the FAILED verdict, or a table could
	// not even be resolved (bad source catalog reference, storage init failure, invalid config).
	// This is the field a caller should check before trusting the run; it mirrors the process exit
	// code.
	HasErrors bool `json:"hasErrors"`
}

// TableSyncOutput reports the outcome for one dataset (one source table, one or more targets).
type TableSyncOutput struct {
	// TableName is the dataset's logical table name.
	TableName string `json:"tableName"`
	// SourceFormat is the dataset's source table format.
	SourceFormat string `json:"sourceFormat"`
	// Error is set instead of Targets when the dataset failed before any target sync ran — e.g.
	// source catalog resolution or storage initialization.
	Error string `json:"error,omitempty"`
	// Targets holds one entry per target format, sorted by format name so the JSON is
	// deterministic despite the controller returning them in a map.
	Targets []TargetSyncOutput `json:"targets,omitempty"`
}

// TargetSyncOutput reports the outcome of syncing to a single target format.
type TargetSyncOutput struct {
	// TargetFormat is the target table format that was (or would have been) updated.
	TargetFormat string `json:"targetFormat"`
	// Verdict is exactly one of SUCCESS, FAILED, or NO_OP. NO_OP means the incremental path found
	// no new commits since the last synced instant, so nothing was written.
	Verdict string `json:"verdict"`
	// LastInstantSynced is the timestamp of the latest synced commit.
	LastInstantSynced int64 `json:"lastInstantSynced,omitempty"`
	// Duration is the time spent syncing this one target.
	Duration time.Duration `json:"duration"`
	// Error contains the error text if the verdict is FAILED, or a catalog registration error
	// appended to an otherwise-successful sync (see docs/improvement-plan.md T16).
	Error string `json:"error,omitempty"`
}

// buildTableSyncOutput converts one dataset's sync results into the machine-readable shape. tableErr
// is a failure that happened before any target was attempted (catalog resolution, storage init, or
// the controller's own config validation) — when set, results is expected to be nil or empty.
func buildTableSyncOutput(ds *conversion.DatasetConfig, results map[model.TableFormat]*spi.SyncResult, tableErr error) TableSyncOutput {
	out := TableSyncOutput{
		TableName:    ds.TableName,
		SourceFormat: string(ds.SourceFormat),
	}
	if tableErr != nil {
		out.Error = tableErr.Error()
		return out
	}

	formats := make([]string, 0, len(results))
	for f := range results {
		formats = append(formats, string(f))
	}
	sort.Strings(formats)

	out.Targets = make([]TargetSyncOutput, 0, len(formats))
	for _, f := range formats {
		res := results[model.TableFormat(f)]
		out.Targets = append(out.Targets, TargetSyncOutput{
			TargetFormat:      f,
			Verdict:           string(res.Verdict()),
			LastInstantSynced: res.LastInstantSynced,
			Duration:          res.Duration,
			Error:             res.Error,
		})
	}
	return out
}

// hasFailure reports whether a TableSyncOutput represents a failed dataset: either it never reached
// a target (Error set), or at least one target's verdict is FAILED.
func (t TableSyncOutput) hasFailure() bool {
	if t.Error != "" {
		return true
	}
	for _, target := range t.Targets {
		if target.Verdict == string(spi.SyncVerdictFailed) {
			return true
		}
	}
	return false
}

// InspectOutput is the machine-readable document `xtable inspect --output json` writes to stdout.
type InspectOutput struct {
	// TableName is the table's logical name.
	TableName string `json:"tableName"`
	// Format is the table's storage format (e.g. DELTA, ICEBERG).
	Format string `json:"format"`
	// BasePath is the table's root storage path.
	BasePath string `json:"basePath"`
	// LatestCommitTimeMillis is the latest commit time, in epoch milliseconds.
	LatestCommitTimeMillis int64 `json:"latestCommitTimeMillis"`
	// ActiveDataFiles is the number of active data files in the current snapshot.
	ActiveDataFiles int `json:"activeDataFiles"`
	// Fields describes the table's read schema, top-level fields in schema order.
	Fields []InspectField `json:"fields"`
	// PartitionFields describes the table's partitioning, if any.
	PartitionFields []InspectPartitionField `json:"partitionFields,omitempty"`
}

// InspectField describes a single top-level schema field.
type InspectField struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
	Nullable bool   `json:"nullable"`
}

// InspectPartitionField describes a single partitioning field.
type InspectPartitionField struct {
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// buildInspectOutput converts a table and its current snapshot into the machine-readable shape.
func buildInspectOutput(table *model.Table, snapshot *model.Snapshot) InspectOutput {
	out := InspectOutput{
		TableName:              table.Name,
		Format:                 string(table.TableFormat),
		BasePath:               table.BasePath,
		LatestCommitTimeMillis: table.LatestCommitTime,
		ActiveDataFiles:        len(snapshot.DataFiles),
		Fields:                 make([]InspectField, 0, len(table.ReadSchema.Fields)),
		PartitionFields:        make([]InspectPartitionField, 0, len(table.PartitioningFields)),
	}

	for _, f := range table.ReadSchema.Fields {
		out.Fields = append(out.Fields, InspectField{
			Name:     f.Name,
			DataType: string(f.Schema.DataType),
			Nullable: f.Schema.IsNullable,
		})
	}

	for _, pf := range table.PartitioningFields {
		out.PartitionFields = append(out.PartitionFields, InspectPartitionField{
			Name:      pf.SourceField.Name,
			Transform: string(pf.TransformType),
		})
	}
	if len(out.PartitionFields) == 0 {
		out.PartitionFields = nil
	}

	return out
}
