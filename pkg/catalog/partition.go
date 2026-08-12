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

package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/incubator-xtable-go/pkg/model"
)

// DefaultMaxPartitionsPerRequest bounds how many partitions are sent per catalog call. It mirrors
// Java's HMSCatalogConfig.maxPartitionsPerRequest. Catalog-specific hard limits are enforced by the
// implementations themselves, which may batch more finely than this.
const DefaultMaxPartitionsPerRequest = 1000

// Partition is a single catalog partition: the ordered partition-key values and where its data lives.
type Partition struct {
	// Values are the partition key values in partition-spec order.
	Values []string
	// StorageLocation is the directory holding this partition's data files.
	StorageLocation string
}

// Key renders the partition values as a stable identity for diffing. Location is deliberately
// excluded so a relocated partition is reported as changed rather than as an unrelated pair.
func (p Partition) Key() string {
	return strings.Join(p.Values, "\x1f")
}

// PartitionEventType describes what must happen to a partition to reconcile the catalog.
type PartitionEventType string

// Partition reconciliation actions.
const (
	PartitionEventAdd    PartitionEventType = "ADD"
	PartitionEventUpdate PartitionEventType = "UPDATE"
	PartitionEventDrop   PartitionEventType = "DROP"
)

// PartitionEvent pairs a reconciliation action with the partition it applies to.
type PartitionEvent struct {
	Type      PartitionEventType
	Partition Partition
}

// PartitionSyncOperations is the catalog-side partition API. It is separate from SyncClient because
// not every catalog tracks partitions: Iceberg and Delta carry partition data in their own metadata,
// so only Hive-style catalogs such as Glue need this.
type PartitionSyncOperations interface {
	// GetAllPartitions lists every partition the catalog currently records for the table.
	GetAllPartitions(ctx context.Context, id TableIdentifier) ([]Partition, error)

	// AddPartitions registers new partitions.
	AddPartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error

	// UpdatePartitions rewrites existing partitions, typically because their location moved.
	UpdatePartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error

	// DropPartitions removes partitions that no longer exist in the table.
	DropPartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error
}

// DiffPartitions computes the events needed to bring `existing` (what the catalog has) in line with
// `desired` (what the table has). Results are sorted by partition key so the output is deterministic
// regardless of input ordering — the same guarantee model.DiffFiles fails to make.
func DiffPartitions(existing, desired []Partition) []PartitionEvent {
	existingByKey := make(map[string]Partition, len(existing))
	for _, p := range existing {
		existingByKey[p.Key()] = p
	}
	desiredByKey := make(map[string]Partition, len(desired))
	for _, p := range desired {
		desiredByKey[p.Key()] = p
	}

	var events []PartitionEvent
	for key, want := range desiredByKey {
		have, found := existingByKey[key]
		switch {
		case !found:
			events = append(events, PartitionEvent{Type: PartitionEventAdd, Partition: want})
		case have.StorageLocation != want.StorageLocation:
			events = append(events, PartitionEvent{Type: PartitionEventUpdate, Partition: want})
		}
	}
	for key, have := range existingByKey {
		if _, found := desiredByKey[key]; !found {
			events = append(events, PartitionEvent{Type: PartitionEventDrop, Partition: have})
		}
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].Type != events[j].Type {
			return events[i].Type < events[j].Type
		}
		return events[i].Partition.Key() < events[j].Partition.Key()
	})
	return events
}

// PartitionsFromSnapshot derives the partitions a table currently has from its data files. Files
// carrying no partition values yield no partitions, which is the correct answer for an unpartitioned
// table. The storage location is the directory containing the file.
func PartitionsFromSnapshot(snapshot *model.Snapshot) []Partition {
	if snapshot == nil {
		return nil
	}

	byKey := make(map[string]Partition)
	for _, file := range snapshot.DataFiles {
		if file == nil || len(file.PartitionValues) == 0 {
			continue
		}
		values := make([]string, 0, len(file.PartitionValues))
		for _, pv := range file.PartitionValues {
			if pv == nil || pv.Range == nil {
				continue
			}
			values = append(values, fmt.Sprintf("%v", pv.Range.MinValue))
		}
		if len(values) == 0 {
			continue
		}
		p := Partition{Values: values, StorageLocation: parentDir(file.PhysicalPath)}
		byKey[p.Key()] = p
	}

	partitions := make([]Partition, 0, len(byKey))
	for _, p := range byKey {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Key() < partitions[j].Key() })
	return partitions
}

// parentDir returns everything before the final "/" segment. path.Dir must not be used here: it
// normalises "s3://bucket/x" to "s3:/bucket/x", silently corrupting every object-store location.
func parentDir(p string) string {
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return p
	}
	return p[:idx]
}

// SyncPartitions reconciles a table's partitions in the catalog. Every action is attempted even when
// an earlier one fails, so one bad batch does not hide the rest; the errors are joined.
func SyncPartitions(ctx context.Context, ops PartitionSyncOperations, id TableIdentifier, desired []Partition, batchSize int) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if batchSize <= 0 {
		batchSize = DefaultMaxPartitionsPerRequest
	}

	existing, err := ops.GetAllPartitions(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to list partitions for %s: %w", id, err)
	}

	var adds, updates, drops []Partition
	for _, event := range DiffPartitions(existing, desired) {
		switch event.Type {
		case PartitionEventAdd:
			adds = append(adds, event.Partition)
		case PartitionEventUpdate:
			updates = append(updates, event.Partition)
		case PartitionEventDrop:
			drops = append(drops, event.Partition)
		}
	}

	var errs []error
	if err := inBatches(adds, batchSize, func(batch []Partition) error {
		return ops.AddPartitions(ctx, id, batch)
	}); err != nil {
		errs = append(errs, fmt.Errorf("failed to add partitions to %s: %w", id, err))
	}
	if err := inBatches(updates, batchSize, func(batch []Partition) error {
		return ops.UpdatePartitions(ctx, id, batch)
	}); err != nil {
		errs = append(errs, fmt.Errorf("failed to update partitions on %s: %w", id, err))
	}
	if err := inBatches(drops, batchSize, func(batch []Partition) error {
		return ops.DropPartitions(ctx, id, batch)
	}); err != nil {
		errs = append(errs, fmt.Errorf("failed to drop partitions from %s: %w", id, err))
	}

	return errors.Join(errs...)
}

// inBatches applies fn to slices of at most batchSize, continuing past failures and joining errors.
func inBatches(partitions []Partition, batchSize int, fn func([]Partition) error) error {
	var errs []error
	for start := 0; start < len(partitions); start += batchSize {
		end := start + batchSize
		if end > len(partitions) {
			end = len(partitions)
		}
		if err := fn(partitions[start:end]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
