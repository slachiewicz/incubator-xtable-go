//go:build !js

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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// AWS Glue caps how many partitions a single batch call accepts. These are service limits, not
// tuning knobs: exceeding them fails the request outright.
const (
	glueMaxCreatePartitionsPerCall = 100
	glueMaxDeletePartitionsPerCall = 25
)

// gluePartitionAPI is the slice of the Glue API the partition operations need, declared so the
// implementation is testable without reaching AWS. *glue.Client satisfies it.
type gluePartitionAPI interface {
	GetPartitions(ctx context.Context, params *glue.GetPartitionsInput, optFns ...func(*glue.Options)) (*glue.GetPartitionsOutput, error)
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
	BatchCreatePartition(ctx context.Context, params *glue.BatchCreatePartitionInput, optFns ...func(*glue.Options)) (*glue.BatchCreatePartitionOutput, error)
	BatchDeletePartition(ctx context.Context, params *glue.BatchDeletePartitionInput, optFns ...func(*glue.Options)) (*glue.BatchDeletePartitionOutput, error)
	UpdatePartition(ctx context.Context, params *glue.UpdatePartitionInput, optFns ...func(*glue.Options)) (*glue.UpdatePartitionOutput, error)
}

// GluePartitionSyncOperations implements PartitionSyncOperations against the AWS Glue Data Catalog.
type GluePartitionSyncOperations struct {
	client    gluePartitionAPI
	catalogID *string
}

var _ PartitionSyncOperations = (*GluePartitionSyncOperations)(nil)

// NewGluePartitionSyncOperations creates Glue-backed partition operations over an existing client.
func NewGluePartitionSyncOperations(client gluePartitionAPI, catalogID *string) *GluePartitionSyncOperations {
	return &GluePartitionSyncOperations{client: client, catalogID: catalogID}
}

// GetAllPartitions lists every partition Glue records for the table, following pagination.
func (g *GluePartitionSyncOperations) GetAllPartitions(ctx context.Context, id TableIdentifier) ([]Partition, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}

	var partitions []Partition
	var nextToken *string
	for {
		out, err := g.client.GetPartitions(ctx, &glue.GetPartitionsInput{
			CatalogId:    g.catalogID,
			DatabaseName: aws.String(id.Database),
			TableName:    aws.String(id.Table),
			NextToken:    nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list Glue partitions for %s: %w", id, err)
		}

		for _, p := range out.Partitions {
			location := ""
			if p.StorageDescriptor != nil {
				location = aws.ToString(p.StorageDescriptor.Location)
			}
			partitions = append(partitions, Partition{Values: p.Values, StorageLocation: location})
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		nextToken = out.NextToken
	}

	return partitions, nil
}

// AddPartitions registers new partitions, chunked to Glue's per-call limit.
func (g *GluePartitionSyncOperations) AddPartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error {
	if len(partitions) == 0 {
		return nil
	}
	descriptor, err := g.tableStorageDescriptor(ctx, id)
	if err != nil {
		return err
	}

	return inBatches(partitions, glueMaxCreatePartitionsPerCall, func(batch []Partition) error {
		inputs := make([]gluetypes.PartitionInput, 0, len(batch))
		for _, p := range batch {
			inputs = append(inputs, gluetypes.PartitionInput{
				Values:            p.Values,
				StorageDescriptor: partitionDescriptor(descriptor, p.StorageLocation),
			})
		}

		out, err := g.client.BatchCreatePartition(ctx, &glue.BatchCreatePartitionInput{
			CatalogId:          g.catalogID,
			DatabaseName:       aws.String(id.Database),
			TableName:          aws.String(id.Table),
			PartitionInputList: inputs,
		})
		if err != nil {
			return err
		}
		return glueBatchErrors("create", out.Errors)
	})
}

// UpdatePartitions rewrites partitions one at a time; Glue exposes no batch update.
func (g *GluePartitionSyncOperations) UpdatePartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error {
	if len(partitions) == 0 {
		return nil
	}
	descriptor, err := g.tableStorageDescriptor(ctx, id)
	if err != nil {
		return err
	}

	var errs []error
	for _, p := range partitions {
		_, err := g.client.UpdatePartition(ctx, &glue.UpdatePartitionInput{
			CatalogId:          g.catalogID,
			DatabaseName:       aws.String(id.Database),
			TableName:          aws.String(id.Table),
			PartitionValueList: p.Values,
			PartitionInput: &gluetypes.PartitionInput{
				Values:            p.Values,
				StorageDescriptor: partitionDescriptor(descriptor, p.StorageLocation),
			},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("partition %v: %w", p.Values, err))
		}
	}
	return errors.Join(errs...)
}

// DropPartitions removes partitions, chunked to Glue's per-call delete limit.
func (g *GluePartitionSyncOperations) DropPartitions(ctx context.Context, id TableIdentifier, partitions []Partition) error {
	if len(partitions) == 0 {
		return nil
	}

	return inBatches(partitions, glueMaxDeletePartitionsPerCall, func(batch []Partition) error {
		toDelete := make([]gluetypes.PartitionValueList, 0, len(batch))
		for _, p := range batch {
			toDelete = append(toDelete, gluetypes.PartitionValueList{Values: p.Values})
		}

		out, err := g.client.BatchDeletePartition(ctx, &glue.BatchDeletePartitionInput{
			CatalogId:          g.catalogID,
			DatabaseName:       aws.String(id.Database),
			TableName:          aws.String(id.Table),
			PartitionsToDelete: toDelete,
		})
		if err != nil {
			return err
		}
		return glueBatchDeleteErrors(out.Errors)
	})
}

// tableStorageDescriptor fetches the table's storage descriptor so partitions inherit its serde and
// column layout. Registering a partition without one leaves it unreadable by query engines.
func (g *GluePartitionSyncOperations) tableStorageDescriptor(ctx context.Context, id TableIdentifier) (*gluetypes.StorageDescriptor, error) {
	out, err := g.client.GetTable(ctx, &glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(id.Database),
		Name:         aws.String(id.Table),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read table %s before syncing partitions: %w", id, err)
	}
	if out == nil || out.Table == nil || out.Table.StorageDescriptor == nil {
		return nil, fmt.Errorf("table %s has no storage descriptor in Glue", id)
	}
	return out.Table.StorageDescriptor, nil
}

// partitionDescriptor copies the table descriptor and points it at the partition's own location.
func partitionDescriptor(table *gluetypes.StorageDescriptor, location string) *gluetypes.StorageDescriptor {
	cp := *table
	cp.Location = aws.String(location)
	return &cp
}

func glueBatchErrors(op string, entries []gluetypes.PartitionError) error {
	var errs []error
	for _, e := range entries {
		msg := ""
		if e.ErrorDetail != nil {
			msg = aws.ToString(e.ErrorDetail.ErrorMessage)
		}
		errs = append(errs, fmt.Errorf("failed to %s partition %v: %s", op, e.PartitionValues, msg))
	}
	return errors.Join(errs...)
}

func glueBatchDeleteErrors(entries []gluetypes.PartitionError) error {
	return glueBatchErrors("delete", entries)
}
