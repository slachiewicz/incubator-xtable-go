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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// GlueCatalogSyncClient manages table and partition metadata synchronization to AWS Glue Data Catalog.
type GlueCatalogSyncClient struct {
	client       *glue.Client
	databaseName string
	catalogID    *string
	// Embedded so the four PartitionSyncOperations methods promote onto this client: Glue is a
	// Hive-style catalog that tracks partitions separately from the table definition.
	*GluePartitionSyncOperations
}

var (
	_ SyncClient              = (*GlueCatalogSyncClient)(nil)
	_ PartitionSyncOperations = (*GlueCatalogSyncClient)(nil)
)

// NewGlueCatalogSyncClient creates a new AWS Glue Catalog sync client.
func NewGlueCatalogSyncClient(ctx context.Context, cfg *Config) (*GlueCatalogSyncClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	client := glue.NewFromConfig(awsCfg)
	var catID *string
	if cfg.CatalogID != "" {
		catID = aws.String(cfg.CatalogID)
	}

	return &GlueCatalogSyncClient{
		client:                      client,
		databaseName:                cfg.DatabaseName,
		catalogID:                   catID,
		GluePartitionSyncOperations: NewGluePartitionSyncOperations(client, catID),
	}, nil
}

// NewGlueCatalogSyncClientWithClient creates a client with an existing glue.Client.
func NewGlueCatalogSyncClientWithClient(client *glue.Client, databaseName string, catalogID *string) *GlueCatalogSyncClient {
	return &GlueCatalogSyncClient{
		client:                      client,
		databaseName:                databaseName,
		catalogID:                   catalogID,
		GluePartitionSyncOperations: NewGluePartitionSyncOperations(client, catalogID),
	}
}

// CatalogType returns AWS_GLUE.
func (g *GlueCatalogSyncClient) CatalogType() CatalogType {
	return CatalogTypeGlue
}

// CreateOrUpdateTable registers or updates the table definition in AWS Glue Data Catalog.
func (g *GlueCatalogSyncClient) CreateOrUpdateTable(ctx context.Context, table *model.Table, snapshot *model.Snapshot) error {
	if table == nil {
		return fmt.Errorf("table cannot be nil")
	}

	tableName := table.Name
	tableInput := g.buildTableInput(table, snapshot)

	// Check if table already exists in Glue
	_, err := g.client.GetTable(ctx, &glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(g.databaseName),
		Name:         aws.String(tableName),
	})

	if err != nil {
		var notFound *gluetypes.EntityNotFoundException
		var apiErr smithy.APIError
		if errors.As(err, &notFound) || (errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityNotFoundException") {
			// Create Table
			_, createErr := g.client.CreateTable(ctx, &glue.CreateTableInput{
				CatalogId:    g.catalogID,
				DatabaseName: aws.String(g.databaseName),
				TableInput:   tableInput,
			})
			if createErr != nil {
				return fmt.Errorf("failed to create table %s in glue database %s: %w", tableName, g.databaseName, createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to check existing table in glue: %w", err)
	}

	// Update existing Table
	_, updateErr := g.client.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(g.databaseName),
		TableInput:   tableInput,
	})
	if updateErr != nil {
		return fmt.Errorf("failed to update table %s in glue database %s: %w", tableName, g.databaseName, updateErr)
	}
	return nil
}

// DropTable removes the table from AWS Glue.
func (g *GlueCatalogSyncClient) DropTable(ctx context.Context, databaseName, tableName string) error {
	db := g.databaseName
	if databaseName != "" {
		db = databaseName
	}
	_, err := g.client.DeleteTable(ctx, &glue.DeleteTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(db),
		Name:         aws.String(tableName),
	})
	return err
}

// Close is a no-op for GlueCatalogSyncClient.
func (g *GlueCatalogSyncClient) Close() error {
	return nil
}

func (g *GlueCatalogSyncClient) buildTableInput(table *model.Table, _ *model.Snapshot) *gluetypes.TableInput {
	var columns []gluetypes.Column
	partFieldMap := make(map[string]bool)
	for _, pf := range table.PartitioningFields {
		partFieldMap[pf.SourceField.Name] = true
	}

	if table.ReadSchema != nil {
		for _, f := range table.ReadSchema.Fields {
			if !partFieldMap[f.Name] {
				columns = append(columns, gluetypes.Column{
					Name:    aws.String(f.Name),
					Type:    aws.String(ModelTypeToGlueType(f.Schema)),
					Comment: aws.String(f.Schema.Comment),
				})
			}
		}
	}

	var partitionKeys []gluetypes.Column
	for _, pf := range table.PartitioningFields {
		partitionKeys = append(partitionKeys, gluetypes.Column{
			Name: aws.String(pf.SourceField.Name),
			Type: aws.String(ModelTypeToGlueType(pf.SourceField.Schema)),
		})
	}

	parameters := make(map[string]string)
	parameters["EXTERNAL"] = "TRUE"
	parameters["xtable_synced_time"] = fmt.Sprintf("%d", table.LatestCommitTime)

	inputFormat := "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	outputFormat := "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
	serdeLib := "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

	switch table.TableFormat {
	case model.TableFormatDelta:
		parameters["spark.sql.sources.provider"] = "delta"
		parameters["table_type"] = "DELTA"
	case model.TableFormatIceberg:
		parameters["table_type"] = "ICEBERG"
		inputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
		outputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
		serdeLib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
	case model.TableFormatHudi:
		parameters["spark.sql.sources.provider"] = "hudi"
		inputFormat = "org.apache.hudi.hadoop.HoodieParquetInputFormat"
	}

	return &gluetypes.TableInput{
		Name:          aws.String(table.Name),
		TableType:     aws.String("EXTERNAL_TABLE"),
		Parameters:    parameters,
		PartitionKeys: partitionKeys,
		StorageDescriptor: &gluetypes.StorageDescriptor{
			Location:     aws.String(table.BasePath),
			Columns:      columns,
			InputFormat:  aws.String(inputFormat),
			OutputFormat: aws.String(outputFormat),
			SerdeInfo: &gluetypes.SerDeInfo{
				SerializationLibrary: aws.String(serdeLib),
			},
		},
	}
}

// ModelTypeToGlueType converts canonical model.Schema type into AWS Glue Hive data type string.
func ModelTypeToGlueType(s *model.Schema) string {
	if s == nil {
		return "string"
	}
	switch s.DataType {
	case model.TypeBoolean:
		return "boolean"
	case model.TypeInt:
		return "int"
	case model.TypeLong:
		return "bigint"
	case model.TypeFloat:
		return "float"
	case model.TypeDouble:
		return "double"
	case model.TypeString, model.TypeEnum, model.TypeUUID:
		return "string"
	case model.TypeBytes, model.TypeFixed:
		return "binary"
	case model.TypeDate:
		return "date"
	case model.TypeTimestamp, model.TypeTimestampNTZ:
		return "timestamp"
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		return fmt.Sprintf("decimal(%d,%d)", precision, scale)
	case model.TypeList:
		elemType := ModelTypeToGlueType(s.ElementSchema.Schema)
		return fmt.Sprintf("array<%s>", elemType)
	case model.TypeMap:
		kType := ModelTypeToGlueType(s.KeySchema.Schema)
		vType := ModelTypeToGlueType(s.ValueSchema.Schema)
		return fmt.Sprintf("map<%s,%s>", kType, vType)
	default:
		return "string"
	}
}
