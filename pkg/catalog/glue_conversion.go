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
	"fmt"
	"iter"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// glueTableReader is the slice of the Glue API this conversion source needs. Declaring it here keeps
// the source testable without reaching AWS; *glue.Client satisfies it. The GetTables signature is
// glue.GetTablesAPIClient's, so an implementation can be handed straight to the SDK's paginator.
type glueTableReader interface {
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

// GlueConversionSource resolves tables registered in the AWS Glue Data Catalog.
type GlueConversionSource struct {
	client    glueTableReader
	catalogID *string
}

var _ ConversionSource = (*GlueConversionSource)(nil)

// NewGlueConversionSource creates a Glue-backed catalog conversion source.
func NewGlueConversionSource(ctx context.Context, cfg *Config) (*GlueConversionSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	var catID *string
	if cfg.CatalogID != "" {
		catID = aws.String(cfg.CatalogID)
	}

	return &GlueConversionSource{client: glue.NewFromConfig(awsCfg), catalogID: catID}, nil
}

// NewGlueConversionSourceWithClient creates a source over an existing Glue API client.
func NewGlueConversionSourceWithClient(client glueTableReader, catalogID *string) *GlueConversionSource {
	return &GlueConversionSource{client: client, catalogID: catalogID}
}

// CatalogType returns AWS_GLUE.
func (g *GlueConversionSource) CatalogType() CatalogType {
	return CatalogTypeGlue
}

// GetSourceTable resolves a Glue table into a SourceTable.
func (g *GlueConversionSource) GetSourceTable(ctx context.Context, id TableIdentifier) (*SourceTable, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}

	out, err := g.client.GetTable(ctx, &glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(id.Database),
		Name:         aws.String(id.Table),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get table %s from Glue: %w", id, err)
	}
	if out == nil || out.Table == nil {
		return nil, fmt.Errorf("glue returned no table for %s", id)
	}

	table := out.Table
	format, err := TableFormatFromProperties(table.Parameters)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", id, err)
	}

	if table.StorageDescriptor == nil || aws.ToString(table.StorageDescriptor.Location) == "" {
		return nil, fmt.Errorf("table %s has no storage location recorded in Glue", id)
	}
	basePath := aws.ToString(table.StorageDescriptor.Location)

	dataPath, err := DataLocationForFormat(format, basePath, table.Parameters)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", id, err)
	}

	// Copy so callers cannot mutate the SDK's map.
	properties := make(map[string]string, len(table.Parameters))
	for k, v := range table.Parameters {
		properties[k] = v
	}

	name := aws.ToString(table.Name)
	if name == "" {
		name = id.Table
	}

	return &SourceTable{
		Name:       name,
		BasePath:   basePath,
		DataPath:   dataPath,
		Format:     format,
		Properties: properties,
	}, nil
}

// ListTables pages through a Glue database with the SDK's GetTables paginator, yielding every table
// that passes filter. A failing page yields the error and ends the sequence, so a caller that only
// checks identifiers cannot mistake a truncated listing for a complete one.
func (g *GlueConversionSource) ListTables(ctx context.Context, database string, filter TableFilter) iter.Seq2[TableIdentifier, error] {
	return func(yield func(TableIdentifier, error) bool) {
		database = strings.TrimSpace(database)
		if database == "" {
			yield(TableIdentifier{}, fmt.Errorf("catalog table listing requires a database"))
			return
		}

		paginator := glue.NewGetTablesPaginator(g.client, &glue.GetTablesInput{
			CatalogId:    g.catalogID,
			DatabaseName: aws.String(database),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(TableIdentifier{}, fmt.Errorf("failed to list tables in glue database %s: %w", database, err))
				return
			}
			for _, table := range page.TableList {
				name := aws.ToString(table.Name)
				if name == "" || !filter.Matches(table.Parameters) {
					continue
				}
				if !yield(TableIdentifier{Database: database, Table: name}, nil) {
					return
				}
			}
		}
	}
}

// Close releases resources. The Glue SDK client needs no explicit teardown.
func (g *GlueConversionSource) Close() error {
	return nil
}
