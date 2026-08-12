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

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// glueTableReader is the slice of the Glue API this conversion source needs. Declaring it here keeps
// the source testable without reaching AWS; *glue.Client satisfies it.
type glueTableReader interface {
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
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

// Close releases resources. The Glue SDK client needs no explicit teardown.
func (g *GlueConversionSource) Close() error {
	return nil
}
