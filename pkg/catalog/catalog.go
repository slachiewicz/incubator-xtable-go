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

	"github.com/apache/incubator-xtable-go/pkg/model"
)

// CatalogType represents supported external metastores / data catalogs.
type CatalogType string

const (
	CatalogTypeGlue        CatalogType = "AWS_GLUE"
	CatalogTypeHMS         CatalogType = "HIVE_METASTORE"
	CatalogTypeIcebergREST CatalogType = "ICEBERG_REST"
)

// Config holds configuration parameters for connecting to an external catalog.
type Config struct {
	Type         CatalogType       `json:"type" yaml:"type"`
	CatalogID    string            `json:"catalogId,omitempty" yaml:"catalogId,omitempty"`
	DatabaseName string            `json:"databaseName" yaml:"databaseName"`
	URI          string            `json:"uri,omitempty" yaml:"uri,omitempty"`
	Properties   map[string]string `json:"properties,omitempty" yaml:"properties,omitempty"`
}

// Validate validates catalog configuration settings.
func (c *Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("catalog type is required")
	}
	if c.DatabaseName == "" {
		return fmt.Errorf("databaseName is required")
	}
	return nil
}

// SyncClient defines the standard contract for synchronizing table metadata into external data catalogs.
type SyncClient interface {
	// CatalogType returns the catalog type identifier.
	CatalogType() CatalogType

	// CreateOrUpdateTable registers or updates the table metadata and schema in the external catalog.
	CreateOrUpdateTable(ctx context.Context, table *model.Table, snapshot *model.Snapshot) error

	// DropTable removes the table registration from the external catalog.
	DropTable(ctx context.Context, databaseName, tableName string) error

	// Close releases any network connections.
	Close() error
}
