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

package conversion

import (
	"context"
	"fmt"

	"github.com/slachiewicz/xtable-go/pkg/catalog"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// StorageConfig carries optional object-store overrides (custom endpoint, path-style addressing).
type StorageConfig struct {
	// Region is the AWS region for S3 storage (e.g., "us-west-2").
	Region string `json:"region,omitempty" yaml:"region,omitempty"`
	// Endpoint is the custom S3 endpoint URL (e.g., for MinIO or other S3-compatible services).
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// UsePathStyle enables path-style addressing for S3 (default is virtual-hosted style).
	UsePathStyle bool `json:"usePathStyle,omitempty" yaml:"usePathStyle,omitempty"`
}

// DatasetConfig defines the synchronization configuration for a single table dataset.
type DatasetConfig struct {
	// SourceFormat is the input table format (e.g. DELTA, ICEBERG, HUDI).
	SourceFormat model.TableFormat `json:"sourceFormat" yaml:"sourceFormat"`
	// TargetFormats is the list of formats to synchronize to (e.g. [ICEBERG, DELTA]).
	TargetFormats []model.TableFormat `json:"targetFormats" yaml:"targetFormats"`
	// TableBasePath is the root directory path of the table.
	TableBasePath string `json:"tableBasePath" yaml:"tableBasePath"`
	// TableDataPath is the optional data directory path (defaults to TableBasePath).
	TableDataPath string `json:"tableDataPath,omitempty" yaml:"tableDataPath,omitempty"`
	// TableName is the logical table name.
	TableName string `json:"tableName,omitempty" yaml:"tableName,omitempty"`
	// Namespace is an optional database/catalog namespace (e.g. "default.sales").
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// SyncMode controls FULL snapshot sync vs INCREMENTAL sync.
	SyncMode spi.SyncMode `json:"syncMode,omitempty" yaml:"syncMode,omitempty"`
	// Storage carries optional object-store overrides (custom endpoint, path-style addressing).
	Storage *StorageConfig `json:"storage,omitempty" yaml:"storage,omitempty"`
	// Catalogs lists external catalogs to register the synced table in. Optional.
	Catalogs []catalog.Config `json:"catalogs,omitempty" yaml:"catalogs,omitempty"`
	// SourceCatalog resolves the source table from an external catalog instead of requiring
	// tableBasePath. Exactly one of the two must be supplied.
	SourceCatalog *SourceCatalogConfig `json:"sourceCatalog,omitempty" yaml:"sourceCatalog,omitempty"`
}

// SourceCatalogConfig names a table inside an external catalog, so a dataset can be addressed as
// db.table rather than by a storage path the caller has to know in advance.
type SourceCatalogConfig struct {
	// Catalog is the catalog to resolve against. Its DatabaseName supplies the database.
	Catalog catalog.Config `json:"catalog" yaml:"catalog"`
	// Table is the table name within the catalog database.
	Table string `json:"table" yaml:"table"`
}

// Validate reports whether the source catalog reference is usable.
func (s *SourceCatalogConfig) Validate() error {
	if s.Table == "" {
		return fmt.Errorf("sourceCatalog.table is required")
	}
	if s.Catalog.DatabaseName == "" {
		return fmt.Errorf("sourceCatalog.catalog.databaseName is required")
	}
	return s.Catalog.Validate()
}

// ResolveSourceCatalog fills SourceFormat, TableBasePath, TableDataPath and TableName from the
// catalog entry named by SourceCatalog. It must run before storage is constructed, since the
// storage backend is chosen from the resolved base path.
//
// newSource is injectable for tests; pass nil to use catalog.NewConversionSource.
func ResolveSourceCatalog(ctx context.Context, cfg *DatasetConfig,
	newSource func(context.Context, *catalog.Config) (catalog.ConversionSource, error)) error {
	if cfg == nil || cfg.SourceCatalog == nil {
		return nil
	}
	if err := cfg.SourceCatalog.Validate(); err != nil {
		return err
	}
	if newSource == nil {
		newSource = catalog.NewConversionSource
	}

	src, err := newSource(ctx, &cfg.SourceCatalog.Catalog)
	if err != nil {
		return fmt.Errorf("failed to create catalog conversion source: %w", err)
	}
	defer func() { _ = src.Close() }()

	id := catalog.TableIdentifier{Database: cfg.SourceCatalog.Catalog.DatabaseName, Table: cfg.SourceCatalog.Table}
	resolved, err := src.GetSourceTable(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to resolve %s from catalog: %w", id, err)
	}

	// Explicit configuration wins, so a resolved table can still be overridden field by field.
	if cfg.SourceFormat == "" {
		cfg.SourceFormat = resolved.Format
	}
	if cfg.TableBasePath == "" {
		cfg.TableBasePath = resolved.BasePath
	}
	if cfg.TableDataPath == "" {
		cfg.TableDataPath = resolved.DataPath
	}
	if cfg.TableName == "" {
		cfg.TableName = resolved.Name
	}
	return nil
}

// ToS3OptionFuncs converts StorageConfig to S3 option functions for storage initialization.
func (c *StorageConfig) ToS3OptionFuncs() []func(*io.S3Options) {
	if c == nil {
		return nil
	}

	optFns := make([]func(*io.S3Options), 0, 3)

	if c.Region != "" {
		optFns = append(optFns, func(opts *io.S3Options) {
			opts.Region = c.Region
		})
	}

	if c.Endpoint != "" {
		optFns = append(optFns, func(opts *io.S3Options) {
			opts.Endpoint = c.Endpoint
		})
	}

	if c.UsePathStyle {
		optFns = append(optFns, func(opts *io.S3Options) {
			opts.UsePathStyle = true
		})
	}

	return optFns
}

// Validate validates that required dataset configuration parameters are provided.
func (c *DatasetConfig) Validate() error {
	if c.SourceFormat == "" {
		return fmt.Errorf("sourceFormat is required")
	}
	if len(c.TargetFormats) == 0 {
		return fmt.Errorf("at least one targetFormat must be specified")
	}
	if c.TableBasePath == "" {
		// A source catalog reference stands in for the path; ResolveSourceCatalog fills it in
		// before storage is constructed.
		if c.SourceCatalog == nil {
			return fmt.Errorf("either tableBasePath or sourceCatalog is required")
		}
		return c.SourceCatalog.Validate()
	}
	if c.SyncMode == "" {
		c.SyncMode = spi.SyncModeIncremental
	}
	return nil
}

// Config represents top-level YAML / JSON configuration file containing multiple datasets.
type Config struct {
	SourceFormat  model.TableFormat   `json:"sourceFormat,omitempty" yaml:"sourceFormat,omitempty"`
	TargetFormats []model.TableFormat `json:"targetFormats,omitempty" yaml:"targetFormats,omitempty"`
	Datasets      []*DatasetConfig    `json:"datasets" yaml:"datasets"`
}
