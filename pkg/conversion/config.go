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

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// StorageConfig carries optional object-store overrides (custom endpoint, path-style addressing).
// Its top-level fields configure S3 and are named without a prefix for backward compatibility; the
// Azure block is nested.
type StorageConfig struct {
	// Region is the AWS region for S3 storage (e.g., "us-west-2").
	Region string `json:"region,omitempty" yaml:"region,omitempty"`
	// Endpoint is the custom S3 endpoint URL (e.g., for MinIO or other S3-compatible services).
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// UsePathStyle enables path-style addressing for S3 (default is virtual-hosted style).
	UsePathStyle bool `json:"usePathStyle,omitempty" yaml:"usePathStyle,omitempty"`
	// Azure carries Azure Data Lake Storage and OneLake overrides.
	Azure *AzureStorageConfig `json:"azure,omitempty" yaml:"azure,omitempty"`
	// GCS carries Google Cloud Storage overrides.
	GCS *GCSStorageConfig `json:"gcs,omitempty" yaml:"gcs,omitempty"`
}

// AzureStorageConfig carries Azure Data Lake Storage and OneLake overrides.
//
// It holds no credentials on purpose, matching S3: secrets reach the process through the
// environment or the Entra ID credential chain, never through a configuration file that gets
// committed, logged or POSTed to the REST service. AccountKeyEnv and SASTokenEnv follow the same
// rule at one remove — they name the environment variable holding a secret, never the secret
// itself, so a config file naming two different variables lets one process serve two storage
// accounts with different keys.
type AzureStorageConfig struct {
	// Endpoint overrides the blob service URL derived from the path's host. Azurite needs it, and
	// so does any deployment whose blob host is not derivable from its abfss:// host.
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// AccountName overrides the storage account parsed from the path's host. Azurite needs it:
	// its service URL carries the account in the path rather than the host.
	AccountName string `json:"accountName,omitempty" yaml:"accountName,omitempty"`
	// AccountKeyEnv names the environment variable that holds the shared account key — never
	// the key itself. This lets a daemon serving multiple datasets point each one at a
	// different account's key without the two colliding on the well-known AZURE_STORAGE_KEY,
	// and without ever writing the secret into a config file that gets committed, logged, or
	// POSTed to the REST service.
	AccountKeyEnv string `json:"accountKeyEnv,omitempty" yaml:"accountKeyEnv,omitempty"`
	// SASTokenEnv names the environment variable that holds the SAS token — never the token
	// itself, for the same reason as AccountKeyEnv.
	SASTokenEnv string `json:"sasTokenEnv,omitempty" yaml:"sasTokenEnv,omitempty"`
	// Anonymous selects unauthenticated access, for a public container.
	Anonymous bool `json:"anonymous,omitempty" yaml:"anonymous,omitempty"`
}

// GCSStorageConfig carries Google Cloud Storage overrides.
//
// It holds no credentials on purpose, matching Azure and S3: secrets reach the process through
// the Application Default Credentials chain (GOOGLE_APPLICATION_CREDENTIALS, gcloud user
// credentials, or GCE/GKE workload identity), never through a configuration file that gets
// committed, logged or POSTed to the REST service. CredentialsFile names a service-account JSON
// file's path, never the JSON itself.
type GCSStorageConfig struct {
	// Endpoint overrides the storage service URL. Required for a fake or emulator, e.g.
	// fake-gcs-server.
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// AnonymousAccess selects unauthenticated access, for a public bucket.
	AnonymousAccess bool `json:"anonymousAccess,omitempty" yaml:"anonymousAccess,omitempty"`
	// CredentialsFile names a service-account JSON file's path, never its contents.
	CredentialsFile string `json:"credentialsFile,omitempty" yaml:"credentialsFile,omitempty"`
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
func ResolveSourceCatalog(ctx context.Context, cfg *DatasetConfig, newSource CatalogSourceFactory) error {
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

// ToOptionFuncs converts StorageConfig to storage option functions. It is the single place this
// translation lives: the CLI, the daemon, the REST server and the C bindings all call it, and the
// one that used to inline its own copy drifted.
func (c *StorageConfig) ToOptionFuncs() []func(*io.Options) {
	if c == nil {
		return nil
	}

	optFns := make([]func(*io.Options), 0, 8)

	if c.Region != "" {
		optFns = append(optFns, func(opts *io.Options) {
			opts.S3.Region = c.Region
		})
	}

	if c.Endpoint != "" {
		optFns = append(optFns, func(opts *io.Options) {
			opts.S3.Endpoint = c.Endpoint
		})
	}

	if c.UsePathStyle {
		optFns = append(optFns, func(opts *io.Options) {
			opts.S3.UsePathStyle = true
		})
	}

	if c.Azure != nil {
		if c.Azure.Endpoint != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.Azure.Endpoint = c.Azure.Endpoint
			})
		}

		if c.Azure.AccountName != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.Azure.AccountName = c.Azure.AccountName
			})
		}

		if c.Azure.AccountKeyEnv != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.Azure.AccountKeyEnv = c.Azure.AccountKeyEnv
			})
		}

		if c.Azure.SASTokenEnv != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.Azure.SASTokenEnv = c.Azure.SASTokenEnv
			})
		}

		if c.Azure.Anonymous {
			optFns = append(optFns, func(opts *io.Options) {
				opts.Azure.Anonymous = true
			})
		}
	}

	if c.GCS != nil {
		if c.GCS.Endpoint != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.GCS.Endpoint = c.GCS.Endpoint
			})
		}

		if c.GCS.AnonymousAccess {
			optFns = append(optFns, func(opts *io.Options) {
				opts.GCS.AnonymousAccess = true
			})
		}

		if c.GCS.CredentialsFile != "" {
			optFns = append(optFns, func(opts *io.Options) {
				opts.GCS.CredentialsFile = c.GCS.CredentialsFile
			})
		}
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
