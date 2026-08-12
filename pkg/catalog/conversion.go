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
	"strings"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// Table property keys used to describe a table registered in an external catalog. These mirror the
// keys Java XTable reads and the ones GlueCatalogSyncClient writes, so a table registered by either
// implementation resolves identically.
const (
	// PropTableType is the primary format marker, e.g. "ICEBERG" or "DELTA".
	PropTableType = "table_type"
	// PropSparkSQLSourcesProvider is Spark's data-source provider, used as a fallback format marker.
	// Delta tables registered by Spark carry "delta" here and often nothing in table_type.
	PropSparkSQLSourcesProvider = "spark.sql.sources.provider"
	// PropWriteDataLocation and the two below are the Iceberg table properties that relocate data
	// away from <basePath>/data, checked in this order.
	PropWriteDataLocation          = "write.data.path"
	PropWriteFolderStorageLocation = "write.folder-storage.path"
	PropObjectStorePath            = "write.object-storage.path"
)

// TableIdentifier addresses a table inside an external catalog. It is the catalog-side equivalent of
// a base path: the whole point of a ConversionSource is to accept one of these instead.
type TableIdentifier struct {
	// Database is the catalog database or namespace holding the table.
	Database string
	// Table is the table name within that database.
	Table string
}

// String renders the identifier as "database.table".
func (t TableIdentifier) String() string {
	return t.Database + "." + t.Table
}

// Validate reports whether the identifier is usable.
func (t TableIdentifier) Validate() error {
	if t.Database == "" {
		return fmt.Errorf("catalog table identifier requires a database")
	}
	if t.Table == "" {
		return fmt.Errorf("catalog table identifier requires a table name")
	}
	return nil
}

// SourceTable describes a table resolved out of an external catalog, carrying everything needed to
// construct a ConversionSource for it without the caller knowing a base path up front.
type SourceTable struct {
	// Name is the table name as the catalog records it.
	Name string
	// BasePath is the table's root location in storage.
	BasePath string
	// DataPath is where the data files live. It equals BasePath for Delta and Hudi; Iceberg tables
	// may relocate it via write.data.path and friends.
	DataPath string
	// Format is the table format resolved from the catalog's table properties.
	Format model.TableFormat
	// Properties is the full property map the catalog returned, for callers needing more.
	Properties map[string]string
}

// ConversionSource resolves a table registered in an external catalog into a SourceTable. This is
// the read half of catalog integration; SyncClient is the write half.
type ConversionSource interface {
	// CatalogType returns the catalog type identifier.
	CatalogType() CatalogType

	// GetSourceTable resolves a catalog entry into a SourceTable.
	GetSourceTable(ctx context.Context, id TableIdentifier) (*SourceTable, error)

	// Close releases any network connections.
	Close() error
}

// TableFormatFromProperties resolves a table format from catalog table properties. It prefers
// table_type and falls back to spark.sql.sources.provider, matching Java's TableFormatUtils.
func TableFormatFromProperties(properties map[string]string) (model.TableFormat, error) {
	raw := strings.TrimSpace(properties[PropTableType])
	if raw == "" {
		raw = strings.TrimSpace(properties[PropSparkSQLSourcesProvider])
	}
	if raw == "" {
		return "", fmt.Errorf("catalog table properties carry neither %q nor %q, so the table format cannot be determined",
			PropTableType, PropSparkSQLSourcesProvider)
	}
	return model.ParseTableFormat(strings.ToUpper(raw))
}

// DataLocationForFormat resolves where a table's data files live. Delta and Hudi keep data under the
// table location; Iceberg may relocate it, defaulting to <basePath>/data.
func DataLocationForFormat(format model.TableFormat, basePath string, properties map[string]string) (string, error) {
	switch format {
	case model.TableFormatDelta, model.TableFormatHudi, model.TableFormatPaimon, model.TableFormatParquet:
		return basePath, nil
	case model.TableFormatIceberg:
		for _, key := range []string{PropWriteDataLocation, PropWriteFolderStorageLocation, PropObjectStorePath} {
			if v := strings.TrimSpace(properties[key]); v != "" {
				return v, nil
			}
		}
		return strings.TrimSuffix(basePath, "/") + "/data", nil
	default:
		return "", fmt.Errorf("unsupported table format for data location: %s", format)
	}
}

// NewConversionSource creates a catalog conversion source for the configured catalog type.
func NewConversionSource(ctx context.Context, cfg *Config) (ConversionSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	switch cfg.Type {
	case CatalogTypeGlue:
		return NewGlueConversionSource(ctx, cfg)
	case CatalogTypeIcebergREST:
		return NewIcebergRESTConversionSource(cfg)
	case CatalogTypeHMS:
		return nil, fmt.Errorf("%w: Hive Metastore (HMS) catalog support is not implemented", ErrCatalogNotImplemented)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", cfg.Type)
	}
}
