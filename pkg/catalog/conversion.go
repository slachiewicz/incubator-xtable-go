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

	"github.com/slachiewicz/polytable/pkg/model"
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
	// PropTargetFormats lists the formats a discovered table should be converted to, as a
	// comma-separated list of model.TableFormat values (e.g. "ICEBERG,DELTA"). This key is
	// polytable's own convention: Java XTable has no target-format property at all, and the AWS
	// Lambda reference architecture's xtable_target_formats is that solution's private convention,
	// deliberately not adopted here. The polytable_ prefix matches the polytable_synced_time
	// parameter GlueCatalogSyncClient already writes.
	PropTargetFormats = "polytable_target_formats"
)

// TableFilter selects which catalog tables a listing yields. Its zero value selects every table.
type TableFilter struct {
	// RequireConversionMarkers keeps only tables carrying a non-blank PropTargetFormats, i.e. the
	// tables whose owner opted them into conversion.
	RequireConversionMarkers bool
}

// Matches reports whether a table with these properties passes the filter.
func (f TableFilter) Matches(properties map[string]string) bool {
	if f.RequireConversionMarkers && strings.TrimSpace(properties[PropTargetFormats]) == "" {
		return false
	}
	return true
}

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

	// ListTables walks every table in a catalog database that passes filter. The sequence yields a
	// zero identifier with a non-nil error when the catalog call fails, and stops there rather than
	// truncating silently; a caller that stops early stops the paging with it.
	ListTables(ctx context.Context, database string, filter TableFilter) iter.Seq2[TableIdentifier, error]

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

// TargetFormatsFromProperties resolves the formats a table is marked for conversion to, from the
// PropTargetFormats property. A table carrying no marker returns (nil, nil): it is not opted in, so
// discovery skips it rather than failing it. A marker naming a format this build does not know is
// an error, since that is a typo the operator wants to hear about.
func TargetFormatsFromProperties(properties map[string]string) ([]model.TableFormat, error) {
	raw := strings.TrimSpace(properties[PropTargetFormats])
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	formats := make([]model.TableFormat, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		format, err := model.ParseTableFormat(part)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", PropTargetFormats, err)
		}
		formats = append(formats, format)
	}
	if len(formats) == 0 {
		return nil, nil
	}
	return formats, nil
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
