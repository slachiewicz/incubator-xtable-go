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
)

// CatalogSourceFactory constructs a catalog conversion source from its configuration. It is the
// read-side counterpart of CatalogClientFactory, and the seam tests use to stand in a fake catalog.
type CatalogSourceFactory func(ctx context.Context, cfg *catalog.Config) (catalog.ConversionSource, error)

// DiscoverDatasets turns a catalog database into one DatasetConfig per table marked for conversion,
// so a caller can sync a whole database without writing a config file. Source format, paths and name
// come from the catalog entry; target formats come from the table's catalog.PropTargetFormats
// property.
//
// A table carrying no target-format marker is skipped, not failed — an unmarked table has simply not
// opted in. Anything else is fatal to the whole scan: a listing error surfaces rather than
// truncating the result, and a table that is marked but unreadable (no format, no location, an
// unknown target format) stops discovery naming that table, since silently dropping it would look
// identical to it not being marked.
//
// newSource is injectable for tests; pass nil to use catalog.NewConversionSource.
func DiscoverDatasets(ctx context.Context, cfg *catalog.Config, newSource CatalogSourceFactory) ([]*DatasetConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("a catalog configuration is required to discover tables")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if newSource == nil {
		newSource = catalog.NewConversionSource
	}

	src, err := newSource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog conversion source: %w", err)
	}
	defer func() { _ = src.Close() }()

	var datasets []*DatasetConfig
	filter := catalog.TableFilter{RequireConversionMarkers: true}

	for id, listErr := range src.ListTables(ctx, cfg.DatabaseName, filter) {
		if listErr != nil {
			return nil, fmt.Errorf("failed to list tables in catalog database %s: %w", cfg.DatabaseName, listErr)
		}

		resolved, err := src.GetSourceTable(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve %s from catalog: %w", id, err)
		}

		targets, err := catalog.TargetFormatsFromProperties(resolved.Properties)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", id, err)
		}
		if len(targets) == 0 {
			// The filter already drops unmarked tables; a source that filters less strictly than
			// Glue does still must not turn an unmarked table into a failure.
			continue
		}

		name := resolved.Name
		if name == "" {
			name = id.Table
		}
		datasets = append(datasets, &DatasetConfig{
			SourceFormat:  resolved.Format,
			TargetFormats: targets,
			TableBasePath: resolved.BasePath,
			TableDataPath: resolved.DataPath,
			TableName:     name,
			Namespace:     id.Database,
		})
	}

	return datasets, nil
}
