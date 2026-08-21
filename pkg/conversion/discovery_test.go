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

// Tests for T23 (docs/improvement-plan.md): turning a catalog database into dataset configurations.
package conversion_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/model"
)

// discoveredTable builds the catalog view of a table marked for the given target formats.
func discoveredTable(name, targets string) *catalog.SourceTable {
	properties := map[string]string{"table_type": "DELTA"}
	if targets != "" {
		properties[catalog.PropTargetFormats] = targets
	}
	return &catalog.SourceTable{
		Name:       name,
		BasePath:   "s3://lake/" + name,
		DataPath:   "s3://lake/" + name,
		Format:     model.TableFormatDelta,
		Properties: properties,
	}
}

func TestDiscoverDatasets(t *testing.T) {
	t.Parallel()

	glueCfg := func() *catalog.Config {
		return &catalog.Config{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"}
	}

	t.Run("materialises one dataset per marked table", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{
			listed: []string{"events", "orders"},
			tables: map[string]*catalog.SourceTable{
				"events": discoveredTable("events", "ICEBERG"),
				"orders": discoveredTable("orders", "ICEBERG,HUDI"),
			},
		}

		got, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.NoError(t, err)
		require.Len(t, got, 2)

		assert.Equal(t, "events", got[0].TableName)
		assert.Equal(t, model.TableFormatDelta, got[0].SourceFormat)
		assert.Equal(t, []model.TableFormat{model.TableFormatIceberg}, got[0].TargetFormats)
		assert.Equal(t, "s3://lake/events", got[0].TableBasePath)
		assert.Equal(t, "s3://lake/events", got[0].TableDataPath)
		assert.Equal(t, "analytics", got[0].Namespace)
		assert.NoError(t, got[0].Validate(), "a discovered dataset must be directly syncable")

		assert.Equal(t, []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi}, got[1].TargetFormats)
		assert.True(t, fake.gotFilter.RequireConversionMarkers, "discovery must ask the catalog for marked tables only")
		assert.Equal(t, []string{"analytics"}, fake.gotListDBs)
		assert.True(t, fake.closed, "the conversion source must be closed")
	})

	t.Run("a table with no target-format property is skipped, not failed", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{
			listed: []string{"events", "legacy"},
			tables: map[string]*catalog.SourceTable{
				"events": discoveredTable("events", "ICEBERG"),
				"legacy": discoveredTable("legacy", ""),
			},
		}

		got, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.NoError(t, err, "an unmarked table is opted out, not broken")
		require.Len(t, got, 1)
		assert.Equal(t, "events", got[0].TableName)
	})

	t.Run("a listing error surfaces rather than truncating the result", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{
			listed:  []string{"events"},
			listErr: errors.New("ThrottlingException: rate exceeded"),
			tables:  map[string]*catalog.SourceTable{"events": discoveredTable("events", "ICEBERG")},
		}

		got, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.Error(t, err)
		assert.Contains(t, err.Error(), "analytics")
		assert.Contains(t, err.Error(), "rate exceeded")
		assert.Nil(t, got, "a partial listing must not be returned as if it were complete")
	})

	t.Run("an unresolvable marked table fails the scan naming the table", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{
			listed:    []string{"events"},
			tableErrs: map[string]error{"events": errors.New("access denied")},
		}

		_, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.Error(t, err)
		assert.Contains(t, err.Error(), "analytics.events")
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("an unknown target format fails the scan naming the table", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{
			listed: []string{"events"},
			tables: map[string]*catalog.SourceTable{"events": discoveredTable("events", "ICEBERG,ICEBUG")},
		}

		_, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.Error(t, err)
		assert.Contains(t, err.Error(), "analytics.events")
		assert.Contains(t, err.Error(), catalog.PropTargetFormats)
	})

	t.Run("an empty database yields no datasets and no error", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{}
		got, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("an incomplete catalog configuration is rejected before any call", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			cfg     *catalog.Config
			wantErr string
		}{
			{name: "nil config", cfg: nil, wantErr: "catalog configuration"},
			{name: "no type", cfg: &catalog.Config{DatabaseName: "analytics"}, wantErr: "catalog type"},
			{name: "no database", cfg: &catalog.Config{Type: catalog.CatalogTypeGlue}, wantErr: "databaseName"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				called := false
				_, err := conversion.DiscoverDatasets(context.Background(), tt.cfg,
					func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
						called = true
						return &fakeConversionSource{}, nil
					})

				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.False(t, called, "an invalid configuration must not reach the catalog")
			})
		}
	})

	t.Run("a factory failure surfaces", func(t *testing.T) {
		t.Parallel()

		_, err := conversion.DiscoverDatasets(context.Background(), glueCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return nil, errors.New("no AWS credentials")
			})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no AWS credentials")
	})
}
