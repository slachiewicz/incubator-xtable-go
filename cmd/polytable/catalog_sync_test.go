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

// Tests for T23 (docs/improvement-plan.md): `polytable sync --catalog glue --database <db>`, driven
// against a fake catalog conversion source. Same package-main rationale as main_test.go.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/model"
)

// fakeCatalogSource serves a canned database listing, filtering it the way the Glue implementation
// does so the CLI sees the same skip behaviour without an AWS client.
type fakeCatalogSource struct {
	order  []string
	tables map[string]*catalog.SourceTable
	closed bool
}

func (f *fakeCatalogSource) CatalogType() catalog.CatalogType { return catalog.CatalogTypeGlue }
func (f *fakeCatalogSource) Close() error                     { f.closed = true; return nil }

func (f *fakeCatalogSource) GetSourceTable(_ context.Context, id catalog.TableIdentifier) (*catalog.SourceTable, error) {
	table, ok := f.tables[id.Table]
	if !ok {
		return nil, fmt.Errorf("no such table %s", id)
	}
	return table, nil
}

func (f *fakeCatalogSource) ListTables(_ context.Context, database string,
	filter catalog.TableFilter) iter.Seq2[catalog.TableIdentifier, error] {
	return func(yield func(catalog.TableIdentifier, error) bool) {
		for _, name := range f.order {
			if !filter.Matches(f.tables[name].Properties) {
				continue
			}
			if !yield(catalog.TableIdentifier{Database: database, Table: name}, nil) {
				return
			}
		}
	}
}

// newSyncTestCmd returns a command whose output streams are captured buffers.
func newSyncTestCmd(t *testing.T) (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := &cobra.Command{Use: "sync"}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetContext(context.Background())
	return cmd, stdout, stderr
}

// buildCatalogFixture writes two marked Delta tables and one unmarked one, returning the fake
// catalog that describes them.
func buildCatalogFixture(t *testing.T, ctx context.Context) *fakeCatalogSource {
	t.Helper()

	fake := &fakeCatalogSource{
		order:  []string{"events", "legacy", "orders"},
		tables: map[string]*catalog.SourceTable{},
	}
	for _, tc := range []struct{ name, targets string }{
		{name: "events", targets: "ICEBERG"},
		{name: "legacy", targets: ""},
		{name: "orders", targets: "ICEBERG"},
	} {
		basePath := filepath.Join(t.TempDir(), tc.name)
		buildLocalDeltaSource(t, ctx, basePath, tc.name)

		properties := map[string]string{"spark.sql.sources.provider": "delta"}
		if tc.targets != "" {
			properties[catalog.PropTargetFormats] = tc.targets
		}
		fake.tables[tc.name] = &catalog.SourceTable{
			Name:       tc.name,
			BasePath:   basePath,
			DataPath:   basePath,
			Format:     model.TableFormatDelta,
			Properties: properties,
		}
	}
	return fake
}

func TestRunSync_CatalogDiscovery(t *testing.T) {
	t.Parallel()

	t.Run("converts every marked table in the database and skips the unmarked one", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		fake := buildCatalogFixture(t, ctx)
		cmd, stdout, stderr := newSyncTestCmd(t)

		err := runSync(cmd, &syncOptions{
			catalogStr: "glue",
			database:   "analytics",
			outputStr:  "json",
			modeStr:    "full",
			newCatalogSource: func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return fake, nil
			},
		})
		require.NoError(t, err)

		var out SyncOutput
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &out), "stdout must carry only the JSON document")
		assert.False(t, out.HasErrors)
		require.Len(t, out.Tables, 2, "the unmarked table must be skipped, not failed")

		assert.Equal(t, "events", out.Tables[0].TableName)
		assert.Equal(t, "DELTA", out.Tables[0].SourceFormat)
		require.Len(t, out.Tables[0].Targets, 1)
		assert.Equal(t, "ICEBERG", out.Tables[0].Targets[0].TargetFormat)
		assert.Equal(t, "SUCCESS", out.Tables[0].Targets[0].Verdict)
		assert.Equal(t, "orders", out.Tables[1].TableName)
		assert.Equal(t, "SUCCESS", out.Tables[1].Targets[0].Verdict)

		assert.Contains(t, stderr.String(), "Found 2 marked table(s)", "progress chatter belongs on stderr in JSON mode")
		assert.True(t, fake.closed, "the catalog source must be closed after discovery")
	})

	t.Run("dry run reports the same tables without writing them", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		fake := buildCatalogFixture(t, ctx)
		cmd, stdout, _ := newSyncTestCmd(t)

		err := runSync(cmd, &syncOptions{
			catalogStr: "glue",
			database:   "analytics",
			outputStr:  "json",
			dryRun:     true,
			newCatalogSource: func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return fake, nil
			},
		})
		require.NoError(t, err)

		var out SyncOutput
		require.NoError(t, json.Unmarshal(stdout.Bytes(), &out))
		assert.True(t, out.DryRun)
		require.Len(t, out.Tables, 2)

		for _, table := range out.Tables {
			_, statErr := os.Stat(filepath.Join(fake.tables[table.TableName].BasePath, "metadata"))
			assert.True(t, os.IsNotExist(statErr),
				"--dry-run must not write Iceberg metadata for discovered table %s", table.TableName)
		}
	})

	t.Run("a discovery failure is reported, not silently an empty run", func(t *testing.T) {
		t.Parallel()

		cmd, stdout, _ := newSyncTestCmd(t)
		err := runSync(cmd, &syncOptions{
			catalogStr: "glue",
			database:   "analytics",
			outputStr:  "json",
			newCatalogSource: func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return nil, fmt.Errorf("ExpiredTokenException")
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "analytics")
		assert.Contains(t, err.Error(), "ExpiredTokenException")
		assert.Empty(t, stdout.String(), "a failed scan must not emit a JSON document claiming success")
	})

	t.Run("text output still names each discovered table", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		fake := buildCatalogFixture(t, ctx)
		cmd, stdout, _ := newSyncTestCmd(t)

		require.NoError(t, runSync(cmd, &syncOptions{
			catalogStr: "glue",
			database:   "analytics",
			newCatalogSource: func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return fake, nil
			},
		}))

		assert.Contains(t, stdout.String(), "Syncing Table 'events'")
		assert.Contains(t, stdout.String(), "Syncing Table 'orders'")
		assert.NotContains(t, stdout.String(), "legacy")
	})
}

func TestRunSync_SourceSelectionFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    syncOptions
		wantErr string
	}{
		{
			name:    "a config file and a catalog scan are mutually exclusive",
			opts:    syncOptions{configPath: "config.yaml", catalogStr: "glue", database: "analytics"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "a config file and a bare database are mutually exclusive too",
			opts:    syncOptions{configPath: "config.yaml", database: "analytics"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "a catalog without a database is rejected",
			opts:    syncOptions{catalogStr: "glue"},
			wantErr: "--database is required",
		},
		{
			name:    "a database without a catalog is rejected",
			opts:    syncOptions{database: "analytics"},
			wantErr: "--database requires --catalog",
		},
		{
			name:    "naming neither source is rejected",
			opts:    syncOptions{},
			wantErr: "either --datasetConfig or --catalog",
		},
		{
			name:    "an unsupported catalog is rejected up front",
			opts:    syncOptions{catalogStr: "hive", database: "analytics"},
			wantErr: "--catalog must be",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd, stdout, _ := newSyncTestCmd(t)
			opts := tt.opts
			opts.newCatalogSource = func(context.Context, *catalog.Config) (catalog.ConversionSource, error) {
				return nil, fmt.Errorf("the catalog must not be reached for an invalid flag set")
			}

			err := runSync(cmd, &opts)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, stdout.String())
		})
	}
}

func TestParseCatalogTypeFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    catalog.CatalogType
		wantErr bool
	}{
		{name: "empty means no discovery", input: "", want: ""},
		{name: "glue", input: "glue", want: catalog.CatalogTypeGlue},
		{name: "padded and upper case", input: "  GLUE ", want: catalog.CatalogTypeGlue},
		{name: "the config file spelling also works", input: "AWS_GLUE", want: catalog.CatalogTypeGlue},
		{name: "hive metastore cannot list yet", input: "hms", wantErr: true},
		{name: "iceberg rest cannot list yet", input: "iceberg_rest", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseCatalogTypeFlag(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
