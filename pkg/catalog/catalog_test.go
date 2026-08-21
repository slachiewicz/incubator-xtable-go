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

package catalog_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestModelTypeToGlueType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schema   *model.Schema
		expected string
	}{
		{
			name:     "int",
			schema:   model.NewPrimitiveSchema(model.TypeInt, false),
			expected: "int",
		},
		{
			name:     "long",
			schema:   model.NewPrimitiveSchema(model.TypeLong, false),
			expected: "bigint",
		},
		{
			name:     "string",
			schema:   model.NewPrimitiveSchema(model.TypeString, false),
			expected: "string",
		},
		{
			name:     "decimal",
			schema:   model.NewDecimalSchema(18, 4, false),
			expected: "decimal(18,4)",
		},
		{
			name: "list of strings",
			schema: &model.Schema{
				DataType: model.TypeList,
				ElementSchema: &model.Field{
					Name:   "element",
					Schema: model.NewPrimitiveSchema(model.TypeString, false),
				},
			},
			expected: "array<string>",
		},
		{
			name: "map of string to int",
			schema: &model.Schema{
				DataType: model.TypeMap,
				KeySchema: &model.Field{
					Name:   "key",
					Schema: model.NewPrimitiveSchema(model.TypeString, false),
				},
				ValueSchema: &model.Field{
					Name:   "value",
					Schema: model.NewPrimitiveSchema(model.TypeInt, false),
				},
			},
			expected: "map<string,int>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := catalog.ModelTypeToGlueType(tt.schema)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIcebergRESTCatalogClient_CreateOrUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "OK"}`))
	}))
	defer server.Close()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "analytics_db",
		URI:          server.URL,
	}

	client, err := catalog.NewIcebergRESTCatalogClient(cfg)
	require.NoError(t, err)

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("events", []*model.Field{idField}, false)

	table := &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         "s3://my-bucket/events",
		LatestCommitTime: time.Now().UnixMilli(),
	}

	err = client.CreateOrUpdateTable(ctx, table, nil)
	require.NoError(t, err)
	assert.Equal(t, "/v1/namespaces/analytics_db/tables", receivedPath)
}

func TestCatalogTypeImplemented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		catalogType catalog.CatalogType
		want        bool
	}{
		{name: "glue is implemented", catalogType: catalog.CatalogTypeGlue, want: true},
		{name: "iceberg rest is implemented", catalogType: catalog.CatalogTypeIcebergREST, want: true},
		{name: "hms is declared but not implemented", catalogType: catalog.CatalogTypeHMS, want: false},
		{name: "unknown type is not implemented", catalogType: catalog.CatalogType("NOPE"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.catalogType.Implemented())
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *catalog.Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid Glue config",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeGlue,
				DatabaseName: "my_database",
			},
			wantErr: false,
		},
		{
			name: "valid Iceberg REST config with URI",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "my_database",
				URI:          "http://localhost:8080",
			},
			wantErr: false,
		},
		{
			name: "missing type",
			cfg: &catalog.Config{
				Type:         "",
				DatabaseName: "my_database",
			},
			wantErr: true,
			errMsg:  "catalog type is required",
		},
		{
			name: "missing database name",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeGlue,
				DatabaseName: "",
			},
			wantErr: true,
			errMsg:  "databaseName is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewSyncClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "OK"}`))
	}))
	defer server.Close()

	tests := []struct {
		name        string
		cfg         *catalog.Config
		wantErr     bool
		errCheck    func(error) bool
		catalogType catalog.CatalogType
	}{
		{
			name: "creates Glue client successfully",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeGlue,
				DatabaseName: "my_database",
			},
			wantErr:     false,
			catalogType: catalog.CatalogTypeGlue,
		},
		{
			name: "creates Iceberg REST client successfully",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "my_database",
				URI:          server.URL,
			},
			wantErr:     false,
			catalogType: catalog.CatalogTypeIcebergREST,
		},
		{
			name: "fails for invalid config - missing database",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeGlue,
				DatabaseName: "",
			},
			wantErr: true,
			errCheck: func(err error) bool {
				return err != nil && err.Error() == "databaseName is required"
			},
		},
		{
			name: "fails for HMS - not implemented",
			cfg: &catalog.Config{
				Type:         catalog.CatalogTypeHMS,
				DatabaseName: "my_database",
			},
			wantErr: true,
			errCheck: func(err error) bool {
				return err != nil && errors.Is(err, catalog.ErrCatalogNotImplemented)
			},
		},
		{
			name: "fails for unsupported catalog type",
			cfg: &catalog.Config{
				Type:         catalog.CatalogType("UNKNOWN_TYPE"),
				DatabaseName: "my_database",
			},
			wantErr: true,
			errCheck: func(err error) bool {
				return err != nil && err.Error() == "unsupported catalog type: UNKNOWN_TYPE"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, err := catalog.NewSyncClient(ctx, tt.cfg)

			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, client)
				require.NotNil(t, tt.errCheck, "errCheck must be provided for error cases")
				assert.True(t, tt.errCheck(err), "Error check failed")
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				assert.Equal(t, tt.catalogType, client.CatalogType())
				_ = client.Close()
			}
		})
	}
}
