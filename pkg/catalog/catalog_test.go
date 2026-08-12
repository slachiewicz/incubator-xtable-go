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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/catalog"
	"github.com/apache/incubator-xtable-go/pkg/model"
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
