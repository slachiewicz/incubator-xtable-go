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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/catalog"
	"github.com/apache/incubator-xtable-go/pkg/model"
)

func TestTableFormatFromProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		properties map[string]string
		want       model.TableFormat
		wantErr    string
	}{
		{
			name:       "table_type wins",
			properties: map[string]string{"table_type": "ICEBERG"},
			want:       model.TableFormatIceberg,
		},
		{
			name:       "spark provider is the fallback",
			properties: map[string]string{"spark.sql.sources.provider": "delta"},
			want:       model.TableFormatDelta,
		},
		{
			name:       "table_type takes precedence over the spark provider",
			properties: map[string]string{"table_type": "HUDI", "spark.sql.sources.provider": "delta"},
			want:       model.TableFormatHudi,
		},
		{
			name:       "lowercase table_type is normalised",
			properties: map[string]string{"table_type": "iceberg"},
			want:       model.TableFormatIceberg,
		},
		{
			name:       "no markers at all is an error, not a guess",
			properties: map[string]string{"owner": "analytics"},
			wantErr:    "neither",
		},
		{
			name:       "blank markers are treated as absent",
			properties: map[string]string{"table_type": "   "},
			wantErr:    "neither",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := catalog.TableFormatFromProperties(tt.properties)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDataLocationForFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     model.TableFormat
		basePath   string
		properties map[string]string
		want       string
	}{
		{name: "delta keeps the table location", format: model.TableFormatDelta, basePath: "s3://b/t", want: "s3://b/t"},
		{name: "hudi keeps the table location", format: model.TableFormatHudi, basePath: "s3://b/t", want: "s3://b/t"},
		{name: "iceberg defaults to <base>/data", format: model.TableFormatIceberg, basePath: "s3://b/t", want: "s3://b/t/data"},
		{
			name:       "iceberg honours write.data.path",
			format:     model.TableFormatIceberg,
			basePath:   "s3://b/t",
			properties: map[string]string{"write.data.path": "s3://other/data"},
			want:       "s3://other/data",
		},
		{
			name:       "iceberg falls back through the location properties in order",
			format:     model.TableFormatIceberg,
			basePath:   "s3://b/t",
			properties: map[string]string{"write.object-storage.path": "s3://obj/data"},
			want:       "s3://obj/data",
		},
		{
			name:     "iceberg trailing slash does not double up",
			format:   model.TableFormatIceberg,
			basePath: "s3://b/t/",
			want:     "s3://b/t/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := catalog.DataLocationForFormat(tt.format, tt.basePath, tt.properties)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// fakeGlueTableReader serves a canned GetTable response.
type fakeGlueTableReader struct {
	table *gluetypes.Table
	err   error
}

func (f *fakeGlueTableReader) GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &glue.GetTableOutput{Table: f.table}, nil
}

func TestGlueConversionSourceGetSourceTable(t *testing.T) {
	t.Parallel()

	id := catalog.TableIdentifier{Database: "analytics", Table: "events"}

	t.Run("resolves a delta table", func(t *testing.T) {
		t.Parallel()

		reader := &fakeGlueTableReader{table: &gluetypes.Table{
			Name:              aws.String("events"),
			Parameters:        map[string]string{"spark.sql.sources.provider": "delta"},
			StorageDescriptor: &gluetypes.StorageDescriptor{Location: aws.String("s3://lake/events")},
		}}

		src := catalog.NewGlueConversionSourceWithClient(reader, nil)
		got, err := src.GetSourceTable(context.Background(), id)

		require.NoError(t, err)
		assert.Equal(t, model.TableFormatDelta, got.Format)
		assert.Equal(t, "s3://lake/events", got.BasePath)
		assert.Equal(t, "s3://lake/events", got.DataPath, "delta data lives under the table location")
		assert.Equal(t, "events", got.Name)
		assert.Equal(t, catalog.CatalogTypeGlue, src.CatalogType())
	})

	t.Run("iceberg data path defaults below the table location", func(t *testing.T) {
		t.Parallel()

		reader := &fakeGlueTableReader{table: &gluetypes.Table{
			Name:              aws.String("events"),
			Parameters:        map[string]string{"table_type": "ICEBERG"},
			StorageDescriptor: &gluetypes.StorageDescriptor{Location: aws.String("s3://lake/events")},
		}}

		got, err := catalog.NewGlueConversionSourceWithClient(reader, nil).GetSourceTable(context.Background(), id)

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/events/data", got.DataPath)
	})

	t.Run("a table with no format marker is rejected", func(t *testing.T) {
		t.Parallel()

		reader := &fakeGlueTableReader{table: &gluetypes.Table{
			Name:              aws.String("events"),
			Parameters:        map[string]string{},
			StorageDescriptor: &gluetypes.StorageDescriptor{Location: aws.String("s3://lake/events")},
		}}

		_, err := catalog.NewGlueConversionSourceWithClient(reader, nil).GetSourceTable(context.Background(), id)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table format")
	})

	t.Run("a table with no location is rejected", func(t *testing.T) {
		t.Parallel()

		reader := &fakeGlueTableReader{table: &gluetypes.Table{
			Name:       aws.String("events"),
			Parameters: map[string]string{"table_type": "DELTA"},
		}}

		_, err := catalog.NewGlueConversionSourceWithClient(reader, nil).GetSourceTable(context.Background(), id)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "storage location")
	})

	t.Run("an incomplete identifier is rejected before any call", func(t *testing.T) {
		t.Parallel()

		_, err := catalog.NewGlueConversionSourceWithClient(&fakeGlueTableReader{}, nil).
			GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "analytics"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table name")
	})

	t.Run("returned properties are a copy the caller cannot use to mutate the source", func(t *testing.T) {
		t.Parallel()

		params := map[string]string{"table_type": "DELTA", "owner": "analytics"}
		reader := &fakeGlueTableReader{table: &gluetypes.Table{
			Name:              aws.String("events"),
			Parameters:        params,
			StorageDescriptor: &gluetypes.StorageDescriptor{Location: aws.String("s3://lake/events")},
		}}

		got, err := catalog.NewGlueConversionSourceWithClient(reader, nil).GetSourceTable(context.Background(), id)
		require.NoError(t, err)

		got.Properties["owner"] = "someone-else"
		assert.Equal(t, "analytics", params["owner"])
	})
}

func TestIcebergRESTConversionSourceGetSourceTable(t *testing.T) {
	t.Parallel()

	id := catalog.TableIdentifier{Database: "analytics", Table: "events"}

	t.Run("resolves location and data path from the load-table response", func(t *testing.T) {
		t.Parallel()

		var gotPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"metadata-location": "s3://lake/events/metadata/00001.metadata.json",
				"metadata": {"location": "s3://lake/events", "properties": {"write.data.path": "s3://lake/events/custom"}}
			}`))
		}))
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "analytics", "")
		got, err := src.GetSourceTable(context.Background(), id)

		require.NoError(t, err)
		assert.Equal(t, "/v1/namespaces/analytics/tables/events", gotPath)
		assert.Equal(t, model.TableFormatIceberg, got.Format)
		assert.Equal(t, "s3://lake/events", got.BasePath)
		assert.Equal(t, "s3://lake/events/custom", got.DataPath)
		assert.Equal(t, catalog.CatalogTypeIcebergREST, src.CatalogType())
	})

	t.Run("falls back to deriving the base path from the metadata location", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"metadata-location": "s3://lake/events/metadata/00001.metadata.json", "metadata": {}}`))
		}))
		defer server.Close()

		got, err := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "analytics", "").
			GetSourceTable(context.Background(), id)

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/events", got.BasePath)
		assert.Equal(t, "s3://lake/events/data", got.DataPath)
	})

	t.Run("404 reports the table as missing", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		_, err := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "analytics", "").
			GetSourceTable(context.Background(), id)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("the bearer token is sent when configured", func(t *testing.T) {
		t.Parallel()

		var gotAuth string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"metadata": {"location": "s3://lake/events"}}`))
		}))
		defer server.Close()

		_, err := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "analytics", "s3cr3t").
			GetSourceTable(context.Background(), id)

		require.NoError(t, err)
		assert.Equal(t, "Bearer s3cr3t", gotAuth)
	})
}

func TestNewConversionSourceRejectsHMS(t *testing.T) {
	t.Parallel()

	_, err := catalog.NewConversionSource(context.Background(), &catalog.Config{
		Type:         catalog.CatalogTypeHMS,
		DatabaseName: "analytics",
	})

	require.ErrorIs(t, err, catalog.ErrCatalogNotImplemented)
}
