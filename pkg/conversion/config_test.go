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

package conversion_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestStorageConfig_ToOptionFuncs_NilConfig(t *testing.T) {
	t.Parallel()

	var config *conversion.StorageConfig
	optFns := config.ToOptionFuncs()

	assert.Nil(t, optFns, "nil config should produce nil option functions")
}

func TestStorageConfig_ToOptionFuncs_EmptyConfig(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{}
	optFns := config.ToOptionFuncs()

	assert.NotNil(t, optFns, "empty config should produce non-nil slice")
	assert.Empty(t, optFns, "empty config should produce empty slice")
}

func TestStorageConfig_ToOptionFuncs_RegionOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region: "us-west-2",
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "region-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Equal(t, "us-west-2", opts.S3.Region)
	assert.Empty(t, opts.S3.Endpoint)
	assert.False(t, opts.S3.UsePathStyle)
}

func TestStorageConfig_ToOptionFuncs_EndpointOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Endpoint: "http://localhost:9000",
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "endpoint-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.S3.Region)
	assert.Equal(t, "http://localhost:9000", opts.S3.Endpoint)
	assert.False(t, opts.S3.UsePathStyle)
}

func TestStorageConfig_ToOptionFuncs_UsePathStyleOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		UsePathStyle: true,
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "path-style-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.S3.Region)
	assert.Empty(t, opts.S3.Endpoint)
	assert.True(t, opts.S3.UsePathStyle)
}

func TestStorageConfig_ToOptionFuncs_AllOptions(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:       "eu-west-1",
		Endpoint:     "https://minio.example.com",
		UsePathStyle: true,
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 3, "all options should produce three option functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "eu-west-1", opts.S3.Region)
	assert.Equal(t, "https://minio.example.com", opts.S3.Endpoint)
	assert.True(t, opts.S3.UsePathStyle)
}

func TestStorageConfig_ToOptionFuncs_PartialOptions(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:   "ap-southeast-2",
		Endpoint: "http://s3-gateway.local",
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 2, "partial options should produce two option functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "ap-southeast-2", opts.S3.Region)
	assert.Equal(t, "http://s3-gateway.local", opts.S3.Endpoint)
	assert.False(t, opts.S3.UsePathStyle)
}

func TestDatasetConfig_StorageField(t *testing.T) {
	t.Parallel()

	config := &conversion.DatasetConfig{
		Storage: &conversion.StorageConfig{
			Region:   "us-east-1",
			Endpoint: "http://localhost:9000",
		},
	}

	require.NotNil(t, config.Storage, "storage field should be set")
	optFns := config.Storage.ToOptionFuncs()

	require.Len(t, optFns, 2, "dataset storage should produce option functions")
}

func TestDatasetConfig_NilStorage(t *testing.T) {
	t.Parallel()

	config := &conversion.DatasetConfig{}

	require.Nil(t, config.Storage, "storage field should be nil when not set")
}

func TestStorageConfig_ToOptionFuncs_AzureEndpointOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			Endpoint: "https://myaccount.blob.core.windows.net",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "Azure endpoint-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Equal(t, "https://myaccount.blob.core.windows.net", opts.Azure.Endpoint)
	assert.Empty(t, opts.Azure.AccountName)
	assert.False(t, opts.Azure.Anonymous)
}

func TestStorageConfig_ToOptionFuncs_AzureAccountNameOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			AccountName: "myaccount",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "Azure account name-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.Azure.Endpoint)
	assert.Equal(t, "myaccount", opts.Azure.AccountName)
	assert.False(t, opts.Azure.Anonymous)
}

func TestStorageConfig_ToOptionFuncs_AzureAnonymousOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			Anonymous: true,
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "Azure anonymous-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.Azure.Endpoint)
	assert.Empty(t, opts.Azure.AccountName)
	assert.True(t, opts.Azure.Anonymous)
}

func TestStorageConfig_ToOptionFuncs_AzureAccountKeyEnvOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			AccountKeyEnv: "ACCT1_STORAGE_KEY",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "Azure account-key-env-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Equal(t, "ACCT1_STORAGE_KEY", opts.Azure.AccountKeyEnv)
	assert.Empty(t, opts.Azure.SASTokenEnv)
}

func TestStorageConfig_ToOptionFuncs_AzureSASTokenEnvOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{ //nolint:gosec // SASTokenEnv names an env var, holds no secret
			SASTokenEnv: "ACCT1_SAS_TOKEN",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "Azure SAS-token-env-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.Azure.AccountKeyEnv)
	assert.Equal(t, "ACCT1_SAS_TOKEN", opts.Azure.SASTokenEnv)
}

func TestStorageConfig_ToOptionFuncs_AllFieldsS3AndAzure(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:       "us-west-2",
		Endpoint:     "https://minio.example.com",
		UsePathStyle: true,
		Azure: &conversion.AzureStorageConfig{ //nolint:gosec // AccountKeyEnv/SASTokenEnv name env vars, hold no secret
			Endpoint:      "https://myaccount.blob.core.windows.net",
			AccountName:   "myaccount",
			AccountKeyEnv: "ACCT1_STORAGE_KEY",
			SASTokenEnv:   "ACCT1_SAS_TOKEN",
			Anonymous:     true,
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 8, "fully populated config should produce eight option functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "us-west-2", opts.S3.Region)
	assert.Equal(t, "https://minio.example.com", opts.S3.Endpoint)
	assert.True(t, opts.S3.UsePathStyle)
	assert.Equal(t, "https://myaccount.blob.core.windows.net", opts.Azure.Endpoint)
	assert.Equal(t, "myaccount", opts.Azure.AccountName)
	assert.Equal(t, "ACCT1_STORAGE_KEY", opts.Azure.AccountKeyEnv)
	assert.Equal(t, "ACCT1_SAS_TOKEN", opts.Azure.SASTokenEnv)
	assert.True(t, opts.Azure.Anonymous)
}

func TestStorageConfig_ToOptionFuncs_AzureZeroFields(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region: "us-east-1",
		Azure:  &conversion.AzureStorageConfig{
			// All fields are zero-valued
		},
	}

	optFns := config.ToOptionFuncs()

	// Only the Region field should contribute one function; zero-valued Azure fields should not
	require.Len(t, optFns, 1, "Azure block with all zero fields should not contribute any functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "us-east-1", opts.S3.Region)
	assert.Empty(t, opts.Azure.Endpoint)
	assert.Empty(t, opts.Azure.AccountName)
	assert.False(t, opts.Azure.Anonymous)
}

func TestStorageConfig_ToOptionFuncs_GCSEndpointOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		GCS: &conversion.GCSStorageConfig{
			Endpoint: "http://127.0.0.1:4443/storage/v1/",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "GCS endpoint-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Equal(t, "http://127.0.0.1:4443/storage/v1/", opts.GCS.Endpoint)
	assert.False(t, opts.GCS.AnonymousAccess)
	assert.Empty(t, opts.GCS.CredentialsFile)
}

func TestStorageConfig_ToOptionFuncs_GCSAnonymousAccessOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		GCS: &conversion.GCSStorageConfig{
			AnonymousAccess: true,
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "GCS anonymous-access-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.GCS.Endpoint)
	assert.True(t, opts.GCS.AnonymousAccess)
	assert.Empty(t, opts.GCS.CredentialsFile)
}

func TestStorageConfig_ToOptionFuncs_GCSCredentialsFileOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		GCS: &conversion.GCSStorageConfig{ //nolint:gosec // CredentialsFile names a service-account JSON path, holds no secret
			CredentialsFile: "/etc/polytable/gcs-service-account.json",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 1, "GCS credentials-file-only config should produce one option function")

	opts := &io.Options{}
	optFns[0](opts)

	assert.Empty(t, opts.GCS.Endpoint)
	assert.False(t, opts.GCS.AnonymousAccess)
	assert.Equal(t, "/etc/polytable/gcs-service-account.json", opts.GCS.CredentialsFile)
}

func TestStorageConfig_ToOptionFuncs_GCSZeroFields(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region: "us-east-1",
		GCS:    &conversion.GCSStorageConfig{
			// All fields are zero-valued
		},
	}

	optFns := config.ToOptionFuncs()

	// Only the Region field should contribute one function; zero-valued GCS fields should not.
	require.Len(t, optFns, 1, "GCS block with all zero fields should not contribute any functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "us-east-1", opts.S3.Region)
	assert.Empty(t, opts.GCS.Endpoint)
	assert.False(t, opts.GCS.AnonymousAccess)
	assert.Empty(t, opts.GCS.CredentialsFile)
}

func TestStorageConfig_ToOptionFuncs_AllFieldsS3AzureAndGCS(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:       "us-west-2",
		Endpoint:     "https://minio.example.com",
		UsePathStyle: true,
		Azure: &conversion.AzureStorageConfig{ //nolint:gosec // AccountKeyEnv/SASTokenEnv name env vars, hold no secret
			Endpoint:      "https://myaccount.blob.core.windows.net",
			AccountName:   "myaccount",
			AccountKeyEnv: "ACCT1_STORAGE_KEY",
			SASTokenEnv:   "ACCT1_SAS_TOKEN",
			Anonymous:     true,
		},
		GCS: &conversion.GCSStorageConfig{ //nolint:gosec // CredentialsFile names a service-account JSON path, holds no secret
			Endpoint:        "http://127.0.0.1:4443/storage/v1/",
			AnonymousAccess: true,
			CredentialsFile: "/etc/polytable/gcs-service-account.json",
		},
	}

	optFns := config.ToOptionFuncs()

	require.Len(t, optFns, 11, "fully populated config should produce eleven option functions")

	opts := &io.Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "us-west-2", opts.S3.Region)
	assert.Equal(t, "https://minio.example.com", opts.S3.Endpoint)
	assert.True(t, opts.S3.UsePathStyle)
	assert.Equal(t, "https://myaccount.blob.core.windows.net", opts.Azure.Endpoint)
	assert.Equal(t, "myaccount", opts.Azure.AccountName)
	assert.Equal(t, "ACCT1_STORAGE_KEY", opts.Azure.AccountKeyEnv)
	assert.Equal(t, "ACCT1_SAS_TOKEN", opts.Azure.SASTokenEnv)
	assert.True(t, opts.Azure.Anonymous)
	assert.Equal(t, "http://127.0.0.1:4443/storage/v1/", opts.GCS.Endpoint)
	assert.True(t, opts.GCS.AnonymousAccess)
	assert.Equal(t, "/etc/polytable/gcs-service-account.json", opts.GCS.CredentialsFile)
}

// fakeConversionSource returns a canned catalog lookup. For discovery it also serves a canned table
// listing: listed names, optionally cut short by listErr, resolved against tables by name and
// falling back to the single table field when tables is nil.
type fakeConversionSource struct {
	table    *catalog.SourceTable
	err      error
	gotID    catalog.TableIdentifier
	closed   bool
	callests int

	listed     []string
	listErr    error
	gotFilter  catalog.TableFilter
	tables     map[string]*catalog.SourceTable
	tableErrs  map[string]error
	listCalls  int
	gotListDBs []string
}

func (f *fakeConversionSource) CatalogType() catalog.CatalogType { return catalog.CatalogTypeGlue }
func (f *fakeConversionSource) Close() error                     { f.closed = true; return nil }
func (f *fakeConversionSource) GetSourceTable(_ context.Context, id catalog.TableIdentifier) (*catalog.SourceTable, error) {
	f.gotID = id
	f.callests++
	if err, ok := f.tableErrs[id.Table]; ok {
		return nil, err
	}
	if f.tables != nil {
		table, ok := f.tables[id.Table]
		if !ok {
			return nil, fmt.Errorf("no such table %s", id)
		}
		return table, nil
	}
	return f.table, f.err
}

func (f *fakeConversionSource) ListTables(_ context.Context, database string,
	filter catalog.TableFilter) iter.Seq2[catalog.TableIdentifier, error] {
	return func(yield func(catalog.TableIdentifier, error) bool) {
		f.listCalls++
		f.gotFilter = filter
		f.gotListDBs = append(f.gotListDBs, database)
		for _, name := range f.listed {
			if !yield(catalog.TableIdentifier{Database: database, Table: name}, nil) {
				return
			}
		}
		if f.listErr != nil {
			yield(catalog.TableIdentifier{}, f.listErr)
		}
	}
}

func TestResolveSourceCatalog(t *testing.T) {
	t.Parallel()

	newCfg := func() *conversion.DatasetConfig {
		return &conversion.DatasetConfig{
			TargetFormats: []model.TableFormat{model.TableFormatIceberg},
			SourceCatalog: &conversion.SourceCatalogConfig{
				Catalog: catalog.Config{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"},
				Table:   "events",
			},
		}
	}

	t.Run("fills format, paths and name from the catalog entry", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{table: &catalog.SourceTable{
			Name:     "events",
			BasePath: "s3://lake/events",
			DataPath: "s3://lake/events/data",
			Format:   model.TableFormatIceberg,
		}}
		cfg := newCfg()

		err := conversion.ResolveSourceCatalog(context.Background(), cfg,
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.NoError(t, err)
		assert.Equal(t, model.TableFormatIceberg, cfg.SourceFormat)
		assert.Equal(t, "s3://lake/events", cfg.TableBasePath)
		assert.Equal(t, "s3://lake/events/data", cfg.TableDataPath)
		assert.Equal(t, "events", cfg.TableName)
		assert.Equal(t, catalog.TableIdentifier{Database: "analytics", Table: "events"}, fake.gotID)
		assert.True(t, fake.closed, "the conversion source must be closed")
		assert.NoError(t, cfg.Validate(), "a resolved config must pass validation")
	})

	t.Run("explicit configuration overrides the catalog", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{table: &catalog.SourceTable{
			Name: "events", BasePath: "s3://lake/events", Format: model.TableFormatIceberg,
		}}
		cfg := newCfg()
		cfg.TableBasePath = "s3://override/path"
		cfg.SourceFormat = model.TableFormatDelta

		require.NoError(t, conversion.ResolveSourceCatalog(context.Background(), cfg,
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil }))

		assert.Equal(t, "s3://override/path", cfg.TableBasePath)
		assert.Equal(t, model.TableFormatDelta, cfg.SourceFormat)
	})

	t.Run("no source catalog is a no-op", func(t *testing.T) {
		t.Parallel()

		cfg := &conversion.DatasetConfig{TableBasePath: "s3://lake/x"}
		require.NoError(t, conversion.ResolveSourceCatalog(context.Background(), cfg, nil))
		assert.Equal(t, "s3://lake/x", cfg.TableBasePath)
	})

	t.Run("a lookup failure surfaces with the identifier", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{err: errors.New("access denied")}
		err := conversion.ResolveSourceCatalog(context.Background(), newCfg(),
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })

		require.Error(t, err)
		assert.Contains(t, err.Error(), "analytics.events")
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("an incomplete reference is rejected", func(t *testing.T) {
		t.Parallel()

		cfg := newCfg()
		cfg.SourceCatalog.Table = ""
		err := conversion.ResolveSourceCatalog(context.Background(), cfg, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sourceCatalog.table")
	})
}

func TestDatasetConfigValidateAcceptsSourceCatalogInsteadOfPath(t *testing.T) {
	t.Parallel()

	cfg := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		SourceCatalog: &conversion.SourceCatalogConfig{
			Catalog: catalog.Config{Type: catalog.CatalogTypeGlue, DatabaseName: "analytics"},
			Table:   "events",
		},
	}
	assert.NoError(t, cfg.Validate(), "sourceCatalog stands in for tableBasePath")

	bare := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
	}
	err := bare.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "either tableBasePath or sourceCatalog")
}
